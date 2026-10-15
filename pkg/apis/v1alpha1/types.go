// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1alpha1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

const (
	APIVersion     = "ax.io/v1alpha1"
	KindTask       = "Task"
	KindWorkspace  = "Workspace"
	KindModel      = "Model"
	KindCredential = "Credential"

	DefaultTaskImage       = "gcr.io/ax-substrate/ate-images/ax-task-runner"
	DefaultSystemNamespace = "ax-system"

	// PhaseTerminating marks a task whose deletion has been requested and whose
	// actor is being torn down. The record disappears once cleanup completes.
	PhaseTerminating = "Terminating"
)

// YAML encoding.
//
// The API kinds are protobuf messages, so protojson is the single source of
// truth for their wire names (lowerCamelCase), timestamp format (RFC 3339), and
// which fields are omitted when unset. YAML is bridged through it: manifests are
// decoded generically, converted to JSON, and then decoded strictly by protojson,
// so unknown or misspelled fields are rejected. Output is protojson rendered as
// a yaml.Node so field order follows the proto definition instead of being
// sorted.
//
// A few legacy manifest shapes are still accepted. They are rewritten on the
// generic document before strict decoding by the per-kind normalizers below.

var (
	protoMarshal   = protojson.MarshalOptions{}
	protoUnmarshal = protojson.UnmarshalOptions{} // unknown fields are errors
)

// normalizer rewrites a generically decoded document in place so legacy shapes
// match the current schema before strict decoding.
type normalizer func(doc map[string]any)

func marshalYAML(m proto.Message) (any, error) {
	data, err := protoMarshal.Marshal(m)
	if err != nil {
		return nil, err
	}
	return jsonToYAMLNode(data)
}

func unmarshalYAML(value *yaml.Node, m proto.Message, normalize normalizer) error {
	var doc any
	if err := value.Decode(&doc); err != nil {
		return err
	}
	if obj, ok := doc.(map[string]any); ok && normalize != nil {
		normalize(obj)
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("converting yaml to json: %w", err)
	}
	return protoUnmarshal.Unmarshal(data, m)
}

// jsonToYAMLNode converts a JSON document into a yaml.Node tree, preserving
// object key order and tagging scalars so the YAML encoder quotes strings that
// would otherwise read as another type (for example a status of "True").
func jsonToYAMLNode(data []byte) (*yaml.Node, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return readJSONValue(dec)
}

func readJSONValue(dec *json.Decoder) (*yaml.Node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			n := &yaml.Node{Kind: yaml.MappingNode}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, _ := keyTok.(string)
				val, err := readJSONValue(dec)
				if err != nil {
					return nil, err
				}
				n.Content = append(n.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, val)
			}
			_, err = dec.Token() // closing '}'
			return n, err
		case '[':
			n := &yaml.Node{Kind: yaml.SequenceNode}
			for dec.More() {
				val, err := readJSONValue(dec)
				if err != nil {
					return nil, err
				}
				n.Content = append(n.Content, val)
			}
			_, err = dec.Token() // closing ']'
			return n, err
		}
		return nil, fmt.Errorf("unexpected json delimiter %q", v)
	case string:
		// protojson renders timestamps as RFC 3339 strings. Let those stay plain
		// scalars, which YAML reads as timestamps; every other string is tagged so
		// values like "True" or "1" are quoted and read back as strings.
		tag := "!!str"
		if _, err := time.Parse(time.RFC3339Nano, v); err == nil {
			tag = "!!timestamp"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: v}, nil
	case json.Number:
		tag := "!!int"
		if strings.ContainsAny(v.String(), ".eE") {
			tag = "!!float"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: v.String()}, nil
	case bool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(v)}, nil
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}, nil
	}
	return nil, fmt.Errorf("unexpected json token %v", tok)
}

// Legacy manifest shapes.

// normalizeWorkspace accepts spec.mcp written as a bare list of servers.
func normalizeWorkspace(doc map[string]any) {
	spec, _ := doc["spec"].(map[string]any)
	if list, ok := spec["mcp"].([]any); ok {
		spec["mcp"] = map[string]any{"servers": list}
	}
}

// normalizeModel accepts older spellings of the secret reference: spec.secretKeyRef,
// spec.apiKey.secretKeyRef, and a bare string meaning the secret name and key are
// the same.
func normalizeModel(doc map[string]any) {
	spec, ok := doc["spec"].(map[string]any)
	if !ok {
		return
	}
	if _, has := spec["secretKey"]; !has {
		if ref, ok := spec["secretKeyRef"]; ok {
			spec["secretKey"] = ref
		} else if apiKey, ok := spec["apiKey"].(map[string]any); ok {
			if ref, ok := apiKey["secretKeyRef"]; ok {
				spec["secretKey"] = ref
			}
		}
	}
	delete(spec, "secretKeyRef")
	delete(spec, "apiKey")
	if s, ok := spec["secretKey"].(string); ok {
		spec["secretKey"] = map[string]any{"name": s, "key": s}
	}
}

// Per-kind YAML methods. Nested messages are handled by protojson as part of
// their parent, so only the kinds that are encoded on their own need these.

func (t *Task) MarshalYAML() (any, error)        { return marshalYAML(t) }
func (t *Task) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, t, nil) }

func (w *Workspace) MarshalYAML() (any, error)        { return marshalYAML(w) }
func (w *Workspace) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, w, normalizeWorkspace) }

func (m *Model) MarshalYAML() (any, error)        { return marshalYAML(m) }
func (m *Model) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, m, normalizeModel) }

// A spec that sets two credential types is rejected by protojson itself, because
// the types are members of a proto oneof.
func (c *Credential) MarshalYAML() (any, error)        { return marshalYAML(c) }
func (c *Credential) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, c, nil) }

// Workspace bindings.
//
// A task binds one or more workspaces through spec.workspaces. Every entry is
// treated the same way regardless of how many there are.

// DefaultWorkspacePath is the durable volume every workspace lives under. A
// binding without a path is mounted at DefaultWorkspacePath/<name>, and a task
// with no bindings gets an empty working directory at DefaultWorkspacePath.
const DefaultWorkspacePath = "/workspace"

// WorkspaceRefs returns the workspaces the task binds, in declaration order,
// with nil entries dropped.
func (s *TaskSpec) WorkspaceRefs() []*WorkspaceRef {
	if s == nil {
		return nil
	}
	out := make([]*WorkspaceRef, 0, len(s.GetWorkspaces()))
	for _, r := range s.GetWorkspaces() {
		if r != nil {
			out = append(out, r)
		}
	}
	return out
}

// WorkspacePaths returns the mount path of each workspace in WorkspaceRefs, in
// the same order. An explicit path wins; otherwise the workspace lands at
// DefaultWorkspacePath/<name>.
func (s *TaskSpec) WorkspacePaths() []string {
	refs := s.WorkspaceRefs()
	paths := make([]string, len(refs))
	for i, r := range refs {
		if r.GetPath() != "" {
			paths[i] = r.GetPath()
		} else {
			paths[i] = DefaultWorkspacePath + "/" + r.GetName()
		}
	}
	return paths
}

// Resource names.
//
// A resource's name and atespace become Substrate resource names (the task's
// actor and ActorTemplate, and the atespace itself), which must be lowercase
// RFC 1123 labels. Checking them at apply time turns an asynchronous
// ActorCreationFailed condition into an immediate error.

// MaxNameLength is the longest name or atespace a resource may have.
const MaxNameLength = 63

var nameRegexp = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// MaxSecretKeyLength is the longest data key a Kubernetes Secret may have.
const MaxSecretKeyLength = 253

// MaxSecretNameLength is the longest name a Kubernetes Secret may have. Unlike
// MaxNameLength's RFC 1123 label, used for AX's own resource names, a Secret
// name is a full RFC 1123 DNS subdomain: '.'-separated labels and a longer
// overall length.
const MaxSecretNameLength = 253

// secretNameRegexp matches a valid Kubernetes Secret name: one or more
// '.'-separated labels, each a lowercase alphanumeric run optionally containing '-'.
var secretNameRegexp = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// secretKeyRegexp matches a valid Kubernetes Secret data key: the charset the
// API server itself enforces for ConfigMap and Secret keys.
var secretKeyRegexp = regexp.MustCompile(`^[-._a-zA-Z0-9]+$`)

// validateSecretKeyRef rejects references that can never identify real Secret
// data: a name that breaks Kubernetes naming rules, or a key with characters the
// API server refuses (notably the JSONPath metacharacters "[", "]" and "'", which
// GetKubernetesSecret's kubectl fallback embeds into a jsonpath expression). A
// credential that cannot resolve should fail when applied, not when a task first
// needs its token.
func validateSecretKeyRef(field string, ref *SecretKeyRef) error {
	if ref == nil {
		return fmt.Errorf("%s: a secret reference is required", field)
	}
	name := ref.GetName()
	if name == "" || len(name) > MaxSecretNameLength || !secretNameRegexp.MatchString(name) {
		return fmt.Errorf("%s.name: invalid value %q: must be a lowercase RFC 1123 DNS subdomain of at most %d characters", field, name, MaxSecretNameLength)
	}
	key := ref.GetKey()
	if key == "" || len(key) > MaxSecretKeyLength || !secretKeyRegexp.MatchString(key) {
		return fmt.Errorf("%s.key: invalid value %q: must be 1-%d characters, each alphanumeric, '-', '_', or '.'", field, key, MaxSecretKeyLength)
	}
	return nil
}

// ValidateName reports whether s can be used as a resource name or atespace: a
// lowercase RFC 1123 label of at most MaxNameLength characters.
func ValidateName(s string) error {
	if s == "" {
		return errors.New("must not be empty")
	}
	if len(s) > MaxNameLength || !nameRegexp.MatchString(s) {
		return fmt.Errorf("must be a lowercase RFC 1123 label: at most %d lowercase alphanumeric characters or '-', starting and ending with an alphanumeric character", MaxNameLength)
	}
	return nil
}

// ValidateObjectMeta checks a resource's name and, when set, its atespace. An
// empty atespace is fine here; the API server defaults it.
func ValidateObjectMeta(meta *ObjectMeta) error {
	if err := ValidateName(meta.GetName()); err != nil {
		return fmt.Errorf("metadata.name: invalid value %q: %w", meta.GetName(), err)
	}
	if atespace := meta.GetAtespace(); atespace != "" {
		if err := ValidateName(atespace); err != nil {
			return fmt.Errorf("metadata.atespace: invalid value %q: %w", atespace, err)
		}
	}
	return nil
}

// ValidateWorkspace reports the first problem with a workspace that would make
// it unusable. It is called by the API server before saving.
func ValidateWorkspace(w *Workspace) error {
	if err := ValidateObjectMeta(w.GetMetadata()); err != nil {
		return err
	}
	if w.GetSpec() != nil {
		for i, f := range w.GetSpec().GetFiles() {
			field := fmt.Sprintf("spec.files[%d]", i)
			if f == nil || f.GetPath() == "" {
				return fmt.Errorf("%s: path is required", field)
			}
		}
	}
	return nil
}

// ValidateModel reports the first problem with a model that would make it
// unusable. It is called by the API server before saving.
func ValidateModel(m *Model) error {
	return ValidateObjectMeta(m.GetMetadata())
}

// Credentials.

// CredentialType returns the name of the credential type a spec sets, using the
// manifest field name (githubApp, oauthAccessToken or oauthRefreshToken), or ""
// when none is set.
func CredentialType(spec *CredentialSpec) string {
	switch spec.GetType().(type) {
	case *CredentialSpec_GithubApp:
		return "githubApp"
	case *CredentialSpec_OauthAccessToken:
		return "oauthAccessToken"
	case *CredentialSpec_OauthRefreshToken:
		return "oauthRefreshToken"
	}
	return ""
}

// ValidateCredential reports the first problem with a credential that would
// make it unusable. It is called by the API server before saving.
func ValidateCredential(c *Credential) error {
	if err := ValidateObjectMeta(c.GetMetadata()); err != nil {
		return err
	}
	switch t := c.GetSpec().GetType().(type) {
	case *CredentialSpec_GithubApp:
		return validateGitHubApp(t.GithubApp)
	case *CredentialSpec_OauthAccessToken:
		return validateSecretKeyRef("spec.oauthAccessToken.tokenSecret", t.OauthAccessToken.GetTokenSecret())
	case *CredentialSpec_OauthRefreshToken:
		return validateOAuthRefreshToken(t.OauthRefreshToken)
	}
	return errors.New("spec: exactly one credential type (githubApp, oauthAccessToken or oauthRefreshToken) is required")
}

// maxGitHubRepositories is how many repositories one installation token request
// may be scoped to.
const maxGitHubRepositories = 500

func validateGitHubApp(app *GitHubAppCredential) error {
	if app.GetAppId() <= 0 {
		return errors.New("spec.githubApp.appId: must be positive")
	}
	if app.GetInstallationId() <= 0 {
		return errors.New("spec.githubApp.installationId: must be positive")
	}
	if err := validateSecretKeyRef("spec.githubApp.privateKeySecret", app.GetPrivateKeySecret()); err != nil {
		return err
	}
	if n := len(app.GetRepositories()); n == 0 || n > maxGitHubRepositories {
		return fmt.Errorf("spec.githubApp.repositories: must contain 1 to %d names", maxGitHubRepositories)
	}
	seen := make(map[string]bool, len(app.GetRepositories()))
	for _, name := range app.GetRepositories() {
		folded := strings.ToLower(name)
		if name == "" || strings.ContainsAny(name, "/ \\@") || seen[folded] {
			return fmt.Errorf("spec.githubApp.repositories: invalid or duplicate name %q", name)
		}
		seen[folded] = true
	}
	if len(app.GetPermissions()) == 0 {
		return errors.New("spec.githubApp.permissions: at least one permission is required")
	}
	for name, level := range app.GetPermissions() {
		if !githubAppRepositoryPermissions[name] {
			return fmt.Errorf("spec.githubApp.permissions: unknown permission %q", name)
		}
		if level != "read" && level != "write" {
			return fmt.Errorf("spec.githubApp.permissions: invalid permission %q=%q (must be \"read\" or \"write\")", name, level)
		}
	}
	return nil
}

// githubAppRepositoryPermissions lists the repository-level permission keys
// GitHub's installation-token endpoint accepts. GitHub rejects an unknown key
// only when the token is requested, so without this check a typo would surface
// when a task first needs the token instead of when the credential is applied.
var githubAppRepositoryPermissions = map[string]bool{
	"actions":                      true,
	"administration":               true,
	"checks":                       true,
	"contents":                     true,
	"deployments":                  true,
	"environments":                 true,
	"issues":                       true,
	"metadata":                     true,
	"packages":                     true,
	"pages":                        true,
	"pull_requests":                true,
	"repository_custom_properties": true,
	"repository_hooks":             true,
	"repository_projects":          true,
	"secret_scanning_alerts":       true,
	"secrets":                      true,
	"security_events":              true,
	"single_file":                  true,
	"statuses":                     true,
	"vulnerability_alerts":         true,
	"workflows":                    true,
}

func validateOAuthRefreshToken(o *OAuthRefreshTokenCredential) error {
	if err := validateHTTPSURL("spec.oauthRefreshToken.tokenUrl", o.GetTokenUrl()); err != nil {
		return err
	}
	if o.GetClientId() == "" {
		return errors.New("spec.oauthRefreshToken.clientId: is required")
	}
	// A client secret is optional: public clients authenticate with the client id alone.
	if o.GetClientSecret() != nil {
		if err := validateSecretKeyRef("spec.oauthRefreshToken.clientSecret", o.GetClientSecret()); err != nil {
			return err
		}
	}
	if err := validateSecretKeyRef("spec.oauthRefreshToken.refreshTokenSecret", o.GetRefreshTokenSecret()); err != nil {
		return err
	}
	if o.GetRevocationUrl() != "" {
		return validateHTTPSURL("spec.oauthRefreshToken.revocationUrl", o.GetRevocationUrl())
	}
	return nil
}

// validateHTTPSURL requires an https URL with a host, because the credential
// endpoints receive client and refresh secrets.
func validateHTTPSURL(field, raw string) error {
	u, err := url.Parse(raw)
	if raw == "" || err != nil || u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("%s: invalid value %q: must be an https URL", field, raw)
	}
	return nil
}

// envNameRegexp matches the names a container environment variable can portably have.
var envNameRegexp = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// reservedEnvPrefix is claimed by the variables AX sets itself, such as AX_TASK_YAML.
const reservedEnvPrefix = "AX_"

// validateCredentialBindings checks spec.credentials. A binding's variable must
// not also be set by spec.env: the runner re-applies spec.env from AX_TASK_YAML
// on top of the container environment, which would overwrite the token.
func validateCredentialBindings(spec *TaskSpec) error {
	specEnv := make(map[string]bool, len(spec.GetEnv()))
	for _, e := range spec.GetEnv() {
		specEnv[e.GetName()] = true
	}
	usedBy := make(map[string]string, len(spec.GetCredentials()))
	for i, b := range spec.GetCredentials() {
		field := fmt.Sprintf("spec.credentials[%d]", i)
		if err := ValidateName(b.GetName()); err != nil {
			return fmt.Errorf("%s.name: invalid value %q: %w", field, b.GetName(), err)
		}
		env := b.GetEnv()
		if !envNameRegexp.MatchString(env) {
			return fmt.Errorf("%s.env: invalid value %q: must match %s", field, env, envNameRegexp)
		}
		if strings.HasPrefix(env, reservedEnvPrefix) {
			return fmt.Errorf("%s.env: invalid value %q: must not start with %q", field, env, reservedEnvPrefix)
		}
		if other, dup := usedBy[env]; dup {
			return fmt.Errorf("%s.env: env %q is already used by credential %q", field, env, other)
		}
		usedBy[env] = b.GetName()
		if specEnv[env] {
			return fmt.Errorf("%s.env: env %q collides with spec.env", field, env)
		}
	}
	return nil
}

// ValidateTask reports the first problem with a task that would make it
// impossible to run correctly. It is called by the API server before saving.
func ValidateTask(t *Task) error {
	if err := ValidateObjectMeta(t.GetMetadata()); err != nil {
		return err
	}
	spec := t.GetSpec()
	if spec == nil {
		return nil
	}
	if err := validateCredentialBindings(spec); err != nil {
		return err
	}
	refs := spec.WorkspaceRefs()
	paths := spec.WorkspacePaths()
	names := make(map[string]bool, len(refs))
	seen := make(map[string]string, len(refs))
	for i, r := range refs {
		field := fmt.Sprintf("spec.workspaces[%d]", i)
		if r.GetName() == "" {
			return fmt.Errorf("%s: name is required", field)
		}
		if names[r.GetName()] {
			return fmt.Errorf("%s: workspace %q is bound more than once", field, r.GetName())
		}
		names[r.GetName()] = true
		if r.GetPath() != "" && !strings.HasPrefix(r.GetPath(), "/") {
			return fmt.Errorf("%s: path %q must be absolute", field, r.GetPath())
		}
		p := strings.TrimRight(paths[i], "/")
		if p == "" {
			p = "/"
		}
		if other, dup := seen[p]; dup {
			return fmt.Errorf("%s: path %q is already used by workspace %q", field, paths[i], other)
		}
		seen[p] = r.GetName()
	}
	return nil
}
