# GitHub App credentials

`CredentialProvider` issues a GitHub App installation token when an AX Task
starts. One provider describes one installation and a list of up to 500
repository **names** in that installation. All Git repositories in all
Workspaces bound to the Task can use the same token. Repositories from another
installation need a separate Task and provider.

Create a Kubernetes Secret in the Task's atespace containing the GitHub App's
PEM private key. Then apply a provider and a Task reference:

```yaml
apiVersion: ax.io/v1alpha1
kind: CredentialProvider
metadata:
  name: source-app
  atespace: default
spec:
  githubApp:
    appId: "12345"
    installationId: "67890"
    privateKeySecret:
      name: source-app-key
      key: private-key.pem
    repositories: [frontend, backend, tools]
    permissions:
      contents: read
---
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: build
spec:
  credentialProvider:
    name: source-app
  workspaces:
    - name: application
    - name: tooling
```

Use HTTPS `github.com` URLs in each Workspace's `spec.git`. AX rejects a
GitHub repository whose name is absent from the provider. The runner uses a
process-local Git credential helper for Workspace fetches and Task Git commands;
the token is never put in remote URLs, command arguments, or Git config files.
The private key stays in the controller. A Task with a provider cannot also
set `GITHUB_TOKEN` in `spec.env`.

AX mints one restricted token per actor start. It revokes the token after a
successful `ax suspend task` or deletion. `ax resume task` mints a new token
and switches the suspended actor to a new template, keeping the durable
Workspace volume. GitHub installation tokens expire after one hour, so Git
operations in a continuously running Task can fail after that time. Router
wakes do not rotate the token. Credential or authenticated Workspace setup
failure marks the Task failed.

The AX API is a trusted network boundary: anyone who can submit a Task in an
atespace can use a provider in that atespace. Restrict AX API access and
provider/Secret access accordingly.

See [GitHub's installation token documentation](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-an-installation-access-token-for-a-github-app)
for repository and permission limits.
