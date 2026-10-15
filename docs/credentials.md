# Credentials

A `Credential` tells AX how to obtain a token that a task needs, such as a GitHub installation token or a Claude Code OAuth token, without putting the secret material in the task. The secrets stay in Kubernetes Secrets; the `Credential` only references them. A task binds a credential with `spec.credentials`, and AX hands the token to the task's container as an environment variable.

A credential has exactly one of three types. Setting two is rejected when you apply it.

Secret references are read from the Kubernetes namespace named after the credential's atespace.

## `githubApp`

Mints a [GitHub App installation token](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-an-installation-access-token-for-a-github-app), scoped to the repositories and permissions you list. The App's private key is the Secret.

```bash
kubectl create secret generic github-app-key --from-file=private-key.pem=./app.private-key.pem
```

```yaml
apiVersion: ax.io/v1alpha1
kind: Credential
metadata:
  name: gh
  atespace: default
spec:
  githubApp:
    appId: 123456
    installationId: 7890123
    privateKeySecret:
      name: github-app-key
      key: private-key.pem
    repositories: [my-service, team-tools]   # 1 to 500 repository names, without the owner
    permissions:
      contents: read
      pull_requests: write
```

Permission names are GitHub's repository permissions, and the level is `read` or `write`. The token expires after one hour.

## `oauthAccessToken`

A long-lived OAuth access token that is used as it is. The typical case is the token `claude setup-token` prints for Claude Code, which a task passes to `claude` as `CLAUDE_CODE_OAUTH_TOKEN`.

```bash
claude setup-token    # prints sk-ant-oat01-...
kubectl create secret generic claude-oauth --from-literal=token='sk-ant-oat01-...'
```

```yaml
apiVersion: ax.io/v1alpha1
kind: Credential
metadata:
  name: claude
spec:
  oauthAccessToken:
    tokenSecret:
      name: claude-oauth
      key: token
```

AX knows no expiry for such a token and never revokes it: revoking would invalidate the token stored in the Secret.

## `oauthRefreshToken`

Exchanges a refresh token for short-lived access tokens with the OAuth `refresh_token` grant.

```yaml
apiVersion: ax.io/v1alpha1
kind: Credential
metadata:
  name: idp
spec:
  oauthRefreshToken:
    tokenUrl: https://idp.example.com/oauth/token    # must be https
    clientId: ax
    clientSecret:                                     # optional; omit for a public client
      name: idp-client
      key: client-secret
    scopes: [repo, read:user]
    refreshTokenSecret:                               # the seed, see below
      name: idp-refresh
      key: refresh-token
    revocationUrl: https://idp.example.com/oauth/revoke   # optional, https, RFC 7009
```

- **Client authentication.** With a `clientSecret`, AX sends it with HTTP Basic authentication. Without one, it sends `client_id` in the request body.
- **Rotation.** Many providers retire a refresh token the moment it is used and return a new one. AX stores the new token itself (in Redis, see [Limitations](#limitations)) before it uses the access token, so the Secret is only the *seed*. Refreshes of one credential never run concurrently.
- **Re-authorizing.** Update the seed Secret. AX notices the change and starts again from it. Changing `tokenUrl` or `clientId` has the same effect.
- **Revocation.** If `revocationUrl` is set, AX revokes access tokens there. Otherwise it cannot revoke them.

Changing `tokenUrl`, `clientId` or `refreshTokenSecret`, or switching the credential to another type, discards the stored refresh token, because it was issued for the old endpoint, client or seed. A provider that rotates then needs the seed Secret updated. Other edits, such as `scopes`, `clientSecret` or `revocationUrl`, keep it.

## Binding a credential to a task

```yaml
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: review
spec:
  command: ["claude", "-p", "Review the open pull request"]
  credentials:
    - name: claude                       # a Credential in the task's atespace
      env: CLAUDE_CODE_OAUTH_TOKEN       # the variable that receives the token
    - name: gh
      env: GITHUB_TOKEN
```

- Every named credential must exist when the task is created. If one is missing, `ax apply` fails and nothing is saved.
- `env` must be a valid variable name, unique among the bindings, must not start with `AX_`, and must not also be set in `spec.env`. The runner applies `spec.env` last, which would overwrite the token.
- One credential can be bound to several variables; it is issued once.

AX issues the tokens when it creates the task's ActorTemplate on Agent Substrate, which happens when the task is created. The tokens become part of that template, so suspending and resuming the task reuses them and mints nothing new.

If a token cannot be issued, the task goes to `Failed` and its `Ready` condition is False with reason `CredentialIssueFailed`. A timeout or an unavailable Substrate leaves the task `Pending` instead, and creating it again retries. AX does not fall back to the default template, which would start the task without its credentials. Tokens already issued for that attempt are revoked where the type allows it.

## Testing a credential

```console
$ ax test credential gh
credential.ax.io/gh ok
  type:    githubApp
  token:   ghs_…a1b2
  expires: 2026-10-15T14:03:00Z (in 59m)
  revoked: yes
```

`ax test` makes the server obtain a token exactly as a task would, shows its first and last four characters, and then revokes it if the type allows. The token itself never leaves the server. For `oauthRefreshToken` the test performs a real refresh, so it rotates the stored refresh token.

## Limitations

- **Tokens are minted once, at task creation.** A GitHub token expires after an hour and is not renewed when the task resumes, and nothing revokes it when the task is suspended or deleted. A task that outlives its token needs to be recreated. Editing a `Credential` does not change tasks that already exist.
- **Tokens are visible to the task and stored in its ActorTemplate.** Anything that runs in the sandbox, including `ax ssh`, can read the variable.
- **Rotated refresh tokens are stored in plaintext in Redis**, under the `credential-state` key of the credential. Protect Redis like you protect the Secrets.
- **The AX API is the trust boundary.** AX does not authorize individual users. Whoever can apply a `Credential` or create a `Task` can make the server read the Secrets of that atespace's namespace and send them to the URLs the credential names.
- **No automatic cleanup on deletion.** Deleting a task or a credential does not revoke tokens that were already issued.
