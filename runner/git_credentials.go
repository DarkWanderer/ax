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

package runner

import (
	"os"
	"strings"
)

// gitCredentialEnv holds the environment variables that install the
// process-local Git credential helper. No token appears in a remote URL,
// command argument, or Git configuration file.
//
// Exposed as a slice (rather than only setting os.Environ) so the runner can
// re-apply these exact entries after a task's own spec.env, guaranteeing a
// task cannot disable or redirect the helper by supplying its own
// GIT_CONFIG_COUNT/GIT_CONFIG_KEY_*/GIT_CONFIG_VALUE_*/GIT_TERMINAL_PROMPT.
var gitCredentialEnv = []string{
	"GIT_TERMINAL_PROMPT=0",
	"GIT_CONFIG_COUNT=2",
	"GIT_CONFIG_KEY_0=credential.helper",
	"GIT_CONFIG_VALUE_0=",
	"GIT_CONFIG_KEY_1=credential.https://github.com.helper",
	`GIT_CONFIG_VALUE_1=!f() { [ "$1" = get ] || exit 0; protocol=; host=; while IFS= read -r line; do case "$line" in protocol=*) protocol=${line#protocol=} ;; host=*) host=${line#host=} ;; esac; done; host=$(printf '%s' "$host" | tr '[:upper:]' '[:lower:]'); host=${host%:443}; if [ "$protocol" = https ] && [ "$host" = github.com ] && [ -n "$GITHUB_TOKEN" ]; then printf 'username=x-access-token\npassword=%s\n' "$GITHUB_TOKEN"; fi; }; f`,
}

// configureGitCredentials gives Git a process-local helper. No token appears
// in a remote URL, command argument, or Git configuration file.
func configureGitCredentials() {
	for _, kv := range gitCredentialEnv {
		name, value, _ := strings.Cut(kv, "=")
		_ = os.Setenv(name, value)
	}
}
