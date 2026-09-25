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

import "os"

// configureGitCredentials gives Git a process-local helper. No token appears
// in a remote URL, command argument, or Git configuration file.
func configureGitCredentials() {
	_ = os.Setenv("GIT_TERMINAL_PROMPT", "0")
	_ = os.Setenv("GIT_CONFIG_COUNT", "2")
	_ = os.Setenv("GIT_CONFIG_KEY_0", "credential.helper")
	_ = os.Setenv("GIT_CONFIG_VALUE_0", "")
	_ = os.Setenv("GIT_CONFIG_KEY_1", "credential.https://github.com.helper")
	_ = os.Setenv("GIT_CONFIG_VALUE_1", `!f() { [ "$1" = get ] || exit 0; protocol=; host=; while IFS= read -r line; do case "$line" in protocol=*) protocol=${line#protocol=} ;; host=*) host=${line#host=} ;; esac; done; if [ "$protocol" = https ] && [ "$host" = github.com ] && [ -n "$GITHUB_TOKEN" ]; then printf 'username=x-access-token\npassword=%s\n' "$GITHUB_TOKEN"; fi; }; f`)
}
