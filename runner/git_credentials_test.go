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
	"os/exec"
	"strings"
	"testing"
)

func TestGitCredentialHelperUsesEnvironmentOnlyForGitHub(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "ghs_test_secret")
	configureGitCredentials()
	for _, tc := range []struct {
		host string
		want bool
	}{{"github.com", true}, {"example.com", false}} {
		cmd := exec.Command("git", "credential", "fill")
		cmd.Env = os.Environ()
		cmd.Stdin = strings.NewReader("protocol=https\nhost=" + tc.host + "\n\n")
		out, err := cmd.CombinedOutput()
		if tc.want {
			if err != nil || !strings.Contains(string(out), "password=ghs_test_secret") {
				t.Fatalf("host %s: %v %q", tc.host, err, out)
			}
		} else if strings.Contains(string(out), "ghs_test_secret") {
			t.Fatalf("token leaked to %s", tc.host)
		}
	}
}
