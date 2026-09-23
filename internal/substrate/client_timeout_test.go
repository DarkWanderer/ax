package substrate

import "testing"

func TestBuildActorTemplateAllowsRepositorySetup(t *testing.T) {
	tmpl := BuildActorTemplate("default", "coding-task", "runner", nil, nil, "")
	if got := tmpl.GetContainers()[0].GetReadyz().GetTimeoutSeconds(); got < 300 {
		t.Fatalf("readiness timeout = %d seconds, need at least 300 for repository setup", got)
	}
}
