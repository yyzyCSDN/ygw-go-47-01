package model_test

import (
	"testing"

	"coordination/internal/model"
)

func TestRoleTransitions(t *testing.T) {
	if next, ok := model.Transition(model.RoleFollower, model.RoleCandidate); !ok || next != model.RoleCandidate {
		t.Fatalf("follower->candidate = %v/%v", next, ok)
	}
	if _, ok := model.Transition(model.RoleLeader, model.RoleLeader); ok {
		t.Fatal("leader->leader should be invalid")
	}
	if !model.CanVote(model.RoleFollower) || model.CanLead(model.RoleFollower) {
		t.Fatal("follower can vote but not lead")
	}
}

func TestEventCloneAndMatch(t *testing.T) {
	payload := []byte("payload")
	event := model.NewPut("k", payload, 7)
	payload[0] = 'X'
	if string(event.Value) == "Xayload" {
		t.Fatal("event payload was mutated through caller slice")
	}
	if !event.Match("k") || !event.Match("") || event.Match("other") {
		t.Fatal("match semantics broken")
	}
}
