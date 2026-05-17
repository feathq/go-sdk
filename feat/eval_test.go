package feat

import (
	"encoding/json"
	"testing"
)

// Parity suite: mirrors test/eval.test.ts in @feathq/feat-js-sdk so a flag
// served by either SDK returns the same variation for the same context.
// New eval cases should land in both files (or a shared JSON fixture
// suite — TODO once the Python + Ruby ports also land).

func raw(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

var (
	trueVar  = VariationSpec{ID: "var-true", Name: "true", Value: raw(true)}
	falseVar = VariationSpec{ID: "var-false", Name: "false", Value: raw(false)}
)

func makeDatafile(flags map[string]FlagSpec, segments map[string]SegmentSpec) *Datafile {
	if segments == nil {
		segments = map[string]SegmentSpec{}
	}
	if flags == nil {
		flags = map[string]FlagSpec{}
	}
	return &Datafile{
		SchemaVersion: 1,
		EnvID:         "env-1",
		EnvKey:        "staging",
		ProjectID:     "proj-1",
		Version:       1,
		Etag:          "etag",
		GeneratedAt:   "2026-05-17T00:00:00Z",
		Flags:         flags,
		Segments:      segments,
		ContextKinds: map[string]ContextKindSpec{
			"user": {Key: "user", AvailableForRules: true, AvailableForExperiments: true},
		},
	}
}

func boolFlag() FlagSpec {
	def := falseVar.ID
	return FlagSpec{
		ID:                 "flag-1",
		Key:                "checkout",
		ValueType:          "boolean",
		Salt:               "abcdef0123456789",
		IsEnabled:          true,
		OffVariationID:     falseVar.ID,
		DefaultVariationID: &def,
		Variations:         []VariationSpec{trueVar, falseVar},
	}
}

func ctxUser(key string, attrs map[string]any) EvalContext {
	return EvalContext{
		Kinds: map[string]ContextKindObject{
			"user": {Key: key, Attrs: attrs},
		},
	}
}

func asBool(t *testing.T, r EvaluationResult) bool {
	t.Helper()
	var v bool
	if err := json.Unmarshal(r.Value, &v); err != nil {
		t.Fatalf("expected bool value, got %s: %v", string(r.Value), err)
	}
	return v
}

func TestEvalReturnsOffWhenArchived(t *testing.T) {
	flag := boolFlag()
	flag.Archived = true
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, nil)
	r := Evaluate("checkout", raw(false), ctxUser("u1", nil), df)
	if asBool(t, r) != false || r.Reason != ReasonDisabled {
		t.Fatalf("got value=%v reason=%v", asBool(t, r), r.Reason)
	}
}

func TestEvalReturnsOffWhenDisabled(t *testing.T) {
	flag := boolFlag()
	flag.IsEnabled = false
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, nil)
	r := Evaluate("checkout", raw(true), ctxUser("u1", nil), df)
	if asBool(t, r) != false || r.Reason != ReasonDisabled {
		t.Fatalf("got value=%v reason=%v", asBool(t, r), r.Reason)
	}
}

func TestEvalDefaultWhenNoTargeting(t *testing.T) {
	df := makeDatafile(map[string]FlagSpec{"checkout": boolFlag()}, nil)
	r := Evaluate("checkout", raw(true), ctxUser("u1", nil), df)
	if asBool(t, r) != false || r.Reason != ReasonFallthrough {
		t.Fatalf("got value=%v reason=%v", asBool(t, r), r.Reason)
	}
}

func TestEvalIndividualTargetBeatsRules(t *testing.T) {
	flag := boolFlag()
	flag.Targets = []TargetSpec{{ContextKindKey: "user", ContextKey: "u-vip", VariationID: trueVar.ID}}
	flag.Rules = []RuleSpec{{
		ID: "r1",
		VariationID: ptr(falseVar.ID),
		Groups: []ConditionGroupSpec{{
			Conditions: []ConditionSpec{{
				AttributePath: "user.key",
				Operator:      "is_one_of",
				Values:        []json.RawMessage{raw("u-vip")},
			}},
		}},
	}}
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, nil)
	r := Evaluate("checkout", raw(false), ctxUser("u-vip", nil), df)
	if asBool(t, r) != true || r.Reason != ReasonTargetingMatch {
		t.Fatalf("got value=%v reason=%v", asBool(t, r), r.Reason)
	}
}

func TestEvalRuleEndsWithOnEmail(t *testing.T) {
	flag := boolFlag()
	flag.Rules = []RuleSpec{{
		ID: "r1",
		VariationID: ptr(trueVar.ID),
		Groups: []ConditionGroupSpec{{
			Conditions: []ConditionSpec{{
				AttributePath: "user.email",
				Operator:      "ends_with",
				Values:        []json.RawMessage{raw("@example.com")},
			}},
		}},
	}}
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, nil)
	r := Evaluate("checkout", raw(false), ctxUser("u1", map[string]any{"email": "alice@example.com"}), df)
	if asBool(t, r) != true || r.Reason != ReasonTargetingMatch {
		t.Fatalf("got value=%v reason=%v", asBool(t, r), r.Reason)
	}
}

func TestEvalRuleMultipleGroupsOR(t *testing.T) {
	flag := boolFlag()
	flag.Rules = []RuleSpec{{
		ID: "r1",
		VariationID: ptr(trueVar.ID),
		Groups: []ConditionGroupSpec{
			{Conditions: []ConditionSpec{{
				AttributePath: "user.email",
				Operator:      "ends_with",
				Values:        []json.RawMessage{raw("@nope.com")},
			}}},
			{Conditions: []ConditionSpec{{
				AttributePath: "user.plan",
				Operator:      "is_one_of",
				Values:        []json.RawMessage{raw("pro"), raw("enterprise")},
			}}},
		},
	}}
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, nil)
	r := Evaluate("checkout", raw(false), ctxUser("u1", map[string]any{"email": "x@elsewhere.com", "plan": "pro"}), df)
	if asBool(t, r) != true {
		t.Fatalf("expected true, got %v", asBool(t, r))
	}
}

func TestEvalRolloutDeterministic(t *testing.T) {
	flag := boolFlag()
	flag.DefaultVariationID = nil
	flag.DefaultRollout = &Rollout{
		BucketingContextKindKey: "user",
		Variations: []RolloutVariation{
			{VariationID: trueVar.ID, Weight: 50_000},
			{VariationID: falseVar.ID, Weight: 50_000},
		},
	}
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, nil)
	r1 := Evaluate("checkout", raw(false), ctxUser("stable-key", nil), df)
	r2 := Evaluate("checkout", raw(false), ctxUser("stable-key", nil), df)
	if asBool(t, r1) != asBool(t, r2) {
		t.Fatalf("bucketing is non-deterministic: %v vs %v", asBool(t, r1), asBool(t, r2))
	}
	if r1.Reason != ReasonSplit {
		t.Fatalf("expected SPLIT reason, got %v", r1.Reason)
	}
}

func TestEvalRollout100Percent(t *testing.T) {
	flag := boolFlag()
	flag.DefaultVariationID = nil
	flag.DefaultRollout = &Rollout{
		BucketingContextKindKey: "user",
		Variations:              []RolloutVariation{{VariationID: trueVar.ID, Weight: 100_000}},
	}
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, nil)
	for _, key := range []string{"u1", "u2", "u3", "u4", "u5"} {
		r := Evaluate("checkout", raw(false), ctxUser(key, nil), df)
		if asBool(t, r) != true {
			t.Fatalf("expected true for %s, got %v", key, asBool(t, r))
		}
	}
}

func TestEvalSegmentMatch(t *testing.T) {
	flag := boolFlag()
	flag.Rules = []RuleSpec{{
		ID: "r1",
		VariationID: ptr(trueVar.ID),
		Groups: []ConditionGroupSpec{{
			Conditions: []ConditionSpec{{
				AttributePath: "",
				Operator:      "segment_match",
				Values:        []json.RawMessage{raw("internal-users")},
			}},
		}},
	}}
	segs := map[string]SegmentSpec{
		"internal-users": {
			Key: "internal-users",
			Rules: []SegmentRuleSpec{{
				Conditions: []ConditionSpec{{
					AttributePath: "user.email",
					Operator:      "ends_with",
					Values:        []json.RawMessage{raw("@feathq.com")},
				}},
			}},
		},
	}
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, segs)

	hit := Evaluate("checkout", raw(false), ctxUser("u1", map[string]any{"email": "bob@feathq.com"}), df)
	if asBool(t, hit) != true {
		t.Fatalf("expected segment hit, got %v", asBool(t, hit))
	}
	miss := Evaluate("checkout", raw(false), ctxUser("u2", map[string]any{"email": "bob@other.com"}), df)
	if asBool(t, miss) != false {
		t.Fatalf("expected segment miss, got %v", asBool(t, miss))
	}
}

func TestEvalSemverGte(t *testing.T) {
	flag := boolFlag()
	flag.Rules = []RuleSpec{{
		ID: "r1",
		VariationID: ptr(trueVar.ID),
		Groups: []ConditionGroupSpec{{
			Conditions: []ConditionSpec{{
				AttributePath: "user.app_version",
				Operator:      "semver_gte",
				Values:        []json.RawMessage{raw("1.2.0")},
			}},
		}},
	}}
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, nil)

	newer := Evaluate("checkout", raw(false), ctxUser("u1", map[string]any{"app_version": "1.5.0"}), df)
	if asBool(t, newer) != true {
		t.Fatalf("expected newer to match, got %v", asBool(t, newer))
	}
	older := Evaluate("checkout", raw(false), ctxUser("u2", map[string]any{"app_version": "1.1.5"}), df)
	if asBool(t, older) != false {
		t.Fatalf("expected older to miss, got %v", asBool(t, older))
	}
}

func TestEvalMissingFlagReturnsError(t *testing.T) {
	df := makeDatafile(nil, nil)
	r := Evaluate("missing", raw("fallback"), ctxUser("u1", nil), df)
	if r.Reason != ReasonError {
		t.Fatalf("expected ERROR reason, got %v", r.Reason)
	}
	var v string
	if err := json.Unmarshal(r.Value, &v); err != nil || v != "fallback" {
		t.Fatalf("expected fallback string, got %s err=%v", string(r.Value), err)
	}
}

func ptr[T any](v T) *T { return &v }
