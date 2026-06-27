// Package feat is the Go SDK for the feat feature-flag platform.
//
// The wire format mirrors @feathq/datafile-schema (TypeScript) — keep the
// JSON field tags here in sync with that package's `Datafile`. The eval
// engine in feat/eval mirrors @feathq/feat-eval bit-for-bit so a flag
// served by the JS SDK and the Go SDK returns the same variation for the
// same context.
package feat

import "encoding/json"

// Datafile is the per-environment evaluatable snapshot served to SDKs.
// Keyed by flag/segment/kind *key* (not id) for fast lookup on the eval
// hot path.
type Datafile struct {
	SchemaVersion int                       `json:"schemaVersion"`
	EnvID         string                    `json:"envId"`
	EnvKey        string                    `json:"envKey"`
	ProjectID     string                    `json:"projectId"`
	Version       int64                     `json:"version"`
	Etag          string                    `json:"etag"`
	GeneratedAt   string                    `json:"generatedAt"`
	Flags         map[string]FlagSpec       `json:"flags"`
	Segments      map[string]SegmentSpec    `json:"segments"`
	ContextKinds  map[string]ContextKindSpec `json:"contextKinds"`
}

// FlagSpec is one flag's evaluatable config for the env this datafile
// belongs to. Exactly one of DefaultVariationID or DefaultRollout is set
// when IsEnabled; the producer enforces that invariant.
type FlagSpec struct {
	ID                              string           `json:"id"`
	Key                             string           `json:"key"`
	ValueType                       string           `json:"valueType"`
	Salt                            string           `json:"salt"`
	Archived                        bool             `json:"archived"`
	IsEnabled                       bool             `json:"isEnabled"`
	OffVariationID                  string           `json:"offVariationId"`
	DefaultVariationID              *string          `json:"defaultVariationId"`
	DefaultRollout                  *Rollout         `json:"defaultRollout"`
	DefaultBucketingContextKindKey  *string          `json:"defaultBucketingContextKindKey"`
	Variations                      []VariationSpec  `json:"variations"`
	Targets                         []TargetSpec     `json:"targets"`
	Rules                           []RuleSpec       `json:"rules"`
}

type VariationSpec struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Value json.RawMessage `json:"value"`
}

type TargetSpec struct {
	ContextKindKey string `json:"contextKindKey"`
	ContextKey     string `json:"contextKey"`
	VariationID    string `json:"variationId"`
}

// RuleSpec — exactly one of VariationID or Rollout is set. Groups are
// OR'd; conditions within a group are AND'd.
type RuleSpec struct {
	ID                       string               `json:"id"`
	BucketingContextKindKey  *string              `json:"bucketingContextKindKey"`
	VariationID              *string              `json:"variationId"`
	Rollout                  *Rollout             `json:"rollout"`
	Groups                   []ConditionGroupSpec `json:"groups"`
}

type ConditionGroupSpec struct {
	Conditions []ConditionSpec `json:"conditions"`
}

// ConditionSpec holds one match against an attribute path. Values is the
// RHS list; type interpretation depends on the operator (string list for
// is_one_of, semver strings for semver_*, etc.). See feat/eval/operators.go.
type ConditionSpec struct {
	AttributePath string            `json:"attributePath"`
	Operator      string            `json:"operator"`
	Values        []json.RawMessage `json:"values"`
}

// Rollout is a percentage split across variations. Weights sum to exactly
// 100000. The eval engine hashes (salt + key + context-key) and walks
// cumulative weights to pick a variation deterministically.
type Rollout struct {
	BucketingContextKindKey string             `json:"bucketingContextKindKey"`
	Variations              []RolloutVariation `json:"variations"`
}

type RolloutVariation struct {
	VariationID string `json:"variationId"`
	Weight      int    `json:"weight"`
}

type SegmentSpec struct {
	Key   string            `json:"key"`
	Rules []SegmentRuleSpec `json:"rules"`
}

type SegmentRuleSpec struct {
	Conditions []ConditionSpec `json:"conditions"`
}

type ContextKindSpec struct {
	Key                     string `json:"key"`
	AvailableForRules       bool   `json:"availableForRules"`
	AvailableForExperiments bool   `json:"availableForExperiments"`
}

// datafilePatch is an incremental delta between two datafile versions, pushed
// as an `event: patch` SSE frame. It carries only the flags and segments that
// changed (Flags / Segments) plus the keys that were dropped (RemovedFlags /
// RemovedSegments). From is the version this delta applies on top of; To is the
// version it produces. A patch is applied only when the in-memory datafile is
// exactly at From; otherwise it is ignored and a reconnect re-seeds a full put.
type datafilePatch struct {
	From            int64                  `json:"from"`
	To              int64                  `json:"to"`
	Etag            string                 `json:"etag"`
	GeneratedAt     string                 `json:"generatedAt"`
	Flags           map[string]FlagSpec    `json:"flags"`
	RemovedFlags    []string               `json:"removedFlags"`
	Segments        map[string]SegmentSpec `json:"segments"`
	RemovedSegments []string               `json:"removedSegments"`
}

// patchDatafile returns a new datafile with the delta applied: changed flags
// and segments merged in, removed keys dropped, and version/etag/generatedAt
// advanced to the patch's To. It never mutates cur (live readers hold it
// lock-free), building fresh flag and segment maps instead. ContextKinds are
// not touched by a patch, so the immutable map is shared with cur.
func patchDatafile(cur *Datafile, p *datafilePatch) *Datafile {
	next := *cur
	next.Version = p.To
	next.Etag = p.Etag
	next.GeneratedAt = p.GeneratedAt

	next.Flags = make(map[string]FlagSpec, len(cur.Flags)+len(p.Flags))
	for k, v := range cur.Flags {
		next.Flags[k] = v
	}
	for k, v := range p.Flags {
		next.Flags[k] = v
	}
	for _, k := range p.RemovedFlags {
		delete(next.Flags, k)
	}

	next.Segments = make(map[string]SegmentSpec, len(cur.Segments)+len(p.Segments))
	for k, v := range cur.Segments {
		next.Segments[k] = v
	}
	for k, v := range p.Segments {
		next.Segments[k] = v
	}
	for _, k := range p.RemovedSegments {
		delete(next.Segments, k)
	}
	return &next
}
