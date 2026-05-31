package feat

import "encoding/json"

// Reason is the eval-result reason code, mirroring OpenFeature's enum.
type Reason string

const (
	ReasonTargetingMatch Reason = "TARGETING_MATCH"
	ReasonSplit          Reason = "SPLIT"
	ReasonFallthrough    Reason = "FALLTHROUGH"
	ReasonDefault        Reason = "DEFAULT"
	ReasonDisabled       Reason = "DISABLED"
	ReasonError          Reason = "ERROR"
	ReasonStatic         Reason = "STATIC"
)

// EvaluationResult holds the raw JSON value the engine selected plus the
// reason it was selected. Callers should use the typed helpers on Client
// (GetBooleanValue, GetStringValue, etc.) for ergonomics.
type EvaluationResult struct {
	Value        json.RawMessage
	VariationID  string
	Reason       Reason
	ErrorMessage string
}

// Evaluate runs the precedence pipeline against a datafile. Mirrors the
// JS engine's eval order so a flag served by both SDKs returns the same
// variation for the same context:
//
//  1. flag archived → off
//  2. flag !isEnabled → off
//  3. individual target → target variation
//  4. first matching rule → variation or rollout
//  5. default → default variation or rollout
//  6. nothing matched → off
func Evaluate(flagKey string, defaultValue json.RawMessage, ctx EvalContext, df *Datafile) EvaluationResult {
	flag, ok := df.Flags[flagKey]
	if !ok {
		return EvaluationResult{
			Value:        defaultValue,
			Reason:       ReasonError,
			ErrorMessage: "flag could not be evaluated",
		}
	}

	if flag.Archived || !flag.IsEnabled {
		return resolveVariation(flag, flag.OffVariationID, ReasonDisabled, defaultValue)
	}

	for _, target := range flag.Targets {
		ctxKey, ok := readContextKey(ctx, target.ContextKindKey)
		if ok && ctxKey == target.ContextKey {
			return resolveVariation(flag, target.VariationID, ReasonTargetingMatch, defaultValue)
		}
	}

	for _, rule := range flag.Rules {
		if !matchRule(rule, ctx, df) {
			continue
		}
		if rule.VariationID != nil {
			return resolveVariation(flag, *rule.VariationID, ReasonTargetingMatch, defaultValue)
		}
		if rule.Rollout != nil {
			if v, ok := pickRollout(flag, *rule.Rollout, ctx); ok {
				return resolveVariation(flag, v, ReasonSplit, defaultValue)
			}
		}
	}

	if flag.DefaultVariationID != nil {
		return resolveVariation(flag, *flag.DefaultVariationID, ReasonFallthrough, defaultValue)
	}
	if flag.DefaultRollout != nil {
		if v, ok := pickRollout(flag, *flag.DefaultRollout, ctx); ok {
			return resolveVariation(flag, v, ReasonSplit, defaultValue)
		}
	}

	return resolveVariation(flag, flag.OffVariationID, ReasonDefault, defaultValue)
}

func matchRule(rule RuleSpec, ctx EvalContext, df *Datafile) bool {
	if len(rule.Groups) == 0 {
		return false
	}
	for _, group := range rule.Groups {
		if matchGroup(group, ctx, df) {
			return true
		}
	}
	return false
}

func matchGroup(group ConditionGroupSpec, ctx EvalContext, df *Datafile) bool {
	if len(group.Conditions) == 0 {
		return false
	}
	for _, cond := range group.Conditions {
		if !matchCondition(cond, ctx, df) {
			return false
		}
	}
	return true
}

func pickRollout(flag FlagSpec, rollout Rollout, ctx EvalContext) (string, bool) {
	ctxKey, ok := readContextKey(ctx, rollout.BucketingContextKindKey)
	if !ok {
		return "", false
	}
	return pickByWeight(bucket(flag.Salt, flag.Key, ctxKey), rollout.Variations)
}

func resolveVariation(flag FlagSpec, variationID string, reason Reason, defaultValue json.RawMessage) EvaluationResult {
	for _, v := range flag.Variations {
		if v.ID == variationID {
			return EvaluationResult{
				Value:       v.Value,
				VariationID: variationID,
				Reason:      reason,
			}
		}
	}
	return EvaluationResult{
		Value:        defaultValue,
		Reason:       ReasonError,
		ErrorMessage: "flag could not be evaluated",
	}
}
