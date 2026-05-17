package feat

import "encoding/json"

// matchSegment returns true iff the context matches the named segment.
// Mirrors the JS engine: rules are OR'd, conditions within a rule are
// AND'd, and segment_match / segment_not_match conditions recurse so
// "segment of segments" works even though the admin UI doesn't expose it
// yet. Unknown segment keys evaluate to false (consistent with the
// control plane's "delete cascade prevented" invariant).
func matchSegment(segmentKey string, ctx EvalContext, df *Datafile) bool {
	seg, ok := df.Segments[segmentKey]
	if !ok {
		return false
	}
	for _, rule := range seg.Rules {
		if matchSegmentRule(rule, ctx, df) {
			return true
		}
	}
	return false
}

func matchSegmentRule(rule SegmentRuleSpec, ctx EvalContext, df *Datafile) bool {
	for _, cond := range rule.Conditions {
		if !matchCondition(cond, ctx, df) {
			return false
		}
	}
	return len(rule.Conditions) > 0
}

func matchCondition(cond ConditionSpec, ctx EvalContext, df *Datafile) bool {
	switch cond.Operator {
	case "segment_match":
		for _, raw := range cond.Values {
			var key string
			if err := json.Unmarshal(raw, &key); err == nil {
				if matchSegment(key, ctx, df) {
					return true
				}
			}
		}
		return false
	case "segment_not_match":
		for _, raw := range cond.Values {
			var key string
			if err := json.Unmarshal(raw, &key); err == nil {
				if matchSegment(key, ctx, df) {
					return false
				}
			}
		}
		return true
	}
	lhs, _ := resolveAttribute(ctx, cond.AttributePath)
	return matchOperator(cond.Operator, lhs, cond.Values)
}
