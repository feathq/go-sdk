package feat

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// matchOperator applies a single operator predicate. Returns false on
// type-mismatch / parse-failure — mirrors the JS engine's defensive
// posture so malformed contexts evaluate to "no match" rather than
// crashing the SDK.
//
// segment_match / segment_not_match are dispatched by the rule evaluator
// (they recurse into the datafile's segments map), not by this function.
func matchOperator(operator string, lhs any, values []json.RawMessage) bool {
	switch operator {
	case "is_one_of":
		return anyEq(lhs, values)
	case "is_not_one_of":
		return !anyEq(lhs, values)
	case "is_empty":
		return isEmpty(lhs)
	case "is_not_empty":
		return !isEmpty(lhs)
	case "contains":
		return stringPredicateAny(lhs, values, strings.Contains)
	case "does_not_contain":
		if _, ok := lhs.(string); !ok {
			return true
		}
		return !stringPredicateAny(lhs, values, strings.Contains)
	case "starts_with":
		return stringPredicateAny(lhs, values, strings.HasPrefix)
	case "ends_with":
		return stringPredicateAny(lhs, values, strings.HasSuffix)
	case "matches_regex":
		s, ok := lhs.(string)
		if !ok {
			return false
		}
		return anyValue(values, func(v string) bool {
			// Length cap; Go's RE2 is linear-time so we don't need the
			// nested-quantifier check the JS / Python / Ruby ports have.
			if len(v) > 512 {
				return false
			}
			re, err := regexp.Compile(v)
			if err != nil {
				return false
			}
			return re.MatchString(s)
		})
	case "gt":
		return numericCompare(lhs, values, func(a, b float64) bool { return a > b })
	case "gte":
		return numericCompare(lhs, values, func(a, b float64) bool { return a >= b })
	case "lt":
		return numericCompare(lhs, values, func(a, b float64) bool { return a < b })
	case "lte":
		return numericCompare(lhs, values, func(a, b float64) bool { return a <= b })
	case "before":
		return dateCompare(lhs, values, func(a, b time.Time) bool { return a.Before(b) })
	case "after":
		return dateCompare(lhs, values, func(a, b time.Time) bool { return a.After(b) })
	case "semver_eq":
		return semverCompare(lhs, values, func(cmp int) bool { return cmp == 0 })
	case "semver_gt":
		return semverCompare(lhs, values, func(cmp int) bool { return cmp > 0 })
	case "semver_gte":
		return semverCompare(lhs, values, func(cmp int) bool { return cmp >= 0 })
	case "semver_lt":
		return semverCompare(lhs, values, func(cmp int) bool { return cmp < 0 })
	case "semver_lte":
		return semverCompare(lhs, values, func(cmp int) bool { return cmp <= 0 })
	case "segment_match", "segment_not_match":
		return false
	}
	return false
}

func isEmpty(lhs any) bool {
	if lhs == nil {
		return true
	}
	s, ok := lhs.(string)
	return ok && s == ""
}

func anyEq(lhs any, values []json.RawMessage) bool {
	for _, raw := range values {
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			continue
		}
		if deepEq(lhs, v) {
			return true
		}
	}
	return false
}

// deepEq is JSON-shape equality with the same string/number coercion rule
// the JS engine uses — keeps is_one_of usable when context attributes are
// stored as strings (e.g. from HTTP headers) but the rule values are
// numbers (or vice versa).
func deepEq(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	switch av := a.(type) {
	case string:
		switch bv := b.(type) {
		case string:
			return av == bv
		case float64:
			return av == strconv.FormatFloat(bv, 'f', -1, 64)
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			return av == bv
		case string:
			return strconv.FormatFloat(av, 'f', -1, 64) == bv
		}
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	}
	// Fallback: JSON marshal both sides and compare. Handles
	// object/array equality the same way the JS engine does
	// (JSON.stringify(a) === JSON.stringify(b)).
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return string(aj) == string(bj)
}

func stringPredicateAny(lhs any, values []json.RawMessage, pred func(string, string) bool) bool {
	s, ok := lhs.(string)
	if !ok {
		return false
	}
	return anyValue(values, func(v string) bool { return pred(s, v) })
}

func anyValue(values []json.RawMessage, pred func(string) bool) bool {
	for _, raw := range values {
		var v string
		if err := json.Unmarshal(raw, &v); err != nil {
			continue
		}
		if pred(v) {
			return true
		}
	}
	return false
}

func numericCompare(lhs any, values []json.RawMessage, cmp func(float64, float64) bool) bool {
	a, ok := toFloat(lhs)
	if !ok {
		return false
	}
	for _, raw := range values {
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			continue
		}
		b, bok := toFloat(v)
		if bok && cmp(a, b) {
			return true
		}
	}
	return false
}

func toFloat(x any) (float64, bool) {
	switch v := x.(type) {
	case float64:
		return v, true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

func dateCompare(lhs any, values []json.RawMessage, cmp func(time.Time, time.Time) bool) bool {
	a, ok := toTime(lhs)
	if !ok {
		return false
	}
	for _, raw := range values {
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			continue
		}
		b, bok := toTime(v)
		if bok && cmp(a, b) {
			return true
		}
	}
	return false
}

func toTime(x any) (time.Time, bool) {
	switch v := x.(type) {
	case string:
		t, err := time.Parse(time.RFC3339, v)
		if err == nil {
			return t, true
		}
		t2, err2 := time.Parse(time.RFC3339Nano, v)
		if err2 == nil {
			return t2, true
		}
	case float64:
		return time.UnixMilli(int64(v)), true
	}
	return time.Time{}, false
}

type semver struct {
	Major, Minor, Patch int
	Pre                 string
}

var semverRE = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$`)

func parseSemver(x any) (semver, bool) {
	s, ok := x.(string)
	if !ok {
		return semver{}, false
	}
	m := semverRE.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return semver{}, false
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	patch, _ := strconv.Atoi(m[3])
	return semver{Major: major, Minor: minor, Patch: patch, Pre: m[4]}, true
}

func compareSemver(a, b semver) int {
	if a.Major != b.Major {
		return a.Major - b.Major
	}
	if a.Minor != b.Minor {
		return a.Minor - b.Minor
	}
	if a.Patch != b.Patch {
		return a.Patch - b.Patch
	}
	if a.Pre == b.Pre {
		return 0
	}
	// Release > pre-release. Within pre-releases, lexicographic.
	if a.Pre == "" {
		return 1
	}
	if b.Pre == "" {
		return -1
	}
	return strings.Compare(a.Pre, b.Pre)
}

func semverCompare(lhs any, values []json.RawMessage, pred func(int) bool) bool {
	a, ok := parseSemver(lhs)
	if !ok {
		return false
	}
	for _, raw := range values {
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			continue
		}
		b, bok := parseSemver(v)
		if bok && pred(compareSemver(a, b)) {
			return true
		}
	}
	return false
}
