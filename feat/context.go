package feat

// EvalContext is the SDK-consumer-supplied context for a flag evaluation.
// Mirrors OpenFeature's pattern: a TargetingKey shorthand for "user.key",
// and a nested map per kind matching the datafile's ContextKinds map.
//
// Example:
//
//	feat.EvalContext{
//	    TargetingKey: "user-123",
//	    Kinds: map[string]feat.ContextKindObject{
//	        "user":         {Key: "user-123", Attrs: map[string]any{"email": "u@example.com"}},
//	        "organization": {Key: "acme", Attrs: map[string]any{"plan": "pro"}},
//	    },
//	}
type EvalContext struct {
	TargetingKey string
	Kinds        map[string]ContextKindObject
}

type ContextKindObject struct {
	Key   string
	Attrs map[string]any
}

// resolveAttribute walks an attribute path like "user.email" or
// "user.address.city" into the context. Returns (value, true) on hit or
// (nil, false) on any missing segment — operators treat the miss as a
// non-match rather than an error.
func resolveAttribute(ctx EvalContext, attributePath string) (any, bool) {
	if attributePath == "" {
		return nil, false
	}
	parts := splitFirst(attributePath, '.')
	kindKey := parts[0]
	kindObj, ok := readKind(ctx, kindKey)
	if !ok {
		return nil, false
	}
	if len(parts) == 1 {
		return kindObj.Key, true
	}
	rest := parts[1]
	if rest == "" {
		return kindObj.Key, true
	}
	return walkAttrs(kindObj.Attrs, rest)
}

func walkAttrs(attrs map[string]any, path string) (any, bool) {
	parts := splitAll(path, '.')
	var cur any = attrs
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		v, present := m[p]
		if !present {
			return nil, false
		}
		cur = v
	}
	return cur, true
}

func readKind(ctx EvalContext, kindKey string) (ContextKindObject, bool) {
	if kindKey == "user" {
		if obj, ok := ctx.Kinds["user"]; ok {
			return obj, true
		}
		if ctx.TargetingKey != "" {
			return ContextKindObject{Key: ctx.TargetingKey}, true
		}
		return ContextKindObject{}, false
	}
	obj, ok := ctx.Kinds[kindKey]
	return obj, ok
}

func readContextKey(ctx EvalContext, kindKey string) (string, bool) {
	obj, ok := readKind(ctx, kindKey)
	if !ok {
		return "", false
	}
	return obj.Key, true
}

// splitFirst splits s into [head, tail] at the first occurrence of sep.
// If sep isn't present, returns a single-element slice.
func splitFirst(s string, sep byte) []string {
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			return []string{s[:i], s[i+1:]}
		}
	}
	return []string{s}
}

func splitAll(s string, sep byte) []string {
	out := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}
