package feat

import (
	"crypto/sha1" //nolint:gosec // non-cryptographic use; matches JS SDK's hash
	"math/big"
)

// bucketScale matches the JS engine's 100000. Weights sum to this exactly.
const bucketScale = 100_000

// bucket returns a deterministic integer in [0, bucketScale) for the
// (flagSalt, flagKey, contextKey) triple. Same algorithm as the JS engine:
//
//	sha1(salt + "." + flagKey + "." + contextKey)
//	→ first 8 bytes as a 64-bit big-endian unsigned integer
//	→ shift right 4 (drop low bits) for exactly 60 bits
//	→ modulo bucketScale
//
// Ports of this engine MUST match this byte-for-byte so a user bucketed
// into variation A by the JS SDK is also bucketed into A by the Go SDK.
func bucket(salt, flagKey, contextKey string) int {
	input := salt + "." + flagKey + "." + contextKey
	digest := sha1.Sum([]byte(input)) //nolint:gosec
	var first8 uint64
	for i := 0; i < 8; i++ {
		first8 = (first8 << 8) | uint64(digest[i])
	}
	sixty := new(big.Int).SetUint64(first8 >> 4)
	mod := sixty.Mod(sixty, big.NewInt(int64(bucketScale)))
	return int(mod.Int64())
}

// pickByWeight walks cumulative weights and returns the variation whose
// cumulative range contains bucketValue. Falls back to the last variation
// if upstream weights underflow the scale (defensive, shouldn't happen
// since the producer enforces sum == 100000).
func pickByWeight(bucketValue int, variations []RolloutVariation) (string, bool) {
	cumulative := 0
	for _, v := range variations {
		cumulative += v.Weight
		if bucketValue < cumulative {
			return v.VariationID, true
		}
	}
	if len(variations) > 0 {
		return variations[len(variations)-1].VariationID, true
	}
	return "", false
}
