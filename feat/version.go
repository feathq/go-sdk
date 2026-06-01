package feat

// Version is the SDK's reported version, embedded in the User-Agent
// header sent on every data-plane request. Bumped together with the git
// tag on every release; Go modules don't read this for resolution but
// keeping it in sync makes the wire identity match the consumer's
// require directive.
const Version = "0.1.1"
