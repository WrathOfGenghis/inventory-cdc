package idempotency

// setIfAbsentLua performs an atomic check-and-set on a single Redis key.
// It returns 1 if the key was newly inserted (i.e. this is the first time
// the event is being applied) and 0 if the key already exists (duplicate).
//
// The script runs server-side in a single Redis command, so two consumer
// pods that briefly believe they own the same partition during a rebalance
// cannot both observe the key as absent.
//
// KEYS[1] = idempotency key (e.g. "cdc:applied:<event_id>")
// ARGV[1] = value to store (typically "1")
// ARGV[2] = TTL in seconds
const setIfAbsentLua = `
if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
return 1
`

// keyPrefix is prepended to every event_id stored in the idempotency cache.
// Keeping a stable prefix lets ops scan the namespace quickly with
// SCAN MATCH cdc:applied:* during incident triage.
const keyPrefix = "cdc:applied:"

func key(eventID string) string {
	return keyPrefix + eventID
}
