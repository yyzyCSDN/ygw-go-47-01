package watch

import "coordination/internal/model"

// Topic routes a subscription to a delivery shard using the store hash. The
// routing value is exposed to the console so operators can verify that keys
// under watch are spread across shards.
func Topic(key model.Key, shards int) uint32 {
	return uint32(hashKey(key) % uint64(shards))
}
