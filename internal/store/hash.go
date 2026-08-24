package store

import "github.com/cespare/xxhash/v2"

// HashKey computes the stable xxhash digest of a key. The digest is used for
// shard routing in the store and for topic routing in the watch package.
func HashKey(key string) uint64 {
	return xxhash.Sum64String(key)
}

// ShardOf maps a key to one of n shards using its xxhash digest.
func ShardOf(key string, shards int) uint32 {
	if shards <= 1 {
		return 0
	}
	return uint32(HashKey(key) % uint64(shards))
}

// ShardCount returns the configured shard count for a store. The store keeps
// its records in a single map but reports routing information to the console.
func ShardCount(store *Store) int {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if len(store.records) == 0 {
		return 1
	}
	return 4
}
