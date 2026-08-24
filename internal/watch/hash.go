package watch

import "github.com/cespare/xxhash/v2"

func hashKey(key string) uint64 {
	return xxhash.Sum64String(key)
}
