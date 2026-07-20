/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package bloom

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"time"
)

// BloomFilter is a space-efficient probabilistic data structure for set membership testing.
// It may return false positives but never false negatives.
type BloomFilter struct {
	mu      sync.RWMutex
	bits    []byte
	numBits uint64
	numHash uint32
}

// NewBloomFilter creates a Bloom filter optimized for the expected number of elements
// and desired false positive rate.
func NewBloomFilter(expectedElements uint64, falsePositiveRate float64) *BloomFilter {
	// MESH-F12: clamp inputs. expectedElements==0 makes numBits==0, and the
	// first Add then panics on `hash % 0`; an out-of-range false-positive rate
	// makes math.Log misbehave. DefaultBloomConfig is safe, but a caller-supplied
	// BloomConfig{ExpectedElements:0} would crash on the first ledger Append.
	if expectedElements == 0 {
		expectedElements = 1
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}
	// Calculate optimal number of bits
	// m = -(n * ln(p)) / (ln(2)^2)
	numBits := uint64(math.Ceil(-float64(expectedElements) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2))))
	if numBits == 0 {
		numBits = 1
	}

	// Calculate optimal number of hash functions
	// k = (m/n) * ln(2)
	numHash := uint32(math.Ceil((float64(numBits) / float64(expectedElements)) * math.Log(2)))

	// Ensure at least 1 hash function
	if numHash == 0 {
		numHash = 1
	}

	numBytes := (numBits + 7) / 8

	return &BloomFilter{
		bits:    make([]byte, numBytes),
		numBits: numBits,
		numHash: numHash,
	}
}

// Add inserts an element into the Bloom filter
func (bf *BloomFilter) Add(data []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := uint32(0); i < bf.numHash; i++ {
		hash := bf.hash(data, i)
		idx := hash % bf.numBits
		bf.bits[idx/8] |= 1 << (idx % 8)
	}
}

// Contains checks if an element might be in the set
// Returns true if the element might be present (with false positive rate)
// Returns false if the element is definitely not present
func (bf *BloomFilter) Contains(data []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for i := uint32(0); i < bf.numHash; i++ {
		hash := bf.hash(data, i)
		idx := hash % bf.numBits
		if bf.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

// Clear resets the Bloom filter
func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := range bf.bits {
		bf.bits[i] = 0
	}
}

// Serialize returns the filter state as bytes for storage
func (bf *BloomFilter) Serialize() []byte {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	// Format: [numBits:8][numHash:4][bits:...]
	result := make([]byte, 12+len(bf.bits))
	binary.LittleEndian.PutUint64(result[0:8], bf.numBits)
	binary.LittleEndian.PutUint32(result[8:12], bf.numHash)
	copy(result[12:], bf.bits)

	return result
}

// Deserialize loads filter state from bytes
func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("bloom: invalid serialized data, too short")
	}

	numBits := binary.LittleEndian.Uint64(data[0:8])
	numHash := binary.LittleEndian.Uint32(data[8:12])
	bits := make([]byte, len(data)-12)
	copy(bits, data[12:])

	// MESH-F02: validate numBits against the actual bit-slice length. A corrupt
	// row with numBits==0 makes Add panic on `hash % 0`; numBits > len(bits)*8
	// makes Add index bits[idx/8] out of range. Either crashes the ledger on the
	// next updateBloomFilter→Add. The old code only checked len(data) < 12.
	if numBits == 0 || numBits > uint64(len(bits))*8 {
		return nil, fmt.Errorf("bloom: invalid numBits=%d for %d-byte bit slice", numBits, len(bits))
	}
	if numHash == 0 {
		numHash = 1
	}

	return &BloomFilter{
		bits:    bits,
		numBits: numBits,
		numHash: numHash,
	}, nil
}

// hash generates the i-th hash value for the given data
func (bf *BloomFilter) hash(data []byte, i uint32) uint64 {
	h := fnv.New64a()

	// Write data
	h.Write(data)

	// Add salt based on hash function index
	salt := make([]byte, 4)
	binary.LittleEndian.PutUint32(salt, i)
	h.Write(salt)

	return h.Sum64()
}

// EstimatedFillRatio returns the approximate proportion of bits set in the filter
func (bf *BloomFilter) EstimatedFillRatio() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	setBits := 0
	for _, b := range bf.bits {
		for i := 0; i < 8; i++ {
			if b&(1<<uint(i)) != 0 {
				setBits++
			}
		}
	}

	return float64(setBits) / float64(bf.numBits)
}

// BloomFilterManager manages Bloom filters for different scopes (topics/tenants)
type BloomFilterManager struct {
	mu      sync.RWMutex
	filters map[string]*BloomFilter
	config  BloomConfig
}

// BloomConfig holds Bloom filter configuration
type BloomConfig struct {
	ExpectedElements  uint64  // Expected elements per scope
	FalsePositiveRate float64 // Desired false positive rate (e.g., 0.01 for 1%)
	RebuildInterval   time.Duration
}

// DefaultBloomConfig returns sensible defaults
func DefaultBloomConfig() BloomConfig {
	return BloomConfig{
		ExpectedElements:  10000,              // 10k elements per topic/tenant
		FalsePositiveRate: 0.01,               // 1% false positive rate
		RebuildInterval:   7 * 24 * time.Hour, // Rebuild weekly
	}
}

// NewBloomFilterManager creates a new manager
func NewBloomFilterManager(cfg BloomConfig) *BloomFilterManager {
	return &BloomFilterManager{
		filters: make(map[string]*BloomFilter),
		config:  cfg,
	}
}

// GetOrCreate returns an existing filter or creates a new one for the scope
func (bfm *BloomFilterManager) GetOrCreate(scope string) *BloomFilter {
	bfm.mu.RLock()
	if filter, ok := bfm.filters[scope]; ok {
		bfm.mu.RUnlock()
		return filter
	}
	bfm.mu.RUnlock()

	bfm.mu.Lock()
	defer bfm.mu.Unlock()

	// Double-check after acquiring write lock
	if filter, ok := bfm.filters[scope]; ok {
		return filter
	}

	filter := NewBloomFilter(bfm.config.ExpectedElements, bfm.config.FalsePositiveRate)
	bfm.filters[scope] = filter
	return filter
}

// Get retrieves a filter for the scope, or nil if not exists
func (bfm *BloomFilterManager) Get(scope string) *BloomFilter {
	bfm.mu.RLock()
	defer bfm.mu.RUnlock()
	return bfm.filters[scope]
}

// Clear removes all filters
func (bfm *BloomFilterManager) Clear() {
	bfm.mu.Lock()
	defer bfm.mu.Unlock()
	bfm.filters = make(map[string]*BloomFilter)
}

// Set sets a filter for a specific scope (used for loading persisted filters)
func (bfm *BloomFilterManager) Set(scope string, filter *BloomFilter) {
	bfm.mu.Lock()
	defer bfm.mu.Unlock()
	bfm.filters[scope] = filter
}

// Filters returns a snapshot of all scope->filter mappings for persistence
func (bfm *BloomFilterManager) Filters() map[string]*BloomFilter {
	bfm.mu.RLock()
	defer bfm.mu.RUnlock()

	// Return a copy to avoid concurrent map access issues
	copy := make(map[string]*BloomFilter, len(bfm.filters))
	for k, v := range bfm.filters {
		copy[k] = v
	}
	return copy
}
