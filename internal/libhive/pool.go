package libhive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"sort"
	"strings"
	"sync"
)

// LabelHivePoolKey is set on pool-managed containers so we can recover
// their pool key when releasing them back into the pool.
const LabelHivePoolKey = "hive.pool.key"

// ClientPool keeps a small set of stopped client containers per
// (image, env, genesis) bucket so they can be restarted across tests
// instead of created from scratch every time.
//
// The pool is opt-in via --client.pool.size. When size <= 0, all pool
// methods are no-ops and Acquire always returns "".
type ClientPool struct {
	backend     ContainerBackend
	maxPerKey   int
	mu          sync.Mutex
	idle        map[string][]string // pool key -> container IDs (LIFO)
	knownByID   map[string]string   // container ID -> pool key (so cleanup knows what's pooled)
	closed      bool

	// Counters for end-of-run summary.
	hits      uint64
	misses    uint64
	released  uint64
	rejected  uint64 // Releases dropped (bucket full or stop failed)
}

// NewClientPool returns a pool that holds at most maxPerKey idle containers
// for any single (image, env, genesis) bucket. maxPerKey <= 0 disables the
// pool entirely; in that mode every method is a cheap no-op.
func NewClientPool(backend ContainerBackend, maxPerKey int) *ClientPool {
	return &ClientPool{
		backend:   backend,
		maxPerKey: maxPerKey,
		idle:      make(map[string][]string),
		knownByID: make(map[string]string),
	}
}

// Enabled reports whether pooling is active.
func (p *ClientPool) Enabled() bool {
	return p != nil && p.maxPerKey > 0
}

// ComputePoolKey produces a stable hash over the inputs that determine
// "this container is interchangeable with another for the next test":
// the image name, the sanitized HIVE_* environment, and the contents
// of files passed in (notably /genesis.json).
//
// Any change to the hash yields a different pool bucket, which means
// the caller will get a fresh container instead of a state-mismatched one.
func ComputePoolKey(image string, env map[string]string, files map[string]*multipart.FileHeader) (string, error) {
	h := sha256.New()
	io.WriteString(h, "image\x00")
	io.WriteString(h, image)
	io.WriteString(h, "\x00env\x00")

	// Sort env keys for determinism.
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		io.WriteString(h, k)
		io.WriteString(h, "=")
		io.WriteString(h, env[k])
		io.WriteString(h, "\x00")
	}

	io.WriteString(h, "files\x00")
	// Sort file paths so the order in which the multipart form was
	// iterated doesn't perturb the hash.
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		fh := files[p]
		io.WriteString(h, p)
		io.WriteString(h, "\x00")
		f, err := fh.Open()
		if err != nil {
			return "", fmt.Errorf("pool key: open %s: %w", p, err)
		}
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			return "", fmt.Errorf("pool key: read %s: %w", p, err)
		}
		f.Close()
		io.WriteString(h, "\x00")
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// Acquire returns a stopped container ID from the pool that matches key,
// or "" if the pool is empty or disabled. The caller must subsequently
// call backend.StartContainer to bring it back online.
func (p *ClientPool) Acquire(key string) string {
	if !p.Enabled() {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ""
	}
	bucket := p.idle[key]
	if len(bucket) == 0 {
		p.misses++
		return ""
	}
	// LIFO: hottest container first (least likely to have been swapped out).
	id := bucket[len(bucket)-1]
	p.idle[key] = bucket[:len(bucket)-1]
	delete(p.knownByID, id)
	p.hits++
	return id
}

// Release stops the container and returns it to the bucket for key.
// If the bucket is full (or pooling is disabled), the container is
// deleted instead. Returns true when the container was retained.
func (p *ClientPool) Release(containerID, key string) bool {
	if !p.Enabled() || containerID == "" || key == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	if len(p.idle[key]) >= p.maxPerKey {
		p.rejected++
		return false
	}
	if err := p.backend.StopContainer(containerID); err != nil {
		// Best-effort: if the stop fails the caller will fall back to delete.
		slog.Warn("pool: stop failed, not retaining", "container", containerID[:8], "err", err)
		p.rejected++
		return false
	}
	p.idle[key] = append(p.idle[key], containerID)
	p.knownByID[containerID] = key
	p.released++
	return true
}

// PoolKeyOf returns the pool key associated with containerID if it is
// currently idle in the pool (used for diagnostics).
func (p *ClientPool) PoolKeyOf(containerID string) string {
	if !p.Enabled() {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.knownByID[containerID]
}

// Drain removes every idle container in the pool. It is safe to call
// at hive shutdown.
func (p *ClientPool) Drain(ctx context.Context) {
	if !p.Enabled() {
		return
	}
	p.mu.Lock()
	p.closed = true
	all := p.idle
	hits, misses, released, rejected := p.hits, p.misses, p.released, p.rejected
	p.idle = nil
	p.knownByID = nil
	p.mu.Unlock()

	totalAcquires := hits + misses
	hitRate := 0.0
	if totalAcquires > 0 {
		hitRate = 100 * float64(hits) / float64(totalAcquires)
	}
	slog.Info("pool: summary",
		"hits", hits,
		"misses", misses,
		"hit_rate_pct", fmt.Sprintf("%.1f", hitRate),
		"released", released,
		"rejected", rejected,
	)

	for key, ids := range all {
		for _, id := range ids {
			if err := p.backend.DeleteContainer(id); err != nil {
				slog.Warn("pool drain: delete failed", "container", id[:8], "key", shortKey(key), "err", err)
			}
		}
	}
}

func shortKey(k string) string {
	if len(k) > 12 {
		return k[:12]
	}
	return k
}

// EnvFingerprint returns a short, human-readable summary of an env map,
// useful for logging.
func EnvFingerprint(env map[string]string) string {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(env[k])
	}
	return b.String()
}
