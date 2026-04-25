package libhive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// LabelHivePoolKey is set on pool-managed containers so we can recover
// their pool key when releasing them back into the pool.
const LabelHivePoolKey = "hive.pool.key"

// poolResetEndpoint is where each client container exposes its reset RPC.
// We talk to the standard JSON-RPC port (HIVE_CHECK_LIVE_PORT default).
const poolResetPort = 8545

// poolResetTimeout caps how long a single debug_setHead RPC can run.
// In practice the call is sub-millisecond inside Erigon (one-block
// staged-sync unwind); this is a generous safety net.
const poolResetTimeout = 5 * time.Second

// PoolEntry is the per-container record we stash in the pool. We need
// the IP because the reset RPC has to address the running daemon by IP
// (the container is on a Docker bridge network, not on hive's host).
type PoolEntry struct {
	ID string
	IP string
}

// ClientPool retains running client containers across tests. After a
// test ends, instead of stopping/removing the container, we send a
// JSON-RPC `debug_setHead(0)` to revert the chain to genesis and park
// the entry under its (image, env, genesis) key. The next test that
// matches the key reuses the already-running daemon — no docker
// create/start, no `erigon init`, no daemon boot.
//
// The pool is opt-in via --client.pool.size. When size <= 0, all pool
// methods are no-ops and Acquire returns nothing.
type ClientPool struct {
	backend   ContainerBackend
	maxPerKey int
	// reset performs the chain-state reset on a parked container. The
	// default implementation sends debug_setHead(0) to ip:8545; tests
	// override it to avoid needing a live HTTP endpoint.
	reset     func(ip string) error
	mu        sync.Mutex
	idle      map[string][]PoolEntry // pool key -> entries (LIFO)
	knownByID map[string]string      // container ID -> pool key
	closed    bool

	// Counters for end-of-run summary.
	hits        uint64
	misses      uint64
	released    uint64
	rejected    uint64 // Releases dropped (bucket full or reset failed)
	resetFailed uint64
}

// NewClientPool returns a pool that holds at most maxPerKey idle
// containers per (image, env, genesis) bucket. maxPerKey <= 0 disables
// the pool entirely; in that mode every method is a cheap no-op.
func NewClientPool(backend ContainerBackend, maxPerKey int) *ClientPool {
	p := &ClientPool{
		backend:   backend,
		maxPerKey: maxPerKey,
		idle:      make(map[string][]PoolEntry),
		knownByID: make(map[string]string),
	}
	p.reset = p.defaultReset
	return p
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

// Acquire returns a pooled entry for key, or nil if the bucket is
// empty or the pool is disabled. The caller can use the entry's
// container ID and IP directly; no docker start is needed, the daemon
// is already running and was reset to genesis on Release.
func (p *ClientPool) Acquire(key string) *PoolEntry {
	if !p.Enabled() {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	bucket := p.idle[key]
	if len(bucket) == 0 {
		p.misses++
		return nil
	}
	// LIFO: hottest entry first (most recently reset, warmest caches).
	entry := bucket[len(bucket)-1]
	p.idle[key] = bucket[:len(bucket)-1]
	delete(p.knownByID, entry.ID)
	p.hits++
	return &entry
}

// Release sends a debug_setHead(0) RPC to the running daemon to reset
// the chain to genesis, then parks the entry in the bucket for key.
// On RPC failure or full bucket the caller is told to fall back to
// delete by a `false` return.
func (p *ClientPool) Release(entry PoolEntry, key string) bool {
	if !p.Enabled() || entry.ID == "" || entry.IP == "" || key == "" {
		return false
	}

	// Bucket fullness check is racy with the reset RPC, but the worst
	// case is wasted reset work — never an inconsistent pool state.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false
	}
	if len(p.idle[key]) >= p.maxPerKey {
		p.rejected++
		p.mu.Unlock()
		return false
	}
	p.mu.Unlock()

	// Reset chain state outside the lock — this is an HTTP round trip
	// (~ms in practice) and we don't want to block other Acquire/Release
	// callers on it.
	if err := p.reset(entry.IP); err != nil {
		slog.Warn("pool: reset failed, not retaining",
			"container", shortID(entry.ID), "ip", entry.IP, "err", err)
		p.mu.Lock()
		p.resetFailed++
		p.rejected++
		p.mu.Unlock()
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		// Race: pool was drained while we were resetting. Caller deletes.
		return false
	}
	if len(p.idle[key]) >= p.maxPerKey {
		// Race: bucket filled while we were resetting.
		p.rejected++
		return false
	}
	p.idle[key] = append(p.idle[key], entry)
	p.knownByID[entry.ID] = key
	p.released++
	return true
}

// defaultReset calls debug_setHead(0) on the client's standard JSON-RPC
// endpoint. Erigon's debug_setHead is a thin wrapper over the staged-sync
// unwind path: for a 1-block rewind to genesis it completes in the order
// of microseconds inside the daemon, plus HTTP overhead.
func (p *ClientPool) defaultReset(ip string) error {
	ctx, cancel := context.WithTimeout(context.Background(), poolResetTimeout)
	defer cancel()

	url := fmt.Sprintf("http://%s/", net.JoinHostPort(ip, fmt.Sprintf("%d", poolResetPort)))
	body := strings.NewReader(`{"jsonrpc":"2.0","method":"debug_setHead","params":["0x0"],"id":1}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(buf))
	}

	// Parse the JSON-RPC envelope and check for an `error` field. Erigon
	// returns 200 OK even on RPC errors (matches geth convention).
	var rpcResp struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&rpcResp); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if rpcResp.Error != nil {
		return fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	return nil
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

// Drain removes every parked container in the pool. It is safe to call
// at hive shutdown.
func (p *ClientPool) Drain(ctx context.Context) {
	if !p.Enabled() {
		return
	}
	p.mu.Lock()
	p.closed = true
	all := p.idle
	hits, misses, released, rejected, resetFailed := p.hits, p.misses, p.released, p.rejected, p.resetFailed
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
		"reset_failed", resetFailed,
	)

	for key, entries := range all {
		for _, e := range entries {
			if err := p.backend.DeleteContainer(e.ID); err != nil {
				slog.Warn("pool drain: delete failed", "container", shortID(e.ID), "key", shortKey(key), "err", err)
			}
		}
	}
}

// shortID truncates a container ID to its first 8 characters for logs.
// Tolerates short test IDs (real Docker IDs are 64 hex chars).
func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
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
