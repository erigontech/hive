package libhive

import (
	"bytes"
	"context"
	"mime/multipart"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// recordingBackend is the minimum ContainerBackend surface area used by the
// pool. Methods that the pool doesn't touch are fine to leave as no-ops.
type recordingBackend struct {
	mu      sync.Mutex
	stops   []string
	deletes []string
	stopErr error
}

func (b *recordingBackend) StopContainer(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopErr != nil {
		return b.stopErr
	}
	b.stops = append(b.stops, id)
	return nil
}

func (b *recordingBackend) DeleteContainer(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deletes = append(b.deletes, id)
	return nil
}

func (b *recordingBackend) Build(context.Context, Builder) error                       { return nil }
func (b *recordingBackend) SetHiveInstanceInfo(string, string)                         {}
func (b *recordingBackend) GetDockerClient() interface{}                               { return nil }
func (b *recordingBackend) ServeAPI(context.Context, http.Handler) (APIServer, error)  { return nil, nil }
func (b *recordingBackend) CreateContainer(context.Context, string, ContainerOptions) (string, error) {
	return "", nil
}
func (b *recordingBackend) StartContainer(context.Context, string, ContainerOptions) (*ContainerInfo, error) {
	return nil, nil
}
func (b *recordingBackend) PauseContainer(string) error                            { return nil }
func (b *recordingBackend) UnpauseContainer(string) error                          { return nil }
func (b *recordingBackend) RunProgram(context.Context, string, []string) (*ExecInfo, error) {
	return nil, nil
}
func (b *recordingBackend) NetworkNameToID(string) (string, error)             { return "", nil }
func (b *recordingBackend) CreateNetwork(string) (string, error)               { return "", nil }
func (b *recordingBackend) RemoveNetwork(string) error                         { return nil }
func (b *recordingBackend) ContainerIP(string, string) (net.IP, error)         { return nil, nil }
func (b *recordingBackend) ConnectContainer(string, string) error              { return nil }
func (b *recordingBackend) DisconnectContainer(string, string) error           { return nil }

// poolWithStubReset returns a pool whose reset function records calls
// instead of doing HTTP, so unit tests don't need a live RPC endpoint.
func poolWithStubReset(t *testing.T, maxPerKey int, resetErr error) (*ClientPool, *recordingBackend, *[]string) {
	t.Helper()
	be := &recordingBackend{}
	p := NewClientPool(be, maxPerKey)
	var resetIPs []string
	p.reset = func(ip string) error {
		resetIPs = append(resetIPs, ip)
		return resetErr
	}
	return p, be, &resetIPs
}

func TestPoolDisabled(t *testing.T) {
	be := &recordingBackend{}
	p := NewClientPool(be, 0)
	if p.Enabled() {
		t.Fatal("pool with size 0 should be disabled")
	}
	if got := p.Acquire("k"); got != nil {
		t.Fatalf("disabled pool should return nil on Acquire, got %+v", got)
	}
	if p.Release(PoolEntry{ID: "c", IP: "1.2.3.4"}, "k") {
		t.Fatal("disabled pool should not retain on Release")
	}
}

func TestPoolAcquireRelease(t *testing.T) {
	p, _, resetIPs := poolWithStubReset(t, 2, nil)

	if got := p.Acquire("k"); got != nil {
		t.Fatalf("empty bucket should return nil, got %+v", got)
	}

	e1 := PoolEntry{ID: "c1", IP: "10.0.0.1"}
	e2 := PoolEntry{ID: "c2", IP: "10.0.0.2"}
	e3 := PoolEntry{ID: "c3", IP: "10.0.0.3"}
	if !p.Release(e1, "k") {
		t.Fatal("Release into empty bucket should succeed")
	}
	if !p.Release(e2, "k") {
		t.Fatal("Release up to capacity should succeed")
	}
	if p.Release(e3, "k") {
		t.Fatal("Release past capacity should fail")
	}

	// LIFO: hottest first.
	if got := p.Acquire("k"); got == nil || got.ID != "c2" {
		t.Fatalf("Acquire should return most-recent (c2), got %+v", got)
	}
	if got := p.Acquire("k"); got == nil || got.ID != "c1" {
		t.Fatalf("Acquire should return c1 next, got %+v", got)
	}
	if got := p.Acquire("k"); got != nil {
		t.Fatalf("Acquire on drained bucket should return nil, got %+v", got)
	}

	// c1 and c2 were Released successfully; c3 was over capacity and
	// rejected before reset was attempted.
	if len(*resetIPs) != 2 {
		t.Fatalf("expected 2 reset calls (c1, c2), got %v", *resetIPs)
	}
}

func TestPoolReleaseFailsOnResetError(t *testing.T) {
	p, _, _ := poolWithStubReset(t, 4, errStubResetFailed)
	if p.Release(PoolEntry{ID: "c1", IP: "10.0.0.1"}, "k") {
		t.Fatal("Release should fail when reset returns an error")
	}
	if got := p.Acquire("k"); got != nil {
		t.Fatalf("nothing should have been parked; got %+v", got)
	}
}

func TestPoolDrain(t *testing.T) {
	p, be, _ := poolWithStubReset(t, 4, nil)

	p.Release(PoolEntry{ID: "c1", IP: "10.0.0.1"}, "k1")
	p.Release(PoolEntry{ID: "c2", IP: "10.0.0.2"}, "k2")
	p.Release(PoolEntry{ID: "c3", IP: "10.0.0.3"}, "k1")

	p.Drain(context.Background())

	if len(be.deletes) != 3 {
		t.Fatalf("Drain should delete all retained, got %d", len(be.deletes))
	}
	// After drain, Acquire is a no-op.
	if got := p.Acquire("k1"); got != nil {
		t.Fatalf("Acquire after Drain should return nil, got %+v", got)
	}
	if p.Release(PoolEntry{ID: "c4", IP: "10.0.0.4"}, "k1") {
		t.Fatal("Release after Drain should fail")
	}
}

var errStubResetFailed = errStubReset{}

type errStubReset struct{}

func (errStubReset) Error() string { return "stub reset failed" }

func TestComputePoolKeyDeterministic(t *testing.T) {
	env1 := map[string]string{"HIVE_CHAIN_ID": "1", "HIVE_FORK_HOMESTEAD": "0"}
	env2 := map[string]string{"HIVE_FORK_HOMESTEAD": "0", "HIVE_CHAIN_ID": "1"} // same set, diff iteration

	files1 := buildMultipartFiles(t, map[string]string{"/genesis.json": `{"alloc":{}}`})
	files2 := buildMultipartFiles(t, map[string]string{"/genesis.json": `{"alloc":{}}`})

	k1, err := ComputePoolKey("img:tag", env1, files1)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := ComputePoolKey("img:tag", env2, files2)
	if err != nil {
		t.Fatal(err)
	}
	if k1 != k2 {
		t.Fatalf("key should not depend on env iteration order: %q vs %q", k1, k2)
	}

	// Different genesis -> different key.
	otherFiles := buildMultipartFiles(t, map[string]string{
		"/genesis.json": `{"alloc":{"0x00":{"balance":"0x1"}}}`,
	})
	k3, err := ComputePoolKey("img:tag", env1, otherFiles)
	if err != nil {
		t.Fatal(err)
	}
	if k1 == k3 {
		t.Fatal("key should change with genesis bytes")
	}

	// Different image -> different key.
	k4, err := ComputePoolKey("img:other", env1, buildMultipartFiles(t, map[string]string{"/genesis.json": `{"alloc":{}}`}))
	if err != nil {
		t.Fatal(err)
	}
	if k1 == k4 {
		t.Fatal("key should change with image")
	}
}

func TestComputePoolKeyEmpty(t *testing.T) {
	k, err := ComputePoolKey("img", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if k == "" {
		t.Fatal("key should be non-empty even with no env/files")
	}
}

// buildMultipartFiles constructs real *multipart.FileHeader values by writing
// to a multipart.Writer and parsing it back. This mirrors what
// http.Request.ParseMultipartForm produces in api.startClient.
func buildMultipartFiles(t *testing.T, contents map[string]string) map[string]*multipart.FileHeader {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for name, content := range contents {
		fw, err := mw.CreateFormFile(name, name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}

	mr := multipart.NewReader(strings.NewReader(body.String()), mw.Boundary())
	form, err := mr.ReadForm(int64(body.Len()) + 1024)
	if err != nil {
		t.Fatal(err)
	}

	out := make(map[string]*multipart.FileHeader, len(contents))
	for k, v := range form.File {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}
