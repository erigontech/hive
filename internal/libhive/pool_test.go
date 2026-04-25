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

func TestPoolDisabled(t *testing.T) {
	be := &recordingBackend{}
	p := NewClientPool(be, 0)
	if p.Enabled() {
		t.Fatal("pool with size 0 should be disabled")
	}
	if got := p.Acquire("k"); got != "" {
		t.Fatalf("disabled pool should return empty on Acquire, got %q", got)
	}
	if p.Release("c", "k") {
		t.Fatal("disabled pool should not retain on Release")
	}
}

func TestPoolAcquireRelease(t *testing.T) {
	be := &recordingBackend{}
	p := NewClientPool(be, 2)

	if got := p.Acquire("k"); got != "" {
		t.Fatalf("empty bucket should return empty, got %q", got)
	}

	if !p.Release("c1", "k") {
		t.Fatal("Release into empty bucket should succeed")
	}
	if !p.Release("c2", "k") {
		t.Fatal("Release up to capacity should succeed")
	}
	if p.Release("c3", "k") {
		t.Fatal("Release past capacity should fail")
	}

	// LIFO: hottest first.
	if got := p.Acquire("k"); got != "c2" {
		t.Fatalf("Acquire should return most-recent, got %q", got)
	}
	if got := p.Acquire("k"); got != "c1" {
		t.Fatalf("Acquire should return next, got %q", got)
	}
	if got := p.Acquire("k"); got != "" {
		t.Fatalf("Acquire on drained bucket should return empty, got %q", got)
	}

	// c3 was rejected on Release; backend should not have stopped it.
	if len(be.stops) != 2 {
		t.Fatalf("expected 2 stops (c1,c2), got %v", be.stops)
	}
}

func TestPoolDrain(t *testing.T) {
	be := &recordingBackend{}
	p := NewClientPool(be, 4)

	p.Release("c1", "k1")
	p.Release("c2", "k2")
	p.Release("c3", "k1")

	p.Drain(context.Background())

	if len(be.deletes) != 3 {
		t.Fatalf("Drain should delete all retained, got %d", len(be.deletes))
	}
	// After drain, Acquire is a no-op.
	if got := p.Acquire("k1"); got != "" {
		t.Fatalf("Acquire after Drain should return empty, got %q", got)
	}
	if p.Release("c4", "k1") {
		t.Fatal("Release after Drain should fail")
	}
}

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
