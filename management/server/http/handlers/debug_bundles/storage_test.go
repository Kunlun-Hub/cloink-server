package debug_bundles

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

const testNamespace = "0b2095f5e2f3fb3d4f29d03e6ca9f00c11b782a0f42a49e51025a6e7aa94a39e"

func TestStorageUploadDownloadAndConsumeToken(t *testing.T) {
	store := newStorage(t.TempDir())
	key, token, err := store.reserve(testNamespace)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("debug bundle")
	meta, err := store.put(key, token, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Uploaded || meta.Size != int64(len(payload)) || meta.SHA256 == "" {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
	if _, err := store.put(key, token, bytes.NewReader(payload)); err == nil {
		t.Fatal("upload token was reusable")
	}
	file, _, err := store.open(key)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got, _ := io.ReadAll(file)
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

func TestStorageRejectsTraversalAndOversize(t *testing.T) {
	store := newStorage(t.TempDir())
	if _, _, err := store.reserve("../escape"); err == nil {
		t.Fatal("accepted traversal namespace")
	}
	key, token, err := store.reserve(testNamespace)
	if err != nil {
		t.Fatal(err)
	}
	tooLarge := io.LimitReader(zeroReader{}, maxBundleSize+1)
	if _, err := store.put(key, token, tooLarge); err == nil {
		t.Fatal("accepted oversized bundle")
	}
	if _, _, err := store.open(key); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversized upload left a readable bundle: %v", err)
	}
}

func TestStorageCleanupRemovesExpiredBundle(t *testing.T) {
	now := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	store := newStorage(t.TempDir())
	store.now = func() time.Time { return now.Add(-8 * 24 * time.Hour) }
	key, token, err := store.reserve(testNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.put(key, token, bytes.NewReader([]byte("old"))); err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	if err := store.cleanup(7 * 24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.open(key); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired bundle still exists: %v", err)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
