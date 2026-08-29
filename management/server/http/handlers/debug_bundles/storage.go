package debug_bundles

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/google/uuid"
)

const maxBundleSize = 50 * 1024 * 1024

var keyPartPattern = regexp.MustCompile(`^[a-f0-9-]{16,128}$`)

type metadata struct {
	Key       string    `json:"key"`
	TokenHash string    `json:"tokenHash"`
	CreatedAt time.Time `json:"createdAt"`
	Uploaded  bool      `json:"uploaded"`
	Size      int64     `json:"size,omitempty"`
	SHA256    string    `json:"sha256,omitempty"`
}

type storage struct {
	root string
	now  func() time.Time
}

func newStorage(root string) *storage {
	return &storage{root: root, now: time.Now}
}

func (s *storage) reserve(namespace string) (key, token string, err error) {
	if !keyPartPattern.MatchString(namespace) {
		return "", "", fmt.Errorf("invalid namespace")
	}
	if err := os.MkdirAll(filepath.Join(s.root, namespace), 0o700); err != nil {
		return "", "", err
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", "", err
	}
	token = base64.RawURLEncoding.EncodeToString(tokenBytes)
	key = namespace + "/" + uuid.NewString()
	meta := metadata{
		Key:       key,
		TokenHash: tokenDigest(token),
		CreatedAt: s.now().UTC(),
	}
	if err := s.writeMetadata(meta); err != nil {
		return "", "", err
	}
	return key, token, nil
}

func (s *storage) put(key, token string, body io.Reader) (metadata, error) {
	meta, err := s.readMetadata(key)
	if err != nil {
		return metadata{}, err
	}
	if meta.Uploaded || tokenDigest(token) != meta.TokenHash {
		return metadata{}, errors.New("invalid or consumed upload token")
	}
	dir, _, err := s.paths(key)
	if err != nil {
		return metadata{}, err
	}
	temp, err := os.CreateTemp(dir, ".bundle-*")
	if err != nil {
		return metadata{}, err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return metadata{}, err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(temp, hash), io.LimitReader(body, maxBundleSize+1))
	closeErr := temp.Close()
	if copyErr != nil {
		return metadata{}, copyErr
	}
	if closeErr != nil {
		return metadata{}, closeErr
	}
	if written > maxBundleSize {
		return metadata{}, fmt.Errorf("debug bundle exceeds %d bytes", maxBundleSize)
	}
	_, bundlePath, err := s.paths(key)
	if err != nil {
		return metadata{}, err
	}
	if err := os.Rename(tempName, bundlePath); err != nil {
		return metadata{}, err
	}
	meta.Uploaded = true
	meta.Size = written
	meta.SHA256 = hex.EncodeToString(hash.Sum(nil))
	meta.TokenHash = ""
	if err := s.writeMetadata(meta); err != nil {
		os.Remove(bundlePath)
		return metadata{}, err
	}
	return meta, nil
}

func (s *storage) open(key string) (*os.File, metadata, error) {
	meta, err := s.readMetadata(key)
	if err != nil || !meta.Uploaded {
		return nil, metadata{}, os.ErrNotExist
	}
	_, bundlePath, err := s.paths(key)
	if err != nil {
		return nil, metadata{}, err
	}
	file, err := os.Open(bundlePath)
	return file, meta, err
}

func (s *storage) delete(key string) error {
	dir, bundlePath, err := s.paths(key)
	if err != nil {
		return err
	}
	_, metaPath, _ := s.metadataPaths(key)
	if err := os.Remove(bundlePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(metaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	_ = os.Remove(dir)
	return nil
}

func (s *storage) cleanup(olderThan time.Duration) error {
	entries, err := os.ReadDir(s.root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	cutoff := s.now().Add(-olderThan)
	for _, namespace := range entries {
		if !namespace.IsDir() || !keyPartPattern.MatchString(namespace.Name()) {
			continue
		}
		files, _ := os.ReadDir(filepath.Join(s.root, namespace.Name()))
		for _, file := range files {
			if filepath.Ext(file.Name()) != ".json" {
				continue
			}
			data, readErr := os.ReadFile(filepath.Join(s.root, namespace.Name(), file.Name()))
			var meta metadata
			if readErr == nil && json.Unmarshal(data, &meta) == nil && meta.CreatedAt.Before(cutoff) {
				_ = s.delete(meta.Key)
			}
		}
	}
	return nil
}

func (s *storage) paths(key string) (string, string, error) {
	parts, err := validateKey(key)
	if err != nil {
		return "", "", err
	}
	dir := filepath.Join(s.root, parts[0])
	return dir, filepath.Join(dir, parts[1]+".zip"), nil
}

func (s *storage) metadataPaths(key string) (string, string, error) {
	parts, err := validateKey(key)
	if err != nil {
		return "", "", err
	}
	dir := filepath.Join(s.root, parts[0])
	return dir, filepath.Join(dir, parts[1]+".json"), nil
}

func (s *storage) readMetadata(key string) (metadata, error) {
	_, path, err := s.metadataPaths(key)
	if err != nil {
		return metadata{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return metadata{}, err
	}
	var meta metadata
	if err := json.Unmarshal(data, &meta); err != nil || meta.Key != key {
		return metadata{}, os.ErrNotExist
	}
	return meta, nil
}

func (s *storage) writeMetadata(meta metadata) error {
	dir, path, err := s.metadataPaths(meta.Key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, ".meta-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func validateKey(key string) ([]string, error) {
	parts := regexp.MustCompile(`/`).Split(key, -1)
	if len(parts) != 2 || !keyPartPattern.MatchString(parts[0]) || !keyPartPattern.MatchString(parts[1]) {
		return nil, fmt.Errorf("invalid debug bundle key")
	}
	return parts, nil
}

func tokenDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
