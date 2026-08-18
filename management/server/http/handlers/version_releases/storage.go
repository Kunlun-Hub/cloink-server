package version_releases

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

const defaultMaxArtifactSize int64 = 1024 * 1024 * 1024

type artifactStorage struct {
	rootDir  string
	maxBytes int64
}

func newArtifactStorage(rootDir string) *artifactStorage {
	return &artifactStorage{
		rootDir:  filepath.Clean(rootDir),
		maxBytes: defaultMaxArtifactSize,
	}
}

func (s *artifactStorage) save(artifactID string, src io.Reader) (int64, string, error) {
	path, err := s.path(artifactID)
	if err != nil {
		return 0, "", err
	}
	if err := os.MkdirAll(s.rootDir, 0o700); err != nil {
		return 0, "", fmt.Errorf("create version release storage: %w", err)
	}

	temp, err := os.CreateTemp(s.rootDir, ".upload-*")
	if err != nil {
		return 0, "", fmt.Errorf("create temporary artifact: %w", err)
	}
	tempPath := temp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()

	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(temp, hash), io.LimitReader(src, s.maxBytes+1))
	if closeErr := temp.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return 0, "", fmt.Errorf("write artifact: %w", copyErr)
	}
	if written > s.maxBytes {
		return 0, "", fmt.Errorf("artifact exceeds %d byte limit", s.maxBytes)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return 0, "", fmt.Errorf("publish artifact: %w", err)
	}
	removeTemp = false
	return written, hex.EncodeToString(hash.Sum(nil)), nil
}

func (s *artifactStorage) open(artifactID string) (*os.File, error) {
	path, err := s.path(artifactID)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open artifact: %w", err)
	}
	return file, nil
}

func (s *artifactStorage) delete(artifactID string) error {
	path, err := s.path(artifactID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete artifact: %w", err)
	}
	return nil
}

func (s *artifactStorage) path(artifactID string) (string, error) {
	if _, err := uuid.Parse(artifactID); err != nil {
		return "", fmt.Errorf("invalid artifact ID")
	}
	return filepath.Join(s.rootDir, artifactID), nil
}
