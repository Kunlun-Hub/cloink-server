package version_releases

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	clientversion "github.com/netbirdio/netbird/version"
)

type releaseSigner func(clientversion.PublicRelease) (string, error)

var errReleaseSignerNotConfigured = errors.New("release signing key is not configured")

func loadReleaseSigner(path string) (releaseSigner, error) {
	if path == "" {
		return nil, errReleaseSignerNotConfigured
	}
	keyPEM, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", EnvVersionReleaseSigningKeyFile, err)
	}
	signer, err := newReleaseSigner(keyPEM)
	if err != nil {
		return nil, err
	}
	if err := validateReleaseSigner(signer); err != nil {
		return nil, err
	}
	return signer, nil
}

func newReleaseSigner(keyPEM []byte) (releaseSigner, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, fmt.Errorf("decode Ed25519 private key PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse Ed25519 private key: %w", err)
	}
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("configured release signing key is not Ed25519")
	}
	return func(release clientversion.PublicRelease) (string, error) {
		signature := ed25519.Sign(privateKey, clientversion.ReleaseSignaturePayload(release))
		return base64.StdEncoding.EncodeToString(signature), nil
	}, nil
}

func (s releaseSigner) sign(release clientversion.PublicRelease) (string, error) {
	if s == nil {
		return "", fmt.Errorf("release signing key is not configured")
	}
	return s(release)
}

func validateReleaseSigner(signer releaseSigner) error {
	probe := clientversion.PublicRelease{
		Version:      "0.0.0",
		Platform:     "linux",
		Architecture: "amd64",
		Channel:      defaultChannel,
		SHA256:       "0000000000000000000000000000000000000000000000000000000000000000",
	}
	signature, err := signer.sign(probe)
	if err != nil {
		return fmt.Errorf("test release signing key: %w", err)
	}
	probe.Signature = signature
	if err := clientversion.VerifyReleaseSignature(probe); err != nil {
		return fmt.Errorf("release signing key does not match the public key embedded in clients: %w", err)
	}
	return nil
}
