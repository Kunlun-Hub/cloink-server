package version_releases

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	clientversion "github.com/netbirdio/netbird/version"
)

func TestReleaseSignerSignsCanonicalMetadata(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	signer, err := newReleaseSigner(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	require.NoError(t, err)
	release := clientversion.PublicRelease{
		Version: "0.77.3", Platform: "windows", Architecture: "amd64",
		Channel: "stable", SHA256: strings.Repeat("0", 64),
	}

	signature, err := signer.sign(release)
	require.NoError(t, err)
	decoded, err := base64.StdEncoding.DecodeString(signature)
	require.NoError(t, err)
	require.True(t, ed25519.Verify(publicKey, clientversion.ReleaseSignaturePayload(release), decoded))
}

func TestNewReleaseSignerRejectsNonEd25519Key(t *testing.T) {
	_, err := newReleaseSigner([]byte("not a private key"))
	require.ErrorContains(t, err, "decode Ed25519 private key PEM")
}

func TestValidateReleaseSignerRejectsMismatchedKey(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer := releaseSigner(func(release clientversion.PublicRelease) (string, error) {
		return base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, clientversion.ReleaseSignaturePayload(release))), nil
	})

	require.ErrorContains(t, validateReleaseSigner(signer), "does not match the public key embedded in clients")
}
