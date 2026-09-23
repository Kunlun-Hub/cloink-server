package types

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeneratePasswordResetToken(t *testing.T) {
	hashed, plain, err := GeneratePasswordResetToken()
	require.NoError(t, err)

	assert.Len(t, plain, PasswordResetTokenLength)
	assert.True(t, strings.HasPrefix(plain, PasswordResetTokenPrefix))
	assert.Equal(t, HashPasswordResetToken(plain), hashed)
	require.NoError(t, ValidatePasswordResetToken(plain))
}

func TestGeneratePasswordResetTokenIsUnique(t *testing.T) {
	seen := make(map[string]struct{}, 50)
	for i := 0; i < 50; i++ {
		_, plain, err := GeneratePasswordResetToken()
		require.NoError(t, err)
		_, duplicate := seen[plain]
		require.False(t, duplicate, "reset tokens must not repeat")
		seen[plain] = struct{}{}
	}
}

func TestValidatePasswordResetTokenRejectsTamperedTokens(t *testing.T) {
	_, plain, err := GeneratePasswordResetToken()
	require.NoError(t, err)

	tests := map[string]string{
		"empty":             "",
		"too short":         plain[:len(plain)-1],
		"wrong prefix":      "nbi_" + plain[len(PasswordResetTokenPrefix):],
		"tampered secret":   plain[:len(PasswordResetTokenPrefix)] + "a" + plain[len(PasswordResetTokenPrefix)+1:],
		"tampered checksum": plain[:len(plain)-1] + string(tamperByte(plain[len(plain)-1])),
	}

	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Error(t, ValidatePasswordResetToken(token))
		})
	}
}

func tamperByte(b byte) byte {
	if b == 'a' {
		return 'b'
	}
	return 'a'
}

func TestHashPasswordResetEmailNormalizesInput(t *testing.T) {
	assert.Equal(t, HashPasswordResetEmail("User@Example.COM"), HashPasswordResetEmail("  user@example.com "))
	assert.NotEqual(t, HashPasswordResetEmail("user@example.com"), HashPasswordResetEmail("other@example.com"))
}

func TestPasswordResetRecordIsExpired(t *testing.T) {
	var nilRecord *PasswordResetRecord
	assert.True(t, nilRecord.IsExpired(), "a nil record is never usable")

	expired := &PasswordResetRecord{ExpiresAt: time.Now().Add(-time.Second)}
	assert.True(t, expired.IsExpired())

	valid := &PasswordResetRecord{ExpiresAt: time.Now().Add(time.Minute)}
	assert.False(t, valid.IsExpired())
}

func TestPasswordResetRecordCopyIsIndependent(t *testing.T) {
	original := &PasswordResetRecord{ID: "id", Email: "user@example.com"}
	clone := original.Copy()
	clone.Email = "changed@example.com"

	assert.Equal(t, "user@example.com", original.Email)
	assert.Equal(t, "changed@example.com", clone.Email)
	assert.Nil(t, (*PasswordResetRecord)(nil).Copy())
}
