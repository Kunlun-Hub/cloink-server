package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmailSettingsPasswordRequiresFieldEncryption(t *testing.T) {
	settings := &EmailSettings{Password: "secret"}
	require.Error(t, settings.EncryptSensitiveData(nil))
	require.Equal(t, "secret", settings.Password)

	settings = &EmailSettings{PasswordEncrypted: "ciphertext"}
	require.Error(t, settings.DecryptSensitiveData(nil))
}
