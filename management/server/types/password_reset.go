package types

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	b "github.com/hashicorp/go-secure-stdlib/base62"
	"github.com/rs/xid"

	"github.com/netbirdio/netbird/base62"
	"github.com/netbirdio/netbird/util/crypt"
)

const (
	// PasswordResetTokenPrefix is the prefix for password reset tokens.
	PasswordResetTokenPrefix = "nbr_"
	// PasswordResetTokenSecretLength is the length of the random secret part.
	PasswordResetTokenSecretLength = 30
	// PasswordResetTokenChecksumLength is the length of the encoded checksum.
	PasswordResetTokenChecksumLength = 6
	// PasswordResetTokenLength is the total length of the token (4 + 30 + 6 = 40).
	PasswordResetTokenLength = 40
	// DefaultPasswordResetExpirationSeconds is how long a reset link stays valid (30 minutes).
	DefaultPasswordResetExpirationSeconds = 1800
	// PasswordResetRateLimitWindow is the sliding window used to throttle reset requests.
	PasswordResetRateLimitWindow = time.Hour
	// PasswordResetRateLimitMax is the maximum number of reset links issued per email per window.
	PasswordResetRateLimitMax = 5
)

// PasswordResetRecord stores a single-use password reset token. The plain token
// is never persisted: only its SHA-256 hash is stored, alongside an expiry and
// the account the link was issued for.
type PasswordResetRecord struct {
	ID        string `gorm:"primaryKey"`
	AccountID string `gorm:"index;not null"`
	UserID    string `gorm:"index;not null"`
	Email     string `gorm:"not null"`
	// EmailHash is the SHA-256 hash (base64) of the lowercased email. Encrypted
	// emails use random IVs, so the hash is what lets us find and rate limit
	// requests for a given address without decrypting every row.
	EmailHash   string    `gorm:"index;not null"`
	HashedToken string    `gorm:"index;not null"`
	ExpiresAt   time.Time `gorm:"not null"`
	CreatedAt   time.Time `gorm:"not null"`
	// CreatedBy is the admin user ID for console-triggered links and empty for
	// self-service requests from the sign-in page.
	CreatedBy string `gorm:"default:''"`
}

// TableName returns the table name for GORM.
func (PasswordResetRecord) TableName() string {
	return "password_reset_tokens"
}

// PasswordResetLink is the result of issuing a reset link.
type PasswordResetLink struct {
	URL       string    `json:"url"`
	Email     string    `json:"email"`
	ExpiresAt time.Time `json:"expires_at"`
	EmailSent bool      `json:"email_sent"`
	EmailErr  string    `json:"email_error,omitempty"`
}

// NewPasswordResetID generates a new reset record ID using xid.
func NewPasswordResetID() string {
	return xid.New().String()
}

// GeneratePasswordResetToken creates a new reset token in the format
// nbr_<secret><checksum>. It returns the hashed token (for storage) and the
// plain token (to embed in the reset URL).
func GeneratePasswordResetToken() (hashedToken string, plainToken string, err error) {
	secret, err := b.Random(PasswordResetTokenSecretLength)
	if err != nil {
		return "", "", fmt.Errorf("failed to generate random secret: %w", err)
	}

	checksum := crc32.ChecksumIEEE([]byte(secret))
	encodedChecksum := base62.Encode(checksum)
	paddedChecksum := encodedChecksum
	if len(paddedChecksum) < PasswordResetTokenChecksumLength {
		paddedChecksum = strings.Repeat("0", PasswordResetTokenChecksumLength-len(paddedChecksum)) + paddedChecksum
	}

	plainToken = PasswordResetTokenPrefix + secret + paddedChecksum

	return HashPasswordResetToken(plainToken), plainToken, nil
}

// HashPasswordResetToken creates a SHA-256 hash of the token (base64 encoded).
func HashPasswordResetToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return base64.StdEncoding.EncodeToString(hash[:])
}

// HashPasswordResetEmail hashes a normalized email so it can be matched and
// counted without decrypting stored rows.
func HashPasswordResetEmail(email string) string {
	hash := sha256.Sum256([]byte(NormalizePasswordResetEmail(email)))
	return base64.StdEncoding.EncodeToString(hash[:])
}

// NormalizePasswordResetEmail lowercases and trims an email address.
func NormalizePasswordResetEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// ValidatePasswordResetToken validates the token format and checksum.
func ValidatePasswordResetToken(token string) error {
	if len(token) != PasswordResetTokenLength {
		return fmt.Errorf("invalid token length")
	}
	if token[:len(PasswordResetTokenPrefix)] != PasswordResetTokenPrefix {
		return fmt.Errorf("invalid token prefix")
	}

	secret := token[len(PasswordResetTokenPrefix) : len(PasswordResetTokenPrefix)+PasswordResetTokenSecretLength]
	encodedChecksum := token[len(PasswordResetTokenPrefix)+PasswordResetTokenSecretLength:]

	verificationChecksum, err := base62.Decode(encodedChecksum)
	if err != nil {
		return fmt.Errorf("checksum decoding failed: %w", err)
	}
	if crc32.ChecksumIEEE([]byte(secret)) != verificationChecksum {
		return fmt.Errorf("checksum does not match")
	}

	return nil
}

// IsExpired reports whether the reset link is past its expiry.
func (r *PasswordResetRecord) IsExpired() bool {
	return r == nil || time.Now().After(r.ExpiresAt)
}

// EncryptSensitiveData encrypts the record's sensitive fields in place.
func (r *PasswordResetRecord) EncryptSensitiveData(enc *crypt.FieldEncrypt) error {
	if enc == nil || r.Email == "" {
		return nil
	}
	encrypted, err := enc.Encrypt(r.Email)
	if err != nil {
		return fmt.Errorf("encrypt email: %w", err)
	}
	r.Email = encrypted
	return nil
}

// DecryptSensitiveData decrypts the record's sensitive fields in place.
func (r *PasswordResetRecord) DecryptSensitiveData(enc *crypt.FieldEncrypt) error {
	if enc == nil || r.Email == "" {
		return nil
	}
	decrypted, err := enc.Decrypt(r.Email)
	if err != nil {
		return fmt.Errorf("decrypt email: %w", err)
	}
	r.Email = decrypted
	return nil
}

// Copy creates a deep copy of the record.
func (r *PasswordResetRecord) Copy() *PasswordResetRecord {
	if r == nil {
		return nil
	}
	clone := *r
	return &clone
}
