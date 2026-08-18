package types

import "time"

// VersionReleasePlatform identifies an installer operating system.
type VersionReleasePlatform string

const (
	VersionReleasePlatformMacOS   VersionReleasePlatform = "macos"
	VersionReleasePlatformWindows VersionReleasePlatform = "windows"
	VersionReleasePlatformLinux   VersionReleasePlatform = "linux"
	VersionReleasePlatformAndroid VersionReleasePlatform = "android"
)

// VersionReleaseArchitecture identifies an installer CPU architecture.
type VersionReleaseArchitecture string

const (
	VersionReleaseArchitectureAMD64     VersionReleaseArchitecture = "amd64"
	VersionReleaseArchitectureARM64     VersionReleaseArchitecture = "arm64"
	VersionReleaseArchitectureARMv7     VersionReleaseArchitecture = "armv7"
	VersionReleaseArchitectureUniversal VersionReleaseArchitecture = "universal"
)

// VersionRelease is the persisted metadata for a signed client installer.
type VersionRelease struct {
	ID           string                     `gorm:"primaryKey"`
	AccountID    string                     `gorm:"index;not null"`
	Version      string                     `gorm:"index;not null"`
	Platform     VersionReleasePlatform     `gorm:"index;not null"`
	Architecture VersionReleaseArchitecture `gorm:"index;not null"`
	Channel      string                     `gorm:"index;not null;default:'stable'"`
	DownloadURL  string                     `gorm:"not null"`
	ArtifactID   string                     `gorm:"index"`
	Description  string                     `gorm:"type:text"`
	SHA256       string                     `gorm:"size:64"`
	Signature    string                     `gorm:"type:text"`
	IsLatest     bool                       `gorm:"index;not null;default:false"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// VersionReleaseArtifact tracks an uploaded installer before and after it is
// attached to release metadata.
type VersionReleaseArtifact struct {
	ID        string `gorm:"primaryKey"`
	AccountID string `gorm:"index;not null"`
	FileName  string `gorm:"not null"`
	Size      int64  `gorm:"not null"`
	SHA256    string `gorm:"size:64;not null"`
	CreatedAt time.Time
}
