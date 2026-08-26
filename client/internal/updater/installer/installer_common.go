//go:build windows || darwin

package installer

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/hashicorp/go-multierror"
	goversion "github.com/hashicorp/go-version"
	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/client/internal/updater/downloader"
	"github.com/netbirdio/netbird/version"
)

type Installer struct {
	tempDir string
}

// New used by the service
func New() *Installer {
	return &Installer{
		tempDir: defaultTempDir,
	}
}

// NewWithDir used by the updater process, get the tempDir from the service via cmd line
func NewWithDir(tempDir string) *Installer {
	return &Installer{
		tempDir: tempDir,
	}
}

// RunInstallation starts the updater process to run the installation
// This will run by the original service process
func (u *Installer) RunInstallation(ctx context.Context, targetVersion string) (err error) {
	resultHandler := NewResultHandler(u.tempDir)
	if err := resultHandler.ClearStaleResult(); err != nil {
		log.Warnf("clear stale installer result: %v", err)
	}

	defer func() {
		if err != nil {
			if writeErr := resultHandler.WriteErr(err); writeErr != nil {
				log.Errorf("failed to write error result: %v", writeErr)
			}
		}
	}()

	if err := validateTargetVersion(targetVersion); err != nil {
		return err
	}

	if err := u.mkTempDir(); err != nil {
		return err
	}

	var installerFile string
	// Download files only when not using any third-party store
	if installerType := TypeOfInstaller(ctx); installerType.Downloadable() {
		log.Infof("download installer")
		var err error
		installerFile, err = u.downloadInstaller(ctx, installerType, targetVersion)
		if err != nil {
			log.Errorf("failed to download installer: %v", err)
			return err
		}

	}

	log.Infof("running installer")
	updaterPath, err := u.copyUpdater()
	if err != nil {
		return err
	}

	// the directory where the service has been installed
	workspace, err := getServiceDir()
	if err != nil {
		return err
	}

	args := []string{
		"--temp-dir", u.tempDir,
		"--service-dir", workspace,
	}

	if isDryRunEnabled() {
		args = append(args, "--dry-run=true")
	}

	if installerFile != "" {
		args = append(args, "--installer-file", installerFile)
	}

	updateCmd := exec.Command(updaterPath, args...)
	log.Infof("starting updater process: %s", updateCmd.String())

	// Configure the updater to run in a separate session/process group
	// so it survives the parent daemon being stopped
	setUpdaterProcAttr(updateCmd)

	// Start the updater process asynchronously
	if err := updateCmd.Start(); err != nil {
		return err
	}

	pid := updateCmd.Process.Pid
	log.Infof("updater started with PID %d", pid)

	// Release the process so the OS can fully detach it
	if err := updateCmd.Process.Release(); err != nil {
		log.Warnf("failed to release updater process: %v", err)
	}

	return nil
}

// CleanUpInstallerFiles
// - the installer file (pkg, exe, msi)
// - the selfcopy updater.exe
func (u *Installer) CleanUpInstallerFiles() error {
	// Check if tempDir exists
	info, err := os.Stat(u.tempDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if !info.IsDir() {
		return nil
	}

	var merr *multierror.Error

	if err := os.Remove(filepath.Join(u.tempDir, updaterBinary)); err != nil && !os.IsNotExist(err) {
		merr = multierror.Append(merr, fmt.Errorf("failed to remove updater binary: %w", err))
	}

	entries, err := os.ReadDir(u.tempDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		for _, ext := range binaryExtensions {
			if strings.HasSuffix(strings.ToLower(name), strings.ToLower(ext)) {
				if err := os.Remove(filepath.Join(u.tempDir, name)); err != nil {
					merr = multierror.Append(merr, fmt.Errorf("failed to remove %s: %w", name, err))
				}
				break
			}
		}
	}

	return merr.ErrorOrNil()
}

func (u *Installer) downloadInstaller(ctx context.Context, installerType Type, targetVersion string) (string, error) {
	releases, err := version.FetchPublicReleases(
		ctx,
		releasePlatformForUpdater(),
		runtime.GOARCH,
		"stable",
		targetVersion,
	)
	if err != nil {
		return "", err
	}
	release := releases[0]
	fileURL := release.DownloadURL

	// Clean up temp directory on error
	var success bool
	defer func() {
		if !success {
			if err := os.RemoveAll(u.tempDir); err != nil {
				log.Errorf("error cleaning up temporary directory: %v", err)
			}
		}
	}()

	outputFilePath := filepath.Join(u.tempDir, "cloink-update"+installerExtension(installerType))
	if err := downloader.DownloadToFile(ctx, downloader.DefaultRetryDelay, fileURL, outputFilePath); err != nil {
		return "", err
	}
	file, err := os.Open(outputFilePath)
	if err != nil {
		return "", err
	}
	verifyErr := version.VerifySHA256(file, release.SHA256)
	closeErr := file.Close()
	if verifyErr != nil {
		return "", verifyErr
	}
	if closeErr != nil {
		return "", closeErr
	}

	success = true
	return outputFilePath, nil
}

func releasePlatformForUpdater() string {
	if runtime.GOOS == "darwin" {
		return "macos"
	}
	return runtime.GOOS
}

func installerExtension(installerType Type) string {
	switch strings.ToLower(installerType.name) {
	case "exe":
		return ".exe"
	case "msi":
		return ".msi"
	case "pkg":
		return ".pkg"
	default:
		return ".bin"
	}
}

func (u *Installer) TempDir() string {
	return u.tempDir
}

func (u *Installer) mkTempDir() error {
	if err := os.MkdirAll(u.tempDir, 0o755); err != nil {
		log.Debugf("failed to create tempdir: %s", u.tempDir)
		return err
	}
	return nil
}

func (u *Installer) copyUpdater() (string, error) {
	src, err := getServiceBinary()
	if err != nil {
		return "", fmt.Errorf("failed to get updater binary: %w", err)
	}

	dst := filepath.Join(u.tempDir, updaterBinary)
	if err := copyFile(src, dst); err != nil {
		return "", fmt.Errorf("failed to copy updater binary: %w", err)
	}

	if err := os.Chmod(dst, 0o755); err != nil {
		return "", fmt.Errorf("failed to set permissions: %w", err)
	}

	return dst, nil
}

func validateTargetVersion(targetVersion string) error {
	if targetVersion == "" {
		return fmt.Errorf("target version cannot be empty")
	}

	_, err := goversion.NewVersion(targetVersion)
	if err != nil {
		return fmt.Errorf("invalid target version %q: %w", targetVersion, err)
	}

	return nil
}

func copyFile(src, dst string) error {
	log.Infof("copying %s to %s", src, dst)
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() {
		if err := in.Close(); err != nil {
			log.Warnf("failed to close source file: %v", err)
		}
	}()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	defer func() {
		if err := out.Close(); err != nil {
			log.Warnf("failed to close destination file: %v", err)
		}
	}()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return nil
}

func getServiceDir() (string, error) {
	exePath, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Dir(exePath), nil
}

func getServiceBinary() (string, error) {
	return os.Executable()
}

func isDryRunEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("NB_AUTO_UPDATE_DRY_RUN")), "true")
}
