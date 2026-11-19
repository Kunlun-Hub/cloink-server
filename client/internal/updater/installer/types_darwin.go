package installer

import (
	"context"
	"os/exec"
)

var (
	TypeHomebrew = Type{name: "Homebrew", downloadable: false}
	TypePKG      = Type{name: "pkg", downloadable: true}
)

func TypeOfInstaller(ctx context.Context) Type {
	for _, identifier := range []string{"io.cloink.client", "io.netbird.client"} {
		cmd := exec.CommandContext(ctx, "pkgutil", "--pkg-info", identifier)
		_, err := cmd.Output()
		if err == nil {
			return TypePKG
		}
		if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 1 {
			return TypePKG
		}
	}

	// Neither Cloink nor NetBird has a package receipt, so this is a Homebrew
	// installation and cannot be silently replaced with a PKG.
	return TypeHomebrew
}
