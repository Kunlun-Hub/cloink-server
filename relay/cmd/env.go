package cmd

import (
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// setFlagsFromEnvVars reads flag values from NB_ variables, falling back to CL_.
func setFlagsFromEnvVars(cmd *cobra.Command) {
	flags := cmd.PersistentFlags()
	flags.VisitAll(func(f *pflag.Flag) {
		envVar := flagNameToEnvVar(f.Name, "NB_")
		value, present := os.LookupEnv(envVar)
		if !present {
			envVar = flagNameToEnvVar(f.Name, "CL_")
			value, present = os.LookupEnv(envVar)
		}
		if !present {
			return
		}

		err := flags.Set(f.Name, value)
		if err != nil {
			log.Infof("unable to configure flag %s using variable %s, err: %v", f.Name, envVar, err)
		}
	})
}

// flagNameToEnvVar converts flag name to environment var name adding a prefix,
// replacing dashes and making all uppercase (e.g. setup-keys is converted to NB_SETUP_KEYS according to the input prefix)
func flagNameToEnvVar(cmdFlag string, prefix string) string {
	parsed := strings.ReplaceAll(cmdFlag, "-", "_")
	upper := strings.ToUpper(parsed)
	return prefix + upper
}
