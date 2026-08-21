package cmd

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/status"

	"github.com/netbirdio/netbird/client/proto"
)

var relayCmd = &cobra.Command{
	Use:     "relay",
	Short:   "List received relays",
	Long:    "List relay servers received by this client and show their current selection weight.",
	Example: "  cloink relay",
	RunE:    listRelays,
}

var relaySetCmd = &cobra.Command{
	Use:   "set <relay>",
	Short: "Force a relay for this client session",
	Long: "Force this client to use one relay from the received relay list. " +
		"The relay can be a full URI, hostname, or unique substring. Use auto, default, or clear to remove the override.",
	Example: "  cloink relay set hz-cucc\n  cloink relay set rels://hz-cucc-relay.example.com:12580\n  cloink relay set auto",
	Args:    cobra.ExactArgs(1),
	RunE:    setRelay,
}

func listRelays(cmd *cobra.Command, _ []string) error {
	conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := proto.NewDaemonServiceClient(conn)
	resp, err := client.ListRelays(cmd.Context(), &proto.EmptyRequest{})
	if err != nil {
		return fmt.Errorf("failed to list relays: %v", status.Convert(err).Message())
	}

	relays := resp.GetRelays()
	if len(relays) == 0 {
		cmd.Println("No relays received.")
		return nil
	}

	cmd.Println("Received relays:")
	for _, relay := range relays {
		labels := relayLabels(relay)
		labelText := ""
		if len(labels) > 0 {
			labelText = "  [" + strings.Join(labels, ", ") + "]"
		}

		cmd.Printf("  - %s\n", relayDisplayID(relay.GetUri()))
		cmd.Printf("    URI: %s\n", relay.GetUri())
		cmd.Printf("    Weight: %d%s\n", relay.GetWeight(), labelText)
		cmd.Printf("    Status: %s\n", relayProbeStatus(relay))
	}

	return nil
}

func setRelay(cmd *cobra.Command, args []string) error {
	conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := proto.NewDaemonServiceClient(conn)
	resp, err := client.SetRelay(cmd.Context(), &proto.SetRelayRequest{Relay: args[0]})
	if err != nil {
		return fmt.Errorf("failed to set relay: %v", status.Convert(err).Message())
	}

	if resp.GetSelected() == "" {
		cmd.Println("Relay override cleared. Using received relay order.")
		return nil
	}

	cmd.Printf("Forced relay set to: %s\n", resp.GetSelected())
	return nil
}

func relayLabels(relay *proto.RelayServer) []string {
	var labels []string
	if relay.GetCurrent() {
		labels = append(labels, "current")
	}
	if relay.GetPreferred() {
		labels = append(labels, "preferred")
	}
	if relay.GetForced() {
		labels = append(labels, "forced")
	}
	return labels
}

func relayProbeStatus(relay *proto.RelayServer) string {
	if relay.GetAvailable() {
		return "Available"
	}
	if relay.GetError() == "" {
		return "Unavailable"
	}
	return fmt.Sprintf("Unavailable, reason: %s", relay.GetError())
}

func relayDisplayID(relayURL string) string {
	parsedURL, err := url.Parse(relayURL)
	if err == nil && parsedURL.Hostname() != "" {
		return parsedURL.Hostname()
	}
	return relayURL
}
