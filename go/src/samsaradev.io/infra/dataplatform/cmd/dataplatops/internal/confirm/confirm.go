// Package confirm provides interactive confirmation prompts for the CLI.
package confirm

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/samsarahq/go/oops"
)

// Prompt asks for y/n confirmation from the user.
// Returns nil if confirmed, error if declined or on input failure.
func Prompt(message string) error {
	fmt.Printf("⚠️  %s [y/N]: ", message)

	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		return oops.Wrapf(err, "failed to read confirmation")
	}

	response = strings.TrimSpace(strings.ToLower(response))
	if response != "y" && response != "yes" {
		return oops.Errorf("operation cancelled by user")
	}
	return nil
}
