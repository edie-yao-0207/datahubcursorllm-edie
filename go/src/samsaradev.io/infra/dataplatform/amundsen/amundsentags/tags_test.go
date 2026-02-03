package amundsentags

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Amundsen doesn't like some characters in tags due to how it constructs URL queries
// This test makes sure tags are valid to disallow developers from creating bad tags
func TestTagNames(t *testing.T) {
	for tag := range allTags {
		strTag := string(tag)
		t.Run(string(tag), func(t *testing.T) {
			require.False(t, strings.Contains(strTag, ":"), "colons (:) are not allowed in tag names")
		})
	}
}
