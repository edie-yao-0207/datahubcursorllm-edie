package databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRemovalAndCreation(t *testing.T) {
	tests := []struct {
		name           string
		old            map[string]struct{}
		new            map[string]struct{}
		expectedAdd    []string
		expectedRemove []string
	}{
		{
			name: "Test empty maps",
			old:  map[string]struct{}{},
			new:  map[string]struct{}{},
		},
		{
			name: "Test only additions",
			old:  map[string]struct{}{},
			new: map[string]struct{}{
				"new1": {},
				"new2": {},
			},
			expectedAdd: []string{"new1", "new2"},
		},
		{
			name: "Test only removals",
			old: map[string]struct{}{
				"old1": {},
				"old2": {},
			},
			new:            map[string]struct{}{},
			expectedRemove: []string{"old1", "old2"},
		},
		{
			name: "Test additions and removals",
			old: map[string]struct{}{
				"old1": {},
				"old2": {},
			},
			new: map[string]struct{}{
				"new1": {},
				"new2": {},
			},
			expectedAdd:    []string{"new1", "new2"},
			expectedRemove: []string{"old1", "old2"},
		},
		{
			name: "Test common elements with additions and removals",
			old: map[string]struct{}{
				"common": {},
				"old1":   {},
			},
			new: map[string]struct{}{
				"common": {},
				"new1":   {},
			},
			expectedAdd:    []string{"new1"},
			expectedRemove: []string{"old1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			add, remove := getRemovalAndCreation(test.old, test.new)
			if !assert.Equal(t, add, test.expectedAdd) {
				t.Errorf("expected additions %v, got %v", test.expectedAdd, add)
			}
			if !assert.Equal(t, remove, test.expectedRemove) {
				t.Errorf("expected removals %v, got %v", test.expectedRemove, remove)
			}
		})
	}
}
