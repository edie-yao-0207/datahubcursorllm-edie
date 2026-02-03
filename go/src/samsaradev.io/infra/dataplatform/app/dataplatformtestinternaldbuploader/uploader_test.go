package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateRowsForNotesTable(t *testing.T) {
	type expected struct {
		startingId int
		note       string
	}
	testCases := []struct {
		startingId int
		numRows    int
		tableName  string
		expected   map[int]expected
	}{
		{
			startingId: 10,
			numRows:    5,
			expected: map[int]expected{
				10: {
					startingId: 10,
					note:       fmt.Sprintf("%s %d", NOTE, 10),
				},
				11: {
					startingId: 11,
					note:       fmt.Sprintf("%s %d", NOTE, 11),
				},
				12: {
					startingId: 12,
					note:       fmt.Sprintf("%s %d", NOTE, 12),
				},
				13: {
					startingId: 13,
					note:       fmt.Sprintf("%s %d", NOTE, 13),
				},
				14: {
					startingId: 14,
					note:       fmt.Sprintf("%s %d", NOTE, 14),
				},
			},
		},
		{
			startingId: 1,
			numRows:    5,
			tableName:  JsonNotesTableName,
			expected: map[int]expected{
				1: {
					startingId: 1,
					note:       `{"note":"random note 1"}`,
				},
				2: {
					startingId: 2,
					note:       `{"note":"random note 2"}`,
				},
				3: {
					startingId: 3,
					note:       `{"note":"random note 3"}`,
				},
				4: {
					startingId: 4,
					note:       `{"note":"random note 4"}`,
				},
				5: {
					startingId: 5,
					note:       `{"note":"random note 5"}`,
				},
			},
		},
	}

	for _, tc := range testCases {
		rows := generateRowsForTable(tc.startingId, tc.numRows, tc.tableName)
		assert.Len(t, rows, tc.numRows)
		for key, value := range rows {
			assert.Equal(t, tc.expected[key].startingId, key)
			assert.Equal(t, tc.expected[key].note, value)
		}
	}
}
