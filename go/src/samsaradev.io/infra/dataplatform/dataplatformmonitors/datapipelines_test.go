package dataplatformmonitors

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes"
)

func TestNodeSlice2InStatement(t *testing.T) {
	testCases := []struct {
		input          []string
		expectedOutput string
	}{
		{
			input:          []string{"node-1", "node-2", "node-3"},
			expectedOutput: "node IN (node-1,node-2,node-3)",
		},
		{
			input:          []string{"node-1"},
			expectedOutput: "node IN (node-1)",
		},
	}

	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("TestNodeMap2InStatement-testCase-%d", idx), func(t *testing.T) {
			actualOutput := nodeSliceToInStatement(testCase.input)
			assert.Equal(t, testCase.expectedOutput, actualOutput)
		})
	}
}

func TestFindOnlySharedNodes(t *testing.T) {
	testCases := []struct {
		testName              string
		whollyOwnedNodesInput map[string]nodetypes.Node
		sharedNodesInput      map[string]nodetypes.Node
		expectedSharedNodes   []nodetypes.Node
	}{
		{
			testName: "no-only-shared-nodes",
			whollyOwnedNodesInput: map[string]nodetypes.Node{
				"node1": nodetypes.BaseNode{NodeName: "node1"},
				"node2": nodetypes.BaseNode{NodeName: "node2"},
				"node3": nodetypes.BaseNode{NodeName: "node3"},
			},
			sharedNodesInput: map[string]nodetypes.Node{
				"node1": nodetypes.BaseNode{NodeName: "node1"},
				"node2": nodetypes.BaseNode{NodeName: "node2"},
				"node3": nodetypes.BaseNode{NodeName: "node3"},
			},
			expectedSharedNodes: []nodetypes.Node(nil),
		},
		{
			testName: "all-only-shared-nodes",
			whollyOwnedNodesInput: map[string]nodetypes.Node{
				"node1": nodetypes.BaseNode{NodeName: "node1"},
				"node2": nodetypes.BaseNode{NodeName: "node2"},
				"node3": nodetypes.BaseNode{NodeName: "node3"},
			},
			sharedNodesInput: map[string]nodetypes.Node{
				"node4": nodetypes.BaseNode{NodeName: "node4"},
				"node5": nodetypes.BaseNode{NodeName: "node5"},
				"node6": nodetypes.BaseNode{NodeName: "node6"},
			},
			expectedSharedNodes: []nodetypes.Node{
				nodetypes.BaseNode{NodeName: "node4"},
				nodetypes.BaseNode{NodeName: "node5"},
				nodetypes.BaseNode{NodeName: "node6"},
			},
		},
		{
			testName: "some-only-shared-nodes",
			whollyOwnedNodesInput: map[string]nodetypes.Node{
				"node1": nodetypes.BaseNode{NodeName: "node1"},
				"node2": nodetypes.BaseNode{NodeName: "node2"},
				"node3": nodetypes.BaseNode{NodeName: "node3"},
			},
			sharedNodesInput: map[string]nodetypes.Node{
				"node2": nodetypes.BaseNode{NodeName: "node2"},
				"node3": nodetypes.BaseNode{NodeName: "node3"},
				"node4": nodetypes.BaseNode{NodeName: "node4"},
				"node5": nodetypes.BaseNode{NodeName: "node5"},
			},
			expectedSharedNodes: []nodetypes.Node{
				nodetypes.BaseNode{NodeName: "node4"},
				nodetypes.BaseNode{NodeName: "node5"},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			actualSharedNodes := findOnlySharedNodes(testCase.whollyOwnedNodesInput, testCase.sharedNodesInput)
			sort.Slice(actualSharedNodes, func(i, j int) bool {
				return actualSharedNodes[i].Name() < actualSharedNodes[j].Name()
			})
			assert.True(t, reflect.DeepEqual(testCase.expectedSharedNodes, actualSharedNodes))
		})
	}
}
