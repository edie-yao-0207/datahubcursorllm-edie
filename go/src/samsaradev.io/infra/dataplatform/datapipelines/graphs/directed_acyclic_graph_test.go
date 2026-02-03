package graphs

import (
	"sort"
	"testing"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/dependencytypes"
	"samsaradev.io/infra/testloader"
)

// Build full DirectedAcyclicGraph for CI verification
func TestBuildFullDAG(t *testing.T) {
	_, err := BuildDAGs()
	require.NoError(t, err)
}

func TestDAG(t *testing.T) {
	var env struct {
		Snapper *snapshotter.Snapshotter
	}
	testloader.MustStart(t, &env)
	snap := env.Snapper

	testCases := []struct {
		testName  string
		dagFunc   func() ([]nodetypes.Node, map[*partition]struct{})
		shouldErr bool
	}{
		{
			testName: "smallFullyConnectedValidDAG",
			dagFunc:  smallFullyConnectedValidDAG,
		},
		{
			testName: "smallSemiConnectedValidDAG",
			dagFunc:  smallSemiConnectedValidDAG,
		},
		{
			testName: "largeFullyConnectedValidDAG",
			dagFunc:  largeFullyConnectedValidDAG,
		},
		{
			testName: "largeSemiConnectedValidDAG",
			dagFunc:  largeSemiConnectedValidDAG,
		},
		{
			testName: "fullyConnectedSinglePathValidDAG",
			dagFunc:  fulllyConnectedSinglePathValidDAG,
		},
		{
			testName:  "smallFullyConnectedInvalidDAG",
			dagFunc:   smallFullyConnectedInvalidDAG,
			shouldErr: true,
		},
		{
			testName:  "largeFullyConnectedInvalidDAG",
			dagFunc:   largeFullyConnectedInvalidDAG,
			shouldErr: true,
		},
		{
			testName:  "smallSemiConnectedInvalidDAG",
			dagFunc:   smallSemiConnectedInvalidDAG,
			shouldErr: true,
		},
		{
			testName:  "largeSemiConnectedInvalidDAG",
			dagFunc:   largeSemiConnectedInvalidDAG,
			shouldErr: true,
		},
	}

	for _, tc := range testCases {
		nodes, _ := tc.dagFunc()
		dag, err := NewDAG(nodes)
		if tc.shouldErr {
			require.Error(t, err)
			require.Nil(t, dag)
		} else {
			verifyParentNodes(t, nodes, dag)
			require.NoError(t, err)
			snap.Snapshot(tc.testName, dag.String())
		}
	}
}

// Validate that the parents field is filled in correctly based on dependencies.
func verifyParentNodes(t *testing.T, expectedNodes []nodetypes.Node, actualDag *DirectedAcyclicGraph) {
	for _, node := range expectedNodes {
		if len(node.Dependencies()) == 0 {
			_, ok := actualDag.parents[node.Name()]
			require.False(t, ok)
			continue
		}
		expectedParents := make([]string, 0, len(node.Dependencies()))
		for _, parent := range node.Dependencies() {
			expectedParents = append(expectedParents, parent.Name())
		}
		actualParents, ok := actualDag.parents[node.Name()]
		require.True(t, ok)
		actualParentsString := make([]string, 0, len(actualParents))
		for _, parent := range actualParents {
			actualParentsString = append(actualParentsString, parent.Name())
		}
		require.ElementsMatch(t, expectedParents, actualParentsString)
	}
}

func TestPartition(t *testing.T) {
	testCases := map[string]func() ([]nodetypes.Node, map[*partition]struct{}){
		"smallFullyConnectedDAG":       smallFullyConnectedValidDAG,
		"smallSemiConnectedDAG":        smallSemiConnectedValidDAG,
		"largeFullyConnectedDAG":       largeFullyConnectedValidDAG,
		"largeSemiConnectedDAG":        largeSemiConnectedValidDAG,
		"fulllyConnectedSinglePathDAG": fulllyConnectedSinglePathValidDAG,
	}

	for name, dagFunc := range testCases {
		t.Run(name, func(t *testing.T) {
			nodes, expectedPartitions := dagFunc()
			dag, err := NewDAG(nodes)
			require.NoError(t, err)
			actualPartitions := dag.partition()
			assertEqualDAGPartitions(t, actualPartitions, expectedPartitions)
		})
	}
}

func TestGetChildrenRecursive(t *testing.T) {
	// make a simple graph
	//
	// node 1 -> node 2 -> n3
	//                  /
	//        -> node 4 -> n5
	//                  -> n6
	n1 := nodetypes.BaseNode{NodeName: "n1"}
	n2 := nodetypes.BaseNode{NodeName: "n2"}
	n3 := nodetypes.BaseNode{NodeName: "n3"}
	n4 := nodetypes.BaseNode{NodeName: "n4"}
	n5 := nodetypes.BaseNode{NodeName: "n5"}
	n6 := nodetypes.BaseNode{NodeName: "n6"}
	n2.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "n1"},
	}
	n4.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "n1"},
	}
	n3.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "n2"},
		dependencytypes.BaseDependency{BaseName: "n4"},
	}
	n5.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "n4"},
	}
	n6.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "n4"},
	}

	dag, err := NewDAG([]nodetypes.Node{n1, n2, n3, n4, n5, n6})
	require.NoError(t, err)

	testCases := map[string]struct {
		node     nodetypes.Node
		expected []nodetypes.Node
	}{
		"node 1": {
			n1,
			[]nodetypes.Node{n2, n3, n4, n5, n6},
		},
		"node 2": {
			n2,
			[]nodetypes.Node{n3},
		},
		"node 3": {
			n3,
			nil,
		},
		"node 4": {
			n4,
			[]nodetypes.Node{n3, n5, n6},
		},
		"node 5": {
			n5,
			nil,
		},
		"node 6": {
			n6,
			nil,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			children := dag.GetNodeChildrenRecursive(testCase.node)
			sort.SliceStable(children, func(i, j int) bool {
				return children[i].Name() < children[j].Name()
			})
			assert.Equal(t, testCase.expected, children)
		})
	}
}

func TestDisjointUnionDAGs(t *testing.T) {

	testCases := map[string]func() ([]nodetypes.Node, map[*partition]struct{}){
		"smallFullyConnectedDAG":       smallFullyConnectedValidDAG,
		"smallSemiConnectedDAG":        smallSemiConnectedValidDAG,
		"largeFullyConnectedDAG":       largeFullyConnectedValidDAG,
		"largeSemiConnectedDAG":        largeSemiConnectedValidDAG,
		"fulllyConnectedSinglePathDAG": fulllyConnectedSinglePathValidDAG,
	}

	expectedDisjointUnionedDAGNodes := map[string][][]nodetypes.Node{
		"smallFullyConnectedDAG": [][]nodetypes.Node{
			{
				nodetypes.BaseNode{NodeName: "node1"},
				nodetypes.BaseNode{NodeName: "node2"},
				nodetypes.BaseNode{NodeName: "node3", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node1"},
					dependencytypes.BaseDependency{BaseName: "node2"},
				}},
			},
		},
		"smallSemiConnectedDAG": [][]nodetypes.Node{
			{
				nodetypes.BaseNode{NodeName: "node4"},
				nodetypes.BaseNode{NodeName: "node5", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node4"},
				}},
				nodetypes.BaseNode{NodeName: "node6", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node5"},
				}},
			},
			{
				nodetypes.BaseNode{NodeName: "node1"},
				nodetypes.BaseNode{NodeName: "node7", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node2"},
				}},
				nodetypes.BaseNode{NodeName: "node2"},
				nodetypes.BaseNode{NodeName: "node3", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node1"},
					dependencytypes.BaseDependency{BaseName: "node2"},
				}},
			},
		},
		"largeFullyConnectedDAG": [][]nodetypes.Node{
			{
				nodetypes.BaseNode{NodeName: "node1"},
				nodetypes.BaseNode{NodeName: "node2", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node1"},
				}},
				nodetypes.BaseNode{NodeName: "node3", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node2"},
					dependencytypes.BaseDependency{BaseName: "node4"},
				}},
				nodetypes.BaseNode{NodeName: "node4"},
				nodetypes.BaseNode{NodeName: "node5"},
				nodetypes.BaseNode{NodeName: "node6", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node2"},
					dependencytypes.BaseDependency{BaseName: "node5"},
				}},
				nodetypes.BaseNode{NodeName: "node7", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node3"},
					dependencytypes.BaseDependency{BaseName: "node6"},
				}},
				nodetypes.BaseNode{NodeName: "node8", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node6"},
				}},
				nodetypes.BaseNode{NodeName: "node9", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node7"},
					dependencytypes.BaseDependency{BaseName: "node8"},
				}},
			},
		},
		"largeSemiConnectedDAG": [][]nodetypes.Node{
			{
				nodetypes.BaseNode{NodeName: "node5"},
				nodetypes.BaseNode{NodeName: "node6", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node5"}}},
				nodetypes.BaseNode{NodeName: "node9", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node6"}}},
			},
			{
				nodetypes.BaseNode{NodeName: "node1"},
				nodetypes.BaseNode{NodeName: "node2", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node1"}}},
				nodetypes.BaseNode{NodeName: "node3", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node2"},
					dependencytypes.BaseDependency{BaseName: "node4"},
				}},
				nodetypes.BaseNode{NodeName: "node4"},
			},
			{
				nodetypes.BaseNode{NodeName: "node7"},
				nodetypes.BaseNode{NodeName: "node8", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node7"}}},
				nodetypes.BaseNode{NodeName: "node10", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "node8"},
					dependencytypes.BaseDependency{BaseName: "node11"},
				}},
				nodetypes.BaseNode{NodeName: "node11", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node7"}}},
				nodetypes.BaseNode{NodeName: "node12", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node11"}}},
			},
		},
		"fulllyConnectedSinglePathDAG": [][]nodetypes.Node{
			{
				nodetypes.BaseNode{NodeName: "node1"},
				nodetypes.BaseNode{NodeName: "node2", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node1"}}},
				nodetypes.BaseNode{NodeName: "node3", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node2"}}},
				nodetypes.BaseNode{NodeName: "node4", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node3"}}},
				nodetypes.BaseNode{NodeName: "node5", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node4"}}},
				nodetypes.BaseNode{NodeName: "node6", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node5"}}},
				nodetypes.BaseNode{NodeName: "node7", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node6"}}},
				nodetypes.BaseNode{NodeName: "node8", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node7"}}},
				nodetypes.BaseNode{NodeName: "node9", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node8"}}},
			},
		},
	}

	for name, dagFunc := range testCases {
		t.Run(name, func(t *testing.T) {
			nodes, _ := dagFunc()
			dags, err := GenerateDAGs(nodes)
			require.NoError(t, err)

			actualUnionedDAGs, err := DisjointUnionDAGs(dags)
			require.NoError(t, err)

			actualUnionedDAGsNodes := [][]nodetypes.Node{}
			for _, d := range actualUnionedDAGs {
				actualUnionedDAGsNodes = append(actualUnionedDAGsNodes, d.GetNodes())
			}

			expectedDAGsNodes := expectedDisjointUnionedDAGNodes[name]
			sort.SliceStable(expectedDAGsNodes, func(i, j int) bool { return len(expectedDAGsNodes[i]) < len(expectedDAGsNodes[j]) })
			sort.SliceStable(actualUnionedDAGsNodes, func(i, j int) bool { return len(actualUnionedDAGsNodes[i]) < len(actualUnionedDAGsNodes[j]) })
			for idx, expectedNodes := range expectedDAGsNodes {
				actualNodes := actualUnionedDAGsNodes[idx]
				require.ElementsMatch(t, expectedNodes, actualNodes)
			}
		})
	}
}

func assertEqualDAGPartitions(t *testing.T, actualPartitions map[*partition]struct{}, expectedPartitions map[*partition]struct{}) {
	assert.Equal(t, len(expectedPartitions), len(actualPartitions))

	expectedStringPartitions := make([]string, 0, len(expectedPartitions))
	for partition := range expectedPartitions {
		expectedStringPartitions = append(expectedStringPartitions, partition.String())
	}
	sort.Strings(expectedStringPartitions)

	actualStringPartitions := make([]string, 0, len(actualPartitions))
	for partition := range actualPartitions {
		actualStringPartitions = append(actualStringPartitions, partition.String())
	}
	sort.Strings(actualStringPartitions)

	assert.Equal(t, expectedStringPartitions, actualStringPartitions)
}

func smallFullyConnectedValidDAG() ([]nodetypes.Node, map[*partition]struct{}) {
	node1 := nodetypes.BaseNode{NodeName: "node1"}
	node2 := nodetypes.BaseNode{NodeName: "node2"}
	node3 := nodetypes.BaseNode{NodeName: "node3"}
	node3.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node1"},
		dependencytypes.BaseDependency{BaseName: "node2"},
	}
	nodes := []nodetypes.Node{node1, node2, node3}
	return nodes, map[*partition]struct{}{
		&partition{
			nodes: nodes,
		}: {},
	}
}

func smallSemiConnectedValidDAG() ([]nodetypes.Node, map[*partition]struct{}) {
	node1 := nodetypes.BaseNode{NodeName: "node1"}
	node2 := nodetypes.BaseNode{NodeName: "node2"}
	node3 := nodetypes.BaseNode{NodeName: "node3"}
	node4 := nodetypes.BaseNode{NodeName: "node4"}
	node5 := nodetypes.BaseNode{NodeName: "node5"}
	node6 := nodetypes.BaseNode{NodeName: "node6"}
	node7 := nodetypes.BaseNode{NodeName: "node7"}
	node3.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node1"},
		dependencytypes.BaseDependency{BaseName: "node2"},
	}
	node5.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node4"},
	}
	node6.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node5"},
	}
	node7.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node2"}}
	nodes := []nodetypes.Node{node1, node2, node3, node4, node5, node6, node7}
	return nodes, map[*partition]struct{}{
		&partition{
			nodes: []nodetypes.Node{node1, node2, node3},
		}: {},
		&partition{
			nodes: []nodetypes.Node{node4, node5, node6},
		}: {},
		&partition{
			nodes: []nodetypes.Node{node2, node7},
		}: {},
	}
}

func largeFullyConnectedValidDAG() ([]nodetypes.Node, map[*partition]struct{}) {
	node1 := nodetypes.BaseNode{NodeName: "node1"}
	node2 := nodetypes.BaseNode{NodeName: "node2"}
	node3 := nodetypes.BaseNode{NodeName: "node3"}
	node4 := nodetypes.BaseNode{NodeName: "node4"}
	node5 := nodetypes.BaseNode{NodeName: "node5"}
	node6 := nodetypes.BaseNode{NodeName: "node6"}
	node7 := nodetypes.BaseNode{NodeName: "node7"}
	node8 := nodetypes.BaseNode{NodeName: "node8"}
	node9 := nodetypes.BaseNode{NodeName: "node9"}
	node2.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node1"}}
	node3.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node2"},
		dependencytypes.BaseDependency{BaseName: "node4"},
	}
	node6.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node2"},
		dependencytypes.BaseDependency{BaseName: "node5"},
	}
	node7.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node3"},
		dependencytypes.BaseDependency{BaseName: "node6"},
	}
	node8.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node6"}}
	node9.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node7"},
		dependencytypes.BaseDependency{BaseName: "node8"},
	}
	nodes := []nodetypes.Node{node1, node2, node3, node4, node5, node6, node7, node8, node9}
	return nodes, map[*partition]struct{}{
		&partition{
			nodes: nodes,
		}: {},
	}
}

func largeSemiConnectedValidDAG() ([]nodetypes.Node, map[*partition]struct{}) {
	node1 := nodetypes.BaseNode{NodeName: "node1"}
	node2 := nodetypes.BaseNode{NodeName: "node2"}
	node3 := nodetypes.BaseNode{NodeName: "node3"}
	node4 := nodetypes.BaseNode{NodeName: "node4"}
	node5 := nodetypes.BaseNode{NodeName: "node5"}
	node6 := nodetypes.BaseNode{NodeName: "node6"}
	node7 := nodetypes.BaseNode{NodeName: "node7"}
	node8 := nodetypes.BaseNode{NodeName: "node8"}
	node9 := nodetypes.BaseNode{NodeName: "node9"}
	node10 := nodetypes.BaseNode{NodeName: "node10"}
	node11 := nodetypes.BaseNode{NodeName: "node11"}
	node12 := nodetypes.BaseNode{NodeName: "node12"}
	node2.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node1"}}
	node3.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node2"},
		dependencytypes.BaseDependency{BaseName: "node4"},
	}
	node6.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node5"}}
	node8.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node7"}}
	node9.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node6"}}
	node10.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node8"},
		dependencytypes.BaseDependency{BaseName: "node11"},
	}
	node11.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node7"}}
	node12.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node11"}}
	nodes := []nodetypes.Node{node1, node2, node3, node4, node5, node6, node7, node8, node9, node10, node11, node12}
	return nodes, map[*partition]struct{}{
		&partition{
			nodes: []nodetypes.Node{node1, node2, node3, node4},
		}: {},
		&partition{
			nodes: []nodetypes.Node{node5, node6, node9},
		}: {},
		&partition{
			nodes: []nodetypes.Node{node7, node8, node10, node11},
		}: {},
		&partition{
			nodes: []nodetypes.Node{node7, node11, node12},
		}: {},
	}
}

// 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9
func fulllyConnectedSinglePathValidDAG() ([]nodetypes.Node, map[*partition]struct{}) {
	node1 := nodetypes.BaseNode{NodeName: "node1"}
	node2 := nodetypes.BaseNode{NodeName: "node2"}
	node3 := nodetypes.BaseNode{NodeName: "node3"}
	node4 := nodetypes.BaseNode{NodeName: "node4"}
	node5 := nodetypes.BaseNode{NodeName: "node5"}
	node6 := nodetypes.BaseNode{NodeName: "node6"}
	node7 := nodetypes.BaseNode{NodeName: "node7"}
	node8 := nodetypes.BaseNode{NodeName: "node8"}
	node9 := nodetypes.BaseNode{NodeName: "node9"}
	node2.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node1"}}
	node3.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node2"}}
	node4.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node3"}}
	node5.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node4"}}
	node6.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node5"}}
	node7.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node6"}}
	node8.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node7"}}
	node9.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node8"}}
	nodes := []nodetypes.Node{node1, node2, node3, node4, node5, node6, node7, node8, node9}
	return nodes, map[*partition]struct{}{
		&partition{
			nodes: nodes,
		}: {},
	}
}

// Cyclic Graph:
//
//	1 -> 2 -> 3 -> 1
func smallFullyConnectedInvalidDAG() ([]nodetypes.Node, map[*partition]struct{}) {
	node1 := nodetypes.BaseNode{NodeName: "node1"}
	node2 := nodetypes.BaseNode{NodeName: "node2"}
	node3 := nodetypes.BaseNode{NodeName: "node3"}
	node1.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node3"}}
	node2.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node1"}}
	node3.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node2"}}
	nodes := []nodetypes.Node{node1, node2, node3}
	return nodes, nil
}

// Cyclic Graph:
//
//	1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7
//	3 -> 1
//	6 -> 2
func largeFullyConnectedInvalidDAG() ([]nodetypes.Node, map[*partition]struct{}) {
	node1 := nodetypes.BaseNode{NodeName: "node1"}
	node2 := nodetypes.BaseNode{NodeName: "node2"}
	node3 := nodetypes.BaseNode{NodeName: "node3"}
	node4 := nodetypes.BaseNode{NodeName: "node4"}
	node5 := nodetypes.BaseNode{NodeName: "node5"}
	node6 := nodetypes.BaseNode{NodeName: "node6"}
	node7 := nodetypes.BaseNode{NodeName: "node7"}
	node1.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node3"}}
	node2.Deps = []dependencytypes.Dependency{
		dependencytypes.BaseDependency{BaseName: "node1"},
		dependencytypes.BaseDependency{BaseName: "node6"},
	}
	node3.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node2"}}
	node4.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node3"}}
	node5.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node4"}}
	node6.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node5"}}
	node7.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node6"}}
	nodes := []nodetypes.Node{node1, node2, node3, node4, node5, node6, node7}
	return nodes, nil
}

// Cyclic Graph:
//
//	1 -> 2 -> 3 -> 1
//	4 -> 5
func smallSemiConnectedInvalidDAG() ([]nodetypes.Node, map[*partition]struct{}) {
	node1 := nodetypes.BaseNode{NodeName: "node1"}
	node2 := nodetypes.BaseNode{NodeName: "node2"}
	node3 := nodetypes.BaseNode{NodeName: "node3"}
	node4 := nodetypes.BaseNode{NodeName: "node4"}
	node5 := nodetypes.BaseNode{NodeName: "node5"}
	node1.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node3"}}
	node2.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node1"}}
	node3.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node2"}}
	node5.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node5"}}
	nodes := []nodetypes.Node{node1, node2, node3, node4, node5}
	return nodes, nil
}

// Cyclic Graph:
//
//	1 -> 2 -> 3
//	2 -> 4
//	4 -> 1
//	5 -> 6 -> 7
func largeSemiConnectedInvalidDAG() ([]nodetypes.Node, map[*partition]struct{}) {
	node1 := nodetypes.BaseNode{NodeName: "node1"}
	node2 := nodetypes.BaseNode{NodeName: "node2"}
	node3 := nodetypes.BaseNode{NodeName: "node3"}
	node4 := nodetypes.BaseNode{NodeName: "node4"}
	node5 := nodetypes.BaseNode{NodeName: "node5"}
	node6 := nodetypes.BaseNode{NodeName: "node6"}
	node7 := nodetypes.BaseNode{NodeName: "node7"}
	node1.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node4"}}
	node2.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node1"}}
	node3.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node2"}}
	node4.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node2"}}
	node6.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node5"}}
	node7.Deps = []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "node6"}}
	nodes := []nodetypes.Node{node1, node2, node3, node4, node5, node6, node7}
	return nodes, nil
}
