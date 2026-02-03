package deltalakedeletion

import (
	"sort"
	"testing"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes"

	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/dependencytypes"
	"samsaradev.io/infra/testloader"
)

func TestInvertMap(t *testing.T) {

	var env struct {
		Snapper *snapshotter.Snapshotter
	}
	testloader.MustStart(t, &env)
	snap := env.Snapper

	testCases := []struct {
		testName string
		dagFunc  func() (map[string][]string, map[string][]string)
	}{
		{
			testName: "smallFullyConnectedDAGLineage",
			dagFunc:  smallFullyConnectedDAGLineage,
		},
		{
			testName: "smallSemiConnectedDAGLineage",
			dagFunc:  smallSemiConnectedDAGLineage,
		},
		{
			testName: "largeFullyConnectedDAGLineage",
			dagFunc:  largeFullyConnectedDAGLineage,
		},
		{
			testName: "largeSemiConnectedDAGLineage",
			dagFunc:  largeSemiConnectedDAGLineage,
		},
		{
			testName: "fullyConnectedSinglePathDAGLineage",
			dagFunc:  fulllyConnectedSinglePathDAGLineage,
		},
	}

	for _, tc := range testCases {
		nodesToChildren, expectedNodesToParents := tc.dagFunc()
		actualNodesToParents := invertMap(nodesToChildren)
		for _, p := range actualNodesToParents {
			sort.Strings(p)
		}
		assert.Equal(t, expectedNodesToParents, actualNodesToParents)
		snap.Snapshot(tc.testName, actualNodesToParents)
	}

}
func TestLoadNodeConfigurationsFromLineageMetadata(t *testing.T) {
	testCases := map[string]func() (map[string][]string, map[string][]string){
		"smallSemiConnectedDAG": smallSemiConnectedDAGLineage,
	}

	expectedNodeConfigurations := []configvalidator.NodeConfiguration{
		{Name: "node1", Dependencies: []dependencytypes.Dependency{}},
		{Name: "node2", Dependencies: []dependencytypes.Dependency{}},
		{Name: "node4", Dependencies: []dependencytypes.Dependency{}},
		{
			Name: "node3",
			Dependencies: []dependencytypes.Dependency{
				dependencytypes.BaseDependency{
					BaseName: "node1",
				},
				dependencytypes.BaseDependency{
					BaseName: "node2",
				},
			},
		},
		{
			Name: "node5",
			Dependencies: []dependencytypes.Dependency{
				dependencytypes.BaseDependency{
					BaseName: "node4",
				},
			},
		},
		{
			Name: "node6",
			Dependencies: []dependencytypes.Dependency{
				dependencytypes.BaseDependency{
					BaseName: "node5",
				},
			},
		},
		{
			Name: "node7",
			Dependencies: []dependencytypes.Dependency{
				dependencytypes.BaseDependency{
					BaseName: "node2",
				},
			},
		},
	}

	for name, dagFunc := range testCases {
		t.Run(name, func(t *testing.T) {
			nodesToChildren, _ := dagFunc()
			actualNodeConfigurations := loadNodeConfigurationsFromLineageMetadata(nodesToChildren)
			for idx, _ := range actualNodeConfigurations {
				sort.Slice(actualNodeConfigurations[idx].Dependencies, func(i, j int) bool {
					return actualNodeConfigurations[idx].Dependencies[i].Name() < actualNodeConfigurations[idx].Dependencies[j].Name()
				})
			}
			require.ElementsMatch(t, actualNodeConfigurations, expectedNodeConfigurations)
		})
	}
}

func TestBuildDeletionChain(t *testing.T) {
	testCases := map[string]func() ([]TableDeletionSpec, [][]nodetypes.Node){
		"largeSemiConnectedValidDAG":  largeSemiConnectedValidDAG,
		"largeFullyConnectedValidDAG": largeFullyConnectedValidDAG,
	}

	expectedDeletionChains := map[string]DeletionChain{
		"largeSemiConnectedValidDAG": DeletionChain{
			PriorityLevelOne: []DeletionPlan{
				DeletionPlan{
					{
						Database:                     "db",
						Table:                        "node7",
						CustomerIdentificationColumn: "org_id",
						Group:                        "kinesisstats",
						Format:                       "delta",
					},
				},
			},
			PriorityLevelTwo: []DeletionPlan{
				DeletionPlan{
					{
						Database:                     "db",
						Table:                        "node1",
						CustomerIdentificationColumn: "org_id",
						Group:                        "notebooks",
						Format:                       "delta",
					},
					{
						Database:                     "db",
						Table:                        "node4",
						CustomerIdentificationColumn: "org_id",
						Group:                        "notebooks",
						Format:                       "delta",
					},
					{
						Database:                     "db",
						Table:                        "node3",
						CustomerIdentificationColumn: "org_id",
						Group:                        "datapipelines",
						Format:                       "delta",
					}},
				DeletionPlan{
					{
						Database:                     "db",
						Table:                        "node11",
						CustomerIdentificationColumn: "org_id",
						Group:                        "datapipelines",
						Format:                       "delta",
					},
					{
						Database:                     "db",
						Table:                        "node8",
						CustomerIdentificationColumn: "org_id",
						Group:                        "datapipelines",
						Format:                       "delta",
					},
					{
						Database:                     "db",
						Table:                        "node10",
						CustomerIdentificationColumn: "org_id",
						Group:                        "datapipelines",
						Format:                       "delta",
					},
				},
			},
			PriorityLevelThree: []DeletionPlan{
				DeletionPlan{
					{
						Database:                     "db",
						Table:                        "node5",
						CustomerIdentificationColumn: "org_id",
						Group:                        "datapipelines",
						Format:                       "delta",
					},
				},
			},
		},
		"largeFullyConnectedValidDAG": DeletionChain{
			PriorityLevelOne: []DeletionPlan{
				DeletionPlan{
					{
						Database:                     "db",
						Table:                        "node1",
						CustomerIdentificationColumn: "org_id",
						Group:                        "kinesisstats",
						Format:                       "delta",
					}},
					DeletionPlan{
						{
							Database:                     "db",
							Table:                        "node4",
							CustomerIdentificationColumn: "org_id",
							Group:                        "s3bigstats",
							Format:                       "delta",
						}},
			},
			PriorityLevelTwo: []DeletionPlan{
				DeletionPlan{
					{
						Database:                     "db",
						Table:                        "node2",
						CustomerIdentificationColumn: "org_id",
						Group:                        "datapipelines",
						Format:                       "delta",
					},
					{
						Database:                     "db",
						Table:                        "node5",
						CustomerIdentificationColumn: "org_id",
						Group:                        "datapipelines",
						Format:                       "delta",
					},
					{
						Database:                     "db",
						Table:                        "node6",
						CustomerIdentificationColumn: "org_id",
						Group:                        "datapipelines",
						Format:                       "delta",
					},
					{
						Database:                     "db",
						Table:                        "node8",
						CustomerIdentificationColumn: "org_id",
						Group:                        "datapipelines",
						Format:                       "delta",
					},
				}},
		},
	}

	for name, dagFunc := range testCases {
		t.Run(name, func(t *testing.T) {
			registry, dagsNodes := dagFunc()
			dags := []*graphs.DirectedAcyclicGraph{}
			for _, nodes := range dagsNodes {

				d, err := graphs.NewDAG(nodes)
				require.NoError(t, err)
				dags = append(dags, d)
			}
			actualDeletionChain := buildDeletionChain(registry, dags)

			assert.Equal(t, expectedDeletionChains[name].PriorityLevelOne, actualDeletionChain.PriorityLevelOne)
			assert.Equal(t, expectedDeletionChains[name].PriorityLevelTwo, actualDeletionChain.PriorityLevelTwo)
			assert.Equal(t, expectedDeletionChains[name].PriorityLevelThree, actualDeletionChain.PriorityLevelThree)
		})
	}
}

func smallFullyConnectedDAGLineage() (map[string][]string, map[string][]string) {
	return map[string][]string{
			"node4": []string{"node5"},
			"node5": []string{"node6"},
		}, map[string][]string{
			"node4": []string{},
			"node5": []string{"node4"},
			"node6": []string{"node5"},
		}
}

func smallSemiConnectedDAGLineage() (map[string][]string, map[string][]string) {
	return map[string][]string{
			"node1": []string{"node3"},
			"node2": []string{"node3", "node7"},
			"node4": []string{"node5"},
			"node5": []string{"node6"},
		}, map[string][]string{
			"node1": []string{},
			"node2": []string{},
			"node4": []string{},
			"node7": []string{"node2"},
			"node3": []string{"node1", "node2"},
			"node5": []string{"node4"},
			"node6": []string{"node5"},
		}
}

func largeFullyConnectedDAGLineage() (map[string][]string, map[string][]string) {
	return map[string][]string{
			"node1": []string{"node3"},
			"node2": []string{"node3", "node7"},
			"node4": []string{"node5"},
			"node5": []string{"node6"},
		}, map[string][]string{
			"node1": []string{},
			"node2": []string{},
			"node4": []string{},
			"node7": []string{"node2"},
			"node3": []string{"node1", "node2"},
			"node5": []string{"node4"},
			"node6": []string{"node5"},
		}
}

func largeSemiConnectedDAGLineage() (map[string][]string, map[string][]string) {
	return map[string][]string{
			"node7":  []string{"node8", "node11"},
			"node11": []string{"node10", "node12"},
			"node2":  []string{"node3"},
			"node4":  []string{"node3"},
			"node6":  []string{"node9"},
			"node1":  []string{"node2"},
			"node8":  []string{"node10"},
			"node5":  []string{"node6"},
		}, map[string][]string{
			"node1":  []string{},
			"node4":  []string{},
			"node5":  []string{},
			"node7":  []string{},
			"node10": []string{"node11", "node8"},
			"node12": []string{"node11"},
			"node8":  []string{"node7"},
			"node11": []string{"node7"},
			"node3":  []string{"node2", "node4"},
			"node2":  []string{"node1"},
			"node9":  []string{"node6"},
			"node6":  []string{"node5"},
		}
}

// 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9
func fulllyConnectedSinglePathDAGLineage() (map[string][]string, map[string][]string) {
	return map[string][]string{
			"node1": []string{"node2"},
			"node2": []string{"node3"},
			"node3": []string{"node4"},
			"node4": []string{"node5"},
			"node5": []string{"node6"},
			"node6": []string{"node7"},
			"node7": []string{"node8"},
			"node8": []string{"node9"},
		}, map[string][]string{
			"node9": []string{"node8"},
			"node8": []string{"node7"},
			"node7": []string{"node6"},
			"node6": []string{"node5"},
			"node5": []string{"node4"},
			"node4": []string{"node3"},
			"node3": []string{"node2"},
			"node2": []string{"node1"},
			"node1": []string{},
		}
}

// "node2" -> {"node3" "node6"} "node6" -> {"node7" "node8"} "node1" -> {"node2"} "node3" -> {"node7"} "node4" -> {"node3"} "node5" -> {"node6"} "node7" -> {"node9"} "node8" -> {"node9"} "node9" -> {}
func largeFullyConnectedValidDAG() ([]TableDeletionSpec, [][]nodetypes.Node) {
	return []TableDeletionSpec{
			{
				Database:                     "db",
				Table:                        "node1",
				CustomerIdentificationColumn: "org_id",
				Group:                        "kinesisstats",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node4",
				CustomerIdentificationColumn: "org_id",
				Group:                        "s3bigstats",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node2",
				CustomerIdentificationColumn: "org_id",
				Group:                        "datapipelines",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node6",
				CustomerIdentificationColumn: "org_id",
				Group:                        "datapipelines",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node8",
				CustomerIdentificationColumn: "org_id",
				Group:                        "datapipelines",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node5",
				CustomerIdentificationColumn: "org_id",
				Group:                        "datapipelines",
				Format:                       "delta",
			},
		},
		[][]nodetypes.Node{
			[]nodetypes.Node{
				nodetypes.BaseNode{NodeName: "db.node2", Deps: []dependencytypes.Dependency{}},
				nodetypes.BaseNode{NodeName: "db.node3", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "db.node2"},
				}},
				nodetypes.BaseNode{NodeName: "db.node5"},
				nodetypes.BaseNode{NodeName: "db.node6", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "db.node2"},
					dependencytypes.BaseDependency{BaseName: "db.node5"},
				}},
				nodetypes.BaseNode{NodeName: "db.node7", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "db.node3"},
					dependencytypes.BaseDependency{BaseName: "db.node6"},
				}},
				nodetypes.BaseNode{NodeName: "db.node8", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "db.node6"},
				}},
				nodetypes.BaseNode{NodeName: "db.node9", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "db.node7"},
					dependencytypes.BaseDependency{BaseName: "db.node8"},
				}},
			},
			[]nodetypes.Node{
				nodetypes.BaseNode{NodeName: "db.node1"},
			},
			[]nodetypes.Node{
				nodetypes.BaseNode{NodeName: "db.node4"},
			},
		}
}

// "node7" -> {"node8" "node11"} "node11" -> {"node10" "node12"} "node2" -> {"node3"} "node4" -> {"node3"} "node6" -> {"node9"} "node1" -> {"node2"} "node8" -> {"node10"} "node5" -> {"node6"} "node3" -> {} "node9" -> {} "node10" -> {} "node12" -> {}
func largeSemiConnectedValidDAG() ([]TableDeletionSpec, [][]nodetypes.Node) {
	return []TableDeletionSpec{
			{
				Database:                     "db",
				Table:                        "node1",
				CustomerIdentificationColumn: "org_id",
				Group:                        "notebooks",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node4",
				CustomerIdentificationColumn: "org_id",
				Group:                        "notebooks",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node3",
				CustomerIdentificationColumn: "org_id",
				Group:                        "datapipelines",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node7",
				CustomerIdentificationColumn: "org_id",
				Group:                        "kinesisstats",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node8",
				CustomerIdentificationColumn: "org_id",
				Group:                        "datapipelines",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node11",
				CustomerIdentificationColumn: "org_id",
				Group:                        "datapipelines",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node10",
				CustomerIdentificationColumn: "org_id",
				Group:                        "datapipelines",
				Format:                       "delta",
			},
			{
				Database:                     "db",
				Table:                        "node5",
				CustomerIdentificationColumn: "org_id",
				Group:                        "datapipelines",
				Format:                       "delta",
			},
		},
		[][]nodetypes.Node{
			[]nodetypes.Node{
				nodetypes.BaseNode{NodeName: "db.node5"},
				nodetypes.BaseNode{NodeName: "db.node6", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "db.node5"}}},
				nodetypes.BaseNode{NodeName: "db.node9", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "db.node6"}}},
			},
			[]nodetypes.Node{
				nodetypes.BaseNode{NodeName: "db.node1"},
				nodetypes.BaseNode{NodeName: "db.node2", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "db.node1"}}},
				nodetypes.BaseNode{NodeName: "db.node3", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "db.node2"},
					dependencytypes.BaseDependency{BaseName: "db.node4"},
				}},
				nodetypes.BaseNode{NodeName: "db.node4"},
			},
			[]nodetypes.Node{
				nodetypes.BaseNode{NodeName: "db.node8"},
				nodetypes.BaseNode{NodeName: "db.node10", Deps: []dependencytypes.Dependency{
					dependencytypes.BaseDependency{BaseName: "db.node8"},
					dependencytypes.BaseDependency{BaseName: "db.node11"},
				}},
				nodetypes.BaseNode{NodeName: "db.node11"},
				nodetypes.BaseNode{NodeName: "db.node12", Deps: []dependencytypes.Dependency{dependencytypes.BaseDependency{BaseName: "db.node11"}}},
			},
			[]nodetypes.Node{
				nodetypes.BaseNode{NodeName: "db.node7"},
			},
		}
}
