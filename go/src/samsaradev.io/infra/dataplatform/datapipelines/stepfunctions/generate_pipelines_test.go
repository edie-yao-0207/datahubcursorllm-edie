package stepfunctions

import (
	"fmt"
	"strings"
	"testing"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/dependencytypes"
	"samsaradev.io/infra/testloader"
	"samsaradev.io/team"
)

func TestGenerateStepFunctionDefinition(t *testing.T) {
	var env struct {
		Snap *snapshotter.Snapshotter
	}
	testloader.MustStart(t, &env)

	testCases := []struct {
		testName string
		dagGenFn func() (*graphs.DirectedAcyclicGraph, map[string]*TransformationFiles, error)
	}{
		{
			testName: "linear-pipeline",
			dagGenFn: linearPipeline,
		},
		{
			testName: "parallel-pipeline",
			dagGenFn: parallelPipeline,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("TestGenerateStepFunctionDefinition-%s", testCase.testName), func(t *testing.T) {
			dag, transformationFiles, err := testCase.dagGenFn()
			require.NoError(t, err)
			require.NoError(t, graphs.ValidateDag(dag))

			sfn, err := generateStepFunctionDefinition(dag, transformationFiles)
			require.NoError(t, err)

			rawSfn, err := sfn.MarshalIndentJSON()
			require.NoError(t, err)

			strSfn := strings.Replace(string(rawSfn), "\"", "'", -1)
			jsonPieces := strings.Split(strSfn, "\n")
			env.Snap.Snapshot(testCase.testName, jsonPieces)
		})
	}
}

// node1 --> node2 --> node3 -- node4
func linearPipeline() (*graphs.DirectedAcyclicGraph, map[string]*TransformationFiles, error) {

	node1 := nodetypes.NodeConfigToDAGNode(configvalidator.NodeConfiguration{
		Name:  "node1",
		Owner: team.DataPlatform,
	})
	node2 := nodetypes.NodeConfigToDAGNode(configvalidator.NodeConfiguration{
		Name:  "node2",
		Owner: team.DataPlatform,
		Dependencies: []dependencytypes.Dependency{
			dependencytypes.BaseDependency{
				BaseName: "node1",
			},
		},
	})
	node3 := nodetypes.NodeConfigToDAGNode(configvalidator.NodeConfiguration{
		Name:  "node3",
		Owner: team.DataPlatform,
		Dependencies: []dependencytypes.Dependency{
			dependencytypes.BaseDependency{
				BaseName: "node2",
			},
		},
	})
	node4 := nodetypes.NodeConfigToDAGNode(configvalidator.NodeConfiguration{
		Name:  "node4",
		Owner: team.DataPlatform,
		Dependencies: []dependencytypes.Dependency{
			dependencytypes.BaseDependency{
				BaseName: "node3",
			},
		},
	})

	nodes := []nodetypes.Node{
		node1,
		node2,
		node3,
		node4,
	}

	dag, err := graphs.NewDAG(nodes)
	return dag, genTransformationFilesMap(nodes), err
}

// node1 --> node3
// node2 --> node3 --> node4
func parallelPipeline() (*graphs.DirectedAcyclicGraph, map[string]*TransformationFiles, error) {
	node1 := nodetypes.NodeConfigToDAGNode(configvalidator.NodeConfiguration{
		Name:  "node1",
		Owner: team.DataPlatform,
	})
	node2 := nodetypes.NodeConfigToDAGNode(configvalidator.NodeConfiguration{
		Name:  "node2",
		Owner: team.DataPlatform,
	})
	node3 := nodetypes.NodeConfigToDAGNode(configvalidator.NodeConfiguration{
		Name:  "node3",
		Owner: team.DataPlatform,
		Dependencies: []dependencytypes.Dependency{
			dependencytypes.BaseDependency{
				BaseName: "node1",
			},
			dependencytypes.BaseDependency{
				BaseName: "node2",
			},
		},
	})
	node4 := nodetypes.NodeConfigToDAGNode(configvalidator.NodeConfiguration{
		Name:  "node4",
		Owner: team.DataPlatform,
		Dependencies: []dependencytypes.Dependency{
			dependencytypes.BaseDependency{
				BaseName: "node3",
			},
		},
	})

	nodes := []nodetypes.Node{
		node1,
		node2,
		node3,
		node4,
	}

	dag, err := graphs.NewDAG(nodes)
	return dag, genTransformationFilesMap(nodes), err
}

func genTransformationFilesMap(nodes []nodetypes.Node) map[string]*TransformationFiles {
	transformationFiles := make(map[string]*TransformationFiles, len(nodes))
	for _, node := range nodes {
		transformationFiles[node.Name()] = &TransformationFiles{
			Region: "us-west-2",
		}
	}
	return transformationFiles
}
