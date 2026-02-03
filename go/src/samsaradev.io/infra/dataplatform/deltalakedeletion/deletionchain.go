package deltalakedeletion

import (
	"fmt"
	"sort"
	"strings"

	"samsaradev.io/infra/app/generate_terraform/consts/ni/awsconsts"
	"samsaradev.io/infra/dataplatform/amundsen/metadatagenerator"
	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/dependencytypes"

	"github.com/samsarahq/go/oops"
)

// DeletionPlan is a set of table deletion specifications from the deletion registry
// arranged in the sequence in which deletion will occur. This sequence is generated from known table lineage metadata.
// (See https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17735010/RFC+Data+Lake+Deletion+Retention+Framework#DeletionRegistry---Opting-in)
type DeletionPlan []*TableDeletionSpec

// DeletionChain is a container for prioritizing and parallelizing the execution of DeletionPlans
type DeletionChain struct {
	PriorityLevelOne   []DeletionPlan
	PriorityLevelTwo   []DeletionPlan
	PriorityLevelThree []DeletionPlan
}

// This helper function takes the given map and returns a new map whose keys are the values of the
// incoming map and the values are the keys
func invertMap(l map[string][]string) map[string][]string {

	invertedMap := map[string][]string{}
	for nodeName, childrenNames := range l {
		if _, ok := invertedMap[nodeName]; !ok {
			invertedMap[nodeName] = []string{}
		}
		for _, childName := range childrenNames {
			invertedMap[childName] = append(invertedMap[childName], nodeName)
		}
	}
	return invertedMap
}

func loadNodeConfigurationsFromLineageMetadata(l map[string][]string) []configvalidator.NodeConfiguration {

	// Our data lineage metadata is stored in node -> [children] relationships but the DirectedAcyclicGraph package requires
	// node -> [parents] relationship. This inverts the map of node -> [children] relationships to node -> [parents] relationships.
	nodeParentLineageMetadata := invertMap(l)
	for node, dependencies := range nodeParentLineageMetadata {
		dep := []string{}
		for _, d := range dependencies {
			database := strings.Split(d, ".")[0]
			if database != "kinesisstats" && database != "kinesisstats_history" && database != "s3bigstats" && database != "s3bigstats_history" && database != "datastreams" && database != "datastreams_history" {
				dep = append(dep, d)
			}
		}
		nodeParentLineageMetadata[node] = dep
	}

	var nodeConfigs []configvalidator.NodeConfiguration
	for nodeName, parentNames := range nodeParentLineageMetadata {
		nodeConfig := configvalidator.NodeConfiguration{
			Name: nodeName,
		}
		dependencies := []dependencytypes.Dependency{}
		for _, parentName := range parentNames {
			var dep dependencytypes.Dependency = dependencytypes.BaseDependency{
				BaseName: parentName,
			}
			dependencies = append(dependencies, dep)
		}
		nodeConfig.Dependencies = dependencies
		nodeConfigs = append(nodeConfigs, nodeConfig)
	}
	return nodeConfigs
}

func sortDeletionPlans(plans []DeletionPlan) []DeletionPlan {
	// plans are sorted in lexicographically by name of first table in plan. Since table names are unique
	// we do not expect any collisions in ordering.
	sort.Slice(plans, func(i, j int) bool {
		return fmt.Sprintf("%s.%s", plans[i][0].Database, plans[i][0].Table) < fmt.Sprintf("%s.%s", plans[j][0].Database, plans[j][0].Table)
	})
	return plans
}

func buildDeletionChain(deletionRegistry []TableDeletionSpec, dags []*graphs.DirectedAcyclicGraph) *DeletionChain {
	var dc DeletionChain

	deletionRegistryMap := map[string]TableDeletionSpec{}
	for _, spec := range deletionRegistry {
		name := fmt.Sprintf("%s.%s", spec.Database, spec.Table)
		deletionRegistryMap[name] = spec
	}
	tableDeletionSpecsToDelete := deletionRegistryMap

	// Set the tables from our primary data sources (eg. 'kinesisstats', 's3bigstats', 'datastreams') to be
	// deleted first since these are the root data sources for the majority of tables.
	for key, spec := range tableDeletionSpecsToDelete {
		s := spec
		if spec.Group == "kinesisstats" || spec.Group == "s3bigstats" || spec.Group == "datastreams" {
			dc.PriorityLevelOne = append(dc.PriorityLevelOne, []*TableDeletionSpec{&s})
			delete(tableDeletionSpecsToDelete, key)
		}
	}

	// Build the deletion plan for tables with known lineage.
	// We iterate over each disjoint DAG, performing a breadth-first traversal of the children nodes and
	// appending those in the deletion registry to a deletion plan for that DAG.
	for _, dag := range dags {
		leafNodeMap := map[string]nodetypes.Node{}
		leafNodes := dag.GetLeafNodes()
		sort.Slice(leafNodes, func(i, j int) bool {
			return leafNodes[i].Name() < leafNodes[j].Name()
		})
		for _, node := range leafNodes {
			leafNodeMap[node.Name()] = node
		}

		flattenedNodes := []nodetypes.Node{}
		nodesToTraverse := []nodetypes.Node{}
		// find root nodes in dag (i.e., nodes with no parents)
		for _, node := range dag.GetNodes() {
			if len(dag.GetNodeParents(node)) == 0 {
				flattenedNodes = append(flattenedNodes, node)
				nodesToTraverse = append(nodesToTraverse, node)
			}
		}
		for len(nodesToTraverse) > 0 {
			childrenNodes := []nodetypes.Node{}
			for _, n := range nodesToTraverse {
				nodeChildren := dag.GetNodeChildren(n)
				sort.Slice(nodeChildren, func(i, j int) bool {
					return nodeChildren[i].Name() < nodeChildren[j].Name()
				})
				flattenedNodes = append(flattenedNodes, nodeChildren...)
				childrenNodes = append(childrenNodes, nodeChildren...)
			}
			nodesToTraverse = childrenNodes
		}

		deletionDAG := []*TableDeletionSpec{}
		for _, node := range flattenedNodes {
			if spec, ok := deletionRegistryMap[node.Name()]; ok {
				// skip over leaf nodes since they are explicitly appended at the end
				if _, ok := leafNodeMap[node.Name()]; ok {
					continue
				}
				deletionDAG = append(deletionDAG, &spec)
				delete(tableDeletionSpecsToDelete, node.Name())
			}
		}
		// append any leaf nodes registered for deletion to the end
		for _, node := range leafNodes {
			if spec, ok := deletionRegistryMap[node.Name()]; ok {
				deletionDAG = append(deletionDAG, &spec)
				delete(tableDeletionSpecsToDelete, node.Name())
			}
		}

		// We want to delete tables with known lineage data first.
		// It is likely that tables without known lineage data would depend on at least one table whose
		// lineage is known. Ensuring these tables are deleted afterwards is our 'best effort' to ensure
		// proper deletion order without lineage data.
		if len(deletionDAG) > 1 {
			dc.PriorityLevelTwo = append(dc.PriorityLevelTwo, deletionDAG)
		} else if len(deletionDAG) == 1 {
			dc.PriorityLevelThree = append(dc.PriorityLevelThree, deletionDAG)
		}
	}
	// Add plans for remaining tables from registry
	if len(tableDeletionSpecsToDelete) > 0 {
		for key := range tableDeletionSpecsToDelete {
			spec := tableDeletionSpecsToDelete[key]
			dc.PriorityLevelThree = append(dc.PriorityLevelThree, []*TableDeletionSpec{&spec})
		}
	}

	dc.PriorityLevelOne = sortDeletionPlans(dc.PriorityLevelOne)
	dc.PriorityLevelTwo = sortDeletionPlans(dc.PriorityLevelTwo)
	dc.PriorityLevelThree = sortDeletionPlans(dc.PriorityLevelThree)

	return &dc
}

func GenerateDeletionChain(region string) (*DeletionChain, error) {

	var deletionRegistry []TableDeletionSpec
	switch region {
	case awsconsts.Region.USWest2:
		deletionRegistry = AllUSDeletionRegistry
	case awsconsts.Region.EUWest1:
		deletionRegistry = AllEUDeletionRegistry
	case awsconsts.Region.CACentral1:
		deletionRegistry = AllCARegionDeletionRegistry
	}

	dataLineageMetadata, err := metadatagenerator.LoadDataLineageMetadata()
	if err != nil {
		return nil, oops.Wrapf(err, "failed to load lineage metadata")
	}

	nodeConfigs := loadNodeConfigurationsFromLineageMetadata(dataLineageMetadata)
	var allDAGNodes []nodetypes.Node
	for _, nodeConfig := range nodeConfigs {
		allDAGNodes = append(allDAGNodes, nodetypes.NodeConfigToDAGNode(nodeConfig))
	}

	// Remove nodes from primary data sources. These are deleted in a higher priority level. This also allows us to generate
	// more disjointed dags by removing the most root nodes. This helps increase parallelism since we run deletion on each plan
	// within a priority level concurrently.
	dagNodes := []nodetypes.Node{}
	primaryNodes := []nodetypes.Node{}
	for _, node := range allDAGNodes {
		database := strings.Split(node.Name(), ".")[0]
		if database == "kinesisstats_history" || database == "kinesisstats" || database == "s3bigstats" || database == "datastreams_schema" {
			primaryNodes = append(primaryNodes, node)
		} else {
			dagNodes = append(dagNodes, node)
		}
	}

	dags, err := graphs.GenerateDAGs(dagNodes)
	if err != nil {
		return nil, oops.Wrapf(err, "Error generating DirectedAcyclicGraphs")
	}
	unionedDAGs, err := graphs.DisjointUnionDAGs(dags)
	if err != nil {
		return nil, oops.Wrapf(err, "Error unioning DirectedAcyclicGraphs")
	}

	primaryNodesDAG, err := graphs.NewDAG(primaryNodes)
	if err != nil {
		return nil, oops.Wrapf(err, "Error generating DirectedAcyclicGraphs for primary nodes")
	}
	unionedDAGs = append(unionedDAGs, primaryNodesDAG)
	deletionChain := buildDeletionChain(deletionRegistry, unionedDAGs)

	return deletionChain, nil
}
