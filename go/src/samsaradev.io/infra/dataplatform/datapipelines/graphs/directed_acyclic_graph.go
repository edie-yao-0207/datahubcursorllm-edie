package graphs

import (
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes"
)

type DirectedAcyclicGraph struct {
	nodes               []nodetypes.Node
	edges               map[string][]nodetypes.Node
	parents             map[string][]nodetypes.Node
	nameToNodeReference map[string]nodetypes.Node
}

func NewDAG(nodes []nodetypes.Node) (*DirectedAcyclicGraph, error) {
	dag := &DirectedAcyclicGraph{
		nodes:               nodes,
		edges:               make(map[string][]nodetypes.Node, len(nodes)),
		parents:             make(map[string][]nodetypes.Node, len(nodes)),
		nameToNodeReference: make(map[string]nodetypes.Node, len(nodes)),
	}

	for _, node := range nodes {
		dag.nameToNodeReference[node.Name()] = node
	}

	dag.buildEdges()
	if err := dag.validate(); err != nil {
		return nil, oops.Wrapf(err, "DirectedAcyclicGraph contains cycle.")
	}

	dag.nodes = dag.sortNodesByEdges()
	return dag, nil
}

// A utility function that sums the ascii values from all node names.
// // Used to naively sort a list of DirectedAcyclicGraph pointers.
// We assume that larger DAGs would have larger sums. Collisions can occur
// if the set of tables names in one DirectedAcyclicGraph are anagrams of
// another.
func sumNodeNamesAsciiValues(d *DirectedAcyclicGraph) int {
	sum := 0
	for _, n := range d.GetNodes() {
		for _, char := range n.Name() {
			sum += int(char)
		}
	}
	return sum
}

// A function that sorts a slice of DirectedAcyclicGraph pointers by the sum of the ascii values from
// all of the chars in the DirectedAcyclicGraph node names
func SortDirectedAcyclicGraphs(dags []*DirectedAcyclicGraph) []*DirectedAcyclicGraph {
	sort.Slice(dags, func(i, j int) bool {
		return sumNodeNamesAsciiValues(dags[i]) > sumNodeNamesAsciiValues(dags[j])
	})
	return dags
}

// Name will generate a unique name for a DAG based on the leaf nodes.
// Data Pipelines are generated from leaf nodes in the full DAG so this name guarantees uniqueness.
func (d *DirectedAcyclicGraph) Name() string {
	leafNodes := d.GetLeafNodes()
	parts := make([]string, len(leafNodes))
	for i, node := range leafNodes {
		parts[i] = node.Name()
	}
	sort.Strings(parts)
	return strings.Join(parts, "-")
}

// Utility function to print DirectedAcyclicGraph in .dot format
// Use this string in https://dreampuf.github.io/GraphvizOnline/ to render visualization
func (d *DirectedAcyclicGraph) String() string {
	var dotStr string
	for _, node := range d.nodes {
		dotStr += fmt.Sprintf("%q -> ", node.Name())
		edgeNodes := make([]string, 0, len(d.edges[node.Name()]))
		for _, edgeNode := range d.edges[node.Name()] {
			if _, ok := d.nameToNodeReference[edgeNode.Name()]; ok {
				edgeNodes = append(edgeNodes, fmt.Sprintf("%q", edgeNode.Name()))
			}
		}
		dotStr += fmt.Sprintf("{%s} ", strings.Join(edgeNodes, " "))
	}

	return dotStr
}

func (d *DirectedAcyclicGraph) GetNodes() []nodetypes.Node {
	return d.nodes
}

func (d *DirectedAcyclicGraph) GetNodeChildren(node nodetypes.Node) []nodetypes.Node {
	return d.edges[node.Name()]
}

func (d *DirectedAcyclicGraph) GetNodeParents(node nodetypes.Node) []nodetypes.Node {
	return d.parents[node.Name()]
}

func (d *DirectedAcyclicGraph) GetNodeFromName(name string) nodetypes.Node {
	return d.nameToNodeReference[name]
}

func (d *DirectedAcyclicGraph) GetLeafNodes() []nodetypes.Node {
	var leafNodes []nodetypes.Node
	for _, node := range d.nodes {
		if len(d.edges[node.Name()]) == 0 {
			leafNodes = append(leafNodes, node)
		}
	}

	return leafNodes
}

func (d *DirectedAcyclicGraph) IsProduction() bool {
	for _, node := range d.nodes {
		if len(d.edges[node.Name()]) == 0 {
			if !node.NonProduction() {
				return true
			}
		}
	}

	return false
}

// GetNodeChildrenRecursive returns all the children of the provided node.
// There is no guarantee of ordering provided.
func (d *DirectedAcyclicGraph) GetNodeChildrenRecursive(node nodetypes.Node) []nodetypes.Node {
	seen := make(map[string]struct{})
	stack := append([]nodetypes.Node{}, d.GetNodeChildren(node)...)
	var allChildren []nodetypes.Node

	for len(stack) != 0 {
		// pop latest element
		elem := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// skip processing if we've seen it
		if _, ok := seen[elem.Name()]; ok {
			continue
		}

		// add to all children, and then add elem's children to the stack
		allChildren = append(allChildren, elem)
		stack = append(stack, d.GetNodeChildren(elem)...)
		seen[elem.Name()] = struct{}{}
	}

	return allChildren
}

// sortNodesByEdges sorts a graphs nodes by the number of edges
// This order is needed during graph partitioning
func (d *DirectedAcyclicGraph) sortNodesByEdges() []nodetypes.Node {
	nodes := d.nodes
	sort.SliceStable(nodes, func(i, j int) bool {
		return len(d.edges[nodes[i].Name()]) > len(d.edges[nodes[j].Name()])
	})
	return nodes
}

// buildEdges creates a graph from a slice of nodes
func (d *DirectedAcyclicGraph) buildEdges() {
	for _, node := range d.nodes {
		for _, dep := range node.Dependencies() {
			// If the dependency is external, don't add an entry for its edges
			if dep.IsExternalDep() {
				continue
			}

			// Build graph bottom up. Attach dependency --> node
			d.edges[dep.Name()] = append(d.edges[dep.Name()], node)

			// Add dependency of the node as a parent of the node
			d.parents[node.Name()] = append(d.parents[node.Name()], d.nameToNodeReference[dep.Name()])
		}
	}
}

// validate verifies that the DirectedAcyclicGraph has no cycles and is a valid DirectedAcyclicGraph
func (d *DirectedAcyclicGraph) validate() error {
	marked := make(map[string]struct{}, len(d.nodes)) // map[node.Name()]<marked>
	var visitNode func(node nodetypes.Node) error
	visitNode = func(node nodetypes.Node) error {
		if _, ok := marked[node.Name()]; ok {
			return oops.Errorf("cycle with %s", node.Name())
		}

		// mark node as visited and defer un-marking until path has been walked
		marked[node.Name()] = struct{}{}
		defer delete(marked, node.Name())

		// walk edges of node recursively calling visitNode on edges
		for _, edgeNode := range d.edges[node.Name()] {
			if err := visitNode(edgeNode); err != nil {
				return oops.Wrapf(err, "%s", edgeNode.Name())
			}
		}

		return nil
	}

	for _, node := range d.nodes {
		if err := visitNode(node); err != nil {
			return oops.Wrapf(err, "%s", node.Name())
		}
	}

	return nil
}

type partition struct {
	nodes []nodetypes.Node
}

func (p *partition) toDAG(parentDAG *DirectedAcyclicGraph) (*DirectedAcyclicGraph, error) {
	dag := &DirectedAcyclicGraph{
		nodes:               p.nodes,
		edges:               make(map[string][]nodetypes.Node, len(p.nodes)),
		parents:             make(map[string][]nodetypes.Node, len(p.nodes)),
		nameToNodeReference: make(map[string]nodetypes.Node, len(p.nodes)),
	}
	for _, nodeToAdd := range p.nodes {
		if _, ok := parentDAG.nameToNodeReference[nodeToAdd.Name()]; !ok {
			return nil, oops.Errorf("Node does not exist")
		}
		dag.edges[nodeToAdd.Name()] = parentDAG.edges[nodeToAdd.Name()]
		dag.parents[nodeToAdd.Name()] = parentDAG.parents[nodeToAdd.Name()]
		dag.nameToNodeReference[nodeToAdd.Name()] = parentDAG.nameToNodeReference[nodeToAdd.Name()]
	}

	return dag, nil
}

func (p *partition) String() string {
	nodeNames := make([]string, len(p.nodes))
	for i, node := range p.nodes {
		nodeNames[i] = node.Name()
	}
	sort.Strings(nodeNames)
	return strings.Join(nodeNames, ", ")
}

func (d *DirectedAcyclicGraph) partition() map[*partition]struct{} {
	partitions := make(map[string]*partition, len(d.nodes)) // map[node.Name()]*partition
	var visitNode func(node nodetypes.Node, part *partition, visited map[string]bool) *partition

	// visitNode function recursively walks through a nodes dependencies adding them to the same partition
	visitNode = func(node nodetypes.Node, part *partition, visited map[string]bool) *partition {
		if visited[node.Name()] {
			return part
		}

		for _, dep := range d.nameToNodeReference[node.Name()].Dependencies() {
			if !dep.IsExternalDep() {
				dependencyNode := d.nameToNodeReference[dep.Name()]
				if dependencyNode == nil {
					log.Fatalf(`\nDependency %s not found in Pipeline Graph.\nYou may have forgot to mark it as external.\nCheck the developer guide for more information:\nhttps://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17757182/How+To+Use+the+Data+Pipelines+Framework`, dep.Name())
				}

				leafPartition := visitNode(dependencyNode, part, visited)
				if part == nil {
					part = leafPartition
				}
			}
		}

		if part == nil {
			part = &partition{}
		}

		partitions[node.Name()] = part
		visited[node.Name()] = true
		part.nodes = append(part.nodes, node)
		return part
	}

	// Create graph partitions for each leaf node. We loop over leaf nodes because they are independent of other leaf
	// nodes and each leaf node the anchor of its own DAG
	for _, node := range d.GetLeafNodes() {
		visited := map[string]bool{}
		visitNode(node, nil, visited)
	}

	// Filter partitions for unique partitions
	dagPartitions := map[*partition]struct{}{}
	for _, partition := range partitions {
		if _, ok := dagPartitions[partition]; ok {
			continue
		}

		dagPartitions[partition] = struct{}{}
	}

	return dagPartitions
}

func mergeNodeMaps(x map[string]nodetypes.Node, y map[string]nodetypes.Node) map[string]nodetypes.Node {
	merged := x
	for name, node := range y {
		merged[name] = node
	}
	return merged
}

// DisjointUnionDAGs takes a list of disjointed DirectedAcyclicGraphs and combines them based on shared nodes into a set of larger semiconnected graphs.
// The greedy algorithm groups Nodes that are shared across DAGs and re-generates semiconnected DAGs from them.
// Example:
// Dag1: 1 --> 2 --> 3
// Dag2: 2 --> 4
// Dag3: 5 --> 6
// DisjointUnionDAGs:
// Dag1:
//
//	1 --> 2 --> 3
//					--> 4
//
// Dag2:
//
//	5 --> 6
func DisjointUnionDAGs(d []*DirectedAcyclicGraph) ([]*DirectedAcyclicGraph, error) {

	var unionedDAGs []*DirectedAcyclicGraph
	dags := SortDirectedAcyclicGraphs(d)

	// Iterate over dynamically shrinking list of candidate DAGs.
	// While there are still DAGs to process
	for len(dags) > 0 {
		if len(dags) == 1 {
			unionedDAGs = append(unionedDAGs, dags...)
			dags = []*DirectedAcyclicGraph{}
			continue
		}

		dag := dags[0]
		dags = dags[1:]
		connectedNodesMap := dag.nameToNodeReference
		connectedDAGNodes := []nodetypes.Node{}
		connectedDAGNodes = append(connectedDAGNodes, dag.GetNodes()...)
		var connectedDAGIdx []int
		// find index of dags remaining in daglist that contains node
		for idx, d := range dags {
			// iterate over each node of remaining dags
			for _, node := range d.GetNodes() {
				// check if dag shares node. collect nodes and move to next dag if a shared node is found
				if _, ok := connectedNodesMap[node.Name()]; ok {
					connectedDAGNodes = append(connectedDAGNodes, d.GetNodes()...)
					connectedDAGIdx = append(connectedDAGIdx, idx)
					connectedNodesMap = mergeNodeMaps(connectedNodesMap, d.nameToNodeReference)
					break
				}
			}
		}

		// remove connected dags from remaining daglist and re-slice
		for idx, dagIdx := range connectedDAGIdx {
			connectedDAGNodes = append(connectedDAGNodes, dags[dagIdx-idx].GetNodes()...)
			dags = append(dags[:dagIdx-idx], dags[dagIdx+1-idx:]...)
		}
		unionedDAGNodes := []nodetypes.Node{}
		for _, n := range connectedNodesMap {
			unionedDAGNodes = append(unionedDAGNodes, n)
		}
		// generate NewDAG()
		sort.Slice(unionedDAGNodes, func(i, j int) bool {
			return unionedDAGNodes[i].Name() < unionedDAGNodes[j].Name()
		})
		connectedDAG, err := NewDAG(unionedDAGNodes)
		if err != nil {
			return nil, oops.Wrapf(err, "Error generating DirectedAcyclicGraphs")
		}
		// append NewDag to collection of semiconnected unioned dags
		unionedDAGs = append(unionedDAGs, connectedDAG)
	}
	return unionedDAGs, nil
}

// GenerateDAG builds DAGs from a slice of nodes and validates its structure
func GenerateDAGs(nodes []nodetypes.Node) ([]*DirectedAcyclicGraph, error) {
	dag, err := NewDAG(nodes)
	if err != nil {
		return nil, oops.Wrapf(err, "Invalid list of Nodes")
	}

	if err := ValidateDag(dag); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	// build DAGs from connected partitions
	dagPartitions := dag.partition()
	fullDAG := dag
	dags := make([]*DirectedAcyclicGraph, 0, len(dagPartitions))
	for partition := range dagPartitions {
		dagToAdd, err := partition.toDAG(fullDAG)
		if err != nil {
			return nil, oops.Wrapf(err, "Error creating partition DAGs")
		}
		dags = append(dags, dagToAdd)
	}

	return dags, nil
}

func BuildDAGs() ([]*DirectedAcyclicGraph, error) {
	configNodes, err := configvalidator.ReadNodeConfigurations()
	if err != nil {
		return nil, oops.Wrapf(err, "Error reading node configurations")
	}

	// Convert configuration nodes to DirectedAcyclicGraph nodes
	dagNodes := make([]nodetypes.Node, 0, len(configNodes))
	for _, configNode := range configNodes {
		dagNodes = append(dagNodes, nodetypes.NodeConfigToDAGNode(configNode))
	}

	dags, err := GenerateDAGs(dagNodes)
	if err != nil {
		return nil, oops.Wrapf(err, "Error generating DirectedAcyclicGraph")
	}

	return dags, nil
}

// This validation should be called on the *entire* dag of all nodes.
// It can be called on pipeline dags as well but its not as useful.
func ValidateDag(dag *DirectedAcyclicGraph) error {
	for _, node := range dag.GetNodes() {
		children := dag.GetNodeChildren(node)

		// The daily job run property can only be set on leaf nodes of a DAG
		if node.DailyRunConfig() != nil && len(dag.GetNodeChildren(node)) > 0 {
			childString := ""
			for _, child := range children {
				childString += child.Name() + " "
			}
			return oops.Errorf("Node %s cannot have daily run config set as it is not a leaf node. children: [%s]\n", node.Name(), childString)
		}

		// Pipeline config can only be set on leaf nodes of a DAG
		if node.PipelineConfig() != nil && len(dag.GetNodeChildren(node)) > 0 {
			childString := ""
			for _, child := range children {
				childString += child.Name() + " "
			}
			return oops.Errorf("Node %s cannot have pipeline config set as it is not a leaf node. children: [%s]\n", node.Name(), childString)
		}

		// A node cannot have children of any depth with differing daily configs.
		allChildren := dag.GetNodeChildrenRecursive(node)
		var starts []int64
		for _, child := range allChildren {
			if child.DailyRunConfig() != nil {
				starts = append(starts, child.DailyRunConfig().StartDateOffset)
			}
		}
		for i := 1; i < len(starts)-1; {
			if starts[i] != starts[i-1] {
				return oops.Errorf("Node %s has different daily configs for multiple parents.", node.Name())
			}
		}
	}
	return nil
}
