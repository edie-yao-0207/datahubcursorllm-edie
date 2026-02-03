package dependencytypes

import (
	"encoding/json"

	"github.com/samsarahq/go/oops"
)

// DependencyType is the type of the dependency.
// Currently supported:
//   - table
type DependencyType string

const (
	TableDependencyType = DependencyType("table")
)

// Dependency is an interface that all dependencies will need to implement
type Dependency interface {
	Name() string
	// An External Dependency is a dependency not managed by Step Function/Lambda Data Pipelines.
	// Example: KS and RDS tables are external table dependencies.
	IsExternalDep() bool
}

// BaseDependency is a generic struct for parsing out the type to dictate which type of dependency to build
type BaseDependency struct {
	BaseName string         `json:"-"`
	Type     DependencyType `json:"type"`

	// The external key dictates whether the dependency is external (not managed by the framework i.e notebook or s3 tables deps)
	// These dependencies are not be added to graph or created in St
	External bool `json:"external"`
}

func (bd BaseDependency) Name() string {
	return bd.BaseName
}

func (bd BaseDependency) IsExternalDep() bool {
	return bd.External
}

func ParseRawDependency(rawDep json.RawMessage) (Dependency, error) {
	var baseDep BaseDependency
	err := json.Unmarshal(rawDep, &baseDep)
	if err != nil {
		return nil, err
	}

	var dep Dependency
	switch baseDep.Type {
	case TableDependencyType:
		dep = &TableDependency{}
	default:
		return nil, oops.Errorf("Unknown dependency type: %s", baseDep.Type)
	}

	err = json.Unmarshal(rawDep, dep)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return dep, nil

}
