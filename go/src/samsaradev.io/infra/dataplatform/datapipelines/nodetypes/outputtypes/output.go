package outputtypes

import (
	"encoding/json"

	"github.com/samsarahq/go/oops"
)

// OutputType is the output from a Node.
// Currently supported:
//   - table
type OutputType string

const (
	TableOutputType = OutputType("table")
)

type Output interface {
	name() string
	validate() error
	DAGName() string
	DestinationPrefix() string
	IsDateSharded() bool
}

// Output is a generic struct for parsing out the output type to dictate which type of output to build
type BaseOutput struct {
	Type OutputType `json:"type"`
}

func ParseRawOutput(rawOutput json.RawMessage) (Output, error) {
	var baseOutput BaseOutput
	err := json.Unmarshal(rawOutput, &baseOutput)
	if err != nil {
		return nil, err
	}

	var output Output
	switch baseOutput.Type {
	case TableOutputType:
		output = &TableOutput{}
	default:
		return nil, oops.Errorf("Unknown output type: %s", baseOutput.Type)
	}

	err = json.Unmarshal(rawOutput, output)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	err = output.validate()
	if err != nil {
		return nil, oops.Wrapf(err, "Invalid output field")
	}

	return output, nil
}
