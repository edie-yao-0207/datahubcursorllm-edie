// Package kinesisstats provides operational automation for KinesisStats-related tasks.
package kinesisstats

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/ksdeltalake"
)

// Output formats supported by ShowRegistryOp.
const (
	OutputFormatJSON = "json"
	OutputFormatCSV  = "csv"
)

// StatRegistryEntry describes one stat registry entry.
type StatRegistryEntry struct {
	StatName     string `json:"statName"`
	IsProduction bool   `json:"isProduction"`
	IsDataModel  bool   `json:"isDataModel"`
	Frequency    string `json:"frequency"`
}

// ShowRegistryResult is the typed result returned by ShowRegistryOp.Execute().
// Use type assertion to access: result.(*kinesisstats.ShowRegistryResult)
type ShowRegistryResult struct {
	Entries []StatRegistryEntry `json:"entries"`
}

// ShowRegistryOp prints the KinesisStats/S3BigStats registry entries and their frequency.
// This is a read-only operation: all work happens in Plan(), and Execute() returns the typed result.
type ShowRegistryOp struct {
	// Input
	OutputFormat string

	// Internal state populated during Plan()
	entries []StatRegistryEntry
}

// NewShowRegistryOp creates a new show-registry operation.
func NewShowRegistryOp(outputFormat string) *ShowRegistryOp {
	return &ShowRegistryOp{
		OutputFormat: outputFormat,
	}
}

// Name implements dataplatops.Operation.
func (o *ShowRegistryOp) Name() string {
	return "show-registry"
}

// Description implements dataplatops.Operation.
func (o *ShowRegistryOp) Description() string {
	return "Show the KinesisStats registry entries and their schedules"
}

// Validate implements dataplatops.Operation.
func (o *ShowRegistryOp) Validate(ctx context.Context) error {
	_ = ctx
	if o.OutputFormat == "" {
		o.OutputFormat = OutputFormatJSON
	}
	switch o.OutputFormat {
	case OutputFormatJSON, OutputFormatCSV:
		return nil
	default:
		return oops.Errorf("--output-format must be one of: %s, %s", OutputFormatJSON, OutputFormatCSV)
	}
}

// frequencyHourToLabel converts a replication frequency hour string to a human-readable label.
func frequencyHourToLabel(hour string) string {
	switch hour {
	case "1":
		return "hourly"
	case "3":
		return "every_3_hours"
	case "6":
		return "every_6_hours"
	default:
		return "every_" + hour + "_hours"
	}
}

func tableEntries(tb ksdeltalake.Table) []StatRegistryEntry {
	// Determine KS stat frequency: hourly override takes precedence, otherwise use canonical method.
	ksFrequency := frequencyHourToLabel(tb.GetReplicationFrequencyHour())

	entries := []StatRegistryEntry{
		{
			StatName:     fmt.Sprintf("kinesisstats.%s", tb.Name),
			IsProduction: tb.Production,
			IsDataModel:  tb.DataModelStat,
			Frequency:    ksFrequency,
		},
	}

	if tb.S3BigStatSchema != nil {
		entries = append(entries, StatRegistryEntry{
			StatName:     fmt.Sprintf("s3bigstats.%s", tb.Name),
			IsProduction: tb.BigStatProduction,
			IsDataModel:  tb.DataModelStat,
			Frequency:    ksFrequency,
		})
	}

	return entries
}

func (o *ShowRegistryOp) buildEntries() []StatRegistryEntry {
	var entries []StatRegistryEntry
	for _, tb := range ksdeltalake.AllTables() {
		entries = append(entries, tableEntries(tb)...)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].StatName < entries[j].StatName
	})
	return entries
}

func (o *ShowRegistryOp) printJSON() error {
	out := ShowRegistryResult{Entries: o.entries}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return oops.Wrapf(err, "failed to marshal json")
	}
	fmt.Println(string(b))
	return nil
}

func (o *ShowRegistryOp) printCSV() error {
	w := csv.NewWriter(os.Stdout)
	if err := w.Write([]string{"stat_name", "is_production", "is_data_model", "frequency"}); err != nil {
		return oops.Wrapf(err, "failed to write csv header")
	}
	for _, e := range o.entries {
		if err := w.Write([]string{
			e.StatName,
			fmt.Sprintf("%t", e.IsProduction),
			fmt.Sprintf("%t", e.IsDataModel),
			e.Frequency,
		}); err != nil {
			return oops.Wrapf(err, "failed to write csv row for %q", e.StatName)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return oops.Wrapf(err, "failed to flush csv")
	}
	return nil
}

// Plan implements dataplatops.Operation.
// For read-only operations, Plan does all the work since there's no side effect.
func (o *ShowRegistryOp) Plan(ctx context.Context) error {
	_ = ctx
	o.entries = o.buildEntries()

	switch o.OutputFormat {
	case OutputFormatJSON:
		return o.printJSON()
	case OutputFormatCSV:
		return o.printCSV()
	default:
		// Should be prevented by Validate, but keep defensive.
		return oops.Errorf("unsupported output format: %q", o.OutputFormat)
	}
}

// Execute implements dataplatops.Operation.
// Returns *ShowRegistryResult with the data collected during Plan.
func (o *ShowRegistryOp) Execute(ctx context.Context) (any, error) {
	_ = ctx
	return &ShowRegistryResult{Entries: o.entries}, nil
}
