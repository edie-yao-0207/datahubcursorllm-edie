package dataplatformmonitors

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/app/generate_terraform/util"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/team"
)

var notebookJobRunbook = "https://samsaradev.atlassian.net/wiki/spaces/DATA/pages/17756812/Scheduling+a+Databricks+single-task+job+via+the+backend+repo"

// Match against consecutive non-alpha-numeric characters, except for `-`.
var notebookJobDisallowedCharsRegex = regexp.MustCompile(`[^a-zA-Z0-9\-()\[\]_\s]+`)
var notebookJobSpaceRegex = regexp.MustCompile(`\s+`)

type notebookMetadata struct {
	Owner string                 `json:"owner"`
	Jobs  []*notebookMetadataJob `json:"jobs,omitempty"`
}

type notebookMetadataJob struct {
	Name      string             `json:"name"`
	Regions   []string           `json:"regions,omitempty"`
	SloConfig *notebookSloConfig `json:"slo_config,omitempty"`
}

type notebookSloConfig struct {
	LowUrgencyThresholdHours    int64 `json:"low_urgency_threshold_hours,omitempty"`
	BusinessHoursThresholdHours int64 `json:"business_hours_threshold_hours,omitempty"`
	HighUrgencyThresholdHours   int64 `json:"high_urgency_threshold_hours,omitempty"`
}

type notebookJobMonitorConfig struct {
	thresholdMetric string
	thresholdHours  int64
	severity        dataPlatformMonitorSeverity
	label           string
}

var notebookJobSloMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	metadataFiles, err := findNotebookMetadataFiles(dataplatformterraformconsts.NotebooksRoot)
	if err != nil {
		return nil, oops.Wrapf(err, "find notebook metadata files")
	}

	var monitors []tf.Resource
	for _, metadataFile := range metadataFiles {
		bytes, err := os.ReadFile(metadataFile)
		if err != nil {
			return nil, oops.Wrapf(err, "read notebook metadata: %s", metadataFile)
		}
		var metadata notebookMetadata
		if err := json.Unmarshal(bytes, &metadata); err != nil {
			return nil, oops.Wrapf(err, "parse notebook metadata: %s", metadataFile)
		}

		owner := team.TeamByName[metadata.Owner]
		if owner.Name() == "" {
			return nil, oops.Errorf("notebook metadata %s specifies unknown owner %q", metadataFile, metadata.Owner)
		}

		for _, job := range metadata.Jobs {
			if job == nil || job.SloConfig == nil {
				continue
			}
			monitorConfigs := buildNotebookJobMonitorConfigs(job.SloConfig)
			if len(monitorConfigs) == 0 {
				continue
			}

			for _, region := range job.Regions {
				jobNameTag, jobNameDisplay, err := notebookJobNameForRegion(metadataFile, job.Name, region)
				if err != nil {
					return nil, err
				}

				timeSinceSuccessHours := buildTimeSinceSuccessForJob(region, jobNameTag)
				for _, config := range monitorConfigs {
					query := fmt.Sprintf(
						"avg(last_30m):%s - %s <= 0",
						buildThresholdForJob(config.thresholdMetric, region, jobNameTag),
						timeSinceSuccessHours,
					)
					monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
						Name:        fmt.Sprintf("[%s] Notebook job %s %s threshold exceeded", region, jobNameDisplay, config.label),
						Message:     fmt.Sprintf("Notebook job %s has not succeeded within its %s threshold of %d hours.", jobNameDisplay, config.label, config.thresholdHours),
						Query:       query,
						Severity:    getSeverityForRegion(region, config.severity),
						RunbookLink: notebookJobRunbook,
						DatabricksJobSearchLink: &databricksJobSearchLink{
							region:    region,
							searchKey: jobNameDisplay,
						},
						TeamToPage: owner,
					})
					if err != nil {
						return nil, oops.Wrapf(err, "create notebook job monitor for %s", jobNameDisplay)
					}
					monitors = append(monitors, monitor)
				}
			}
		}
	}

	return monitors, nil
})

func buildNotebookJobMonitorConfigs(config *notebookSloConfig) []notebookJobMonitorConfig {
	if config == nil {
		return nil
	}
	var monitors []notebookJobMonitorConfig
	if config.LowUrgencyThresholdHours > 0 {
		monitors = append(monitors, notebookJobMonitorConfig{
			thresholdMetric: dataplatformconsts.LowUrgencyThreshold,
			thresholdHours:  config.LowUrgencyThresholdHours,
			severity:        lowSeverity,
			label:           "low urgency",
		})
	}
	if config.BusinessHoursThresholdHours > 0 {
		monitors = append(monitors, notebookJobMonitorConfig{
			thresholdMetric: dataplatformconsts.BusinessHoursThreshold,
			thresholdHours:  config.BusinessHoursThresholdHours,
			severity:        businessHoursSeverity,
			label:           "business hours high urgency",
		})
	}
	if config.HighUrgencyThresholdHours > 0 {
		monitors = append(monitors, notebookJobMonitorConfig{
			thresholdMetric: dataplatformconsts.HighUrgencyThreshold,
			thresholdHours:  config.HighUrgencyThresholdHours,
			severity:        highSeverity,
			label:           "high urgency",
		})
	}
	return monitors
}

func notebookJobNameForRegion(metadataFile string, jobName string, region string) (string, string, error) {
	relPath, err := filepath.Rel(dataplatformterraformconsts.NotebooksRoot, metadataFile)
	if err != nil {
		return "", "", oops.Wrapf(err, "rel path for %s", metadataFile)
	}
	baseName := strings.TrimSuffix(filepath.Base(relPath), ".metadata.json")
	subdir := filepath.Dir(relPath)
	notebookPath := filepath.Join("/", "backend", subdir, baseName)
	sanitizedName := tf.SanitizeResourceName(notebookPath + "_" + jobName)
	regionLabel, ok := dataplatformresource.RegionLabels[region]
	if !ok {
		return "", "", oops.Errorf("unknown region %q for notebook job %s", region, jobName)
	}
	jobNameWithRegion := fmt.Sprintf("%s-%s", sanitizedName, regionLabel)
	return cleanNotebookJobName(jobNameWithRegion), jobNameWithRegion, nil
}

func cleanNotebookJobName(name string) string {
	return util.HashTruncate(
		strings.ToLower(
			notebookJobSpaceRegex.ReplaceAllString(
				strings.TrimSpace(notebookJobDisallowedCharsRegex.ReplaceAllString(name, "")),
				"_",
			),
		),
		190, 5, "",
	)
}

func findNotebookMetadataFiles(rootDir string) ([]string, error) {
	var metadataFiles []string
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".json" && strings.HasSuffix(path, ".metadata.json") {
			metadataFiles = append(metadataFiles, path)
		}
		return nil
	})
	if err != nil {
		return nil, oops.Wrapf(err, "find metadata files in: %s", rootDir)
	}
	return metadataFiles, nil
}
