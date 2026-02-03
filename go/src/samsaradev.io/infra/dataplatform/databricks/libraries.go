package databricks

import (
	"context"
	"fmt"
	"net/http"
	"sort"

	"github.com/samsarahq/go/oops"
)

type LibraryInstallStatus string

const (
	LibraryInstallStatusPending            = LibraryInstallStatus("PENDING")
	LibraryInstallStatusResolving          = LibraryInstallStatus("RESOLVING")
	LibraryInstallStatusInstalling         = LibraryInstallStatus("INSTALLING")
	LibraryInstallStatusInstalled          = LibraryInstallStatus("INSTALLED")
	LibraryInstallStatusFailed             = LibraryInstallStatus("FAILED")
	LibraryInstallStatusUninstallOnRestart = LibraryInstallStatus("UNINSTALL_ON_RESTART")
)

type MavenLibrary struct {
	Coordinates string `json:"coordinates"`
}

type PypiLibrary struct {
	Package string `json:"package"`
}

// Library maps to https://docs.databricks.com/api/latest/libraries.html#managedlibrarieslibrary.
type Library struct {
	Jar   string        `json:"jar,omitempty"`
	Maven *MavenLibrary `json:"maven,omitempty"`
	Pypi  *PypiLibrary  `json:"pypi,omitempty"`
	Wheel string        `json:"whl,omitempty"`
}

func (lib Library) ID() string {
	if lib.Jar != "" {
		return fmt.Sprintf("jar:%s", lib.Jar)
	}
	if lib.Maven != nil {
		return fmt.Sprintf("maven:%s", lib.Maven.Coordinates)
	}
	if lib.Pypi != nil {
		return fmt.Sprintf("pypi:%s", lib.Pypi.Package)
	}
	if lib.Wheel != "" {
		return fmt.Sprintf("whl:%s", lib.Wheel)
	}
	return "unknown"
}

func SortLibraries(in []*Library) []*Library {
	out := make([]*Library, len(in))
	copy(out, in)
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID() < out[j].ID()
	})
	return out
}

type LibraryFullStatus struct {
	Library        Library              `json:"library"`
	Status         LibraryInstallStatus `json:"status"`
	Messages       []string             `json:"messages"`
	ForAllClusters bool                 `json:"is_library_for_all_clusters"`
}

type ClusterLibraryStatusesInput struct {
	ClusterId string `json:"cluster_id"`
}

type ClusterLibraryStatusesOutput struct {
	ClusterId       string               `json:"cluster_id"`
	LibraryStatuses []*LibraryFullStatus `json:"library_statuses"`
}

type InstallLibrariesInput struct {
	ClusterId string     `json:"cluster_id"`
	Libraries []*Library `json:"libraries"`
}

type InstallLibrariesOutput struct{}

type UninstallLibrariesInput struct {
	ClusterId string     `json:"cluster_id"`
	Libraries []*Library `json:"libraries"`
}

type UninstallLibrariesOutput struct{}

// https://docs.databricks.com/dev-tools/api/latest/clusters.html
type LibrariesAPI interface {
	ClusterLibraryStatuses(context.Context, *ClusterLibraryStatusesInput) (*ClusterLibraryStatusesOutput, error)
	InstallLibraries(context.Context, *InstallLibrariesInput) (*InstallLibrariesOutput, error)
	UninstallLibraries(context.Context, *UninstallLibrariesInput) (*UninstallLibrariesOutput, error)
}

func (c *Client) ClusterLibraryStatuses(ctx context.Context, input *ClusterLibraryStatusesInput) (*ClusterLibraryStatusesOutput, error) {
	var output ClusterLibraryStatusesOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/libraries/cluster-status", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) InstallLibraries(ctx context.Context, input *InstallLibrariesInput) (*InstallLibrariesOutput, error) {
	var output InstallLibrariesOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/libraries/install", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) UninstallLibraries(ctx context.Context, input *UninstallLibrariesInput) (*UninstallLibrariesOutput, error) {
	var output UninstallLibrariesOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/libraries/uninstall", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
