package databricks

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

var librariesSchema = map[string]*schema.Schema{
	"cluster_id": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
		ForceNew: true,
	},
	"library": &schema.Schema{
		Type:     schema.TypeSet,
		Optional: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"jar": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"whl": &schema.Schema{
					Type:     schema.TypeString,
					Optional: true,
				},
				"maven": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"coordinates": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
				"pypi": &schema.Schema{
					Type:     schema.TypeList,
					Optional: true,
					MinItems: 0,
					MaxItems: 1,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"package": &schema.Schema{
								Type:     schema.TypeString,
								Required: true,
							},
						},
					},
				},
			},
		},
	},
}

func resourceClusterLibraries() *schema.Resource {
	return &schema.Resource{
		Create: resourceClusterLibrariesCreate,
		Read:   resourceClusterLibrariesRead,
		Update: resourceClusterLibrariesUpdate,
		Delete: resourceClusterLibrariesDelete,
		Importer: &schema.ResourceImporter{
			State: resourceClusterLibrariesImport,
		},
		Schema: librariesSchema,
	}
}

func filterInstalledLibraries(statuses []*databricks.LibraryFullStatus) []*databricks.Library {
	var installed []*databricks.Library
	for _, status := range statuses {
		if status.Status == databricks.LibraryInstallStatusUninstallOnRestart {
			// Uninstalled.
			continue
		}
		if status.ForAllClusters {
			// Ignore global libraries since they are not specified in cluster resource.
			continue
		}
		installed = append(installed, &status.Library)
	}
	return databricks.SortLibraries(installed)
}

func resourceClusterLibrariesCreate(d *schema.ResourceData, m interface{}) error {
	if err := resourceClusterLibrariesUpdate(d, m); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceClusterLibrariesRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	statuses, err := client.ClusterLibraryStatuses(context.Background(), &databricks.ClusterLibraryStatusesInput{
		ClusterId: d.Get("cluster_id").(string),
	})
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok && apiErr.Message == fmt.Sprintf("Cluster %s does not exist", d.Get("cluster_id").(string)) {
			d.SetId("")
			return nil
		}
		return oops.Wrapf(err, "ClusterLibraryStatuses")
	}

	librariesId := statuses.ClusterId
	libraries := flattenLibraries(filterInstalledLibraries(statuses.LibraryStatuses))
	if len(libraries) == 0 {
		// If no libraries are found, mark the libraries tf resource as removed
		librariesId = ""
	}

	d.SetId(librariesId)
	if err := d.Set("library", libraries); err != nil {
		return oops.Wrapf(err, "library")
	}
	return nil
}

func resourceClusterLibrariesUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)

	// Compute libraries to install and/or uninstall.
	clusterId := d.Get("cluster_id").(string)
	var installed []*databricks.LibraryFullStatus
	statuses, err := client.ClusterLibraryStatuses(context.Background(), &databricks.ClusterLibraryStatusesInput{
		ClusterId: clusterId,
	})
	if err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok && apiErr.Message == fmt.Sprintf("Cluster %s does not exist", clusterId) {
			installed = nil
		} else {
			return oops.Wrapf(err, "")
		}
	} else {
		installed = statuses.LibraryStatuses
	}

	actual := make(map[string]*databricks.Library)
	for _, library := range filterInstalledLibraries(installed) {
		actual[library.ID()] = library
	}

	desired := make(map[string]*databricks.Library)
	if set, ok := d.Get("library").(*schema.Set); ok {
		for _, library := range expandLibraries(set.List()) {
			desired[library.ID()] = library
		}
	}

	var toInstall []*databricks.Library
	for _, library := range desired {
		if _, ok := actual[library.ID()]; !ok {
			toInstall = append(toInstall, library)
		}
	}

	var toUninstall []*databricks.Library
	for _, library := range actual {
		if _, ok := desired[library.ID()]; !ok {
			toUninstall = append(toUninstall, library)
		}
	}

	if len(toUninstall) != 0 {
		if _, err := client.UninstallLibraries(context.Background(), &databricks.UninstallLibrariesInput{
			ClusterId: clusterId,
			Libraries: toUninstall,
		}); err != nil {
			return oops.Wrapf(err, "uninstall libraries: %s", clusterId)
		}
	}

	if len(toInstall) != 0 {
		// If we try to install libraries on a stopped cluster, we will get this
		// error "APIError(INVALID_PARAMETER_VALUE): Cluster <id> is terminated or
		// does not exist", and the desired library configuration will not be
		// persisted. To work around this, we will start the cluster, only if the
		// cluster has auto-termination policy, before we install on a stopped
		// cluster. We will rely on auto-termination to stop it afterwards.
		cluster, err := client.GetCluster(context.Background(), &databricks.GetClusterInput{
			ClusterId: clusterId,
		})
		if err != nil {
			return oops.Wrapf(err, "get cluster: %s", clusterId)
		}
		switch cluster.State {
		case databricks.ClusterStateRunning:
		case databricks.ClusterStateTerminated, databricks.ClusterStateTerminating:
			if cluster.AutoTerminationMinutes != 0 {
				if _, err := client.StartCluster(context.Background(), &databricks.StartClusterInput{
					ClusterId: clusterId,
				}); err != nil {
					return oops.Wrapf(err, "start cluster: %s", clusterId)
				}
				// Wait a bit. InstallLibraries can still fail otherwise.
				time.Sleep(5 * time.Second)
			} else {
				return oops.Errorf("cluster %s (%s) is not running", clusterId, cluster.ClusterName)
			}
		case databricks.ClusterStatePending, databricks.ClusterStateResizing, databricks.ClusterStateRestarting:
			// Wait if cluster is starting up.
			time.Sleep(5 * time.Second)
		default:
			// ERROR or UNKNOWN.
			return oops.Errorf("cluster %s (%s) is in a bad state: %s", clusterId, cluster.ClusterName, cluster.State)
		}

		if _, err := client.InstallLibraries(context.Background(), &databricks.InstallLibrariesInput{
			ClusterId: clusterId,
			Libraries: toInstall,
		}); err != nil {
			return oops.Wrapf(err, "install libraries: %s", clusterId)
		}
	}

	d.SetId(statuses.ClusterId)
	return nil
}

func resourceClusterLibrariesDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	statuses, err := client.ClusterLibraryStatuses(context.Background(), &databricks.ClusterLibraryStatusesInput{
		ClusterId: d.Id(),
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	installed := filterInstalledLibraries(statuses.LibraryStatuses)
	if _, err := client.UninstallLibraries(context.Background(), &databricks.UninstallLibrariesInput{
		ClusterId: d.Id(),
		Libraries: installed,
	}); err != nil {
		if apiErr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if strings.HasSuffix(apiErr.Message, "does not exist") {
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceClusterLibrariesImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceClusterLibrariesRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
