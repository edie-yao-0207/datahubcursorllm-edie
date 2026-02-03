package dataplatformresource

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
)

var pathPrefixes = []string{
	"python3/samsaradev/",
	"go/src/samsaradev.io/",
}

var nameRe = regexp.MustCompile("[^a-z0-9]+")

func name(s string) string {
	return strings.Trim(nameRe.ReplaceAllString(strings.ToLower(s), "_"), "_")
}

func md5Hash(backendPath string) (string, error) {
	absPath := filepath.Join(filepathhelpers.BackendRoot, backendPath)
	content, err := ioutil.ReadFile(absPath)
	if err != nil {
		return "", oops.Wrapf(err, "read: %s", absPath)
	}
	hash := md5.Sum(content)
	if strings.Contains(backendPath, "dataplatform/tables/s3data") && len(backendPath) > 60 {
		// If backendPath is longer than 60 characters, use only the first 4 characters of SHA
		return hex.EncodeToString(hash[:])[:4], nil
	}
	return hex.EncodeToString(hash[:]), nil
}

func SymLinkPythonScriptFile(region string) (string, error) {

	var symlink_script string

	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		symlink_script = "tools/jars/symlink-shared-python.sh"
	case infraconsts.SamsaraAWSEURegion:
		symlink_script = "tools/jars/symlink-shared-python-eu.sh"
	case infraconsts.SamsaraAWSCARegion:
		symlink_script = "tools/jars/symlink-shared-python-ca.sh"
	default:
		return "", oops.Errorf("unsupported region :%v", region)
	}

	return symlink_script, nil

}
func ArtifactBucket(region string) string {
	return awsregionconsts.RegionPrefix[region] + "dataplatform-deployed-artifacts"
}

func DeployedArtifactObject(region string, backendPath string) (*awsresource.S3BucketObject, error) {
	sourceFile := filepath.Join(filepathhelpers.BackendRoot, backendPath)
	if _, err := os.Stat(sourceFile); err != nil {
		if os.IsNotExist(err) {
			return nil, oops.Wrapf(err, "artifact source file does not exist at %s", sourceFile)
		}
		return nil, oops.Wrapf(err, "")
	}

	projectPath := tf.LocalId("project_path").Reference()
	sourcePath := filepath.Join(
		tf.LocalId("backend_root").Reference(),
		backendPath,
	)
	targetPath := backendPath
	for _, prefix := range pathPrefixes {
		targetPath = strings.TrimPrefix(targetPath, prefix)
	}

	content := fmt.Sprintf(`${file("%s")}`, sourcePath)
	hash, err := md5Hash(backendPath)
	if err != nil {
		return nil, oops.Wrapf(err, "md5: %s", backendPath)
	}

	filename := filepath.Base(targetPath)
	s3KeyPath := fmt.Sprintf("%s/%s/%s/%s", projectPath, targetPath, hash, filename)

	if strings.Contains(backendPath, "dataplatform/tables/s3data") && len(backendPath) > 60 {
		// We have been using filename twice in s3 path which unnecessarily increases the path length.
		// For tables with long paths, we can use the filename only once at the end
		// old: s3://samsara-eu-dataplatform-deployed-artifacts/databricks-eu/s3tables/definitions/canonical_fuel_hydrogen_type/dataplatform/tables/s3data/definitions/canonical_fuel_hydrogen_type.csv/12068de1f64c1a58caeda1a2f2fb2e7f/canonical_fuel_hydrogen_type.csv
		// new: s3://samsara-eu-dataplatform-deployed-artifacts/databricks-eu/s3tables/definitions/canonical_fuel_hydrogen_type/dataplatform/tables/s3data/definitions/12068de1f64c1a58caeda1a2f2fb2e7f/canonical_fuel_hydrogen_type.csv.
		// This should further reduce the filepath.
		// TODO: Sometime in the future, we should think about making this default ensuring we dont break any assumptions.
		s3KeyPath = fmt.Sprintf("%s/%s%s/%s", projectPath, strings.ReplaceAll(targetPath, filename, ""), hash, filename)
	}

	return &awsresource.S3BucketObject{
		ResourceName: name(targetPath),
		Bucket:       ArtifactBucket(region),
		Key:          s3KeyPath,
		Content:      content,
	}, nil
}

func DeployedArtifactByContents(region string, contents string, path string) (*awsresource.S3BucketObject, error) {
	projectPath := tf.LocalId("project_path").Reference()
	targetPath := path
	for _, prefix := range pathPrefixes {
		targetPath = strings.TrimPrefix(targetPath, prefix)
	}

	sum := md5.Sum([]byte(contents))
	hash := hex.EncodeToString(sum[:])

	return &awsresource.S3BucketObject{
		ResourceName: name(targetPath),
		Bucket:       ArtifactBucket(region),
		Key:          fmt.Sprintf("%s/%s/%s/%s", projectPath, targetPath, hash, filepath.Base(targetPath)),
		Content:      contents,
	}, nil
}

func DeployedArtifactObjectNoHash(region string, backendPath string) (*awsresource.S3BucketObject, error) {
	sourceFile := filepath.Join(filepathhelpers.BackendRoot, backendPath)
	if _, err := os.Stat(sourceFile); err != nil {
		if os.IsNotExist(err) {
			return nil, oops.Wrapf(err, "artifact source file does not exist at %s", sourceFile)
		}
		return nil, oops.Wrapf(err, "")
	}

	projectPath := tf.LocalId("project_path").Reference()
	sourcePath := filepath.Join(
		tf.LocalId("backend_root").Reference(),
		backendPath,
	)
	targetPath := backendPath
	for _, prefix := range pathPrefixes {
		targetPath = strings.TrimPrefix(targetPath, prefix)
	}

	content := fmt.Sprintf(`${file("%s")}`, sourcePath)
	hash, err := md5Hash(backendPath)
	if err != nil {
		return nil, oops.Wrapf(err, "md5: %s", backendPath)
	}

	return &awsresource.S3BucketObject{
		ResourceName: name(targetPath + "_nohash"),
		Bucket:       ArtifactBucket(region),
		Key:          fmt.Sprintf("%s/%s/%s", projectPath, targetPath, filepath.Base(targetPath)),
		Tags: map[string]string{
			// Include a hash of the file to force .tf to change when the file content changes.
			"dataplatform-file-hash": hash,
		},
		Content: content,
	}, nil
}
