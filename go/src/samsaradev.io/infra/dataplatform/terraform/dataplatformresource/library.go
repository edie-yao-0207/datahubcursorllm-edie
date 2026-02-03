package dataplatformresource

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
)

const (
	// Spark 3.0.x.
	SparkJarGeoSparkSpark3_1_3_1    = JarName("geospark-1.3.2-SNAPSHOT.jar")
	SparkJarGeoSparkSQLSpark3_1_3_1 = JarName("geospark-sql_3.0-1.3.2-SNAPSHOT.jar")
	// BigQuery connector is bundled in Runtime 7.1.
	// TODO: consider adding flint for Spark 3.

	// Non-Spark External Jars (Command-line tools, etc)
	ExternalJarOsmParquetizer = JarName("osm-parquetizer-1.0.2.jar")

	// Patched spark protobuf jar branched off of spark 3.4.0.
	SamsaraSparkProtobufJar = JarName("spark-protobuf-samsara-package.jar")

	// PySpark packages
	SparkPyPIBokeh   = PyPIName("bokeh")
	SparkPyPIDatadog = PyPIName("datadog")
	SparkPyPIJinja   = PyPIName("jinja2")
	SparkPyPISlack   = PyPIName("slackclient")
	SparkPyPISnappy  = PyPIName("python-snappy")
	SparkPyPIYaml    = PyPIName("pyyaml")

	// Maven packages
	SparkMavenGResearch = MavenName("uk.co.gresearch.spark:spark-extension_2.12:1.3.2-3.0")
	MicrosoftMlSpark    = MavenName("com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc3")

	// Managed Wheels
	DatabricksCloudCredentialLibraryWheel = WheelName("service_credentials-1.0.1-py3-none-any.whl")

	PythonRequirementsFile = "python3/dependencies/dataplatform/requirements.databrickscluster.txt"
)

// https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/allowlist.html#add-maven-coordinates-to-the-allowlist
// You can include versions of a library by allowlist `groupId:artifactId:
var MavenAllowlistedLibraries = map[string]MavenName{
	// Diffing library used in various places to help compare dataframes.
	"gresearch": SparkMavenGResearch,

	// Microsoft ML Library
	"microsoftmlspark": MicrosoftMlSpark,
}

type SparkLibrary interface {
	TfResource(region string, ucEnabled bool) (*databricksresource.Library, error)
	ApiPayload(region string) (*databricks.Library, error)
	String() string
}

type JarName string

var _ SparkLibrary = JarName("")

func (n JarName) TfResource(region string, ucEnabled bool) (*databricksresource.Library, error) {
	if ucEnabled {
		return &databricksresource.Library{
			Jar: n.VolumePath(),
		}, nil
	} else {
		return &databricksresource.Library{
			Jar: fmt.Sprintf("s3://%s/%s", ArtifactBucket(region), n.S3Key()),
		}, nil
	}
}

func (n JarName) ApiPayload(region string) (*databricks.Library, error) {
	return &databricks.Library{
		Jar: n.VolumePath(),
	}, nil
}

func (n JarName) VolumePath() string {
	return fmt.Sprintf("/Volumes/s3/dataplatform-deployed-artifacts/%s", n.S3Key())
}

func (n JarName) S3Key() string {
	return fmt.Sprintf("jars/%s", n)
}

func (n JarName) FilePath() string {
	return filepath.Join(
		tf.LocalId("backend_root").Reference(),
		"tools/jars",
		string(n))
}

func (n JarName) String() string {
	return string(n)
}

type PyPIName string

var _ SparkLibrary = PyPIName("")

func (n PyPIName) TfResource(region string, ucEnabled bool) (*databricksresource.Library, error) {
	pinned, err := pinnedLibrary(n)
	if err != nil {
		return nil, oops.Wrapf(err, "get pinned version")
	}
	return &databricksresource.Library{
		Pypi: databricksresource.PypiLibrary{
			Package: pinned,
		},
	}, nil
}

func (n PyPIName) ApiPayload(region string) (*databricks.Library, error) {
	pinned, err := pinnedLibrary(n)
	if err != nil {
		return nil, oops.Wrapf(err, "get pinned version")
	}
	return &databricks.Library{
		Pypi: &databricks.PypiLibrary{
			Package: pinned,
		},
	}, nil
}

func (n PyPIName) String() string {
	return string(n)
}

func (n PyPIName) PinnedString() string {
	pinned, err := pinnedLibrary(n)
	if err != nil {
		return n.String()
	}
	return pinned
}

// TODO: this doesn't actually implemnet the SparkLibrary interface?
type MavenName string

func (n MavenName) TfResource() (*databricksresource.Library, error) {
	return &databricksresource.Library{
		Maven: databricksresource.MavenLibrary{
			Coordinates: string(n),
		},
	}, nil
}

// At this time we don't support managed python wheels for non-UC clusters.
// This is because we don't plan to have those for too much longer, so theres no
// need to go out of our way here to support them.
type WheelName string

var _ SparkLibrary = WheelName("")

func (n WheelName) TfResource(region string, ucEnabled bool) (*databricksresource.Library, error) {
	return &databricksresource.Library{
		Wheel: n.VolumePath(),
	}, nil
}

func (n WheelName) ApiPayload(region string) (*databricks.Library, error) {
	return &databricks.Library{
		Wheel: n.VolumePath(),
	}, nil
}

func (n WheelName) VolumePath() string {
	return fmt.Sprintf("/Volumes/s3/dataplatform-deployed-artifacts/wheels/%s", n)
}

func (n WheelName) S3Key() string {
	return fmt.Sprintf("wheels/%s", n)
}

func (n WheelName) FilePath() string {
	return filepath.Join(
		tf.LocalId("backend_root").Reference(),
		"tools/wheels",
		string(n))
}

func (n WheelName) String() string {
	return string(n)
}

func AllSpark3Jars() []JarName {
	return []JarName{
		SparkJarGeoSparkSpark3_1_3_1,
		SparkJarGeoSparkSQLSpark3_1_3_1,
	}
}

func AllExternalJars() []JarName {
	return []JarName{
		ExternalJarOsmParquetizer,
	}
}

func AllExternalPythonWheels() []WheelName {
	return []WheelName{
		DatabricksCloudCredentialLibraryWheel,
	}
}

func GeoSparkJars(sparkVersion sparkversion.SparkVersion) []JarName {
	return []JarName{
		SparkJarGeoSparkSpark3_1_3_1,
		SparkJarGeoSparkSQLSpark3_1_3_1,
	}
}

func AllSparkPyPIs() []PyPIName {
	return []PyPIName{
		SparkPyPIBokeh,
		SparkPyPIDatadog,
		SparkPyPIJinja,
		SparkPyPISlack,
		SparkPyPISnappy,
	}
}

func DefaultLibraries(ucEnabled bool) []SparkLibrary {
	var libraries []SparkLibrary
	for _, jar := range AllSpark3Jars() {
		libraries = append(libraries, jar)
	}
	for _, pypi := range AllSparkPyPIs() {
		libraries = append(libraries, pypi)
	}
	if ucEnabled {
		for _, wheel := range AllExternalPythonWheels() {
			libraries = append(libraries, wheel)
		}
	}
	return libraries
}

var pinnedLibraryCache map[PyPIName]string
var pinnedLibraryCacheMutex sync.Mutex

func pinnedLibrary(pkg PyPIName) (string, error) {
	// TODO: **HACK** amundsen-databuilder doesn't play nicely with our requirements.lambda.txt and requirements.databricks71.txt
	// Because of this, we can't pin a version so this function fails.
	// If the package is `amundsen-databuilder`, immediately return amundsen-databuilder and don't look for a pinned version
	// Owner: @Jack R
	if strings.Contains(string(pkg), "amundsen-databuilder") {
		return string(pkg), nil
	}

	pinnedLibraryCacheMutex.Lock()
	defer pinnedLibraryCacheMutex.Unlock()

	if pinnedLibraryCache == nil {
		requirementsFile, err := os.Open(filepath.Join(filepathhelpers.BackendRoot, PythonRequirementsFile))
		if err != nil {
			return "", oops.Wrapf(err, "Cannot open %s", PythonRequirementsFile)
		}
		defer requirementsFile.Close()

		pythonLibraryPins := make(map[PyPIName]string)
		scanner := bufio.NewScanner(requirementsFile)
		for scanner.Scan() {
			line := scanner.Text()
			req := strings.Split(line, " ")[0]
			if !strings.Contains(req, "==") {
				continue
			}
			pkg := strings.Split(req, "==")[0]
			pythonLibraryPins[PyPIName(pkg)] = req
		}
		if err := scanner.Err(); err != nil {
			return "", oops.Wrapf(err, "scan error")
		}
		pinnedLibraryCache = pythonLibraryPins
	}

	pinnedPkg, ok := pinnedLibraryCache[pkg]
	if !ok {
		return string(pkg), nil
	}
	return pinnedPkg, nil
}

func SupportedAPILibraries(sparkVersion sparkversion.SparkVersion, region string) (map[string]*databricks.Library, error) {
	supportedLibraries := make(map[string]*databricks.Library)

	// This function just returns a map of what libraries are supported, so
	// unity catalog being enabled or not shouldn't matter.
	// Setting it as false when calling `ApiPayload`
	for _, jar := range AllSpark3Jars() {
		lib, err := jar.ApiPayload(region)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		supportedLibraries[lib.Jar] = lib
	}

	for _, pypi := range AllSparkPyPIs() {
		lib, err := pypi.ApiPayload(region)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		supportedLibraries[string(pypi)] = lib
	}

	return supportedLibraries, nil
}

func SplitPyPIName(name string) (string, string) {
	releaseClause := regexp.MustCompile("[~!=<>][=<>]?")
	splitName := releaseClause.Split(name, 2)
	if len(splitName) == 2 {
		return splitName[0], splitName[1]
	}
	return splitName[0], ""
}
