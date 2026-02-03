package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/libs/ni/filehelpers"
)

var kinesisstatsSchemaDir = filepath.Join(
	filepathhelpers.BackendRoot,
	"go/src/samsaradev.io/infra/dataplatform/ksdeltalake/schemas")

var s3bigstatsSchemaDir = filepath.Join(
	filepathhelpers.BackendRoot,
	"go/src/samsaradev.io/infra/dataplatform/ksdeltalake/s3bigstatschemas")

func main() {
	newlyWritten := make(map[string]struct{})
	for _, tbl := range ksdeltalake.AllTables() {
		schemaFilename := fmt.Sprintf("%s.sql", tbl.QualifiedName())
		schemaPath := filepath.Join(kinesisstatsSchemaDir, schemaFilename)
		schemaData := []byte(tbl.Schema.SQLColumnsIndent("", "  ") + "\n")
		if err := filehelpers.WriteIfChanged(schemaPath, schemaData, 0644); err != nil {
			log.Fatalln(oops.Wrapf(err, "failed to write %s", schemaPath))
		}

		newlyWritten[schemaFilename] = struct{}{}
		if tbl.S3BigStatSchema != nil {
			schemaFilename := fmt.Sprintf("%s.sql", tbl.S3BigStatsName())
			schemaPath := filepath.Join(s3bigstatsSchemaDir, schemaFilename)
			schemaData := []byte(tbl.S3BigStatSchema.SQLColumnsIndent("", "  ") + "\n")
			if err := filehelpers.WriteIfChanged(schemaPath, schemaData, 0644); err != nil {
				log.Fatalln(oops.Wrapf(err, "failed to write %s", schemaPath))
			}
			newlyWritten[schemaFilename] = struct{}{}

			combinedViewSchemaFilename := fmt.Sprintf("%s.sql", tbl.S3BigStatCombinedViewName())
			combinedViewSchemaPath := filepath.Join(kinesisstatsSchemaDir, combinedViewSchemaFilename)
			combinedViewSchemaData := []byte(tbl.GetS3BBigStatCombinedViewSchema().SQLColumnsIndent("", "  ") + "\n")
			if err := filehelpers.WriteIfChanged(combinedViewSchemaPath, combinedViewSchemaData, 0644); err != nil {
				log.Fatalln(oops.Wrapf(err, "failed to write %s", combinedViewSchemaPath))
			}
			newlyWritten[combinedViewSchemaFilename] = struct{}{}
		}
	}

	kinesisStatsFiles, err := ioutil.ReadDir(kinesisstatsSchemaDir)
	if err != nil {
		log.Fatalln(oops.Wrapf(err, "listing files in %s", kinesisstatsSchemaDir))
	}
	cleanUpDeadFiles(kinesisStatsFiles, kinesisstatsSchemaDir, newlyWritten)

	bigStatsFiles, err := ioutil.ReadDir(s3bigstatsSchemaDir)
	if err != nil {
		log.Fatalln(oops.Wrapf(err, "listing files in %s", s3bigstatsSchemaDir))
	}
	cleanUpDeadFiles(bigStatsFiles, s3bigstatsSchemaDir, newlyWritten)

}

func cleanUpDeadFiles(files []os.FileInfo, dir string, newlyWrittenFiles map[string]struct{}) {
	for _, file := range files {
		if filepath.Ext(file.Name()) != ".sql" {
			continue
		}
		if _, ok := newlyWrittenFiles[file.Name()]; !ok {
			deadPath := filepath.Join(dir, file.Name())
			if err := os.Remove(deadPath); err != nil {
				log.Fatalln(oops.Wrapf(err, "failed to remove dead file %s", deadPath))
			}
		}
	}
}
