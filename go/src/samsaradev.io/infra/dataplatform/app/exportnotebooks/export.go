package exportnotebooks

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/databricks"
)

func Export(ctx context.Context, dbxClient databricks.API, outputPath string) error {
	output, err := filepath.Abs(outputPath)
	if err != nil {
		return oops.Wrapf(err, "abs: %s", output)
	}
	if err = os.MkdirAll(output, 0755); err != nil {
		return oops.Wrapf(err, "mkdirs: %s", output)
	}

	// When we upgrade to Go 1.16, consider implementing io/fs.FS interface.
	// For now, we will invoke the API directly.
	stack := []string{"/"}
	for len(stack) > 0 {
		path := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		list, err := dbxClient.WorkspaceList(ctx, &databricks.WorkspaceListInput{
			Path: path,
		})
		if err != nil {
			if httpError, ok := oops.Cause(err).(*databricks.HTTPStatusError); ok {
				if httpError.StatusCode == http.StatusForbidden {
					slog.Warnw(ctx, fmt.Sprintf("failed to list %s: %s\n", path, err.Error()))
					continue
				}
			}
			if strings.Contains(err.Error(), "APIError(RESOURCE_DOES_NOT_EXIST)") {
				slog.Warnw(ctx, fmt.Sprintf("couldnt get the file for some reason. skipping... %s: %s\n", path, err.Error()))
				continue
			}

			return oops.Wrapf(err, "list: %s", path)
		}

		dir := filepath.Join(outputPath, strings.TrimPrefix(path, "/"))
		if err := os.MkdirAll(dir, 0755); err != nil {
			return oops.Wrapf(err, "mkdir: %s", dir)
		}

		for _, obj := range list.Objects {
			switch obj.ObjectType {
			case databricks.WorkspaceObjectTypeDirectory:
				stack = append(stack, obj.Path)
			case databricks.WorkspaceObjectTypeNotebook:
				export, err := dbxClient.WorkspaceExport(ctx, &databricks.WorkspaceExportInput{
					Path:   obj.Path,
					Format: databricks.ExportFormatSource,
				})
				if err != nil {
					if httpError, ok := oops.Cause(err).(*databricks.HTTPStatusError); ok {
						if httpError.StatusCode == http.StatusForbidden {
							slog.Warnw(ctx, fmt.Sprintf("failed to export %s: %s\n", path, err.Error()))
							continue
						}
					}
					if strings.Contains(err.Error(), "APIError(RESOURCE_DOES_NOT_EXIST)") {
						slog.Warnw(ctx, fmt.Sprintf("couldnt get the file for some reason. skipping... %s: %s\n", path, err.Error()))
						continue
					}

					return oops.Wrapf(err, "export: %s", obj.Path)
				}

				// Build out a header containing a link to the source notebook to help you quickly
				// navigate to the notebook in databricks.
				// This header will be a comment in the language of the notebook (so it doesn't clash
				// with syntax highlighting
				urlHeader := fmt.Sprintf("Notebook Source: %s#notebook/%d \n\n", dbxClient.BaseURL(), obj.ObjectId)
				ext := ""
				switch obj.Language {
				case databricks.NotebookLanguageScala:
					ext = ".scala"
					urlHeader = "// " + urlHeader
				case databricks.NotebookLanguagePython:
					ext = ".py"
					urlHeader = "# " + urlHeader
				case databricks.NotebookLanguageSQL:
					ext = ".sql"
					urlHeader = "-- " + urlHeader
				case databricks.NotebookLanguageR:
					ext = ".r"
					urlHeader = "# " + urlHeader
				default:
					log.Fatalf("unknown language: %s", obj.Language)
				}

				filename := filepath.Join(output, strings.TrimPrefix(obj.Path, "/")) + ext
				fullContent := append(
					[]byte(urlHeader),
					export.Content...,
				)
				if err := ioutil.WriteFile(filename, fullContent, 0644); err != nil {
					return oops.Wrapf(err, "write: %s", filename)
				}

				fmt.Println(strings.Join([]string{
					filename,
					strconv.Itoa(len(fullContent)),
				}, "\t"))
			}
		}
	}

	return nil
}
