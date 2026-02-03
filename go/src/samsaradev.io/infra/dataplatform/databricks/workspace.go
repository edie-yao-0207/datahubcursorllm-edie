package databricks

// Client implementation of https://docs.databricks.com/dev-tools/api/latest/workspace.html

import (
	"context"
	"net/http"

	"github.com/samsarahq/go/oops"
)

type ExportFormat string

const (
	ExportFormatSource  = ExportFormat("SOURCE")
	ExportFormatHTML    = ExportFormat("HTML")
	ExportFormatJupyter = ExportFormat("JUPYTER")
	ExportFormatDBC     = ExportFormat("DBC")
	ExportFormatRAW     = ExportFormat("RAW")
	ExportFormatAuto    = ExportFormat("AUTO")
)

type NotebookLanguage string

const (
	NotebookLanguageScala  = NotebookLanguage("SCALA")
	NotebookLanguagePython = NotebookLanguage("PYTHON")
	NotebookLanguageSQL    = NotebookLanguage("SQL")
	NotebookLanguageR      = NotebookLanguage("R")
)

type WorkspaceObjectType string

const (
	WorkspaceObjectTypeNotebook  = WorkspaceObjectType("NOTEBOOK")
	WorkspaceObjectTypeDirectory = WorkspaceObjectType("DIRECTORY")
	WorkspaceObjectTypeLibrary   = WorkspaceObjectType("LIBRARY")
	WorkspaceObjectTypeFile      = WorkspaceObjectType("FILE")
)

type WorkspaceObjectInfo struct {
	Path       string              `json:"path"`
	Language   NotebookLanguage    `json:"language"`
	ObjectType WorkspaceObjectType `json:"object_type"`
	ObjectId   int64               `json:"object_id"`
}

type WorkspaceDeleteInput struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive"`
}

type WorkspaceDeleteOutput struct{}

type WorkspaceExportInput struct {
	Path   string       `json:"path"`
	Format ExportFormat `json:"format"`
}

type WorkspaceExportOutput struct {
	Content []byte `json:"content"`
}

type WorkspaceGetStatusInput struct {
	Path string `json:"path"`
}

type WorkspaceGetStatusOutput struct {
	WorkspaceObjectInfo
}

type WorkspaceImportInput struct {
	Path      string           `json:"path"`
	Format    ExportFormat     `json:"format"`
	Language  NotebookLanguage `json:"language"`
	Content   []byte           `json:"content"`
	Overwrite bool             `json:"overwrite"`
}

type WorkspaceImportOutput struct{}

type WorkspaceListInput struct {
	Path string `json:"path"`
}

type WorkspaceListOutput struct {
	Objects []WorkspaceObjectInfo `json:"objects"`
}

type WorkspaceMkdirsInput struct {
	Path string `json:"path"`
}

type WorkspaceMkdirsOutput struct{}

type WorkspaceAPI interface {
	WorkspaceDelete(context.Context, *WorkspaceDeleteInput) (*WorkspaceDeleteOutput, error)
	WorkspaceExport(context.Context, *WorkspaceExportInput) (*WorkspaceExportOutput, error)
	WorkspaceGetStatus(context.Context, *WorkspaceGetStatusInput) (*WorkspaceGetStatusOutput, error)
	WorkspaceImport(context.Context, *WorkspaceImportInput) (*WorkspaceImportOutput, error)
	WorkspaceList(context.Context, *WorkspaceListInput) (*WorkspaceListOutput, error)
	WorkspaceMkdirs(context.Context, *WorkspaceMkdirsInput) (*WorkspaceMkdirsOutput, error)
}

func (c *Client) WorkspaceDelete(ctx context.Context, input *WorkspaceDeleteInput) (*WorkspaceDeleteOutput, error) {
	var output WorkspaceDeleteOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/workspace/delete", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) WorkspaceExport(ctx context.Context, input *WorkspaceExportInput) (*WorkspaceExportOutput, error) {
	var output WorkspaceExportOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/workspace/export", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) WorkspaceGetStatus(ctx context.Context, input *WorkspaceGetStatusInput) (*WorkspaceGetStatusOutput, error) {
	var output WorkspaceGetStatusOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/workspace/get-status", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) WorkspaceImport(ctx context.Context, input *WorkspaceImportInput) (*WorkspaceImportOutput, error) {
	var output WorkspaceImportOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/workspace/import", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) WorkspaceList(ctx context.Context, input *WorkspaceListInput) (*WorkspaceListOutput, error) {
	var output WorkspaceListOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/workspace/list", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) WorkspaceMkdirs(ctx context.Context, input *WorkspaceMkdirsInput) (*WorkspaceMkdirsOutput, error) {
	var output WorkspaceMkdirsOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/workspace/mkdirs", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
