package databricks

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
)

func normalizeNotebook(content string, language string) string {
	commentPrefix := map[string]string{
		"PYTHON": "#",
		"SQL":    "--",
	}[language]

	// Databricks always adds this comment at the top.
	content = strings.TrimPrefix(content, commentPrefix+" Databricks notebook source\n")

	separator := "\n" + commentPrefix + " COMMAND ----------" + "\n"
	commands := strings.Split(content, separator)
	var normalized []string
	for _, command := range commands {
		// Databricks adds "\n\n" before and after command separators.
		command = strings.TrimSpace(command)

		lines := strings.Split(command, "\n")
		for j := range lines {
			// Databricks strips trailing space from "MAGIC".
			if lines[j] == commentPrefix+" MAGIC" {
				lines[j] += " "
			}
		}
		command = strings.Join(lines, "\n")

		if command != "" {
			normalized = append(normalized, command)
		}
	}

	return strings.Join(normalized, separator)
}

var notebookSchema = map[string]*schema.Schema{
	"path": &schema.Schema{
		Type:     schema.TypeString,
		ForceNew: true,
		Required: true,
	},
	"language": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
	},
	"content": &schema.Schema{
		Type:     schema.TypeString,
		Required: true,
		DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
			return normalizeNotebook(old, d.Get("language").(string)) == normalizeNotebook(new, d.Get("language").(string))
		},
	},
	"object_id": &schema.Schema{
		Type:     schema.TypeInt,
		Computed: true,
	},
}

func resourceNotebook() *schema.Resource {
	return &schema.Resource{
		Create: resourceNotebookCreate,
		Read:   resourceNotebookRead,
		Update: resourceNotebookUpdate,
		Delete: resourceNotebookDelete,
		Importer: &schema.ResourceImporter{
			State: resourceNotebookImport,
		},
		Schema: notebookSchema,
	}
}

func importNotebook(d *schema.ResourceData, m interface{}, overwrite bool) error {
	client := m.(databricks.API)
	path := d.Get("path").(string)
	if _, err := client.WorkspaceImport(context.Background(), &databricks.WorkspaceImportInput{
		Path:      path,
		Format:    databricks.ExportFormatSource,
		Language:  databricks.NotebookLanguage(d.Get("language").(string)),
		Content:   []byte(d.Get("content").(string)),
		Overwrite: overwrite,
	}); err != nil {
		return oops.Wrapf(err, "")
	}
	d.SetId(path)
	return nil
}

func resourceNotebookCreate(d *schema.ResourceData, m interface{}) error {
	if err := importNotebook(d, m, false); err != nil {
		return oops.Wrapf(err, "")
	}
	if err := resourceNotebookRead(d, m); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceNotebookUpdate(d *schema.ResourceData, m interface{}) error {
	if err := importNotebook(d, m, true); err != nil {
		return oops.Wrapf(err, "")
	}
	if err := resourceNotebookRead(d, m); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceNotebookRead(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	path := d.Get("path").(string)
	status, err := client.WorkspaceGetStatus(context.Background(), &databricks.WorkspaceGetStatusInput{
		Path: path,
	})
	if err != nil {
		if aerr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if aerr.ErrorCode == databricks.ErrorCodeResourceDoesNotExist {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}
	if status.ObjectType != databricks.WorkspaceObjectTypeNotebook {
		return oops.Errorf("%s is not a notebook", path)
	}
	d.SetId(path)
	if err := d.Set("language", status.Language); err != nil {
		return oops.Wrapf(err, "")
	}
	if err := d.Set("object_id", status.ObjectId); err != nil {
		return oops.Wrapf(err, "")
	}

	export, err := client.WorkspaceExport(context.Background(), &databricks.WorkspaceExportInput{
		Path:   path,
		Format: databricks.ExportFormatSource,
	})
	if err != nil {
		return oops.Wrapf(err, "")
	}
	if err := d.Set("content", string(export.Content)); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceNotebookDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(databricks.API)
	path := d.Get("path").(string)
	if _, err := client.WorkspaceDelete(context.Background(), &databricks.WorkspaceDeleteInput{
		Path: path,
	}); err != nil {
		if aerr, ok := oops.Cause(err).(*databricks.APIError); ok {
			if aerr.ErrorCode == databricks.ErrorCodeResourceDoesNotExist {
				d.SetId("")
				return nil
			}
		}
		return oops.Wrapf(err, "")
	}
	return nil
}

func resourceNotebookImport(d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
	if err := resourceNotebookRead(d, m); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
