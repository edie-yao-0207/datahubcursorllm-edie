package databricks

import (
	"context"
	"fmt"
	"net/http"

	"github.com/samsarahq/go/oops"
)

type ObjectType string

// Subset of object types used by Samsara.
// Permissions (2.0): https://docs.databricks.com/dev-tools/api/latest/permissions.html
// Cluster Policy Permissions (2.0) (uses same interface as above, but confusingly is on its own webpage)
// https://docs.databricks.com/dev-tools/api/latest/policies.html#cluster-policy-permissions-api
const (
	ObjectTypeCluster       = ObjectType("cluster")
	ObjectTypeClusterPolicy = ObjectType("cluster-policy")
	ObjectTypeInstancePool  = ObjectType("instance-pool")
	ObjectTypeJob           = ObjectType("job")
	ObjectTypeNotebook      = ObjectType("notebook")
	ObjectTypeDirectory     = ObjectType("directory")
	ObjectTypeSqlDashboard  = ObjectType("dashboard")
	ObjectTypeSqlEndpoint   = ObjectType("sql/endpoint")
	ObjectTypeSqlQuery      = ObjectType("query")
)

func (t ObjectType) Plural() (string, error) {
	switch t {
	case ObjectTypeCluster, ObjectTypeJob, ObjectTypeNotebook, ObjectTypeInstancePool, ObjectTypeSqlDashboard, ObjectTypeSqlEndpoint:
		return string(t) + "s", nil
	case ObjectTypeClusterPolicy:
		return "cluster-policies", nil
	case ObjectTypeDirectory:
		return "directories", nil
	case ObjectTypeSqlQuery:
		return "queries", nil
	default:
		return "", oops.Errorf("unknown object type: %q", t)
	}
}

// UpdateMethod returns the Url used to interact with this resource. Different databricks
// objects use slightly different api semantics.
func (t ObjectType) Url(objectId string) (string, error) {
	typ, err := t.Plural()
	if err != nil {
		return "", oops.Wrapf(err, "")
	}

	switch t {
	case ObjectTypeSqlDashboard, ObjectTypeSqlQuery:
		return "/api/2.0/preview/sql/permissions/" + typ + "/" + objectId, nil
	default:
		return "/api/2.0/preview/permissions/" + typ + "/" + objectId, nil
	}
}

func (t ObjectType) UnpluralizedUrl(objectId string) (string, error) {

	switch t {
	case ObjectTypeSqlDashboard, ObjectTypeSqlQuery:
		return "/api/2.0/preview/sql/permissions/" + fmt.Sprintf("%s", t) + "/" + objectId, nil
	default:
		return "/api/2.0/preview/permissions/" + fmt.Sprintf("%s", t) + objectId, nil
	}
}

// PermissionUpdateMethod returns the HTTP method used to mutate this resource. Different databricks
// objects use slightly different update semantics.
func (t ObjectType) PermissionUpdateMethod() string {
	switch t {
	case ObjectTypeSqlDashboard, ObjectTypeSqlQuery:
		return http.MethodPost
	default:
		return http.MethodPut
	}
}

type PermissionLevel string

const (
	PermissionLevelUndefined    = PermissionLevel("")
	PermissionLevelCanManage    = PermissionLevel("CAN_MANAGE")
	PermissionLevelCanRestart   = PermissionLevel("CAN_RESTART")
	PermissionLevelCanAttachTo  = PermissionLevel("CAN_ATTACH_TO")
	PermissionLevelCanManageRun = PermissionLevel("CAN_MANAGE_RUN")
	PermissionLevelIsOwner      = PermissionLevel("IS_OWNER")
	PermissionLevelCanView      = PermissionLevel("CAN_VIEW")
	PermissionLevelCanRead      = PermissionLevel("CAN_READ")
	PermissionLevelCanRun       = PermissionLevel("CAN_RUN")
	PermissionLevelCanEdit      = PermissionLevel("CAN_EDIT")
	PermissionLevelCanUse       = PermissionLevel("CAN_USE")
	PermissionsLevelCanMonitor  = PermissionLevel("CAN_MONITOR")
)

var PermissionLevelAllowedObjects = map[PermissionLevel][]ObjectType{
	PermissionLevelCanManage:    []ObjectType{ObjectTypeCluster, ObjectTypeInstancePool, ObjectTypeJob, ObjectTypeNotebook, ObjectTypeDirectory, ObjectTypeSqlDashboard, ObjectTypeSqlEndpoint, ObjectTypeSqlQuery},
	PermissionLevelCanRestart:   []ObjectType{ObjectTypeCluster},
	PermissionLevelCanAttachTo:  []ObjectType{ObjectTypeCluster, ObjectTypeInstancePool},
	PermissionLevelCanManageRun: []ObjectType{ObjectTypeJob},
	PermissionLevelIsOwner:      []ObjectType{ObjectTypeJob, ObjectTypeSqlEndpoint},
	PermissionLevelCanView:      []ObjectType{ObjectTypeJob, ObjectTypeSqlDashboard, ObjectTypeSqlQuery},
	PermissionLevelCanRead:      []ObjectType{ObjectTypeNotebook, ObjectTypeDirectory},
	PermissionLevelCanRun:       []ObjectType{ObjectTypeNotebook, ObjectTypeDirectory, ObjectTypeSqlDashboard, ObjectTypeSqlQuery},
	PermissionLevelCanEdit:      []ObjectType{ObjectTypeNotebook, ObjectTypeDirectory, ObjectTypeSqlDashboard, ObjectTypeSqlQuery},
	PermissionLevelCanUse:       []ObjectType{ObjectTypeSqlEndpoint, ObjectTypeClusterPolicy},
	PermissionsLevelCanMonitor:  []ObjectType{ObjectTypeSqlEndpoint},
}

func ObjectTypePermissionAllowed(typ ObjectType, perm PermissionLevel) bool {
	for _, allowed := range PermissionLevelAllowedObjects[perm] {
		if typ == allowed {
			return true
		}
	}
	return false
}

type Permission struct {
	// PermissionLevel varies by object type.
	PermissionLevel PermissionLevel `json:"permission_level"`

	Inherited           bool     `json:"inherited"`
	InheritedFromObject []string `json:"inherited_from_object"`
}

type AccessControlPrincipal struct {
	// Exactly one of UserName and GroupName must be set.

	// UserName is a user's email.
	UserName             string `json:"user_name,omitempty"`
	GroupName            string `json:"group_name,omitempty"`
	ServicePrincipalName string `json:"service_principal_name,omitempty"`
}

type AccessControlRequest struct {
	AccessControlPrincipal
	PermissionLevel PermissionLevel `json:"permission_level"`
}

type AccessControlResponse struct {
	AccessControlPrincipal
	AllPermissions  []*Permission    `json:"all_permissions"`
	PermissionLevel *PermissionLevel `json:"permission_level,omitempty"`
}

type GetPermissionsInput struct {
	ObjectType ObjectType
	ObjectId   string
}

type GetPermissionsOutput struct {
	ObjectType        ObjectType               `json:"object_type"`
	ObjectId          string                   `json:"object_id"`
	AccessControlList []*AccessControlResponse `json:"access_control_list"`
}

type PutPermissionsInput struct {
	ObjectType        ObjectType              `json:",omit"`
	ObjectId          string                  `json:",omit"`
	AccessControlList []*AccessControlRequest `json:"access_control_list"`
}

type PutPermissionsOutput struct {
	ObjectType        ObjectType               `json:"object_type"`
	ObjectId          string                   `json:"object_id"`
	AccessControlList []*AccessControlResponse `json:"access_control_list"`
}

type PatchPermissionsInput struct {
	ObjectType        ObjectType              `json:",omit"`
	ObjectId          string                  `json:",omit"`
	AccessControlList []*AccessControlRequest `json:"access_control_list"`
}

type PatchPermissionsOutput struct {
	ObjectType        ObjectType               `json:"object_type"`
	ObjectId          string                   `json:"object_id"`
	AccessControlList []*AccessControlResponse `json:"access_control_list"`
}

type TransferObjectInput struct {
	ObjectType ObjectType `json:"object_type"`
	ObjectId   string     `json:"object_id"`
	NewOwner   string     `json:"new_owner"`
}

type TransferObjectOutput struct {
	Message string `json:"message"`
}

// Permissions API is under "private preview" as of 2020-05-20.
// See permissions_api.pdf for documentation.
type PermissionsAPI interface {
	GetPermissions(context.Context, *GetPermissionsInput) (*GetPermissionsOutput, error)
	PutPermissions(context.Context, *PutPermissionsInput) (*PutPermissionsOutput, error)
	PatchPermissions(context.Context, *PatchPermissionsInput) (*PatchPermissionsOutput, error)
}

func (c *Client) GetPermissions(ctx context.Context, input *GetPermissionsInput) (*GetPermissionsOutput, error) {
	var output GetPermissionsOutput

	url, err := input.ObjectType.Url(input.ObjectId)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	if err := c.do(ctx, http.MethodGet, url, nil, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) PutPermissions(ctx context.Context, input *PutPermissionsInput) (*PutPermissionsOutput, error) {
	var output PutPermissionsOutput

	url, err := input.ObjectType.Url(input.ObjectId)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	if err := c.do(ctx, input.ObjectType.PermissionUpdateMethod(), url, input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) PatchPermissions(ctx context.Context, input *PatchPermissionsInput) (*PatchPermissionsOutput, error) {
	var output PatchPermissionsOutput

	url, err := input.ObjectType.Url(input.ObjectId)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	if err := c.do(ctx, http.MethodPatch, url, input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) TransferObject(ctx context.Context, input *TransferObjectInput) (*TransferObjectOutput, error) {
	var output TransferObjectOutput

	url, err := input.ObjectType.UnpluralizedUrl(input.ObjectId)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	if err := c.do(ctx, http.MethodPost, url+"/transfer", input, &output); err != nil {
		return &output, oops.Wrapf(err, "")
	}

	return &output, nil
}
