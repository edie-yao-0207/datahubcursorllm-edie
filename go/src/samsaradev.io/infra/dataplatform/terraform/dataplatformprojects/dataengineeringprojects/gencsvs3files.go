package dataengineeringprojects

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/controllers/api/versions/generated"
	"samsaradev.io/fleet/oem/oemproto"
	"samsaradev.io/hubproto"
	"samsaradev.io/hubproto/maptileproto"
	"samsaradev.io/iam/authz"
	"samsaradev.io/iam/authz/authzrole/authzuserrole"
	"samsaradev.io/infra/app/generate_terraform/awsresource"
	s3emitters "samsaradev.io/infra/app/generate_terraform/s3"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/releasemanagement/licenses"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/platform/licenseentity/licenseregistry"
	"samsaradev.io/platform/locationservices/driverassignment/driverassignmentsmodels"
	"samsaradev.io/platform/workflows/actiontypes"
	"samsaradev.io/platform/workflows/triggertypes"
	"samsaradev.io/products"
	"samsaradev.io/safety/behaviorlabel"
	"samsaradev.io/safety/coaching/coachingproto"
	"samsaradev.io/safety/driverstreaks/driverstreaksproto"
	"samsaradev.io/safety/eventdetection/types"
	"samsaradev.io/safety/infra/safetyeventtriagemodels"
	"samsaradev.io/safety/safetyproto"
	"samsaradev.io/stats/statsproto/ni/speedlimitsource"
	"samsaradev.io/stats/trips/tripsproto"
	"samsaradev.io/team"
)

// enumCSVConfig holds configuration for generating a simple enum-to-CSV mapping
type enumCSVConfig struct {
	resourceName string // e.g., "audio_alert_types_mapping"
	s3KeyPath    string // e.g., "databricks/s3tables/definitions/audio_alerts/audio_alert_types.csv"
}

// generateEnumCSVResource creates an S3BucketObject for a simple ID->Name enum mapping.
// This is a generic helper for the common pattern of iterating over a proto enum name map
// and writing (ID, Name) CSV rows sorted by ID.
func generateEnumCSVResource(region string, enumNameMap map[int32]string, config enumCSVConfig) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect and sort entries by ID
	type row struct {
		ID   int32
		Name string
	}
	var allRows []row
	for id, name := range enumNameMap {
		allRows = append(allRows, row{ID: id, Name: name})
	}
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].ID < allRows[j].ID
	})

	// Write CSV rows (ID, Name)
	for _, r := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(r.ID), 10),
			r.Name,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	return &awsresource.S3BucketObject{
		ResourceName: config.resourceName,
		Bucket:       region + "data-eng-mapping-tables",
		Key:          config.s3KeyPath,
		Content:      buffer.String(),
	}, nil
}

func getApiEndpointOwnersResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)
	for _, row := range generated.EndpointsWithOwner {
		if err := csvWriter.Write([]string{
			row.GqlKey,
			row.Team,
			row.Method,
			row.Route,
			row.Version,
			row.SloGroupName,
			strconv.Itoa(row.RateLimitPerSec),
			strconv.Itoa(row.RateLimitPerMin),
			strconv.Itoa(row.RateLimitPerHour),
			strconv.Itoa(row.RateLimitPerDay),
			strconv.Itoa(row.RateLimitBurst),
			strconv.FormatBool(row.RateLimitNotEnforced),
			strconv.FormatBool(row.RateLimitNotEnabled),
			row.Visibility,
			row.ReleaseState,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "api_endpoint_owners_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/api/api_endpoint_owners.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

func getProductsResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)
	var allProducts []products.ProductEntry
	for _, product := range products.ProductsById {
		allProducts = append(allProducts, product)
	}
	// Sort by product Id to ensure that items contents are the same order to
	// avoid terraform diffs between PRs.
	sort.SliceStable(allProducts, func(i, j int) bool {
		return allProducts[i].Id < allProducts[j].Id
	})

	for _, product := range allProducts {
		productId := strconv.FormatInt(product.Id, 10)
		productShortname := product.PublicName(products.VariantNone)
		productGroupId := strconv.FormatUint(uint64(product.ProductGroup), 10)
		if err := csvWriter.Write([]string{productId, productShortname, productGroupId}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "products_info",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/products/products.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil

}

func getProductVariantsResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)
	var allVariants []products.VariantEntry

	for _, product := range products.ProductsById {

		// for each product, get the info of relevant variant attributes
		variantsMap := product.VariantMap()
		for _, variant := range variantsMap {
			allVariants = append(allVariants, variant)
		}

		// Append the default variant attributes for each product
		productDefaultVariant := products.VariantEntry{
			ProductId:            product.Id,
			Id:                   0,
			ShortName:            product.PublicName(products.VariantNone),
			BatchListVariantName: product.BatchListProductName,
		}

		allVariants = append(allVariants, productDefaultVariant)
	}

	// Sort by productId then variant Id to ensure that item contents are in the same order
	// This is to avoid terraform diffs between PRs
	sort.Slice(allVariants, func(i, j int) bool {

		if allVariants[i].ProductId != allVariants[j].ProductId {
			return allVariants[i].ProductId < allVariants[j].ProductId
		}
		return allVariants[i].Id < allVariants[j].Id
	})

	for _, variant := range allVariants {
		ProductId := strconv.FormatInt(variant.ProductId, 10)
		variantId := strconv.FormatInt(int64(variant.Id), 10)
		variantShortName := variant.ShortName
		variantBatchListName := variant.BatchListVariantName

		if err := csvWriter.Write([]string{ProductId, variantId, variantShortName, variantBatchListName}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "product_variants_info",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/product_variants/product_variants.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil

}

// BuiltInRolePermissionRow represents a single row in the built-in roles permissions CSV.
// Each row maps a role to a single permission or permission set grant.
type BuiltInRolePermissionRow struct {
	RoleID       int64
	RoleUUID     string
	RoleName     string
	GrantID      string
	GrantType    string // "permission" or "permission_set"
	ViewAccess   bool
	EditAccess   bool
	CreateAccess bool
	UpdateAccess bool
	DeleteAccess bool
}

func getBuiltInRolesPermissionsResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	var rows []BuiltInRolePermissionRow

	// Iterate through all built-in roles
	for _, role := range authzuserrole.BuiltinRolesArray {
		roleID := int64(role.Id)
		roleUUID := role.Uuid.String()
		roleName := role.Name

		if role.AuthRole != nil {
			// Add permission sets
			for _, grant := range role.AuthRole.PermissionSets {
				rows = append(rows, BuiltInRolePermissionRow{
					RoleID:       roleID,
					RoleUUID:     roleUUID,
					RoleName:     roleName,
					GrantID:      grant.Id,
					GrantType:    "permission_set",
					ViewAccess:   grant.View,
					EditAccess:   grant.Edit,
					CreateAccess: grant.Create,
					UpdateAccess: grant.Update,
					DeleteAccess: grant.Delete,
				})
			}

			// Add individual permissions
			for _, grant := range role.AuthRole.Permissions {
				rows = append(rows, BuiltInRolePermissionRow{
					RoleID:       roleID,
					RoleUUID:     roleUUID,
					RoleName:     roleName,
					GrantID:      grant.Id,
					GrantType:    "permission",
					ViewAccess:   grant.View,
					EditAccess:   grant.Edit,
					CreateAccess: grant.Create,
					UpdateAccess: grant.Update,
					DeleteAccess: grant.Delete,
				})
			}
		}
	}

	// Sort by RoleID, then GrantType, then GrantID for consistent output
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].RoleID != rows[j].RoleID {
			return rows[i].RoleID < rows[j].RoleID
		}
		if rows[i].GrantType != rows[j].GrantType {
			return rows[i].GrantType < rows[j].GrantType
		}
		return rows[i].GrantID < rows[j].GrantID
	})

	// Write data rows
	for _, row := range rows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(row.RoleID, 10),
			row.RoleUUID,
			row.RoleName,
			row.GrantID,
			row.GrantType,
			strconv.FormatBool(row.ViewAccess),
			strconv.FormatBool(row.EditAccess),
			strconv.FormatBool(row.CreateAccess),
			strconv.FormatBool(row.UpdateAccess),
			strconv.FormatBool(row.DeleteAccess),
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "built_in_roles_permissions",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/iam/built_in_roles_permissions.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

// PermissionActionRow represents a single row in the permission-to-actions CSV.
// Each row maps a permission to a single action it grants at a specific access level.
type PermissionActionRow struct {
	PermissionSetID string
	PermissionID    string
	SubPermissionID string // Empty for parent-level actions, set for WriteSubPermissions
	AccessLevel     string // "read", "write", "create", "update", "delete"
	Action          string
}

func getPermissionActionsResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	var rows []PermissionActionRow

	// Iterate through all permission sets from the global Mappings registry
	for _, permissionSet := range authz.Mappings.List() {
		for _, permission := range permissionSet.Permissions {
			// Add read actions
			for action := range permission.Read {
				rows = append(rows, PermissionActionRow{
					PermissionSetID: permissionSet.Id,
					PermissionID:    permission.Id,
					SubPermissionID: "",
					AccessLevel:     "read",
					Action:          string(action),
				})
			}

			// Add write actions
			for action := range permission.Write {
				rows = append(rows, PermissionActionRow{
					PermissionSetID: permissionSet.Id,
					PermissionID:    permission.Id,
					SubPermissionID: "",
					AccessLevel:     "write",
					Action:          string(action),
				})
			}

			// Add write sub-permission actions
			for _, subPerm := range permission.WriteSubPermissions {
				for action := range subPerm.Write {
					rows = append(rows, PermissionActionRow{
						PermissionSetID: permissionSet.Id,
						PermissionID:    permission.Id,
						SubPermissionID: subPerm.Id,
						AccessLevel:     "write",
						Action:          string(action),
					})
				}
			}

			// Add create actions
			for action := range permission.Create {
				rows = append(rows, PermissionActionRow{
					PermissionSetID: permissionSet.Id,
					PermissionID:    permission.Id,
					SubPermissionID: "",
					AccessLevel:     "create",
					Action:          string(action),
				})
			}

			// Add update actions
			for action := range permission.Update {
				rows = append(rows, PermissionActionRow{
					PermissionSetID: permissionSet.Id,
					PermissionID:    permission.Id,
					SubPermissionID: "",
					AccessLevel:     "update",
					Action:          string(action),
				})
			}

			// Add delete actions
			for action := range permission.Delete {
				rows = append(rows, PermissionActionRow{
					PermissionSetID: permissionSet.Id,
					PermissionID:    permission.Id,
					SubPermissionID: "",
					AccessLevel:     "delete",
					Action:          string(action),
				})
			}
		}
	}

	// Sort by PermissionSetID, then PermissionID, then SubPermissionID, then AccessLevel, then Action for consistent output
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].PermissionSetID != rows[j].PermissionSetID {
			return rows[i].PermissionSetID < rows[j].PermissionSetID
		}
		if rows[i].PermissionID != rows[j].PermissionID {
			return rows[i].PermissionID < rows[j].PermissionID
		}
		if rows[i].SubPermissionID != rows[j].SubPermissionID {
			return rows[i].SubPermissionID < rows[j].SubPermissionID
		}
		if rows[i].AccessLevel != rows[j].AccessLevel {
			return rows[i].AccessLevel < rows[j].AccessLevel
		}
		return rows[i].Action < rows[j].Action
	})

	// Write data rows
	for _, row := range rows {
		if err := csvWriter.Write([]string{
			row.PermissionSetID,
			row.PermissionID,
			row.SubPermissionID,
			row.AccessLevel,
			row.Action,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "permission_actions_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/iam/permission_actions.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

// SkuBundleRow represents a single row in the SKU-to-Bundle mapping CSV.
type SkuBundleRow struct {
	Sku      string
	BundleID string
	IsLegacy bool
	IsAddon  bool
}

func getSkuBundlesResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	var rows []SkuBundleRow

	// Get all licenses from the registry
	registry := licenseregistry.NewDefaultRegistry()
	allLicenses := registry.AllLicenses()

	for _, license := range allLicenses {
		// Add each bundle associated with this SKU
		for _, bundle := range license.FeatureBundles {
			rows = append(rows, SkuBundleRow{
				Sku:      license.Sku,
				BundleID: bundle.Id,
				IsLegacy: license.IsLegacy,
				IsAddon:  license.IsAddon,
			})
		}
	}

	// Add virtual legacy "SKUs" for completeness. These are not real licenses—they are
	// assigned at runtime when an org has no P&P licenses. Their bundle/feature definitions
	// exist in bundle_features (from buildVirtualLicenses); we add sku_bundles rows here so
	// the full mapping is documented even though we cannot correlate orgs to them via EDW.
	for _, virtualSku := range []string{
		licenses.LegacySku_GENERAL,
		licenses.LegacySku_CMONLYFEB2021,
		licenses.LegacySku_CMONLY,
		licenses.LegacySku_SLEDLIGHT,
	} {
		rows = append(rows, SkuBundleRow{
			Sku:      virtualSku,
			BundleID: virtualSku,
			IsLegacy: true,
			IsAddon:  false,
		})
	}

	// Sort by SKU, then BundleID for consistent output
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Sku != rows[j].Sku {
			return rows[i].Sku < rows[j].Sku
		}
		return rows[i].BundleID < rows[j].BundleID
	})

	// Write data rows (no header)
	for _, row := range rows {
		if err := csvWriter.Write([]string{
			row.Sku,
			row.BundleID,
			strconv.FormatBool(row.IsLegacy),
			strconv.FormatBool(row.IsAddon),
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "sku_bundles_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/licenses/sku_bundles.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

// BundleFeatureRow represents a single row in the Bundle-to-Feature mapping CSV.
type BundleFeatureRow struct {
	BundleID                 string
	BundleName               string
	FeatureConfigKey         string
	Scope                    string
	RequiresEntityAssignment bool
	IsAddon                  bool
	IsGlobal                 bool
	IsLicenseOptional        bool
}

func getBundleFeaturesResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	var rows []BundleFeatureRow

	// Get all feature bundles from the store
	featureBundleStore := licenses.NewFeatureBundleStore(nil)
	// Use the direct mapping instead of iterating through all features
	keyToBundleIdsToFeature := featureBundleStore.GetKeyToBundleIdsToLicenseFeatureValue()

	for featureKey, bundleIdToFeature := range keyToBundleIdsToFeature {
		for bundleId, feature := range bundleIdToFeature {
			bundle := featureBundleStore.GetFeatureBundleForBundleId(bundleId)
			if bundle == nil {
				continue // Skip if bundle not found
			}

			rows = append(rows, BundleFeatureRow{
				BundleID:                 bundle.Id,
				BundleName:               bundle.Name,
				FeatureConfigKey:         featureKey,
				Scope:                    bundle.Scope.String(),
				RequiresEntityAssignment: bundle.RequireEntityAssignment,
				IsAddon:                  bundle.IsAddOnLicense,
				IsGlobal:                 feature.IsGlobal(),
				IsLicenseOptional:        feature.IsLicenseOptional(),
			})
		}
	}

	// Sort by BundleID, then FeatureConfigKey for consistent output
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].BundleID != rows[j].BundleID {
			return rows[i].BundleID < rows[j].BundleID
		}
		return rows[i].FeatureConfigKey < rows[j].FeatureConfigKey
	})

	// Write data rows (no header)
	for _, row := range rows {
		if err := csvWriter.Write([]string{
			row.BundleID,
			row.BundleName,
			row.FeatureConfigKey,
			row.Scope,
			strconv.FormatBool(row.RequiresEntityAssignment),
			strconv.FormatBool(row.IsAddon),
			strconv.FormatBool(row.IsGlobal),
			strconv.FormatBool(row.IsLicenseOptional),
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "bundle_features_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/licenses/bundle_features.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type triggerTypeCSVRow struct {
	TriggerTypeID   uint32
	TriggerTypeName string
}

func getTriggerTypesResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all trigger type entries
	var allRows []triggerTypeCSVRow

	// Process discrete triggers
	discreteEntries := triggertypes.DiscreteRegistryEntries()
	for _, discrete := range discreteEntries {
		// Skip Invalid trigger type (ID 0)
		if discrete.TriggerType == 0 {
			continue
		}
		triggerTypeName := discrete.TriggerType.String()
		// Remove "TriggerType_" prefix to match existing format
		triggerTypeName = strings.TrimPrefix(triggerTypeName, "TriggerType_")

		allRows = append(allRows, triggerTypeCSVRow{
			TriggerTypeID:   uint32(discrete.TriggerType),
			TriggerTypeName: triggerTypeName,
		})
	}

	// Process measure triggers
	measureEntries := triggertypes.MeasuresRegistryEntries()
	for _, measure := range measureEntries {
		// Skip Invalid trigger type (ID 0)
		if measure.TriggerType == 0 {
			continue
		}
		triggerTypeName := measure.TriggerType.String()
		// Remove "TriggerType_" prefix to match existing format
		triggerTypeName = strings.TrimPrefix(triggerTypeName, "TriggerType_")

		allRows = append(allRows, triggerTypeCSVRow{
			TriggerTypeID:   uint32(measure.TriggerType),
			TriggerTypeName: triggerTypeName,
		})
	}

	// Process scheduled triggers
	scheduledEntries := triggertypes.ScheduledRegistryEntries()
	for _, scheduled := range scheduledEntries {
		// Skip Invalid trigger type (ID 0)
		if scheduled.TriggerType == 0 {
			continue
		}
		triggerTypeName := scheduled.TriggerType.String()
		// Remove "TriggerType_" prefix to match existing format
		triggerTypeName = strings.TrimPrefix(triggerTypeName, "TriggerType_")

		allRows = append(allRows, triggerTypeCSVRow{
			TriggerTypeID:   uint32(scheduled.TriggerType),
			TriggerTypeName: triggerTypeName,
		})
	}

	// Sort by trigger type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].TriggerTypeID < allRows[j].TriggerTypeID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatUint(uint64(row.TriggerTypeID), 10),
			row.TriggerTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "trigger_types_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/triggers/alert_trigger_types.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type actionTypeCSVRow struct {
	ActionTypeID   uint32
	ActionTypeName string
}

func getActionTypesResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all action type entries
	var allRows []actionTypeCSVRow

	actionTypeEntries := actiontypes.GetActionTypeRegistry()
	for _, entry := range actionTypeEntries {
		// Skip NoActions (ID 0)
		if entry.ActionType == 0 {
			continue
		}
		actionTypeName := entry.ActionType.String()

		allRows = append(allRows, actionTypeCSVRow{
			ActionTypeID:   uint32(entry.ActionType),
			ActionTypeName: actionTypeName,
		})
	}

	// Sort by action type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].ActionTypeID < allRows[j].ActionTypeID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatUint(uint64(row.ActionTypeID), 10),
			row.ActionTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "action_types_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/action_types/alert_action_types.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

func getAudioAlertTypesResource(region string) (*awsresource.S3BucketObject, error) {
	return generateEnumCSVResource(region, hubproto.Cm3XAudioAlertInfo_EventType_name, enumCSVConfig{
		resourceName: "audio_alert_types_mapping",
		s3KeyPath:    "databricks/s3tables/definitions/audio_alerts/audio_alert_types.csv",
	})
}

func getBehaviorLabelTypesResource(region string) (*awsresource.S3BucketObject, error) {
	return generateEnumCSVResource(region, safetyproto.Behavior_Label_name, enumCSVConfig{
		resourceName: "behavior_label_types_mapping",
		s3KeyPath:    "databricks/s3tables/definitions/behavior_labels/behavior_label_type_enums.csv",
	})
}

func getCoachableItemTypesResource(region string) (*awsresource.S3BucketObject, error) {
	return generateEnumCSVResource(region, coachingproto.CoachableItemType_name, enumCSVConfig{
		resourceName: "coachable_item_types_mapping",
		s3KeyPath:    "databricks/s3tables/definitions/coaching/coachable_item_type.csv",
	})
}

func getCoachableItemStatusEnumsResource(region string) (*awsresource.S3BucketObject, error) {
	return generateEnumCSVResource(region, coachingproto.CoachableItemStatus_name, enumCSVConfig{
		resourceName: "coachable_item_status_enums_mapping",
		s3KeyPath:    "databricks/s3tables/definitions/coaching/coachable_item_status_enums.csv",
	})
}

func getDriverStreakTypeResource(region string) (*awsresource.S3BucketObject, error) {
	return generateEnumCSVResource(region, driverstreaksproto.DriverStreakType_name, enumCSVConfig{
		resourceName: "driver_streak_type_mapping",
		s3KeyPath:    "databricks/s3tables/definitions/driver_streaks/driver_streak_type.csv",
	})
}

type coachableItemBackingTypeCSVRow struct {
	CoachableItemBackingTypeID   int32
	CoachableItemBackingTypeName string
}

func getCoachableItemBackingTypeResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all coachable item backing type entries from the proto enum name map
	var allRows []coachableItemBackingTypeCSVRow

	for id, name := range coachingproto.CoachableItemBackingType_name {
		allRows = append(allRows, coachableItemBackingTypeCSVRow{
			CoachableItemBackingTypeID:   id,
			CoachableItemBackingTypeName: name,
		})
	}

	// Sort by coachable item backing type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].CoachableItemBackingTypeID < allRows[j].CoachableItemBackingTypeID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.CoachableItemBackingTypeID), 10),
			row.CoachableItemBackingTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "coachable_item_backing_type_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/coaching/coachable_item_backing_type.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type triageStateCSVRow struct {
	TriageStateID   int32
	TriageStateName string
}

func getTriageStateResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all triage state entries from the proto enum name map
	var allRows []triageStateCSVRow

	for id, name := range safetyeventtriagemodels.TriageState_name {
		allRows = append(allRows, triageStateCSVRow{
			TriageStateID:   id,
			TriageStateName: name,
		})
	}

	// Sort by triage state ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].TriageStateID < allRows[j].TriageStateID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.TriageStateID), 10),
			row.TriageStateName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "triage_state_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/triage/triage_state.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type safetyActivityTypeCSVRow struct {
	SafetyActivityTypeID   int32
	SafetyActivityTypeName string
}

func getSafetyActivityTypeEnumsResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all safety activity type entries from the proto enum name map
	var allRows []safetyActivityTypeCSVRow

	for id, name := range safetyproto.SafetyActivityType_name {
		allRows = append(allRows, safetyActivityTypeCSVRow{
			SafetyActivityTypeID:   id,
			SafetyActivityTypeName: name,
		})
	}

	// Sort by safety activity type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].SafetyActivityTypeID < allRows[j].SafetyActivityTypeID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.SafetyActivityTypeID), 10),
			row.SafetyActivityTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "safety_activity_type_enums_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/safety_activity/safety_activity_type_enums.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type oemTypeCSVRow struct {
	OemTypeID   int32
	OemTypeName string
}

func getOemTypeNameResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all OEM type entries from the proto enum name map
	var allRows []oemTypeCSVRow

	for id, name := range oemproto.OemType_name {
		allRows = append(allRows, oemTypeCSVRow{
			OemTypeID:   id,
			OemTypeName: name,
		})
	}

	// Sort by OEM type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].OemTypeID < allRows[j].OemTypeID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.OemTypeID), 10),
			row.OemTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "oem_type_name_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/oem/oem_type_name.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type locationSpeedLimitSourceTypeCSVRow struct {
	LocationSpeedLimitSourceTypeID   int32
	LocationSpeedLimitSourceTypeName string
}

func getLocationSpeedLimitSourceTypeEnumsResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all location speed limit source type entries from the proto enum name map
	var allRows []locationSpeedLimitSourceTypeCSVRow

	for id, name := range speedlimitsource.SpeedLimitSource_name {
		allRows = append(allRows, locationSpeedLimitSourceTypeCSVRow{
			LocationSpeedLimitSourceTypeID:   id,
			LocationSpeedLimitSourceTypeName: name,
		})
	}

	// Sort by location speed limit source type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].LocationSpeedLimitSourceTypeID < allRows[j].LocationSpeedLimitSourceTypeID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.LocationSpeedLimitSourceTypeID), 10),
			row.LocationSpeedLimitSourceTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "location_speed_limit_source_type_enums_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/location/location_speed_limit_source_type_enums.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type harshAccelTypeCSVRow struct {
	HarshAccelTypeID   int32
	HarshAccelTypeName string
}

func getHarshAccelTypeEnumsResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all harsh accel type entries from the proto enum name map
	var allRows []harshAccelTypeCSVRow

	for id, name := range hubproto.HarshAccelTypeEnum_name {
		allRows = append(allRows, harshAccelTypeCSVRow{
			HarshAccelTypeID:   id,
			HarshAccelTypeName: name,
		})
	}

	// Sort by harsh accel type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].HarshAccelTypeID < allRows[j].HarshAccelTypeID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.HarshAccelTypeID), 10),
			row.HarshAccelTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "harsh_accel_type_enums_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/harsh_events/harsh_accel_type_enums.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type detectionTypeCSVRow struct {
	DetectionTypeID   int32
	DetectionTypeName string
}

func getDetectionTypesResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all detection type entries from the proto enum name map
	var allRows []detectionTypeCSVRow

	for id, name := range safetyeventtriagemodels.DetectionType_name {
		allRows = append(allRows, detectionTypeCSVRow{
			DetectionTypeID:   id,
			DetectionTypeName: name,
		})
	}

	// Sort by detection type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].DetectionTypeID < allRows[j].DetectionTypeID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.DetectionTypeID), 10),
			row.DetectionTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "detection_types_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/detections/detection_types.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type detectionTypeToHarshAccelTypeCSVRow struct {
	DetectionTypeID    int32
	DetectionTypeName  string
	HarshAccelTypeID   int32
	HarshAccelTypeName string
}

func getDetectionTypeToHarshAccelTypeResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all harsh accel type to detection type mappings
	// Iterating over harsh accel types captures many-to-one mappings
	// (e.g., both haRolledStopSign and haTileRollingStopSign map to dtRolledStopSign)
	var allRows []detectionTypeToHarshAccelTypeCSVRow

	for harshAccelTypeID, harshAccelTypeName := range hubproto.HarshAccelTypeEnum_name {
		harshAccelType := hubproto.HarshAccelTypeEnum(harshAccelTypeID)
		detectionType := safetyeventtriagemodels.HarshAccelTypeEnumToDetectionType(harshAccelType)

		// Skip entries where the detection type is invalid
		if detectionType == safetyeventtriagemodels.DetectionType_dtInvalid {
			continue
		}

		detectionTypeID := int32(detectionType)
		detectionTypeName := detectionType.String()

		allRows = append(allRows, detectionTypeToHarshAccelTypeCSVRow{
			DetectionTypeID:    detectionTypeID,
			DetectionTypeName:  detectionTypeName,
			HarshAccelTypeID:   harshAccelTypeID,
			HarshAccelTypeName: harshAccelTypeName,
		})
	}

	// Sort by harsh accel type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].HarshAccelTypeID < allRows[j].HarshAccelTypeID
	})

	// Write CSV rows (DetectionTypeID, DetectionTypeName, HarshAccelTypeID, HarshAccelTypeName)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.DetectionTypeID), 10),
			row.DetectionTypeName,
			strconv.FormatInt(int64(row.HarshAccelTypeID), 10),
			row.HarshAccelTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "detection_type_to_harsh_accel_type_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/detections/detection_type_to_harsh_accel_type.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type detectionTypeToAudioAlertTypeCSVRow struct {
	DetectionTypeID    int32
	DetectionTypeName  string
	AudioAlertTypeID   int32
	AudioAlertTypeName string
}

func getDetectionTypeToAudioAlertTypeResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all detection type to audio alert type mappings
	var allRows []detectionTypeToAudioAlertTypeCSVRow

	for detectionTypeID, detectionTypeName := range safetyeventtriagemodels.DetectionType_name {
		detectionType := safetyeventtriagemodels.DetectionType(detectionTypeID)
		audioAlertType := safetyeventtriagemodels.DetectionTypeToAudioAlertEventType(detectionType)

		// Skip entries where the audio alert type is unknown
		if audioAlertType == hubproto.Cm3XAudioAlertInfo_UNKNOWN_EVENT {
			continue
		}

		audioAlertTypeID := int32(audioAlertType)
		audioAlertTypeName := audioAlertType.String()

		allRows = append(allRows, detectionTypeToAudioAlertTypeCSVRow{
			DetectionTypeID:    detectionTypeID,
			DetectionTypeName:  detectionTypeName,
			AudioAlertTypeID:   audioAlertTypeID,
			AudioAlertTypeName: audioAlertTypeName,
		})
	}

	// Sort by detection type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].DetectionTypeID < allRows[j].DetectionTypeID
	})

	// Write CSV rows (DetectionTypeID, DetectionTypeName, AudioAlertTypeID, AudioAlertTypeName)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.DetectionTypeID), 10),
			row.DetectionTypeName,
			strconv.FormatInt(int64(row.AudioAlertTypeID), 10),
			row.AudioAlertTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "detection_type_to_audio_alert_type_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/detections/detection_type_to_audio_alert_type.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type customerFacingFilterReasonCSVRow struct {
	FilterReasonID   int32
	FilterReasonName string
}

func getCustomerFacingFilterReasonResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all filter reason entries from the proto enum name map
	var allRows []customerFacingFilterReasonCSVRow

	for id, name := range types.FilterReason_name {
		allRows = append(allRows, customerFacingFilterReasonCSVRow{
			FilterReasonID:   id,
			FilterReasonName: name,
		})
	}

	// Sort by filter reason ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].FilterReasonID < allRows[j].FilterReasonID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.FilterReasonID), 10),
			row.FilterReasonName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "customer_facing_filter_reason_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/filter_reason/customer_facing_filter_reason.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type behaviorLabelToDetectionTypeCSVRow struct {
	BehaviorLabelID   int32
	BehaviorLabelName string
	DetectionTypeID   int32
	DetectionTypeName string
}

func getBehaviorLabelToDetectionTypeResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all detection type to behavior label mappings
	// Iterating over detection types captures many-to-one mappings
	// (e.g., both dtRolledStopSign and dtTileRollingStopSign map to RollingStop)
	var allRows []behaviorLabelToDetectionTypeCSVRow

	for detectionTypeID, detectionTypeName := range safetyeventtriagemodels.DetectionType_name {
		detectionType := safetyeventtriagemodels.DetectionType(detectionTypeID)
		behaviorLabel := safetyeventtriagemodels.DetectionTypeToSafetyProtoBehaviorLabelEnum(detectionType)

		// Skip entries where the behavior label is invalid
		if behaviorLabel == safetyproto.Behavior_Invalid {
			continue
		}

		behaviorLabelID := int32(behaviorLabel)
		behaviorLabelName := behaviorLabel.String()

		allRows = append(allRows, behaviorLabelToDetectionTypeCSVRow{
			BehaviorLabelID:   behaviorLabelID,
			BehaviorLabelName: behaviorLabelName,
			DetectionTypeID:   detectionTypeID,
			DetectionTypeName: detectionTypeName,
		})
	}

	// Sort by detection type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].DetectionTypeID < allRows[j].DetectionTypeID
	})

	// Write CSV rows (BehaviorLabelID, BehaviorLabelName, DetectionTypeID, DetectionTypeName)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.BehaviorLabelID), 10),
			row.BehaviorLabelName,
			strconv.FormatInt(int64(row.DetectionTypeID), 10),
			row.DetectionTypeName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "behavior_label_to_detection_type_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/behavior_labels/behavior_label_to_detection_type.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type behaviorLabelToCoachableItemTypeCSVRow struct {
	BehaviorLabelID     int32
	CoachableItemTypeID int32
}

func getBehaviorLabelToCoachableItemTypeResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all coachable item type to behavior label mappings
	// Iterating over coachable item types captures many-to-one mappings
	// (e.g., ROLLOVER, ROLLOVER_PROTECTION, etc. all map to Behavior_RolloverProtection)
	var allRows []behaviorLabelToCoachableItemTypeCSVRow

	for coachableItemTypeID := range coachingproto.CoachableItemType_name {
		coachableItemType := coachingproto.CoachableItemType(coachableItemTypeID)
		behaviorLabel := behaviorlabel.ConvertCoachableItemTypeToSafetyEventBehaviorLabel(coachableItemType)

		// Skip entries where the behavior label is invalid
		if behaviorLabel == safetyproto.Behavior_Invalid {
			continue
		}

		behaviorLabelID := int32(behaviorLabel)

		allRows = append(allRows, behaviorLabelToCoachableItemTypeCSVRow{
			BehaviorLabelID:     behaviorLabelID,
			CoachableItemTypeID: coachableItemTypeID,
		})
	}

	// Sort by coachable item type ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].CoachableItemTypeID < allRows[j].CoachableItemTypeID
	})

	// Write CSV rows (BehaviorLabelID, CoachableItemTypeID) - just IDs, no names
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.BehaviorLabelID), 10),
			strconv.FormatInt(int64(row.CoachableItemTypeID), 10),
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "behavior_label_to_coachable_item_type_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/behavior_labels/behavior_label_to_coachable_item_type.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type backendSpeedLimitSourceTypeCSVRow struct {
	SpeedLimitSourceID   int32
	SpeedLimitSourceName string
}

func getBackendSpeedLimitSourceTypeEnumsResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all speed limit source entries from the proto enum name map
	var allRows []backendSpeedLimitSourceTypeCSVRow

	for id, name := range maptileproto.SpeedLimitSource_name {
		allRows = append(allRows, backendSpeedLimitSourceTypeCSVRow{
			SpeedLimitSourceID:   id,
			SpeedLimitSourceName: name,
		})
	}

	// Sort by speed limit source ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].SpeedLimitSourceID < allRows[j].SpeedLimitSourceID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.SpeedLimitSourceID), 10),
			row.SpeedLimitSourceName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "backend_speed_limit_source_type_enums_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/location/backend_speed_limit_source_type_enums.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type mapMatchingMethodTypeCSVRow struct {
	MapMatchingMethodID   int32
	MapMatchingMethodName string
}

func getMapMatchingMethodTypeEnumsResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all map matching method entries from the proto enum name map
	var allRows []mapMatchingMethodTypeCSVRow

	for id, name := range maptileproto.MapMatchingMethod_name {
		allRows = append(allRows, mapMatchingMethodTypeCSVRow{
			MapMatchingMethodID:   id,
			MapMatchingMethodName: name,
		})
	}

	// Sort by map matching method ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].MapMatchingMethodID < allRows[j].MapMatchingMethodID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.MapMatchingMethodID), 10),
			row.MapMatchingMethodName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "map_matching_method_type_enums_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/location/map_matching_method_type_enums.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type surveyNotUsefulReasonCSVRow struct {
	NotUsefulReasonID   int32
	NotUsefulReasonName string
}

func getSurveyNotUsefulReasonResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all survey not useful reason entries from the proto enum name map
	var allRows []surveyNotUsefulReasonCSVRow

	for id, name := range safetyproto.HarshEventSurveyDetail_NotUsefulReason_name {
		allRows = append(allRows, surveyNotUsefulReasonCSVRow{
			NotUsefulReasonID:   id,
			NotUsefulReasonName: name,
		})
	}

	// Sort by not useful reason ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].NotUsefulReasonID < allRows[j].NotUsefulReasonID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.NotUsefulReasonID), 10),
			row.NotUsefulReasonName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "survey_not_useful_reason_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/surveys/survey_not_useful_reason.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type tripEndReasonCSVRow struct {
	TripEndReasonID   int32
	TripEndReasonName string
}

func getTripEndReasonResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all trip end reason entries from the proto enum name map
	var allRows []tripEndReasonCSVRow

	for id, name := range tripsproto.TripEndReason_name {
		allRows = append(allRows, tripEndReasonCSVRow{
			TripEndReasonID:   id,
			TripEndReasonName: name,
		})
	}

	// Sort by trip end reason ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].TripEndReasonID < allRows[j].TripEndReasonID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.TripEndReasonID), 10),
			row.TripEndReasonName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "trip_end_reason_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/trips/trip_end_reason.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

type driverAssignmentSourceCSVRow struct {
	DriverAssignmentSourceID   int32
	DriverAssignmentSourceName string
}

func getDriverAssignmentSourceResource(region string) (*awsresource.S3BucketObject, error) {
	var buffer bytes.Buffer
	csvWriter := csv.NewWriter(&buffer)

	// Collect all driver assignment source entries. We iterate a generous ID range; for IDs not in
	// driverassignmentsmodels.assignmentSourceDescriptions, String() returns the type name
	// "DriverAssignmentSource". We exclude those so only real enum values are included. New sources
	// added in that package are picked up automatically as long as they're within this range.
	var allRows []driverAssignmentSourceCSVRow
	for id := int32(-1); id <= 50; id++ {
		source := driverassignmentsmodels.DriverAssignmentSource(id)
		name := source.String()
		if name == "" || name == "DriverAssignmentSource" {
			continue
		}
		allRows = append(allRows, driverAssignmentSourceCSVRow{
			DriverAssignmentSourceID:   id,
			DriverAssignmentSourceName: name,
		})
	}

	// Sort by driver assignment source ID to ensure consistent ordering and avoid terraform diffs
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].DriverAssignmentSourceID < allRows[j].DriverAssignmentSourceID
	})

	// Write CSV rows (ID, Name)
	for _, row := range allRows {
		if err := csvWriter.Write([]string{
			strconv.FormatInt(int64(row.DriverAssignmentSourceID), 10),
			row.DriverAssignmentSourceName,
		}); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")
	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "driver_assignment_source_mapping",
		Bucket:       region + "data-eng-mapping-tables",
		Key:          "databricks/s3tables/definitions/driver_assignments/driver_assignment_source.csv",
		Content:      buffer.String(),
	}

	return s3Resource, nil
}

func GeneratedCSVS3FilesProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	dataEngMappingBucket := dataplatformresource.Bucket{
		Name:                             "data-eng-mapping-tables",
		Region:                           config.Region,
		LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[config.Region]),
		NonCurrentExpirationDaysOverride: 2,
	}
	rayCrossAccountBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: dataEngMappingBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(config.Cloud, "data-eng-mapping-tables"),
		},
	}

	dataEngMappingPolicy := &awsresource.IAMPolicy{
		ResourceName: "read_write_data_eng_mapping",
		Name:         "read-write-data-eng-mapping",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"s3:Get*",
						"s3:List*",
						"s3:Put*",
						"s3:Delete*",
					},
					Resource: []string{
						dataEngMappingBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dataEngMappingBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
			},
		},
	}

	apiendpointsownerss3Resource, err := getApiEndpointOwnersResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	productsS3Resource, err := getProductsResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	productVariantsS3Resource, err := getProductVariantsResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	builtInRolesPermissionsS3Resource, err := getBuiltInRolesPermissionsResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	permissionActionsS3Resource, err := getPermissionActionsResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	skuBundlesS3Resource, err := getSkuBundlesResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	bundleFeaturesS3Resource, err := getBundleFeaturesResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	triggerTypesS3Resource, err := getTriggerTypesResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	actionTypesS3Resource, err := getActionTypesResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	audioAlertTypesS3Resource, err := getAudioAlertTypesResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	behaviorLabelTypesS3Resource, err := getBehaviorLabelTypesResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	coachableItemTypesS3Resource, err := getCoachableItemTypesResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	coachableItemStatusEnumsS3Resource, err := getCoachableItemStatusEnumsResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	coachableItemBackingTypeS3Resource, err := getCoachableItemBackingTypeResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	triageStateS3Resource, err := getTriageStateResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	safetyActivityTypeEnumsS3Resource, err := getSafetyActivityTypeEnumsResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	oemTypeNameS3Resource, err := getOemTypeNameResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	locationSpeedLimitSourceTypeEnumsS3Resource, err := getLocationSpeedLimitSourceTypeEnumsResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	harshAccelTypeEnumsS3Resource, err := getHarshAccelTypeEnumsResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	detectionTypesS3Resource, err := getDetectionTypesResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	detectionTypeToHarshAccelTypeS3Resource, err := getDetectionTypeToHarshAccelTypeResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	detectionTypeToAudioAlertTypeS3Resource, err := getDetectionTypeToAudioAlertTypeResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	customerFacingFilterReasonS3Resource, err := getCustomerFacingFilterReasonResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	behaviorLabelToDetectionTypeS3Resource, err := getBehaviorLabelToDetectionTypeResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	behaviorLabelToCoachableItemTypeS3Resource, err := getBehaviorLabelToCoachableItemTypeResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	backendSpeedLimitSourceTypeEnumsS3Resource, err := getBackendSpeedLimitSourceTypeEnumsResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	mapMatchingMethodTypeEnumsS3Resource, err := getMapMatchingMethodTypeEnumsResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	surveyNotUsefulReasonS3Resource, err := getSurveyNotUsefulReasonResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	tripEndReasonS3Resource, err := getTripEndReasonResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	driverAssignmentSourceS3Resource, err := getDriverAssignmentSourceResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	driverStreakTypeS3Resource, err := getDriverStreakTypeResource(awsregionconsts.RegionPrefix[config.Region])
	if err != nil {
		return nil, err
	}

	p := &project.Project{
		RootTeam: team.DataEngineering.Name(),
		Provider: providerGroup,
		Class:    "dataEngBackendMetadata",
		Name:     "definitions",
		Group:    "backend_metadata_csvs",
		ResourceGroups: map[string][]tf.Resource{
			"data-eng-mapping-bucket": append(
				dataEngMappingBucket.Resources(),
				dataEngMappingPolicy,
			),
			"api-endpoint-owners-s3file":                    {apiendpointsownerss3Resource},
			"products-metadata-s3file":                      {productsS3Resource, productVariantsS3Resource},
			"built-in-roles-permissions-s3file":             {builtInRolesPermissionsS3Resource},
			"permission-actions-s3file":                     {permissionActionsS3Resource},
			"license-mappings-s3file":                       {skuBundlesS3Resource, bundleFeaturesS3Resource},
			"trigger-types-s3file":                          {triggerTypesS3Resource},
			"action-types-s3file":                           {actionTypesS3Resource},
			"audio-alert-types-s3file":                      {audioAlertTypesS3Resource},
			"behavior-label-types-s3file":                   {behaviorLabelTypesS3Resource},
			"coachable-item-types-s3file":                   {coachableItemTypesS3Resource},
			"coachable-item-status-enums-s3file":            {coachableItemStatusEnumsS3Resource},
			"coachable-item-backing-type-s3file":            {coachableItemBackingTypeS3Resource},
			"triage-state-s3file":                           {triageStateS3Resource},
			"safety-activity-type-enums-s3file":             {safetyActivityTypeEnumsS3Resource},
			"oem-type-name-s3file":                          {oemTypeNameS3Resource},
			"location-speed-limit-source-type-enums-s3file": {locationSpeedLimitSourceTypeEnumsS3Resource},
			"harsh-accel-type-enums-s3file":                 {harshAccelTypeEnumsS3Resource},
			"detection-types-s3file":                        {detectionTypesS3Resource},
			"detection-type-to-harsh-accel-type-s3file":     {detectionTypeToHarshAccelTypeS3Resource},
			"detection-type-to-audio-alert-type-s3file":     {detectionTypeToAudioAlertTypeS3Resource},
			"customer-facing-filter-reason-s3file":          {customerFacingFilterReasonS3Resource},
			"behavior-label-to-detection-type-s3file":       {behaviorLabelToDetectionTypeS3Resource},
			"behavior-label-to-coachable-item-type-s3file":  {behaviorLabelToCoachableItemTypeS3Resource},
			"backend-speed-limit-source-type-enums-s3file":  {backendSpeedLimitSourceTypeEnumsS3Resource},
			"map-matching-method-type-enums-s3file":         {mapMatchingMethodTypeEnumsS3Resource},
			"survey-not-useful-reason-s3file":               {surveyNotUsefulReasonS3Resource},
			"trip-end-reason-s3file":                        {tripEndReasonS3Resource},
			"driver-assignment-source-s3file":               {driverAssignmentSourceS3Resource},
			"driver-streak-type-s3file":                     {driverStreakTypeS3Resource},
			"cross-account-read-policy":                     {rayCrossAccountBucketPolicy},
		},
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
	})

	return p, nil
}
