import os
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    OtherSchemaClass
)
from datahub.emitter.rest_emitter import DatahubRestEmitter

token = os.getenv("DATAHUB_TOKEN")
# this will need to become dynamic
gms_server="http://datahub-gms.internal.samsara.com:8080"

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(
    gms_server=gms_server,
    extra_headers={
        'Authorization': f'Bearer {token}'
    }
)

# Test the connection
emitter.test_connection()

columns = [
    SchemaFieldClass(
        fieldPath="column1",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),  # Assuming column1 is a string
        nativeDataType="VARCHAR",
        description="Description of column1"
    ),
    SchemaFieldClass(
        fieldPath="column2",
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),  # Assuming column2 is a string
        nativeDataType="BIGINT",
        description="Description of column2"
    ),
]

# Create the SchemaMetadataClass instance
schema_metadata = SchemaMetadataClass(
    schemaName="sample_table_2_schema",
    platform=builder.make_data_platform_urn("databricks"),
    version=0,
    hash="",  # You may want to compute a hash for the schema
    platformSchema=OtherSchemaClass(rawSchema="__insert raw schema here__"),  # Provide platformSchema argument
    fields=columns
)

# Create the DatasetPropertiesClass instance
dataset_properties = DatasetPropertiesClass(
    description="This table stored the canonical User profile",
    customProperties={
        "governance": "ENABLED"
    }
)

# Construct MetadataChangeProposalWrapper objects
metadata_event_properties = MetadataChangeProposalWrapper(
    entityUrn=builder.make_dataset_urn("databricks", "sample_table_2"),
    aspect=dataset_properties,
)

metadata_event_schema = MetadataChangeProposalWrapper(
    entityUrn=builder.make_dataset_urn("databricks", "sample_table_2"),
    aspect=schema_metadata,
)

# Emit metadata! These are blocking calls
emitter.emit(metadata_event_properties)
emitter.emit(metadata_event_schema)
