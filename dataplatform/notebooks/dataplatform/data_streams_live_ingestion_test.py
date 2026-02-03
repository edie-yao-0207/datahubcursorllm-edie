import pytest
from datetime import date

from data_streams_live_ingestion import generateCopyIntoQuery, generateCreateTableQuery


def test_generate_copy_into_query():
    test_date = date.fromisoformat("2022-07-26")
    query = generateCopyIntoQuery(
        test_date,
        "test_stream",
        {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "samsara-",
    )
    assert (
        query
        == "COPY INTO datastreams_history.test_stream FROM (SELECT *, cast('2022-07-26' AS DATE) as date, cast('2022-07-26' AS DATE) as _firehoseIngestionDate FROM 's3://samsara-data-stream-lake/test_stream/data/date=2022-07-26') FILEFORMAT = Parquet  COPY_OPTIONS ('mergeSchema' = 'true')"
    )

    test_date = date.fromisoformat("2022-07-25")
    query = generateCopyIntoQuery(
        test_date,
        "test_stream_2",
        {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "samsara-",
    )
    assert (
        query
        == "COPY INTO datastreams_history.test_stream_2 FROM (SELECT *, cast('2022-07-25' AS DATE) as date, cast('2022-07-25' AS DATE) as _firehoseIngestionDate FROM 's3://samsara-data-stream-lake/test_stream_2/data/date=2022-07-25') FILEFORMAT = Parquet  COPY_OPTIONS ('mergeSchema' = 'true')"
    )

    test_date = date.fromisoformat("2022-07-24")
    query = generateCopyIntoQuery(
        test_date,
        "test_stream_3",
        {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(created_at)",
        },
        "samsara-",
    )
    assert (
        query
        == "COPY INTO datastreams_history.test_stream_3 FROM (SELECT *, date(created_at) as date, cast('2022-07-24' AS DATE) as _firehoseIngestionDate FROM 's3://samsara-data-stream-lake/test_stream_3/data/date=2022-07-24') FILEFORMAT = Parquet  COPY_OPTIONS ('mergeSchema' = 'true')"
    )

    test_date = date.fromisoformat("2022-07-24")
    query = generateCopyIntoQuery(
        test_date,
        "test_stream_3",
        {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(created_at)",
        },
        "samsara-eu-",
    )
    assert (
        query
        == "COPY INTO datastreams_history.test_stream_3 FROM (SELECT *, date(created_at) as date, cast('2022-07-24' AS DATE) as _firehoseIngestionDate FROM 's3://samsara-eu-data-stream-lake/test_stream_3/data/date=2022-07-24') FILEFORMAT = Parquet  COPY_OPTIONS ('mergeSchema' = 'true')"
    )


def test_generate_create_table():
    query = generateCreateTableQuery(
        "test_stream",
        {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "samsara-",
    )
    assert (
        query
        == 'CREATE TABLE IF NOT EXISTS datastreams_history.test_stream (`_firehoseIngestionDate` DATE, `date` DATE) USING DELTA LOCATION "s3://samsara-data-streams-delta-lake/test_stream"  PARTITIONED BY  (`date`)'
    )

    query = generateCreateTableQuery(
        "test_stream_2",
        {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "samsara-",
    )
    assert (
        query
        == 'CREATE TABLE IF NOT EXISTS datastreams_history.test_stream_2 (`_firehoseIngestionDate` DATE, `date` DATE) USING DELTA LOCATION "s3://samsara-data-streams-delta-lake/test_stream_2"  PARTITIONED BY  (`date`)'
    )

    query = generateCreateTableQuery(
        "test_stream_3",
        {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(created_at)",
        },
        "samsara-",
    )
    assert (
        query
        == 'CREATE TABLE IF NOT EXISTS datastreams_history.test_stream_3 (`_firehoseIngestionDate` DATE, `date` DATE) USING DELTA LOCATION "s3://samsara-data-streams-delta-lake/test_stream_3"  PARTITIONED BY  (`date`)'
    )

    query = generateCreateTableQuery(
        "test_stream_3",
        {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(created_at)",
        },
        "samsara-eu-",
    )
    assert (
        query
        == 'CREATE TABLE IF NOT EXISTS datastreams_history.test_stream_3 (`_firehoseIngestionDate` DATE, `date` DATE) USING DELTA LOCATION "s3://samsara-eu-data-streams-delta-lake/test_stream_3"  PARTITIONED BY  (`date`)'
    )
