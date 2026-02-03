package datastreamlake

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/fileformatter"
	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/sparktypes"
)

// skiplint: +banselectstar
var pythonLiveIngestionScriptTemplate = `
import time
from datetime import datetime, timedelta
from typing import List

import boto3
import concurrent.futures
import datadog
import service_credentials # required for ssm cloud credentials


def setUpDataDog(region):
    # Get Datadog keys from parameter store and initialize datadog
    boto3_session = boto3.Session(
        botocore_session=dbutils.credentials.getServiceCredentialsProvider(
            'standard-read-parameters-ssm'
        )
    )
    ssm_client = boto3_session.client("ssm", region_name=region)
    api_key = ssm_client.get_parameter(Name="DATADOG_API_KEY", WithDecryption=True)[
        "Parameter"
    ]["Value"]
    app_key = ssm_client.get_parameter(Name="DATADOG_APP_KEY", WithDecryption=True)[
        "Parameter"
    ]["Value"]
    datadog.initialize(api_key=api_key, app_key=app_key)


def send_datadog_count_metric(metric: str, count: int, tags: List[str]):
    try:
        datadog.api.Metric.send(
            metrics=[
                {
                    "metric": metric,
                    "points": [(time.time(), count)],
                    "type": "count",
                    "tags": tags,
                }
            ],
        )
    except Exception as e:
        print(f"Error sending metrics to Datadog: {e}")


def postMetrics(type, start_time, success, tableName):
    tags = [
        f"stream:{tableName}",
        f"region:{boto3.session.Session().region_name}",
        f"success:{success}",
    ]
    send_datadog_count_metric(f"datastreams.{type}.count", 1, tags)
    try:
        datadog.api.Metric.send(
            metric=f"datastreams.{type}.duration",
            points=time.time() - start_time,
            type="gauge",
            tags=tags,
        )
    except Exception as e:
        print(f"Error sending metrics to Datadog: {e}")


def generateCreateHistoryTableQuery(dataStream, dataStreamMetadata, s3prefix):
    columnNames = "{{backtick}}_firehoseIngestionDate{{backtick}} DATE"  # We will always have the firehose ingestion date as a column added to just the delta table
    partitionColumns = []
    for partitionColumn in dataStreamMetadata["partitionColumns"]:
        partitionColumnType = dataStreamMetadata["partitionColumns"][partitionColumn]
        columnNames += (
            f", {{backtick}}{partitionColumn}{{backtick}} {partitionColumnType}"
        )
        partitionColumns += [f"{{backtick}}{partitionColumn}{{backtick}}"]
    partitionColumns = ",".join(partitionColumns)
    createTableQuery = f'CREATE TABLE IF NOT EXISTS datastreams_history.{dataStream} ({columnNames}) USING DELTA LOCATION "s3://{s3prefix}data-streams-delta-lake/{dataStream}" PARTITIONED BY ({partitionColumns})'
    return createTableQuery


def generateCreateOrAlterViewQuery(dataStream):
    # SQL command to check if the view exists
    viewExists = spark.sql("SHOW VIEWS IN datastreams").filter(f"viewName = '{dataStream}'").collect()

    # Check if the view exists.
    if viewExists:
          # If the view exists, alter it.
          createOrAlterQuery = f"ALTER VIEW datastreams.{dataStream} AS SELECT * FROM datastreams_history.{dataStream} WHERE date > date_sub(current_date(), 365)"
    else:
          # If the view does not exist, create it.
          createOrAlterQuery = f"CREATE VIEW datastreams.{dataStream} AS SELECT * FROM datastreams_history.{dataStream} WHERE date > date_sub(current_date(), 365)"
    return createOrAlterQuery


def generateCopyIntoQuery(date, stream, streamInfo, s3prefix):
    if streamInfo["DateColumnOverrideExpression"] != "":
        dateExpression = streamInfo["DateColumnOverrideExpression"] + " as date"
    elif streamInfo["DateColumnOverride"] != "":
        dateExpression = f"date({streamInfo['DateColumnOverride']}) as date"
    else:
        dateExpression = f"cast('{date}' AS DATE) as date"
    query = f"COPY INTO datastreams_history.{stream} FROM (SELECT *, {dateExpression}, cast('{str(date)}' AS DATE) as _firehoseIngestionDate FROM 's3://{s3prefix}data-stream-lake/{stream}/data/date={str(date)}') FILEFORMAT = Parquet  COPY_OPTIONS ('mergeSchema' = 'true')"
    return query


def createTable(datastreamsMetadataInfo, s3prefix):
    # Fetch existing table names in raw and history DBs
    existing_history_tables = set(
        row["tableName"]
        for row in spark.sql("SHOW TABLES IN datastreams_history")
        .select("tableName")
        .collect()
    )
    for dataStream, dataStreamMetadata in datastreamsMetadataInfo.items():
        # Create datastreams_history tables if they don't exist
        if dataStream not in existing_history_tables:
            createHistoryTableQuery = generateCreateHistoryTableQuery(
                dataStream, dataStreamMetadata, s3prefix
            )
            create_history_table_start_time = time.time()
            print(f"Running create table query: {createHistoryTableQuery}")
            spark.sql(createHistoryTableQuery)
            postMetrics(
                "create_history_table",
                create_history_table_start_time,
                True,
                dataStream,
            )

        createOrAlterQuery = generateCreateOrAlterViewQuery(dataStream)
        create_view_start_time = time.time()
        print(f"Running create/alter view query: {createOrAlterQuery}")
        spark.sql(createOrAlterQuery)
        postMetrics("create_view", create_view_start_time, True, dataStream)


def copyPartitionQuery(stream, streamInfo, s3prefix):
    today = datetime.utcnow().date()

    def run_copy(target_date, day_label, metric_name):
        start_time = time.time()
        result = {"success": False, "no_data": False, "error": ""}
        try:
            query = generateCopyIntoQuery(target_date, stream, streamInfo, s3prefix)
            print(f"Running copy into query for {day_label}: {query}")
            spark.sql(query)
            print(f"Finished copying {day_label} data for {stream} query {query}")
            postMetrics(metric_name, start_time, True, stream)
            result["success"] = True
        except Exception as e:
            if "Path does not exist" not in str(e):
                print(
                    f"exception for stream: {stream} in copying {day_label}'s data, date: {target_date} exception: {e}"
                )
                postMetrics(metric_name, start_time, False, stream)
                result["error"] = str(e)
            else:
                # If the path doesn't exist we have no records to ingest and don't want to log it as an error            
                print(
                    f"No data found for {stream} on {target_date}; skipping {day_label} copy."
                )
                # Optional observability: count "no data" occurrences
                send_datadog_count_metric(
                    "datastreams.live_ingestion.no_data_count",
                    1,
                    [
                        f"stream:{stream}",
                        f"region:{boto3.session.Session().region_name}",
                        f"day:{day_label}",
                    ],
                )
                result["no_data"] = True
        return result

    yesterday = today - timedelta(days=1)
    y_res = run_copy(yesterday, "yesterday", "copy_table_yesterday")
    t_res = run_copy(today, "today", "copy_table_today")
    return {"stream": stream, "yesterday": y_res, "today": t_res}


def getActiveDatastreamsMetadata(allDatastreamsMetadata, s3prefix):
    """
    getActiveDatastreamsMetadata filters out any datastreams that were never active from
    allDatastreamsMetadata by checking whether they have ever had raw data written to S3.
    """
    activeDatastreams = {}
    for streamName in allDatastreamsMetadata:
        try :
            dbutils.fs.ls(f"s3://{s3prefix}data-stream-lake/{streamName}/data")
        except:
            print(
                f"Skip processing datastream {streamName} because there is no \
                  raw data in s3://{s3prefix}data-stream-lake/{streamName}/data"
            )
            send_datadog_count_metric(
                "datastreams.live_ingestion.no_data_count",
                1,
                [
                    f"stream:{streamName}",
                    f"region:{boto3.session.Session().region_name}",
                ],
            )
            continue
        activeDatastreams[streamName] = allDatastreamsMetadata[streamName]
    return activeDatastreams


def main():
    region = boto3.session.Session().region_name
    s3prefix = None
    if region == "us-west-2":
        s3prefix = "samsara-"
    elif region == "eu-west-1":
        s3prefix = "samsara-eu-"
    elif region == "ca-central-1":
        s3prefix = "samsara-ca-"
    else:
        raise Exception("bad region: " + region)

    setUpDataDog(region)
    datastreamsMetadata = {{partitionInfo}}
    activeDatastreamsMetadata = getActiveDatastreamsMetadata(
        datastreamsMetadata, s3prefix
    )
    createTable(activeDatastreamsMetadata, s3prefix)

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        future_to_stream = {}
        for stream in activeDatastreamsMetadata:
            streamInfo = activeDatastreamsMetadata[stream]
            future = executor.submit(copyPartitionQuery, stream, streamInfo, s3prefix)
            future_to_stream[future] = stream

        for future in concurrent.futures.as_completed(future_to_stream):
            stream = future_to_stream[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Copying stream {stream} failed with exception: {e}")
                results.append(
                    {
                        "stream": stream,
                        "yesterday": {"success": False, "no_data": False, "error": str(e)},
                        "today": {"success": False, "no_data": False, "error": str(e)},
                    }
                )

    # Build summary
    yesterday_success = sum(1 for r in results if r["yesterday"]["success"])
    yesterday_failed_streams = [
        r["stream"]
        for r in results
        if not r["yesterday"]["success"] and not r["yesterday"]["no_data"]
    ]
    yesterday_no_data_streams = [
        r["stream"] for r in results if r["yesterday"]["no_data"]
    ]
    today_success = sum(1 for r in results if r["today"]["success"])
    today_failed_streams = [
        r["stream"]
        for r in results
        if not r["today"]["success"] and not r["today"]["no_data"]
    ]
    today_no_data_streams = [r["stream"] for r in results if r["today"]["no_data"]]

    print("DataStreams copy summary:")
    print(f"- Yesterday successful: {yesterday_success}")
    print(
        f"- Yesterday failed: {len(yesterday_failed_streams)}; streams: {yesterday_failed_streams}"
    )
    print(
        f"- Yesterday no data: {len(yesterday_no_data_streams)}; streams: {yesterday_no_data_streams}"
    )
    print(f"- Today successful: {today_success}")
    print(
        f"- Today failed: {len(today_failed_streams)}; streams: {today_failed_streams}"
    )
    print(
        f"- Today no data: {len(today_no_data_streams)}; streams: {today_no_data_streams}"
    )

    if len(yesterday_failed_streams) > 0 or len(today_failed_streams) > 0:
        raise RuntimeError(
            "At least one copyPartitionQuery failed. See error messages above"
        )


if __name__ == "__main__":
    main()
`

// generateDataStreamsPythonInfo reads all the datastreams in the registry and adds information
// about the partition strategy to a dictionary that is in the script. We need this to be updated
// every time the partition strategy changes or we add a new data stream to ensure we can create
// the table correctly.
func generateDataStreamsPythonInfo() string {
	partitionInfo := "{\n"
	for _, datastream := range Registry {
		if datastream.PartitionStrategy != nil {
			partitionColumns := make(map[string]string, len(datastream.PartitionStrategy.PartitionColumns))
			for _, partitionCol := range datastream.PartitionStrategy.PartitionColumns {
				if partitionCol == "date" {
					partitionColumns[partitionCol] = "DATE"
				} else {
					sparkType, err := sparktypes.JsonTypeToSparkType(reflect.TypeOf(datastream.Record), sparktypes.ConversionParams{})
					if err != nil {
						panic(fmt.Sprintf("Unable to get Spark Type from Go Type for registry entry %s", datastream.StreamName))
					}
					colType := ""
					for _, col := range sparkType.Fields {
						if col.Name == partitionCol {
							colType = fmt.Sprintf("%s", col.Type)
						}
					}
					partitionColumns[partitionCol] = colType
				}

			}

			marshalledPartitionColumns, err := json.Marshal(partitionColumns)
			if err != nil {
				panic(fmt.Sprintf("Unable to json partition columns %s for data stream %s", marshalledPartitionColumns, datastream.StreamName))
			}
			partitionInfo += fmt.Sprintf("\"%s\": {\"partitionColumns\":%s, \"DateColumnOverride\":\"%s\", \"DateColumnOverrideExpression\":\"%s\"},\n", datastream.StreamName, string(marshalledPartitionColumns), datastream.PartitionStrategy.DateColumnOverride, datastream.PartitionStrategy.DateColumnOverrideExpression)
		}
	}
	// Remove the trailing new line character and quote
	partitionInfo = partitionInfo[:len(partitionInfo)-2]
	// Add the ending bracket for the dictionary
	partitionInfo += "\n}\n"
	return partitionInfo
}

func backTick() string {
	return "`"
}

// GenerateDataStreamCopyToDeltaNotebook generates the code for a scheduled notebook that reads all the data streams
// in the registry and creates the delta tables/copy data into it from the original tables.
func GenerateDataStreamCopyToDeltaNotebook() error {
	templateFunctions := template.FuncMap{
		"partitionInfo": generateDataStreamsPythonInfo,
		"backtick":      backTick,
	}

	tmpl, err := template.New("data_streams_delta_live_ingestion_format").Funcs(templateFunctions).Parse(pythonLiveIngestionScriptTemplate)
	if err != nil {
		return oops.Wrapf(err, "unable to parse streamWriterTemplate string")
	}

	directoryPath := filepath.Join(filepath.Join(filepathhelpers.BackendRoot, "dataplatform/notebooks/dataplatform/"))
	if err := os.Mkdir(directoryPath, 0777); err != nil && !strings.Contains(string(err.Error()), "file exist") {
		return oops.Wrapf(err, "error creating directory at %s.", directoryPath)
	}

	filePath := filepath.Join(directoryPath, "data_streams_live_ingestion.py")
	f, err := os.Create(filePath)
	if err != nil {
		return oops.Wrapf(err, "error creating file at %s", filePath)
	}

	if err := fileformatter.ExecuteTmplWithFormat(f, nil, tmpl, fileformatter.Python); err != nil {
		return oops.Wrapf(err, "Unable to execute live ingestion file generation for copying data streams to delta tables.")
	}

	return nil
}
