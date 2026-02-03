package deltalake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/parquet2json"
	"samsaradev.io/infra/samsaraaws/s3iface"
)

// https://github.com/delta-io/delta/blob/43f40d7c6fef3b75b8e97026eb22589b127972ea/PROTOCOL.md#last-checkpoint-file
type CheckpointVersion struct {
	Version int64 `json:"version"`
	Size    int64 `json:"size"`
	Parts   int64 `json:"parts"`
}

func (v CheckpointVersion) String() string {
	b, err := json.Marshal(v)
	if err != nil {
		// Should never happen.
		panic(err)
	}
	return string(b)
}

var CheckpointFileRegex = regexp.MustCompile(`^([0-9]{20,20})\.checkpoint.(([0-9]{10,10}).([0-9]{10,10})\.)?parquet$`)

type Format struct {
	Provider string                 `json:"provider"`
	Options  map[string]interface{} `json:"options"`
}

type StringElement struct {
	Element string `json:"element"`
}

type StringArray struct {
	List []StringElement `json:"list"`
}

type ChangeMetadata struct {
	Id               string            `json:"id,omitempty"`
	Name             string            `json:"name,omitempty"`
	Description      string            `json:"description,omitempty"`
	Format           *Format           `json:"format,omitempty"`
	SchemaString     string            `json:"schemaString,omitempty"`
	PartitionColumns StringArray       `json:"partitionColumns,omitempty"`
	Configuration    map[string]string `json:"configuration,omitempty"`
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KeyValues struct {
	KeyValue []*KeyValue `json:"key_value,omitempty"`
}

type Add struct {
	Path             string    `json:"path,omitempty"`
	PartitionValues  KeyValues `json:"partitionValues,omitempty"`
	Size             int64     `json:"size,omitempty"`
	ModificationTime int64     `json:"modificationTime,omitempty"`
	DataChange       bool      `json:"dataChange,omitempty"`
	Tags             KeyValues `json:"tags,omitempty"`
}

type Remove struct {
	Path              string `json:"path,omitempty"`
	DeletionTimestamp int64  `json:"deletionTimestamp,omitempty"`
	DataChange        bool   `json:"dataChange,omitempty"`
}

type Transaction struct {
	AppId   string `json:"appId"`
	Version int64  `json:"version"`
}

type Job struct {
	JobId       string `json:"jobId,omitempty"`
	JobName     string `json:"jobName,omitempty"`
	RunId       string `json:"runId,omitempty"`
	JobOwnerId  string `json:"jobOwnerId,omitempty"`
	TriggerType string `json:"triggerType,omitempty"`
}

type CommitInfo struct {
	Version        int64  `json:"version,omitempty"`
	Timestamp      int64  `json:"timestamp,omitempty"`
	UserId         string `json:"userId,omitempty"`
	UserName       string `json:"userName,omitempty"`
	Operation      string `json:"operation,omitempty"`
	Job            *Job   `json:"job,omitempty"`
	ClusterId      string `json:"clusterId,omitempty"`
	ReadVersion    int64  `json:"readVersion,omitempty"`
	IsolationLevel string `json:"isolationLevel,omitempty"`
	IsBlindAppend  bool   `json:"isBlindAppend,omitempty"`
}

type Protocol struct {
	MinReaderVersion int64 `json:"minReaderVersion,omitempty"`
	MinWriterVersion int64 `json:"minWriterVersion,omitempty"`
}

type Action struct {
	MetaData    *ChangeMetadata `json:"metaData,omitempty"`
	Add         *Add            `json:"add,omitempty"`
	Remove      *Remove         `json:"remove,omitempty"`
	Transaction *Transaction    `json:"txn,omitempty"`
	CommitInfo  *CommitInfo     `json:"commitInfo,omitempty"`
	Protocol    *Protocol       `json:"protocol,omitempty"`
}

type DeltaLogVersion struct {
	Version int64
	Actions []*Action
}

type DeltaLog struct {
	Version int64
	History []*DeltaLogVersion
}

func (log *DeltaLog) Add(v *DeltaLogVersion) error {
	if log.Version != 0 && v.Version != log.Version+1 {
		return oops.Errorf("bad version: %d", v.Version)
	}
	log.Version = v.Version
	log.History = append(log.History, v)
	return nil
}

func readS3File(ctx context.Context, s3API s3iface.S3API, bucket, key string) ([]byte, error) {
	output, err := s3API.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "get s3://%s/%s", bucket, key)
	}
	defer output.Body.Close()

	b, err := ioutil.ReadAll(output.Body)
	if err != nil {
		return nil, oops.Wrapf(err, "read s3://%s/%s", bucket, key)
	}
	return b, nil
}

// readLatestCheckpointVersionBeforeVersion finds the latest checkpoint that is before maxVersion.
func readLatestCheckpointVersionBeforeVersion(ctx context.Context, s3API s3iface.S3API, bucket, prefix string, maxVersion int64) (*CheckpointVersion, error) {
	// S3 cannot list objects in reverse order, so pick a starting version = maxVersion - 100.
	afterVersion := maxVersion - 100
	if afterVersion < 0 {
		afterVersion = 0
	}

	// List checkpoints since afterVersion and break when version exceeds maxVersion.
	var contents []*s3.Object
	startAfter := fmt.Sprintf("%s/%020d.json", prefix, afterVersion)
	endBefore := fmt.Sprintf("%s/%020d.checkpoint", prefix, maxVersion+1)

	err := s3API.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		Prefix:     aws.String(fmt.Sprintf("%s", prefix)),
		StartAfter: aws.String(startAfter),
	}, func(output *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range output.Contents {
			if aws.StringValue(obj.Key) < endBefore {
				contents = append(contents, obj)
				continue
			}
			return false
		}
		return true
	})
	if err != nil {
		return nil, oops.Wrapf(err, "s3 list: %s, %s", prefix, startAfter)
	}

	for i := len(contents) - 1; i >= 0; i-- {
		base := filepath.Base(aws.StringValue(contents[i].Key))
		match := CheckpointFileRegex.FindStringSubmatch(base)
		if len(match) == 0 {
			continue
		}

		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return nil, oops.Wrapf(err, "parse key: %s", base)
		}

		parts := int64(0)
		if match[3] != "" {
			parts, err = strconv.ParseInt(match[3], 10, 64)
			if err != nil {
				return nil, oops.Wrapf(err, "parse key: %s", base)
			}
		}

		return &CheckpointVersion{
			Version: version,
			Parts:   parts,
		}, nil
	}

	if afterVersion == 0 {
		// No checkpoint yet.
		return nil, nil
	}

	return nil, oops.Errorf("checkpoint not found in s3://%s between %s and %s", bucket, startAfter, endBefore)
}

func readLatestCheckpointVersion(ctx context.Context, s3API s3iface.S3API, bucket, prefix string) (*CheckpointVersion, error) {
	key := fmt.Sprintf("%s/%s", prefix, "_last_checkpoint")
	body, err := readS3File(ctx, s3API, bucket, key)
	if err != nil {
		if aerr, ok := oops.Cause(err).(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return nil, nil
		}
		return nil, oops.Wrapf(err, "get last checkpoint s3://%s/%s", bucket, key)
	}
	var version CheckpointVersion
	if err := json.Unmarshal(body, &version); err != nil {
		return nil, oops.Wrapf(err, "parse last checkpoint s3://%s/%s", bucket, key)
	}
	return &version, nil
}

func parseCheckpointParquetFile(b []byte) ([]*Action, error) {
	f, err := ioutil.TempFile("", "*.parquet")
	if err != nil {
		return nil, oops.Wrapf(err, "temp file")
	}
	defer os.Remove(f.Name())

	if _, err := f.Write(b); err != nil {
		return nil, oops.Wrapf(err, "write temp")
	}
	if err := f.Close(); err != nil {
		return nil, oops.Wrapf(err, "close temp")
	}

	r, err := parquet2json.Open(f.Name())
	if err != nil {
		return nil, oops.Wrapf(err, "parquet2json open: %s", f.Name())
	}
	defer r.Close()

	var actions []*Action
	for {
		var action Action
		if err := r.Read(&action); err != nil {
			if err == io.EOF {
				break
			}
			return nil, oops.Wrapf(err, "read")
		}
		actions = append(actions, &action)
	}
	return actions, nil
}

func readCheckpoint(ctx context.Context, s3API s3iface.S3API, bucket, prefix string, version CheckpointVersion) ([]*Action, error) {
	var keys []string
	if version.Parts == 0 {
		// Single checkpoint file.
		keys = append(keys, fmt.Sprintf("%s/%020d.checkpoint.parquet", prefix, version.Version))
	} else {
		// Multi-part checkpoint.
		for o := int64(1); o <= version.Parts; o++ {
			keys = append(keys, fmt.Sprintf("%s/%020d.checkpoint.%010d.%010d.parquet", prefix, version.Version, o, version.Parts))
		}
	}

	var actions []*Action
	for _, key := range keys {
		body, err := readS3File(ctx, s3API, bucket, key)
		if err != nil {
			return nil, oops.Wrapf(err, "read parquet checkpoint: s3://%s/%s", bucket, key)
		}
		partActions, err := parseCheckpointParquetFile(body)
		if err != nil {
			return nil, oops.Wrapf(err, "parse parquet checkpoint: s3://%s/%s", bucket, key)
		}
		actions = append(actions, partActions...)
	}

	return actions, nil
}

func readLog(ctx context.Context, s3API s3iface.S3API, bucket, prefix string, version int64) ([]*Action, error) {
	key := fmt.Sprintf("%s/%020d.json", prefix, version)
	body, err := readS3File(ctx, s3API, bucket, key)
	if err != nil {
		return nil, oops.Wrapf(err, "read log: s3://%s/%s", bucket, key)
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	var actions []*Action
	for {
		var action Action
		if err := decoder.Decode(&action); err != nil {
			if err == io.EOF {
				break
			}
			return nil, oops.Wrapf(err, "parse log: s3://%s/%s", bucket, key)
		}
		actions = append(actions, &action)
	}
	return actions, nil
}

func listLatestVersion(ctx context.Context, s3API s3iface.S3API, bucket, prefix string, since int64) (int64, error) {
	var contents []*s3.Object
	startAfter := fmt.Sprintf("%s/%020d.json", prefix, since)
	err := s3API.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		Prefix:     aws.String(fmt.Sprintf("%s", prefix)),
		StartAfter: aws.String(startAfter),
	}, func(output *s3.ListObjectsV2Output, _ bool) bool {
		contents = append(contents, output.Contents...)
		return true
	})
	if err != nil {
		return 0, oops.Wrapf(err, "s3 list: %s, %s", prefix, startAfter)
	}

	latest := int64(since)
	for _, obj := range contents {
		key := aws.StringValue(obj.Key)
		filename := filepath.Base(key)
		ext := filepath.Ext(filename)
		if ext != ".json" {
			continue
		}

		versionString := strings.TrimSuffix(filename, ext)
		version, err := strconv.ParseInt(versionString, 10, 64)
		if err != nil {
			return 0, oops.Wrapf(err, "parse %s", key)
		}
		if version > latest {
			latest = version
		}
	}
	return latest, nil
}

func ReadDeltaLog(ctx context.Context, s3API s3iface.S3API, s3Url string, minVersion int64) (*DeltaLog, error) {
	u, err := url.Parse(s3Url)
	if err != nil {
		return nil, oops.Wrapf(err, "parse delta log url: %s", s3Url)
	}

	if u.Scheme != "s3" {
		return nil, oops.Errorf("delta log url must start with s3://: %s", s3Url)
	}
	if !strings.HasSuffix(u.Path, "/_delta_log") {
		return nil, oops.Errorf("delta log url must end with /_delta_log: %s", s3Url)
	}
	bucket := u.Host
	prefix := strings.TrimPrefix(u.Path, "/")

	// Step 1: read last checkpoint before minVersion.
	// https://github.com/delta-io/delta/blob/43f40d7c6fef3b75b8e97026eb22589b127972ea/PROTOCOL.md#checkpoints
	var checkpointVersion *CheckpointVersion
	if minVersion != 0 {
		// If minVersion is specified, do not read checkpoint at exactly minVersion (hence -1).
		checkpointVersion, err = readLatestCheckpointVersionBeforeVersion(ctx, s3API, bucket, prefix, minVersion-1)
	} else {
		checkpointVersion, err = readLatestCheckpointVersion(ctx, s3API, bucket, prefix)
	}
	if err != nil {
		return nil, oops.Wrapf(err, "read checkpoint version")
	}

	var deltaLog DeltaLog
	// Skip if none found.
	if checkpointVersion != nil {
		actions, err := readCheckpoint(ctx, s3API, bucket, prefix, *checkpointVersion)
		if err != nil {
			return nil, oops.Wrapf(err, "read checkpoint: %s", checkpointVersion.String())
		}
		deltaLog.Add(&DeltaLogVersion{
			Version: checkpointVersion.Version,
			Actions: actions,
		})
	}

	// Step 2: read logs since last checkpoint.
	latest, err := listLatestVersion(ctx, s3API, bucket, prefix, deltaLog.Version)
	if err != nil {
		return nil, oops.Wrapf(err, "list latest version since %d", deltaLog.Version)
	}

	for v := deltaLog.Version + 1; v <= latest; v++ {
		actions, err := readLog(ctx, s3API, bucket, prefix, v)
		if err != nil {
			return nil, oops.Wrapf(err, "read log version %d", v)
		}
		deltaLog.Add(&DeltaLogVersion{
			Version: v,
			Actions: actions,
		})
	}

	return &deltaLog, nil
}
