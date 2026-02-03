package sparkstreaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/samsaraaws/s3iface"
)

// Offset is a monotonically increasing number similar to batch index in Spark structured streaming.
type Offset int64

// OffsetSeqMetadata mirrors offset metadata structure defined in https://github.com/apache/spark/blob/c941362cb94b24bdf48d4928a1a4dff1b13a1484/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/OffsetSeq.scala#L83.
type OffsetSeqMetadata struct {
	BatchWatermarkMs int64             `json:"batchWatermarkMs,omitempty"`
	BatchTimestampMs int64             `json:"batchTimestampMs,omitempty"`
	Conf             map[string]string `json:"conf,omitempty"`
}

// DeltaSourceOffset mirrors Delta table data source offset defined in https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSourceOffset.scala#L44.
type DeltaSourceOffset struct {
	SourceVersion     int64  `json:"sourceVersion"`
	ReservoirId       string `json:"reservoirId"`
	ReservoirVersion  int64  `json:"reservoirVersion"`
	Index             int64  `json:"index"`
	IsStartingVersion bool   `json:"isStartingVersion"`
}

// OffsetLog mirrors Spark offset log format defined in https://github.com/apache/spark/blob/v2.4.5/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/OffsetSeqLog.scala#L48.
type OffsetLog struct {
	Metadata OffsetSeqMetadata
	Offset   DeltaSourceOffset
}

type StreamingOffset struct {
	Committed *DeltaSourceOffset

	// Pending stores the offset of the next micro-batch. If the next batch is
	// unresolved, this is not set. If Spark has attempted a batch and failed, its
	// offset is stored here permanently, and subsequent retries will always reuse
	// this offset.
	Pending *DeltaSourceOffset
}

func parseOffsetLog(b []byte) (*OffsetLog, error) {
	lines := bytes.SplitN(b, []byte("\n"), 3)
	if len(lines) != 3 {
		return nil, oops.Errorf("expect 3 lines")
	}

	if string(lines[0]) != "v1" {
		return nil, oops.Errorf("expect v1")
	}

	var metadata OffsetSeqMetadata
	if err := json.Unmarshal(lines[1], &metadata); err != nil {
		return nil, oops.Wrapf(err, "unmarshal metadata: %s", string(lines[1]))
	}

	var offset DeltaSourceOffset
	if err := json.Unmarshal(lines[2], &offset); err != nil {
		return nil, oops.Wrapf(err, "unmarshal offset: %s", string(lines[2]))
	}

	return &OffsetLog{
		Metadata: metadata,
		Offset:   offset,
	}, nil
}

func findLatestOffsetFileAtPrefix(ctx context.Context, s3API s3iface.S3API, bucket string, prefix string) (Offset, error) {
	var contents []*s3.Object
	if err := s3API.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		Prefix:     aws.String(prefix),
		StartAfter: aws.String(prefix),
	}, func(output *s3.ListObjectsV2Output, _ bool) bool {
		contents = append(contents, output.Contents...)
		return true
	}); err != nil {
		return 0, oops.Wrapf(err, "list objects s3://%s/%s starting after %s", bucket, prefix, prefix)
	}

	var latest int64
	for _, obj := range contents {
		key := aws.StringValue(obj.Key)
		base := filepath.Base(key)
		version, err := strconv.ParseInt(base, 10, 64)
		if err != nil {
			return 0, oops.Wrapf(err, "parse %s", key)
		}
		if version > latest {
			latest = version
		}
	}
	return Offset(latest), nil
}

func findLatestCommit(ctx context.Context, s3API s3iface.S3API, bucket string, prefix string) (Offset, error) {
	commits := fmt.Sprintf("%s/commits/", prefix)
	offset, err := findLatestOffsetFileAtPrefix(ctx, s3API, bucket, commits)
	if err != nil {
		return 0, oops.Wrapf(err, "find latest commit at s3://%s/%s", bucket, commits)
	}
	return offset, nil
}

func findLatestOffset(ctx context.Context, s3API s3iface.S3API, bucket string, prefix string) (Offset, error) {
	offsets := fmt.Sprintf("%s/offsets/", prefix)
	offset, err := findLatestOffsetFileAtPrefix(ctx, s3API, bucket, offsets)
	if err != nil {
		return 0, oops.Wrapf(err, "find latest offset at s3://%s/%s", bucket, offsets)
	}
	return offset, nil
}

func readOffsetLog(ctx context.Context, s3API s3iface.S3API, bucket string, prefix string, offset Offset) (*OffsetLog, error) {
	key := fmt.Sprintf("%s/offsets/%d", prefix, offset)
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

	log, err := parseOffsetLog(b)
	if err != nil {
		return nil, oops.Wrapf(err, "parse log s3://%s/%s", bucket, key)
	}
	return log, nil
}

// ReadStreamingOffset reads the current streaming offset stored in a checkpoint directory on S3.
func ReadStreamingOffset(ctx context.Context, s3API s3iface.S3API, checkpoint string) (*StreamingOffset, error) {
	u, err := url.Parse(checkpoint)
	if err != nil {
		return nil, oops.Wrapf(err, "parse: %s", checkpoint)
	}
	if u.Scheme != "s3" {
		return nil, oops.Errorf("expect s3")
	}
	bucket, prefix := u.Host, strings.TrimPrefix(u.Path, "/")

	commitOffset, err := findLatestCommit(ctx, s3API, bucket, prefix)
	if err != nil {
		return nil, oops.Wrapf(err, "find latest commit")
	}

	commitLog, err := readOffsetLog(ctx, s3API, bucket, prefix, commitOffset)
	if err != nil {
		return nil, oops.Wrapf(err, "read offset log: %d", commitOffset)
	}

	latestOffset, err := findLatestOffset(ctx, s3API, bucket, prefix)
	if err != nil {
		return nil, oops.Wrapf(err, "find latest offset")
	}

	if latestOffset == commitOffset {
		return &StreamingOffset{
			Committed: &commitLog.Offset,
		}, nil
	}

	pendingLog, err := readOffsetLog(ctx, s3API, bucket, prefix, latestOffset)
	if err != nil {
		return nil, oops.Wrapf(err, "read offset log: %d", latestOffset)
	}

	return &StreamingOffset{
		Committed: &commitLog.Offset,
		Pending:   &pendingLog.Offset,
	}, nil
}
