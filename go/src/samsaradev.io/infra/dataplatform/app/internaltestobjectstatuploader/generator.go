package main

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/hubproto"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/stats/kinesisstats/kinesisstatsproto"
	"samsaradev.io/stats/objectstatsproto"
	"samsaradev.io/stats/s3stats"
)

func generateObjectStats(ctx context.Context, currentTimeMs int64, num int) []*kinesisstatsproto.WriteChunkRequest {
	requests := make([]*kinesisstatsproto.WriteChunkRequest, 0, num)

	for i := 0; i < num; i++ {
		timeMs := currentTimeMs + int64(i)
		series, err := objectStatChunkSeries()
		if err != nil {
			slog.Fatalw(ctx, "failed to format stat chunk series", "error", err)
		}
		bytes, err := objectStatChunkValue(timeMs)
		requests = append(requests, &kinesisstatsproto.WriteChunkRequest{
			Chunk: &kinesisstatsproto.KVChunk{
				Series: series,
				Values: []*kinesisstatsproto.KVValue{
					{
						Time:  timeMs,
						Value: bytes,
					},
				},
			},
		})
	}

	return requests
}

func objectStatChunkSeries() (string, error) {
	return s3stats.FormatObjectStatSeries(
		33026,           // Eileen's test org
		278018081909654, // Eileen's test device
		objectstatproto.ObjectTypeEnum_otTest,
		objectstatproto.ObjectStatEnum_osTDataPlatformInternalTestStat,
	)
}

func objectStatChunkValue(timeMs int64) ([]byte, error) {
	binaryValue, err := proto.Marshal(&hubproto.ObjectStatBinaryMessage{
		DataPlatformInternalTestStat: &hubproto.ObjectStatBinaryMessage_DataPlatformInternalTestStat{
			IntInput: 1,
			BasicDataInput: &hubproto.ObjectStatBinaryMessage_DataPlatformInternalTestStat_BasicData{
				SintInput:   -1,
				UintInput:   1,
				BoolInput:   false,
				StringInput: "foo",
			},
			AdvancedDataInput: &hubproto.ObjectStatBinaryMessage_DataPlatformInternalTestStat_AdvancedData{
				BytesInput: []byte{byte(1)},
			},
		},
	})
	if err != nil {
		return nil, oops.Wrapf(err, "failed to marshal binary message")
	}
	objectStat := &objectstatsproto.ObjectStat{
		Time:           timeMs,
		HasBinaryValue: true,
		BinaryValue:    binaryValue,
	}
	bytes, err := proto.Marshal(objectStat)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to marshal object stat")
	}
	return bytes, nil
}
