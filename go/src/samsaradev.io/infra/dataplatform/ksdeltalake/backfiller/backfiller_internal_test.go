package backfiller

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/stats/kinesisstats/jsonexporter"
	"samsaradev.io/vendormocks/mock_s3iface"
)

func TestFilterChunkFilesByMs(t *testing.T) {
	exCf1 := chunkFile{key: "archive/hash/0005/series/objectstat#object=212014918019353#objecttype=13#organization=42860#stattype=317/chunk/1500000000000-1600000000000.cf2"}
	exCf2 := chunkFile{key: "archive/hash/0005/series/objectstat#object=212014918019353#objecttype=13#organization=42860#stattype=317/chunk/1800000000000-1900000000000.cf2"}
	exCf3 := chunkFile{key: "archive/hash/0005/series/objectstat#object=212014918019353#objecttype=13#organization=42860#stattype=317/chunk/1950000000000-1960000000000.cf2"}
	testCases := map[string]struct {
		files         []chunkFile
		startMs       int64
		endMs         int64
		filteredFiles []chunkFile
	}{
		"testSingleFileCompleteOverlap": {
			files:         []chunkFile{exCf1},
			startMs:       1550000000000,
			endMs:         1560000000000,
			filteredFiles: []chunkFile{exCf1},
		},
		"testSingleFilePartialOverlap": {
			files:         []chunkFile{exCf1},
			startMs:       1580000000000,
			endMs:         1620000000000,
			filteredFiles: []chunkFile{exCf1},
		},
		"testSingleFilePartialOverlapInclusive": {
			files:         []chunkFile{exCf1},
			startMs:       1600000000000,
			endMs:         16560000000000,
			filteredFiles: []chunkFile{exCf1},
		},
		"testSingleFileNoOverlap": {
			files:         []chunkFile{exCf1},
			startMs:       1610000000000,
			endMs:         16560000000000,
			filteredFiles: []chunkFile{},
		},
		"testMultipleFiles": {
			files:         []chunkFile{exCf1, exCf2, exCf3},
			startMs:       1850000000000,
			endMs:         19550000000000,
			filteredFiles: []chunkFile{exCf2, exCf3},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			actualFilteredFiles, err := filterChunkFilesByMs(testCase.files, testCase.startMs, testCase.endMs)
			assert.Equal(t, testCase.filteredFiles, actualFilteredFiles)
			require.NoError(t, err)
		})
	}
}

func TestFindPartialBackfillParameters(t *testing.T) {
	mockS3 := mock_s3iface.NewMockS3API(gomock.NewController(t))
	testCases := map[string]struct {
		table             string
		bucket            string
		key               string
		backfillCompleted bool
		completedAt       time.Time
		expectedArgs      *jsonexporter.PartialBackfillArgs
	}{
		"testOverall": {
			table:             "osDAccelerometer",
			bucket:            "samsara-dataplatform-metadata",
			backfillCompleted: true,
			completedAt:       time.Date(2021, time.Month(3), 1, 0, 0, 0, 0, time.UTC), // March 1, 2021
			expectedArgs: &jsonexporter.PartialBackfillArgs{
				StartMs: 1612137600000, // Feb 1, 2021
				EndMs:   1614556800000, // March 1, 2021
			},
		},
		"testNotCompleted": {
			table:             "osDAccelerometer",
			bucket:            "samsara-dataplatform-metadata",
			backfillCompleted: false,
			completedAt:       time.Date(2021, time.Month(3), 1, 0, 0, 0, 0, time.UTC), // March 1, 2021
			expectedArgs:      nil,
		},
		"testOverlapYear": {
			table:             "osDAccelerometer",
			bucket:            "samsara-dataplatform-metadata",
			backfillCompleted: true,
			completedAt:       time.Date(2021, time.Month(1), 1, 0, 0, 0, 0, time.UTC), // January 1, 2021
			expectedArgs: &jsonexporter.PartialBackfillArgs{
				StartMs: 1606780800000, // Dec 1, 2020
				EndMs:   1609459200000, // Jan 1, 2021
			},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			s3Output := s3.GetObjectOutput{
				LastModified: &testCase.completedAt,
			}
			var returnErr error
			if !testCase.backfillCompleted {
				returnErr = awserr.New(s3.ErrCodeNoSuchKey, "some aws internal error string", nil)
			}

			mockS3.EXPECT().GetObject(gomock.Any()).Return(&s3Output, returnErr).Times(1)
			result, err := findPartialBackfillParameters(testCase.table, testCase.bucket, mockS3)
			if testCase.backfillCompleted {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			assert.Equal(t, testCase.expectedArgs, result)
		})
	}
}
