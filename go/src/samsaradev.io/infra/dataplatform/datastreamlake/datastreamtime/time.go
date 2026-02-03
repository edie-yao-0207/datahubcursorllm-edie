package datastreamtime

import (
	"fmt"
	"time"
)

const ISO8601TimeFormat = "2006-01-02T15:04:05.999999999Z"

// Firehose timestamps need to follow the OpenXSerDe (https://github.com/rcongiu/Hive-JSON-Serde) JSON serializer.
// Golangs time.Time package String() method tacks on the timezone (i.e 2020-10-22T12:07:46.736291-07:00) which causes Firehose record format conversion to fail.
// To circumvent this, DataStreamTime is a custom time.Time implementation with a custom MarshalJSON function with a Format that matches Firehose docs.
// See more: https://docs.aws.amazon.com/firehose/latest/dev/record-format-conversion.html#record-format-conversion-concepts
type Time time.Time

func (t Time) MarshalJSON() ([]byte, error) {
	timestamp := fmt.Sprintf(`"%v"`, t)
	return []byte(timestamp), nil
}

// Implement Stringer interface for use with `fmt` package and others.
// See: https://tour.golang.org/methods/17
func (t Time) String() string {
	return time.Time(t).Format(ISO8601TimeFormat)
}
