package influx

import (
	"bytes"

	pb "github.com/lindb/lindb/rpc/proto/field"
)

type parseState int

const (
	Success parseState = iota
	MissingMetricName
	MissingComma
	MissingWhiteSpace
	MissingFields
)

func parseInfluxLine(content []byte, database string, multiplier int64) (*pb.Metric, parseState) {
	// skip comment line
	if bytes.HasPrefix(content, []byte{'#'}) {
		return nil, Success
	}

	escaped := bytes.IndexByte(content, '\\') >= 0
	var (
		m pb.Metric
	)
	m.Namespace = database
	endAt, state := scanMetricName(content, escaped)
	if state != Success {
		return nil, state
	}
	m.Name = string(content[:endAt])

	return &m, Success
}

// walkToUnescapedChar returns first position of given unescaped char
// abc\,\,, -> 7
// abc, -> 3
// abc\\\\, -> 7
// \\\, -> -1
func walkToUnescapedChar(buf []byte, char byte, startAt int, isEscaped bool) int {
	if len(buf) <= startAt {
		return -1
	}
	for {
		offset := bytes.IndexByte(buf[startAt:], char)
		if offset < 0 {
			return -1
		}
		if !isEscaped {
			return startAt + offset
		}

		cursor := offset + startAt
		for cursor-1 >= startAt && buf[cursor-1] == '\\' {
			cursor--
		}
		if (offset+startAt-cursor)&1 == 1 {
			// seek right
			startAt += offset + 1
			continue
		}
		return offset + startAt
	}
}

// scanMetricName examines the metric-name part of a Point, and returns the end position
func scanMetricName(buf []byte, isEscaped bool) (endAt int, state parseState) {
	// unescaped comma;
	pos := walkToUnescapedChar(buf, ',', 0, isEscaped)
	switch {
	case pos == 0:
		return 0, MissingMetricName
	case pos < 0:
		return 0, MissingComma
	default:
		return pos, Success
	}
}
