// Licensed to LinDB under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. LinDB licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package influx

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/lindb/lindb/constants"
	"github.com/lindb/lindb/pkg/timeutil"
	pb "github.com/lindb/lindb/rpc/proto/field"
)

var (
	ErrMissingMetricName = errors.New("missing_metric_name")
	ErrMissingComma      = errors.New("missing_comma")
	ErrMissingWhiteSpace = errors.New("missing_whitespace")
	ErrBadTags           = errors.New("bad_tags")
	ErrTooManyTags       = errors.New("too_many_tags")
	ErrMissingFields     = errors.New("missing_fields")
	ErrBadTimestamp      = errors.New("bad_timestamp")
)

func parseInfluxLine(content []byte, database string, multiplier int64) (*pb.Metric, error) {
	// skip comment line
	if bytes.HasPrefix(content, []byte{'#'}) {
		return nil, nil
	}

	escaped := bytes.IndexByte(content, '\\') >= 0
	var (
		m pb.Metric
	)
	m.Namespace = database
	// parse metric-name
	metricEndAt, err := scanMetricName(content, escaped)
	if err != nil {
		return nil, err
	}
	m.Name = string(content[:metricEndAt])

	// parse tags
	tagsEndAt, err := scanTagLine(content, metricEndAt+1, escaped)
	if err != nil {
		return nil, err
	}
	if m.Tags, err = parseTags(content, metricEndAt+1, tagsEndAt, escaped); err != nil {
		return nil, err
	}
	if len(m.Tags) >= constants.DefaultMaxTagKeysCount {
		return nil, ErrTooManyTags
	}

	// parse fields
	fieldsEndAt, err := scanFieldLine(content, tagsEndAt+1, escaped)
	if err != nil {
		return nil, err
	}

	// parse timestamp
	if m.Timestamp, err = parseTimestamp(content, fieldsEndAt+1, multiplier); err != nil {
		return nil, err
	}
	return &m, nil
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
func scanMetricName(buf []byte, isEscaped bool) (endAt int, err error) {
	// unescaped comma;
	endAt = walkToUnescapedChar(buf, ',', 0, isEscaped)
	switch {
	case endAt == 0:
		return -1, ErrMissingMetricName
	case endAt < 0:
		return -1, ErrMissingComma
	default:
		return endAt, nil
	}
}

// scanTagLine returns the end position of tags
// weather,location=us-midwest,season=summer temperature=82,humidity=71 1465839830100400200
func scanTagLine(buf []byte, startAt int, isEscaped bool) (endAt int, err error) {
	endAt = walkToUnescapedChar(buf, ' ', startAt, isEscaped)
	switch {
	case endAt < 0:
		return -1, ErrMissingWhiteSpace
	default:
		// if endAt = 0, tags are empty
		return endAt, nil
	}
}

func parseTags(buf []byte, startAt int, endAt int, isEscaped bool) (map[string]string, error) {
	// empty
	tags := make(map[string]string)
	fmt.Println(string(buf[startAt:endAt]))

WalkBeforeComma:
	{
		if startAt >= endAt-1 {
			return tags, nil
		}
		commaAt := walkToUnescapedChar(buf, ',', startAt, isEscaped)
		// '=' does not exist
		equalAt := walkToUnescapedChar(buf, '=', startAt, isEscaped)
		if equalAt <= startAt || equalAt+1 >= endAt {
			return tags, ErrBadTags
		}
		boundaryAt := endAt
		if commaAt > 0 && commaAt <= endAt {
			boundaryAt = commaAt
		}
		// move to next tag pair
		if equalAt+1 >= boundaryAt {
			return tags, ErrBadTags
		}
		// move to next tag pair
		tagKey, tagValue := buf[startAt:equalAt], buf[equalAt+1:boundaryAt]
		tags[string(tagKey)] = string(tagValue)
		startAt = commaAt + 1
		goto WalkBeforeComma
	}
}

// scanFieldLine returns the end position of fields
func scanFieldLine(buf []byte, startAt int, isEscaped bool) (endAt int, err error) {
	endAt = walkToUnescapedChar(buf, ' ', startAt, isEscaped)
	switch {
	case endAt < 0:
		// case: no timestamp
		endAt = len(buf)
		// but field line is empty
		if startAt == endAt {
			return -1, ErrMissingFields
		}
		return endAt, nil
	case endAt == 0:
		return -1, ErrMissingFields
	default:
		return endAt, nil
	}
}

func parseTimestamp(buf []byte, startAt int, multiplier int64) (int64, error) {
	// no timestamp
	if startAt >= len(buf)-1 {
		return timeutil.Now(), nil
	}
	f, err := strconv.ParseInt(string(buf[startAt:]), 10, 64)
	if err != nil {
		return 0, err
	}
	if multiplier > 0 {
		return int64(f * multiplier), nil
	}
	return int64(-1 / multiplier), nil
}
