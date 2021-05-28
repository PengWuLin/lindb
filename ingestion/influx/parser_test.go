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
	pb "github.com/lindb/lindb/rpc/proto/field"
	"github.com/stretchr/testify/assert"

	"testing"
)

func Test_scanMetricName(t *testing.T) {
	s1 := []byte("abc\\,\\,,")
	endAt, err := scanMetricName(s1, true)
	assert.Nil(t, err)
	assert.Equal(t, "abc\\,\\,", string(s1[:endAt]))

	s2 := []byte("abc,")
	endAt, err = scanMetricName(s2, true)
	assert.Nil(t, err)
	assert.Equal(t, "abc", string(s2[:endAt]))

	s3 := []byte("abc\\\\\\\\,")
	endAt, err = scanMetricName(s3, true)
	assert.Nil(t, err)
	assert.Equal(t, "abc\\\\\\\\", string(s3[:endAt]))

	s4 := []byte("\\\\\\,")
	endAt, err = scanMetricName(s4, true)
	assert.Equal(t, ErrMissingComma, err)
	assert.Equal(t, -1, endAt)

	s5 := []byte(",abcd")
	endAt, err = scanMetricName(s5, false)
	assert.Equal(t, ErrMissingMetricName, err)
	assert.Equal(t, -1, endAt)

	s6 := []byte("abcd")
	endAt, err = scanMetricName(s6, false)
	assert.Equal(t, ErrMissingComma, err)
	assert.Equal(t, -1, endAt)
}

func Test_walkToUnescapedChar(t *testing.T) {
	buf := []byte("abcde")
	endAt := walkToUnescapedChar(buf, 'f', 6, false)
	assert.True(t, endAt < 0)
}

type expectedMetric struct {
	db        string
	name      string
	tags      map[string]string
	fields    []*pb.Field
	timestamp int64
}

func assertMatch(t *testing.T, expected *expectedMetric, m *pb.Metric) {
	assert.Equal(t, expected.name, m.Name)
	assert.Equal(t, expected.db, m.Namespace)
	assert.InDeltaMapValues(t, expected.tags, m.Tags, 1e-6)

}

func Test_parseInfluxLine(t *testing.T) {
	line1 := []byte("weather,location=us-midwest,season=summer temperature=82,humidity=71 1465839830100400200")
	m, err := parseInfluxLine(line1, "db1", 1)
	assert.Nil(t, err)
	assertMatch(t, &expectedMetric{
		db:   "db1",
		name: "weather",
		tags: map[string]string{
			"location": "us-midwest",
			"season":   "summer",
		},
	}, m)
}
