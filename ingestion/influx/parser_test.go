package influx

import (
	"github.com/stretchr/testify/assert"

	"testing"
)

func Test_scanMetricName(t *testing.T) {
	s1 := []byte("abc\\,\\,,")
	endAt, state := scanMetricName(s1, true)
	assert.Equal(t, Success, state)
	assert.Equal(t, "abc\\,\\,", string(s1[:endAt]))

	s2 := []byte("abc,")
	endAt, state = scanMetricName(s2, true)
	assert.Equal(t, Success, state)
	assert.Equal(t, "abc", string(s2[:endAt]))

	s3 := []byte("abc\\\\\\\\,")
	endAt, state = scanMetricName(s3, true)
	assert.Equal(t, Success, state)
	assert.Equal(t, "abc\\\\\\\\", string(s3[:endAt]))

	s4 := []byte("\\\\\\,")
	endAt, state = scanMetricName(s4, true)
	assert.Equal(t, MissingComma, state)
	assert.Equal(t, "", string(s4[:endAt]))

	s5 := []byte(",abcd")
	endAt, state = scanMetricName(s5, false)
	assert.Equal(t, MissingMetricName, state)
	assert.Equal(t, "", string(s5[:endAt]))

	s6 := []byte("abcd")
	endAt, state = scanMetricName(s6, false)
	assert.Equal(t, MissingComma, state)
	assert.Equal(t, "", string(s6[:endAt]))
}

func Test_walkToUnescapedChar(t *testing.T) {
	buf := []byte("abcde")
	endAt := walkToUnescapedChar(buf, 'f', 6, false)
	assert.True(t, endAt < 0)
}
