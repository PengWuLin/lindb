package influx

import (
	"io"
	"sync"
)

var chunkReaderPool sync.Pool

// GetChunkReader picks a cached chunk-reader from the pool
func GetChunkReader(r io.Reader) *ChunkReader {
	reader := chunkReaderPool.Get()
	if reader == nil {
		return newChunkReader(r)
	}
	cr := reader.(*ChunkReader)
	cr.Reset(r)
	return cr
}

// PutChunkReader puts chunk-reader back to the pool
func PutChunkReader(cr *ChunkReader) {
	if cr == nil {
		return
	}
	chunkReaderPool.Put(cr)
}
