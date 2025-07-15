package kvdb

import (
	"fmt"
	"io"
	"time"
)

type readerStats struct {
	r        io.Reader
	bytes    int
	readTime time.Duration
}

// withReaderStats returns a new ReaderStats wrapping the given reader
func withReaderStats(r io.Reader) *readerStats {
	return &readerStats{r: r}
}

// Read reads from the underlying reader and records statistics
func (rs *readerStats) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := rs.r.Read(p)
	rs.readTime += time.Since(start)
	rs.bytes += n
	return n, err
}

// Stats returns calls/sec and bytes/sec based only on time spent reading
func (rs *readerStats) Stats() (bytesPerSec float64) {
	seconds := rs.readTime.Seconds()
	if seconds == 0 {
		return 0
	}
	return float64(rs.bytes) / seconds
}

func (rs *readerStats) HumanStats() (bytes string) {
	bytesPerSec := rs.Stats()
	bytes = humanize(bytesPerSec)
	return
}

func humanize(bytesPerSec float64) string {
	if bytesPerSec < 1024 {
		return fmt.Sprintf("%.2f B/s", bytesPerSec)
	} else if bytesPerSec < 1024*1024 {
		return fmt.Sprintf("%.2f KB/s", bytesPerSec/1024)
	} else if bytesPerSec < 1024*1024*1024 {
		return fmt.Sprintf("%.2f MB/s", bytesPerSec/(1024*1024))
	}
	return fmt.Sprintf("%.2f GB/s", bytesPerSec/(1024*1024*1024))
}
