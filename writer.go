package kvdb

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type writer struct {
	lsn      *atomic.Uint64
	dir      string
	file     *os.File
	incoming chan message
	mu       sync.RWMutex // guards channel
	closed   bool
	done     chan error
}

// NewWriter creates a new writer
func newWriter(path string) *writer {
	return &writer{
		dir:      path,
		incoming: make(chan message, 100),
		mu:       sync.RWMutex{},
	}
}

// Send message to the writer
func (w *writer) Send(msg message) error {
	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return ErrClosed
	}
	w.mu.RUnlock()

	cb := msg.Callback()
	if cb == nil {
		return errors.New("callback is nil")
	}
	w.incoming <- msg
	defer close(cb)
	return <-cb
}

// Request writer to write operation into jlog
func (w *writer) Write(op *operation) error {
	return w.Send(newWriteMessage(op))
}

// Request writer to rotate current jlog file
func (w *writer) Rotate() error {
	return w.Send(newRotateMessage())
}

// Request writer to compact jlogs
func (w *writer) Compact(snap map[string]space, lsn uint64) error {
	return w.Send(newCompactMessage(snap, lsn))
}

// Close writer
func (w *writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}

	close(w.incoming)
	// await for all messages to be processed
	if w.done == nil {
		return nil
	}
	return <-w.done
}

// GetLSN returns current LSN
func (w *writer) GetLSN() uint64 {
	return w.getLSN()
}

// Load all jlogs from the directory and apply them to the given function
// Sets LSN of the last applied operation to writer
func (w *writer) Load(applyTxn func(*operation) (uint64, error)) error {
	jlogs, err := w.list(w.dir)
	if err != nil {
		return err
	}

	for _, jlog := range jlogs {
		lsn, err := w.loadJlog(jlog, applyTxn)
		if err != nil {
			return err
		}
		w.setLSN(lsn)
	}

	return nil
}

// Start writer
func (w *writer) Start() error {
	if err := w.rotate(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.done = make(chan error, 1)

	go w.work()
	return nil
}

func lsn2str(lsn uint64) string {
	return fmt.Sprintf("%010d", lsn)
}

func str2lsn(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

func (w *writer) list(dir string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []os.DirEntry{}, nil
		}
		return nil, err
	}

	jlogs := []os.DirEntry{}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		if !ent.Type().IsRegular() {
			continue
		}

		if strings.HasSuffix(ent.Name(), ".jlog") {
			jlogs = append(jlogs, ent)
		}
	}

	sort.Slice(jlogs, func(i, j int) bool {
		return jlogs[i].Name() < jlogs[j].Name()
	})

	return jlogs, nil
}

func (w *writer) setLSN(lsn uint64) {
	if w.lsn == nil {
		w.lsn = &atomic.Uint64{}
	}

	old := w.lsn.Load()
	for old < lsn {
		if w.lsn.CompareAndSwap(old, lsn) {
			break
		}
		old = w.lsn.Load()
	}
}

func (w *writer) getLSN() uint64 {
	if w.lsn == nil {
		return 0
	}
	lsn := w.lsn.Load()
	return lsn
}

func (w *writer) loadFrom(file io.Reader, applyTxn func(*operation) (uint64, error)) (lsn uint64, err error) {
	dec := json.NewDecoder(bufio.NewReader(file))

	lsn = w.getLSN()
	for {
		var op operation
		if err = dec.Decode(&op); err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return
		}

		if op.LSN == 0 {
			// we have to set LSN on our own
			op.LSN = lsn + 1
		}

		if lsn, err = applyTxn(&op); err != nil {
			return lsn, err
		}
	}
	return
}

func (w *writer) loadJlog(jlog os.DirEntry, applyTxn func(*operation) (uint64, error)) (lsn uint64, err error) {
	file, err := os.OpenFile(w.dir+"/"+jlog.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	rs := withReaderStats(file)
	lsn, err = w.loadFrom(rs, applyTxn)
	if err != nil {
		return
	}

	bytesPerSec := rs.HumanStats()
	log.Printf("loadFile %s: %s\n", file.Name(), bytesPerSec)
	return
}

func (w *writer) compact(msg *messageCompact) error {
	lsn := msg.LSN()

	bname := path.Base(w.file.Name())
	fname := strings.TrimSuffix(bname, ".jlog")
	flsn, err := str2lsn(fname)
	if err != nil {
		msg.Callback() <- err
		return err
	}

	jlogs, err := w.list(w.dir)
	if err != nil {
		msg.Callback() <- err
		return err
	}

	// we must exclude w.file from jlogs
	for i, jlog := range jlogs {
		if jlog.Name() == bname {
			jlogs = slices.Delete(jlogs, i, i+1)
			break
		}
	}

	if lsn > flsn {
		// we rotate last jlog
		if err := w.rotate(); err != nil {
			msg.Callback() <- err
			return err
		}
	}

	// nothing to compact
	if len(jlogs) == 0 {
		msg.Callback() <- nil
		return nil
	}

	go w.compactBackground(jlogs, msg)

	return nil
}

func (w *writer) compactBackground(jlogs []os.DirEntry, msg *messageCompact) {
	lsn := msg.LSN()

	var err error
	inprogress, err := os.OpenFile(w.dir+"/"+lsn2str(lsn)+".jlog.inprogress", os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		msg.Callback() <- err
		return
	}

	log.Printf("compaction started %s", inprogress.Name())

	err = w.compactImpl(inprogress, jlogs, msg)
	if err != nil {
		inprogress.Close()
		os.Remove(inprogress.Name())
		msg.Callback() <- err
		return
	}

	if err := inprogress.Close(); err != nil {
		msg.Callback() <- err
		return
	}

	// rename result file
	newName := w.dir + "/" + lsn2str(lsn) + ".jlog"
	log.Printf("renaming %s to %s", inprogress.Name(), newName)

	if err := os.Rename(inprogress.Name(), newName); err != nil {
		msg.Callback() <- err
		return
	}

	// remove old jlogs
	for _, jlog := range jlogs {
		if path.Base(jlog.Name()) != path.Base(newName) {
			log.Printf("removing %s", jlog.Name())
			if err := os.Remove(w.dir + "/" + jlog.Name()); err != nil {
				msg.Callback() <- err
				return
			}
		}
	}

	msg.Callback() <- err
}

func (w *writer) compactJlog(jlog os.DirEntry, snap map[string]space, output *os.File) error {
	file, err := os.OpenFile(w.dir+"/"+jlog.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	rs := withReaderStats(file)

	rn := 0
	wr := 0

	_, err = w.loadFrom(rs, func(op *operation) (lsn uint64, err error) {
		rn++
		lsn = op.LSN

		space, ok := snap[op.Record.Tag]
		if !ok {
			// we skip all operations, that are not in snapshot
			return
		}

		if op.Op != set {
			// we skip all not set operations
			space.Inc()
			return
		}

		has, err := space.HasLSN(op)
		if err != nil {
			return
		}
		if !has {
			space.Inc()
			return
		}

		// otherwise we have to write this set operation
		if err = w.writeTo(op, output); err != nil {
			return
		}
		wr++
		return
	})

	log.Printf("compaction %s: read %d, write %d (compacted %.2f%%), %s\n",
		jlog.Name(), rn, wr, float64(rn-wr)/float64(rn)*100, rs.HumanStats())
	return err
}

func (w *writer) compactImpl(output *os.File, jlogs []os.DirEntry, msg *messageCompact) error {
	snap := msg.Snap()

	for _, jlog := range jlogs {
		err := w.compactJlog(jlog, snap, output)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *writer) rotate() error {
	nextJlog := lsn2str(w.getLSN() + 1)
	jlogName := w.dir + "/" + nextJlog + ".jlog"
	if w.file != nil {
		if jlogName == w.file.Name() {
			return nil
		}
	}
	if fi, err := os.Stat(jlogName); err == nil {
		// File already exists, no need to rotate
		// if file is empty, it's okay
		if fi.Size() != 0 {
			return fmt.Errorf("file %s already exists (and not empty)", jlogName)
		}
	}

	// mktree
	if err := os.MkdirAll(w.dir, 0755); err != nil {
		return err
	}

	// open new file
	newFile, err := os.OpenFile(jlogName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	if w.file != nil {
		// close old file
		if err = w.file.Sync(); err != nil {
			goto failed
		}

		if err = w.file.Close(); err != nil {
			goto failed
		}
	}

	log.Printf("rotating %s", newFile.Name())
	// set new file
	w.file = newFile
	return nil

failed:
	newFile.Close()
	return err
}

func (w *writer) work() {
	for msg := range w.incoming {
		switch msg.Action() {
		case messageActionWrite:
			msg.Callback() <- w.write(msg.Op())
		case messageActionRotate:
			msg.Callback() <- w.rotate()
		case messageActionCompact:
			cpt, ok := msg.(*messageCompact)
			if !ok {
				msg.Callback() <- errors.New("invalid message type")
				continue
			}
			if err := w.compact(cpt); err != nil {
				continue
			}
		}
	}
	errs := w.file.Sync()
	errc := w.file.Close()

	w.done <- errors.Join(errs, errc)
	close(w.done)
}

func (w *writer) write(op *operation) error {
	if op == nil {
		return nil
	}
	lsn := w.getLSN()
	op.LSN = lsn + 1

	if err := w.writeTo(op, w.file); err != nil {
		return err
	}

	w.setLSN(op.LSN)
	return nil
}

func (w *writer) writeTo(op *operation, file *os.File) error {
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}

	data = append(data, '\n')

	if _, err = file.Write(data); err != nil {
		return err
	}
	return nil
}

/******************************************************************************
 * Message
 * Message is a structure that is used to send messages to the writer.
 */

type messageAction uint

const (
	messageActionNone    messageAction = 0
	messageActionWrite   messageAction = 1
	messageActionRotate  messageAction = 2
	messageActionCompact messageAction = 3
)

type message interface {
	Action() messageAction
	Callback() chan error
	Op() *operation
}

type messageBase struct {
	callback chan error
}

func newMessage() messageBase {
	return messageBase{
		callback: make(chan error, 1),
	}
}

func (m messageBase) Callback() chan error {
	return m.callback
}

func (m messageBase) Action() messageAction {
	return messageActionNone
}

func (m messageBase) Op() *operation {
	return nil
}

type messageWrite struct {
	messageBase
	op *operation
}

func (m *messageWrite) Action() messageAction {
	return messageActionWrite
}

func (m *messageWrite) Op() *operation {
	return m.op
}

func newWriteMessage(op *operation) message {
	if op == nil {
		return nil
	}
	return &messageWrite{
		messageBase: newMessage(),
		op:          op,
	}
}

type messageRotate struct {
	messageBase
}

func (m *messageRotate) Action() messageAction {
	return messageActionRotate
}

func newRotateMessage() message {
	return &messageRotate{
		messageBase: newMessage(),
	}
}

type messageCompact struct {
	messageBase
	lsn  uint64
	snap map[string]space
}

func (m *messageCompact) Action() messageAction {
	return messageActionCompact
}

func (m *messageCompact) Snap() map[string]space {
	return m.snap
}

func (m *messageCompact) LSN() uint64 {
	return m.lsn
}

func newCompactMessage(snap map[string]space, lsn uint64) message {
	if snap == nil {
		return nil
	}
	return &messageCompact{
		messageBase: newMessage(),
		snap:        snap,
		lsn:         lsn,
	}
}

/******************************************************************************
 * ReaderStats
 */

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
