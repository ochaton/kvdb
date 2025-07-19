package kvdb

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	JLOG_EXTENSION       = "jlog"
	SNAP_EXTENSION       = "snap"
	INPROGRESS_EXTENSION = "inprogress"
)

type status int

const (
	created = iota
	loaded
	running
	closed
)

type writer interface {
	Load(applyTxn func(*operation) (uint64, error)) error
	Start() error
	Close() error
	Write(op *operation) error
	Rotate() error
	Snapshot(snap *map[string]Space) error
}

type defaultWriter struct {
	lsn      *atomic.Uint64
	dir      string
	file     *os.File
	mu       sync.RWMutex // guards channel
	status   status
	incoming chan task
	done     chan error
}

// NewWriter creates a new writer
func newWriter(path string) *defaultWriter {
	return &defaultWriter{
		status: created,
		dir:    path,
		mu:     sync.RWMutex{},
	}
}

// Load all data files from the directory and apply them to the given function
// Sets LSN of the last applied operation to writer
func (w *defaultWriter) Load(applyTxn func(*operation) (uint64, error)) error {
	if err := os.MkdirAll(w.dir, 0755); err != nil {
		return err
	}

	filePathes, err := w.listActualDataFiles()
	if err != nil {
		return err
	}

	for _, filePath := range filePathes {
		lsn, err := loadDataFile(filePath, applyTxn)
		if err != nil {
			return err
		}
		if lsn > w.getLSN() {
			w.setLSN(lsn)
		}
	}
	w.status = loaded

	return nil
}

// Start writer
// no locks - already under DB lock
func (w *defaultWriter) Start() error {
	if err := w.rotate(); err != nil {
		return err
	}

	if w.status != loaded {
		return ErrWriterInvalidStatus
	}
	w.incoming = make(chan task, 100)
	w.status = running

	go w.work()
	return nil
}

// Close writer
func (w *defaultWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == closed {
		return ErrWriterInvalidStatus
	}

	w.status = closed

	if w.incoming != nil {
		close(w.incoming)
	}
	if w.done != nil {
		// await for all messages to be processed
		return <-w.done
	}
	return nil
}

// Request writer to write operation into jlog
func (w *defaultWriter) Write(op *operation) error {
	return w.send(newWriteTask(op))
}

// Request writer to rotate current jlog file
func (w *defaultWriter) Rotate() error {
	return w.send(newRotateTask())
}

// Request writer to snap jlogs
func (w *defaultWriter) Snapshot(snap *map[string]Space) error {
	return w.send(newSnapshotTask(snap))
}

/******************************************************************************
 * inner background operations
 */

func (w *defaultWriter) work() {
	w.mu.Lock()
	if w.status != running {
		return
	}
	w.done = make(chan error, 1)
	w.mu.Unlock()

	for task := range w.incoming {
		switch task.Action() {
		case taskActionWrite:
			task.SendToCallback(w.write(task.Op()))
		case taskActionRotate:
			task.SendToCallback(w.rotate())
		case taskActionSnapshot:
			cpt, ok := task.(*taskSnapshot)
			if !ok {
				task.SendToCallback(ErrMessageInvalidType)
				continue
			}
			w.snapshot(cpt)
		}
	}
	w.done <- closeFile(w.file)
	close(w.done)
}

/******************************************************************************
 * inner snapshot operation
 */

func (w *defaultWriter) snapshot(task *taskSnapshot) error {
	if err := w.rotate(); err != nil {
		task.SendToCallback(err)
		return err
	}
	lsn, err := getFileLsn(w.file.Name())
	if err != nil {
		return err
	}
	go w.snapBackground(task, lsn-1)

	return nil
}

func (w *defaultWriter) snapBackground(task *taskSnapshot, lsn uint64) {
	// clean old inprogress files
	if err := w.removeOrphanFiles(); err != nil {
		task.SendToCallback(err)
		return
	}

	newFileName := fmt.Sprintf("%s/%s.%s", w.dir, lsn2str(lsn), SNAP_EXTENSION)
	newFileInProgressName := fmt.Sprintf("%s.%s", newFileName, INPROGRESS_EXTENSION)

	// write data to new snapshot
	fh, err := os.OpenFile(newFileInProgressName, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		task.SendToCallback(err)
		return
	}

	for _, space := range *task.Snap() {
		iter := space.Iter()
		for iter.HasNext() {
			ops := operationsFromRecords(iter.collectNext(100), OPERATION_SET)

			err = writeManyTo(ops, fh)
			if err != nil {
				break
			}
		}
		iter.Release()
		if err != nil {
			fh.Close()
			os.Remove(fh.Name())
			task.SendToCallback(err)
			return
		}
	}
	if err := fh.Close(); err != nil {
		task.SendToCallback(err)
		return
	}

	// rename snapshot file name
	if err := os.Rename(fh.Name(), newFileName); err != nil {
		os.Remove(fh.Name())
		task.SendToCallback(err)
		return
	}

	if err := w.removeOldDataFiles(lsn); err != nil {
		task.SendToCallback(err)
		return
	}

	task.SendToCallback(nil)
}

/******************************************************************************
 * inner rotate operation
 */

func (w *defaultWriter) rotate() error {
	nextFileName := fmt.Sprintf("%s/%s.%s", w.dir, lsn2str(w.getLSN()+1), JLOG_EXTENSION)
	if w.file != nil {
		if nextFileName == w.file.Name() {
			return nil
		}
	}
	if fi, err := os.Stat(nextFileName); err == nil {
		// File already exists, no need to rotate
		// if file is empty, it's okay
		if fi.Size() != 0 {
			return fmt.Errorf("file %s already exists (and not empty)", nextFileName)
		}
	}

	// open new file
	newFile, err := os.OpenFile(nextFileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	if w.file != nil {
		// close old file
		if err = closeFile(w.file); err != nil {
			newFile.Close()
			return err
		}
	}

	log.Printf("rotating %s", newFile.Name())
	// set new file
	w.file = newFile
	return nil
}

/******************************************************************************
 * inner base operation
 */

func (w *defaultWriter) write(op *operation) error {
	if op == nil {
		return nil
	}
	lsn := w.getLSN()
	op.LSN = lsn + 1

	if err := writeTo(op, w.file); err != nil {
		return err
	}

	w.setLSN(op.LSN)
	return nil
}

// Send message to the writer
func (w *defaultWriter) send(task task) error {
	w.mu.RLock()
	if w.status != running {
		w.mu.RUnlock()
		return ErrWriterInvalidStatus
	}
	w.incoming <- task
	w.mu.RUnlock()

	return task.Wait()
}

func (w *defaultWriter) setLSN(lsn uint64) {
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

func (w *defaultWriter) getLSN() uint64 {
	if w.lsn == nil {
		return 0
	}
	lsn := w.lsn.Load()
	return lsn
}

func (w *defaultWriter) listActualDataFiles() ([]string, error) {
	result := []string{}
	filePathes, err := listDataFiles(w.dir, []string{SNAP_EXTENSION, JLOG_EXTENSION})
	if err != nil {
		return result, err
	}
	sort.Slice(filePathes, func(i, j int) bool {
		return filePathes[i] < filePathes[j]
	})

	lastSnapPathIdx := -1
	for i := len(filePathes) - 1; i >= 0; i-- {
		if strings.HasSuffix(filePathes[i], SNAP_EXTENSION) {
			lastSnapPathIdx = i
			break
		}
	}
	var snapLSN uint64
	if lastSnapPathIdx != -1 {
		result = append(result, filePathes[lastSnapPathIdx])
		snapLSN, err = getFileLsn(filePathes[lastSnapPathIdx])
		if err != nil {
			return []string{}, err
		}
	}

	for _, filePath := range filePathes {
		if !strings.HasSuffix(filePath, JLOG_EXTENSION) {
			continue
		}
		lsn, err := getFileLsn(filePath)
		if err != nil {
			return []string{}, err
		}
		if lsn < snapLSN {
			continue
		}
		result = append(result, filePath)
	}
	return result, nil
}

func (w *defaultWriter) removeOldDataFiles(lsn uint64) error {
	filePathes, err := listDataFiles(w.dir, []string{SNAP_EXTENSION, JLOG_EXTENSION})
	if err != nil {
		return err
	}
	sort.Slice(filePathes, func(i, j int) bool {
		return filePathes[i] < filePathes[j]
	})
	toRemove := []string{}
	for _, filePath := range filePathes {
		currLSN, err := getFileLsn(filePath)
		if err != nil {
			return err
		}
		if currLSN < lsn {
			toRemove = append(toRemove, filePath)
		}
	}

	if len(toRemove) == 0 {
		return nil
	}

	if err := deleteDataFiles(toRemove); err != nil {
		return err
	}
	return nil
}

func (w *defaultWriter) removeOrphanFiles() error {
	filePathes, err := listDataFiles(w.dir, []string{INPROGRESS_EXTENSION})
	if err != nil {
		return err
	}
	if len(filePathes) == 0 {
		return nil
	}

	if err := deleteDataFiles(filePathes); err != nil {
		return err
	}
	return nil
}
