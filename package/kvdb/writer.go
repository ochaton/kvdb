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
	"sync/atomic"
)

type writer struct {
	lsn      *atomic.Uint64
	dir      string
	file     *os.File
	incoming chan message
}

// Send message to the writer
func (w *writer) Send(msg message) error {
	cb := msg.Callback()
	if cb == nil {
		return errors.New("callback is nil")
	}
	w.incoming <- msg
	defer close(cb)
	return <-cb
}

// Write operation to the writer
func (w *writer) Write(op *operation) error {
	return w.Send(newWriteMessage(op))
}

// Rotate jlog file
func (w *writer) Rotate() error {
	return w.Send(newRotateMessage())
}

// Compact jlog files
func (w *writer) Compact(snap map[string]space, lsn uint64) error {
	return w.Send(newCompactMessage(snap, lsn))
}

func lsn2str(lsn uint64) string {
	return fmt.Sprintf("%010d", lsn)
}

func str2lsn(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

func (w *writer) Start() error {
	if err := w.rotate(); err != nil {
		return err
	}

	go w.work()
	return nil
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

	rs := WithReaderStats(file)
	lsn, err = w.loadFrom(rs, applyTxn)
	if err != nil {
		return
	}

	bytesPerSec := rs.HumanStats()
	log.Printf("loadFile %s: %s\n", file.Name(), bytesPerSec)
	return
}

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
	rs := WithReaderStats(file)

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

func (w *writer) Close() error {
	close(w.incoming)
	return nil
}

func (w *writer) LSN() uint64 {
	return w.getLSN()
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
	w.file.Sync()
	w.file.Close()
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
