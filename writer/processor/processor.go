package processor

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"

	"github.com/ochaton/kvdb/common/record"
	"github.com/ochaton/kvdb/writer/operation"
)

type ApplyOperationFn func(txn *operation.Operation) (uint64, error)

/*
Single thread processor.
Should be protected with locks outside.
*/
type Processor struct {
	dir  string
	file *os.File
	lsn  atomic.Uint64
}

func NewProcessor(path string) *Processor {
	return &Processor{dir: path, lsn: atomic.Uint64{}}
}

func (proc *Processor) Close() error {
	if proc.file != nil {
		return closeFile(proc.file)
	}
	return nil
}

func (proc *Processor) setLSN(lsn uint64) {
	old := proc.lsn.Load()
	for old < lsn {
		if proc.lsn.CompareAndSwap(old, lsn) {
			break
		}
		old = proc.lsn.Load()
	}
}

func (proc *Processor) GetLSN() uint64 {
	return proc.lsn.Load()
}

func (proc *Processor) newFilePath(lsn uint64, extension string) string {
	return fmt.Sprintf("%s/%s.%s", proc.dir, lsn2str(lsn), extension)
}

func (proc *Processor) Rotate() error {
	log.Printf("start rotating")

	nextFilePath := proc.newFilePath(proc.GetLSN()+1, JLOG_EXTENSION)

	// check curr jlog
	if proc.file != nil && proc.file.Name() == nextFilePath {
		log.Printf("finish rotating: dont change file")
		return nil
	}

	// open new file
	newFile, err := openFile(nextFilePath, true)
	if err != nil {
		return fmt.Errorf(
			"finish rotating: failed to open new file '%s' with error '%v'",
			nextFilePath,
			err,
		)
	}

	// close old file if it exists
	if proc.file != nil {
		if err = closeFile(proc.file); err != nil {
			newFile.Close()
			return fmt.Errorf(
				"finish rotating: failed to close old file '%s' with error '%v'",
				proc.file.Name(),
				err,
			)
		}
	}
	proc.file = newFile
	log.Printf("finish rotating: newFile=%s", newFile.Name())
	return nil
}

type Snapshot interface {
	IsFinished() bool
	GetItems(count int) []*record.Record
}

func (proc *Processor) Snapshot(lsn uint64, snap Snapshot) error {
	if err := proc.clean(); err != nil {
		return err
	}

	nextFilePath := proc.newFilePath(lsn, SNAP_EXTENSION)
	nextFilePathTemp := fmt.Sprintf("%s.%s", nextFilePath, INPROGRESS_EXTENSION)

	fh, err := openFile(nextFilePathTemp, true)
	if err != nil {
		return err
	}

	// write data to new snapshot
	for !snap.IsFinished() {
		records := snap.GetItems(100)
		if len(records) == 0 {
			continue
		}
		ops := operation.NewOperationBatch(records, operation.OPERATION_SET)
		err = writeManyTo(fh, ops)
		if err != nil {
			fh.Close()
			os.Remove(fh.Name())
			return err
		}
	}
	if err := fh.Close(); err != nil {
		return err
	}

	// rename snapshot file name
	if err := os.Rename(fh.Name(), nextFilePath); err != nil {
		os.Remove(fh.Name())
		return err
	}

	if err := proc.clean(); err != nil {
		return err
	}

	return nil
}

func (proc *Processor) clean() error {
	lf, err := listDataFiles(proc.dir)
	if err != nil {
		return err
	}
	unusedFiles, err := lf.GetUnusedFiles()
	if err != nil {
		return err
	}

	if err := deleteDataFiles(unusedFiles); err != nil {
		return err
	}
	return nil
}

func (proc *Processor) Write(op *operation.Operation) error {
	if op == nil {
		return nil
	}
	op.SetLSN(proc.GetLSN() + 1)
	if err := writeTo(proc.file, op); err != nil {
		return err
	}
	proc.setLSN(op.LSN)
	return nil
}

func (proc *Processor) WriteBatch(ops *operation.OperationBatch) error {
	if ops == nil {
		return nil
	}
	newLSN := ops.SetLSN(proc.GetLSN() + 1)
	if err := writeManyTo(proc.file, ops); err != nil {
		return err
	}
	proc.setLSN(newLSN)
	return nil
}

func (proc *Processor) Load(applyOpFn ApplyOperationFn) error {
	log.Printf("start loading")
	if err := os.MkdirAll(proc.dir, 0755); err != nil {
		return err
	}

	lf, err := listDataFiles(proc.dir)
	if err != nil {
		return err
	}
	actualFilesPathes, err := lf.GetActualFiles()
	if err != nil {
		return err
	}

	for _, filePath := range actualFilesPathes {
		lsn, err := proc.loadDataFile(filePath, applyOpFn)
		if err != nil {
			return err
		}
		if lsn > proc.GetLSN() {
			proc.setLSN(lsn)
		}
	}
	log.Printf("finish loading")
	return nil
}

func (proc *Processor) loadDataFile(filePath string, applyOpFn ApplyOperationFn) (uint64, error) {
	fh, err := openFile(filePath, false)
	if err != nil {
		return 0, err
	}
	defer fh.Close()

	rs := withReaderStats(fh)
	dec := json.NewDecoder(bufio.NewReader(rs))

	var lsn uint64
	for {
		var op operation.Operation
		if err = dec.Decode(&op); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		lsn, err = applyOpFn(&op)
		if err != nil {
			return 0, err
		}
	}

	log.Printf("loadFile %s (lsn=%d): %s\n", fh.Name(), lsn, rs.HumanStats())
	return lsn, nil
}
