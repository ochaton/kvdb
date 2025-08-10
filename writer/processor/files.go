package processor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"

	"github.com/ochaton/kvdb/writer/operation"
)

const (
	JLOG_EXTENSION       = "jlog"
	SNAP_EXTENSION       = "snap"
	INPROGRESS_EXTENSION = "inprogress"
)

var lsnRegexp = regexp.MustCompile(`^(?:.+/)?(\d+)\.([a-z]+)$`)

var ErrFileIsNil = errors.New("file is nil")
var ErrFileIsNotEmpty = errors.New("file is not empty")

type ListedFiles struct {
	snaps []string
	jlogs []string
	tmp   []string
}

func NewListedFiles(dir string, entries []os.DirEntry) *ListedFiles {
	listedFiles := &ListedFiles{
		snaps: make([]string, 0),
		jlogs: make([]string, 0),
		tmp:   make([]string, 0),
	}
	var fullPath, extension string
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		if !ent.Type().IsRegular() {
			continue
		}
		fullPath = fmt.Sprintf("%s/%s", dir, ent.Name())
		extension = filepath.Ext(fullPath)[1:]
		if extension == SNAP_EXTENSION {
			listedFiles.snaps = append(listedFiles.snaps, fullPath)
		} else if extension == JLOG_EXTENSION {
			listedFiles.jlogs = append(listedFiles.jlogs, fullPath)
		} else if extension == INPROGRESS_EXTENSION {
			listedFiles.tmp = append(listedFiles.tmp, fullPath)
		} else {
			continue
		}
	}
	slices.Sort(listedFiles.snaps)
	slices.Sort(listedFiles.jlogs)
	return listedFiles
}

func (lf *ListedFiles) GetActualFiles() ([]string, error) {
	if len(lf.snaps) == 0 {
		return lf.jlogs, nil
	}
	lastSnapFilePath := lf.snaps[len(lf.snaps)-1]
	lastSnapLSN, err := getFileLsn(lastSnapFilePath)
	if err != nil {
		return []string{}, err
	}

	result := []string{lastSnapFilePath}
	for _, jlogFilePath := range lf.jlogs {
		lsn, err := getFileLsn(jlogFilePath)
		if err != nil {
			return []string{}, err
		}
		if lsn < lastSnapLSN {
			continue
		}
		result = append(result, jlogFilePath)
	}
	return result, nil
}

func (lf *ListedFiles) GetUnusedFiles() ([]string, error) {
	result := make([]string, 0, len(lf.tmp))
	result = append(result, lf.tmp...)

	if len(lf.snaps) == 0 {
		return result, nil
	}
	result = append(result, lf.snaps[:len(lf.snaps)-1]...)
	lastSnapFilePath := lf.snaps[len(lf.snaps)-1]
	lastSnapLSN, err := getFileLsn(lastSnapFilePath)
	if err != nil {
		return []string{}, err
	}
	for _, jlogFilePath := range lf.jlogs {
		lsn, err := getFileLsn(jlogFilePath)
		if err != nil {
			return []string{}, err
		}
		if lsn < lastSnapLSN {
			result = append(result, jlogFilePath)
		}
	}
	return result, nil
}

func listDataFiles(dir string) (*ListedFiles, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	return NewListedFiles(dir, entries), nil
}

func deleteDataFiles(filesPathes []string) error {
	for _, filePath := range filesPathes {
		if err := os.Remove(filePath); err != nil {
			return err
		}
	}
	return nil
}

func openFile(filePath string, new bool) (*os.File, error) {
	if new {
		if fileInfo, err := os.Stat(filePath); err == nil && fileInfo.Size() != 0 {
			return nil, fmt.Errorf("failed to open file: %v", ErrFileIsNotEmpty)
		}
	}
	fh, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	return fh, nil
}

func closeFile(file *os.File) error {
	if file == nil {
		return ErrFileIsNil
	}
	if err := file.Sync(); err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}
	return nil
}

func getFileLsn(filePath string) (uint64, error) {
	match := lsnRegexp.FindStringSubmatch(filePath)
	if len(match) < 2 {
		return 0, fmt.Errorf("wrong fileName to get lsn: %s", filePath)
	}
	return str2lsn(match[1])
}

func writeTo(file *os.File, op *operation.Operation) error {
	data, err := op.ToBytes()
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		return err
	}
	return nil
}

func writeManyTo(file *os.File, ops *operation.OperationBatch) error {
	data, err := ops.ToBytes()
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		return err
	}
	return nil
}

func lsn2str(lsn uint64) string {
	return fmt.Sprintf("%010d", lsn)
}

func str2lsn(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}
