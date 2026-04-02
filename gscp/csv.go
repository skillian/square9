package gscp

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/skillian/curly"
	"github.com/skillian/square9/internal"
	"github.com/skillian/workers"
)

type csvProcessor struct {
	currentRowIndex      int64
	reader               *csv.Reader
	reqs                 chan csvReq
	processingRowIndexes map[workers.WorkerID]*int64
	fieldNames           []string
	workerErrors         chan error
	todoRowIndexes       []int64
}

func localCSVToDest2(ctx context.Context, source, dest *Spec, config *Config) (err error) {
	limit := config.WebSessionPoolLimit
	if limit < 1 {
		limit = 1
	}
	sourceFile, err := OpenFilenameRead(source.ArchivePath)
	if err != nil {
		return err
	}
	defer internal.Catch(&err, sourceFile.Close)
	p := &csvProcessor{
		currentRowIndex:      -1,
		reader:               csv.NewReader(sourceFile),
		reqs:                 make(chan csvReq, limit*3/2),
		processingRowIndexes: make(map[workers.WorkerID]*int64, limit),
	}
	if ctx, err = p.initWorkers(ctx, limit, config); err != nil {
		return
	}
	if err = p.loadProgress(source, dest); err != nil {
		return
	}
	err = p.processCSV(ctx, source, dest)
	return internal.MultiError(
		err,
		p.saveProgress(err, source, dest),
		<-p.workerErrors,
	)
}

func (p *csvProcessor) initWorkers(ctx context.Context, limit int, config *Config) (ctx2 context.Context, err error) {
	ctx2, cancel := context.WithCancel(ctx)
	processingRowIndexesLock := &sync.Mutex{}
	processingRowIndexes := make([]int64, limit)
	results := workers.Work(ctx2, p.reqs, func(ctx context.Context, id workers.WorkerID, req csvReq) (struct{}, error) {
		processingRowIndexesLock.Lock()
		if _, ok := p.processingRowIndexes[id]; !ok {
			p.processingRowIndexes[id] = &processingRowIndexes[0]
			processingRowIndexes = processingRowIndexes[1:]
		}
		processingRowIndexesLock.Unlock()
		p.startRow(id, req.rowIndex)
		err := CopyFromSourceToDestSpec(ctx, req.rowSpec, req.rowDestSpec, config)
		if err == nil {
			p.doneRow(id)
		}
		return struct{}{}, err
	}, workers.WorkerCount(limit))
	errs := make([]error, 0, config.WebSessionPoolLimit)
	p.workerErrors = make(chan error)
	go func() {
		defer close(p.workerErrors)
		logger.Verbose0("starting results reader...")
		defer logger.Verbose0("Results reader stopped.")
		for result := range results {
			if result.Err != nil {
				errs = append(errs, result.Err)
				cancel()
			}
		}
		p.workerErrors <- internal.MultiError(errs...)
	}()
	return ctx2, nil
}

func (p *csvProcessor) loadProgress(source, dest *Spec) (err error) {
	progress, err := readProgressForSpecs(source, dest)
	if err != nil {
		return err
	}
	sort.Sort(int64Sort(progress.PendingLineIndexes))
	p.todoRowIndexes = progress.PendingLineIndexes
	return nil
}

func (p *csvProcessor) startRow(workerID workers.WorkerID, rowIndex int64) {
	atomic.StoreInt64(p.processingRowIndexes[workerID], rowIndex)
}

func (p *csvProcessor) doneRow(workerID workers.WorkerID) {
	atomic.StoreInt64(p.processingRowIndexes[workerID], 0)
}

func (p *csvProcessor) processCSV(ctx context.Context, source, dest *Spec) error {
	defer close(p.reqs)
	if err := p.loadFieldNames(source); err != nil {
		return fmt.Errorf("loading field names: %w", err)
	}
	if err := p.processTodoRows(ctx, dest); err != nil {
		return err
	}
	for {
		row, err := p.readNextCSVRow()
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		}
		if err = p.processCSVRow(ctx, row, dest); err != nil {
			return err
		}
	}
}

func (p *csvProcessor) loadFieldNames(source *Spec) error {
	firstRow, err := p.readNextCSVRow()
	if err != nil {
		return err
	}
	p.fieldNames = firstRow
	if source.Kind.HasAll(IndexSpec) {
		p.fieldNames = p.fieldNames[:len(p.fieldNames)-1]
	}
	return nil
}

func (p *csvProcessor) processTodoRows(ctx context.Context, dest *Spec) error {
	for {
		todoRowIndex, ok := p.getNextTodoRowIndex()
		if !ok {
			return nil
		}
		logger.Info1("(re)processing row %d", todoRowIndex)
		row, err := p.skipRowsUntilRowIndex(todoRowIndex)
		if err != nil {
			return fmt.Errorf(
				"failed to process todo line %d: %w",
				todoRowIndex, err,
			)
		}
		if err := p.processCSVRow(ctx, row, dest); err != nil {
			return err
		}
	}
}

func (p *csvProcessor) getNextTodoRowIndex() (rowIndex int64, ok bool) {
	if len(p.todoRowIndexes) == 0 {
		return 0, false
	}
	rowIndex = p.todoRowIndexes[0]
	p.todoRowIndexes = p.todoRowIndexes[1:]
	return rowIndex, true
}

func (p *csvProcessor) skipRowsUntilRowIndex(rowIndex int64) (row []string, err error) {
	for {
		row, err = p.readNextCSVRow()
		switch {
		case err != nil:
			return nil, err
		case p.currentRowIndex == rowIndex:
			return row, nil
		case p.currentRowIndex > rowIndex:
			return nil, fmt.Errorf(
				"programming error: skipped row %d",
				rowIndex,
			)
		}
	}
}

func (p *csvProcessor) readNextCSVRow() ([]string, error) {
	row, err := p.reader.Read()
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read row from CSV: %w", err,
		)
	}
	p.currentRowIndex++
	return row, nil
}

func (p *csvProcessor) processCSVRow(ctx context.Context, row []string, dest *Spec) error {
	rowSource, err := parseSpecFromRow(row)
	if err != nil {
		return err
	}
	rowDest := dest.Copy(func(s *Spec) {
		for i, fieldName := range p.fieldNames {
			if _, ok := s.Fields[fieldName]; !ok {
				s.Fields[fieldName] = row[i]
			}
		}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.reqs <- csvReq{
		rowIndex:    p.currentRowIndex,
		rowSpec:     rowSource,
		rowDestSpec: rowDest,
	}:
	}
	return nil
}

func parseSpecFromRow(row []string) (rowSpec *Spec, err error) {
	specStr := row[len(row)-1]
	rowSpec, err = ParseSpec(specStr)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to parse row filename "+
				"%v as spec: %w",
			specStr, err,
		)
	}
	return updateParseSpecFromRow(rowSpec, row)
}

func updateParseSpecFromRow(rowSpec *Spec, row []string) (*Spec, error) {
	for comp := range rowSpec.Components() {
		s := comp.Get(rowSpec)
		switch s := s.(type) {
		case string:
			if !strings.ContainsRune(s, '{') {
				continue
			}
			s2, err := curly.Format(s, row)
			if err != nil {
				return nil, fmt.Errorf(
					"formatting spec %s: %w",
					s, err,
				)
			}
			comp.Set(rowSpec, s2)
		case SpecFields:
			var s2 SpecFields
			for k, v := range s {
				if !strings.ContainsRune(v, '{') {
					continue
				}
				v, err := curly.Format(v, row)
				if err != nil {
					return nil, fmt.Errorf(
						"formatting spec field %s: %w",
						v, err,
					)
				}
				if s2 == nil {
					s2 = make(SpecFields, len(s))
					maps.Copy(s2, s)
				}
				s2[k] = v
			}
			if s2 != nil {
				comp.Set(rowSpec, s2)
			}
		}
	}
	return rowSpec, nil
}

func (p *csvProcessor) saveProgress(processCSVErr error, source, dest *Spec) (err error) {
	if processCSVErr == nil {
		filename := progressFilenameForSpecs(source, dest)

		if err = os.Remove(filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf(
				"failed to cleanup progress file %v: %w",
				filename, err,
			)
		}
		return nil
	}
	pendingLineIndexes := make([]int64, len(p.processingRowIndexes))
	i := 0
	for _, ptr := range p.processingRowIndexes {
		pendingLineIndexes[i] = atomic.LoadInt64(ptr)
		i++
	}
	return writeProgressForSpecs(indexProgress{
		PendingLineIndexes: pendingLineIndexes,
	}, source, dest)
}

type csvReq struct {
	rowIndex    int64
	rowSpec     *Spec
	rowDestSpec *Spec
}

type indexProgress struct {
	// PendingLineIndexes holds the line numbers (indexed from 0)
	// that the worker processes are working on.  If there are
	// 8 workers, this slice will have a length of 8.
	//
	// When recovering, all of the lines from 0 to the minimum
	// index in this slice can be skipped in the index file.
	// The lines for each number in this slice need to be
	// reprocessed.  Then every line after the maximum index has
	// to be reprocessed.
	PendingLineIndexes []int64
}

func readProgressForSpecs(source, dest *Spec) (p indexProgress, err error) {
	filename := progressFilenameForSpecs(source, dest)
	if _, err = os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		}
	}
	f, err := OpenFilenameRead(filename)
	if err != nil {
		return p, fmt.Errorf(
			"failed to open progress file: %w", err,
		)
	}
	defer internal.Catch(&err, f.Close)
	err = json.NewDecoder(f).Decode(&p)
	return
}

func writeProgressForSpecs(p indexProgress, source, dest *Spec) (err error) {
	filename := progressFilenameForSpecs(source, dest)
	f, err := CreateLockedFile(filename, true)
	if err != nil {
		return fmt.Errorf("failed to open progress file: %w", err)
	}
	defer internal.Catch(&err, f.Close)
	temp := make([]int64, 0, len(p.PendingLineIndexes))
	for _, i := range p.PendingLineIndexes {
		if i == 0 {
			continue
		}
		temp = append(temp, i)
	}
	p.PendingLineIndexes = temp
	return json.NewEncoder(f).Encode(p)
}

func progressFilenameForSpecs(source, dest *Spec) string {
	appendFilenameSectionForSpec := func(parts []string, sp *Spec) []string {
		appendIf := func(strs *[]string, s string) {
			if s == "" {
				return
			}
			*strs = append(*strs, s)
		}
		appendIf(&parts, sp.Hostname)
		if sp.APIPath != defaultAPIPath {
			appendIf(&parts, sp.APIPath)
		}
		appendIf(&parts, sp.Database)
		appendIf(&parts, sp.ArchivePath)
		appendIf(&parts, sp.Search)
		return parts
	}
	parts := make([]string, 1, 16)
	parts[0] = "gscp"
	parts = appendFilenameSectionForSpec(parts, source)
	parts = appendFilenameSectionForSpec(parts, dest)
	filename := strings.Join(parts, "-")
	filename = cleanFilename(filename)
	return filepath.Join(
		os.TempDir(),
		filename+".json",
	)
}

type int64Sort []int64

var _ sort.Interface = int64Sort{}

func (s int64Sort) Len() int { return len(s) }
func (s int64Sort) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s int64Sort) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
