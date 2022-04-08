package gscp

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/skillian/expr/errors"
	"github.com/skillian/workers"
)

type csvProcessor struct {
	currentRowIndex      int64
	reader               *csv.Reader
	reqs                 chan csvReq
	processingRowIndexes []int64
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
	defer errors.Catch(&err, sourceFile.Close)
	p := &csvProcessor{
		currentRowIndex:      -1,
		reader:               csv.NewReader(sourceFile),
		reqs:                 make(chan csvReq, limit*3/2),
		processingRowIndexes: make([]int64, limit),
	}
	if ctx, err = p.initWorkers(ctx, limit, config); err != nil {
		return
	}
	if err = p.loadFieldNames(ctx, source, dest); err != nil {
		return
	}
	if err = p.loadProgress(source, dest); err != nil {
		return
	}
	err = p.processCSV(ctx, dest)
	return errors.Aggregate(
		err,
		p.saveProgress(err, source, dest),
		<-p.workerErrors,
	)
}

func (p *csvProcessor) initWorkers(ctx context.Context, limit int, config *Config) (ctx2 context.Context, err error) {
	ctx2, cancel := context.WithCancel(ctx)
	results := workers.Work(ctx, p.reqs, func(ctx context.Context, id int, req csvReq) (struct{}, error) {
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
		p.workerErrors <- errors.Aggregate(errs...)
	}()
	return ctx2, nil
}

func (p *csvProcessor) loadFieldNames(ctx context.Context, source, dest *Spec) (err error) {
	p.fieldNames, err = getFieldNamesForSourceSpec(ctx, source, dest)
	if err != nil {
		return errors.Errorf1From(
			err, "failed to get field names for spec: %v",
			source,
		)
	}
	return nil
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

func (p *csvProcessor) startRow(workerID int, rowIndex int64) {
	atomic.StoreInt64(&p.processingRowIndexes[workerID], rowIndex)
}

func (p *csvProcessor) doneRow(workerID int) {
	atomic.StoreInt64(&p.processingRowIndexes[workerID], 0)
}

func (p *csvProcessor) processCSV(ctx context.Context, dest *Spec) error {
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

func (p *csvProcessor) processTodoRows(ctx context.Context, dest *Spec) error {
	for {
		todoRowIndex, ok := p.getNextTodoRowIndex()
		if !ok {
			return nil
		}
		logger.Info1("(re)processing row %d", todoRowIndex)
		row, err := p.skipRowsUntilRowIndex(todoRowIndex)
		if err != nil {
			return errors.Errorf1From(
				err, "failed to process todo line %d",
				todoRowIndex,
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
			return nil, errors.Errorf1(
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
		return nil, errors.Errorf0From(
			err, "failed to read row from CSV",
		)
	}
	p.currentRowIndex++
	return row, nil
}

func (p *csvProcessor) processCSVRow(ctx context.Context, row []string, dest *Spec) error {
	rowSource, err := p.parseSpecFromRow(row)
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

func (p *csvProcessor) parseSpecFromRow(row []string) (*Spec, error) {
	rowSpec, err := ParseSpec(row[len(row)-1])
	if err != nil {
		return nil, errors.Errorf1From(
			err, "failed to parse row filename "+
				"%v as spec",
			row[len(row)-1],
		)
	}
	return rowSpec, nil
}

func (p *csvProcessor) saveProgress(processCSVErr error, source, dest *Spec) (err error) {
	if processCSVErr == nil {
		filename := progressFilenameForSpecs(source, dest)
		if err = os.Remove(filename); err != nil {
			return errors.Errorf1From(
				err, "failed to cleanup progress file %v",
				filename,
			)
		}
		return nil
	}
	pendingLineIndexes := make([]int64, len(p.processingRowIndexes))
	for i := range p.processingRowIndexes {
		pendingLineIndexes[i] = atomic.LoadInt64(&p.processingRowIndexes[i])
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
		return p, errors.Errorf0From(
			err, "failed to open progress file",
		)
	}
	defer errors.Catch(&err, f.Close)
	err = json.NewDecoder(f).Decode(&p)
	return
}

func writeProgressForSpecs(p indexProgress, source, dest *Spec) (err error) {
	filename := progressFilenameForSpecs(source, dest)
	f, err := OpenFilenameCreate(filename, true)
	if err != nil {
		return errors.Errorf0From(
			err, "failed to open progress file",
		)
	}
	defer errors.Catch(&err, f.Close)
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
