package apicollectionv1

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sync"

	json2 "encoding/json/v2"

	"github.com/fulldump/box"

	"github.com/fulldump/inceptiondb/service"
)

var insertMapPool = sync.Pool{
	New: func() any {
		return make(map[string]any)
	},
}

func getInsertMap() map[string]any {
	return insertMapPool.Get().(map[string]any)
}

func putInsertMap(item map[string]any) {
	clear(item)
	insertMapPool.Put(item)
}

func readNDJSONLine(reader *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		part, err := reader.ReadSlice('\n')
		buf = append(buf, part...)
		if err == nil {
			return buf, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(buf) > 0 {
				return buf, io.EOF
			}
			return nil, io.EOF
		}
		return nil, err
	}
}

func insert(ctx context.Context, w http.ResponseWriter, r *http.Request) error {

	wc := http.NewResponseController(w)
	wcerr := wc.EnableFullDuplex()
	if wcerr != nil {
		return wcerr
	}

	s := GetServicer(ctx)
	collectionName := box.GetUrlParameter(ctx, "collectionName")
	collection, err := s.GetCollection(collectionName)
	if err == service.ErrorCollectionNotFound {
		collection, err = s.CreateCollection(collectionName)
		if err != nil {
			return err // todo: handle/wrap this properly
		}
		err = collection.SetDefaults(newCollectionDefaults())
		if err != nil {
			return err // todo: handle/wrap this properly
		}
	}
	if err != nil {
		return err // todo: handle/wrap this properly
	}

	type parseJob struct {
		seq  int
		data []byte
	}

	type parseResult struct {
		seq  int
		item map[string]any
		err  error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	reader := bufio.NewReaderSize(r.Body, 16*1024*1024)

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}

	jobs := make(chan parseJob, workers*2)
	results := make(chan parseResult, workers*4)
	readErrCh := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				item := getInsertMap()
				err := json2.Unmarshal(job.data, &item)
				if err != nil {
					putInsertMap(item)
					select {
					case results <- parseResult{seq: job.seq, err: err}:
					case <-ctx.Done():
					}
					continue
				}

				select {
				case results <- parseResult{seq: job.seq, item: item}:
				case <-ctx.Done():
					putInsertMap(item)
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		var readErr error
		seq := 0
	readLoop:
		for {
			if ctx.Err() != nil {
				readErr = ctx.Err()
				break
			}

			chunk, err := readNDJSONLine(reader)
			if err != nil && !errors.Is(err, io.EOF) {
				readErr = err
				break
			}

			line := chunk
			if len(line) > 0 && line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
				if len(line) > 0 && line[len(line)-1] == '\r' {
					line = line[:len(line)-1]
				}
			}
			if len(line) == 0 {
				if errors.Is(err, io.EOF) {
					break
				}
				continue
			}

			data := append([]byte(nil), line...)

			select {
			case jobs <- parseJob{seq: seq, data: data}:
				seq++
			case <-ctx.Done():
				readErr = ctx.Err()
				break readLoop
			}

			if errors.Is(err, io.EOF) {
				break
			}
		}

		readErrCh <- readErr
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	pending := make(map[int]*parseResult)
	nextSeq := 0
	inserted := 0
	var processErr error

	flushPending := func() {
		for _, pendingRes := range pending {
			if pendingRes.item != nil {
				putInsertMap(pendingRes.item)
			}
		}
		pending = map[int]*parseResult{}
	}

	process := func(res *parseResult) error {
		if res.err != nil {
			if inserted == 0 {
				w.WriteHeader(http.StatusBadRequest)
			}
			return res.err
		}

		row, err := collection.Insert(res.item)
		if err != nil {
			if inserted == 0 {
				w.WriteHeader(http.StatusConflict)
			}
			putInsertMap(res.item)
			return err
		}

		if inserted == 0 {
			w.WriteHeader(http.StatusCreated)
		}

		w.Write(row.Payload)
		w.Write([]byte("\n"))
		inserted++
		putInsertMap(res.item)
		nextSeq++
		return nil
	}

	for res := range results {
		if processErr != nil {
			if res.item != nil {
				putInsertMap(res.item)
			}
			continue
		}

		resCopy := res
		if resCopy.seq == nextSeq {
			if err := process(&resCopy); err != nil {
				processErr = err
				cancel()
				flushPending()
				continue
			}

			for processErr == nil {
				pendingRes, ok := pending[nextSeq]
				if !ok {
					break
				}

				delete(pending, nextSeq)
				if err := process(pendingRes); err != nil {
					processErr = err
					cancel()
					flushPending()
					break
				}
			}

			continue
		}

		pending[resCopy.seq] = &resCopy
	}

	if processErr != nil {
		flushPending()
	}

	readErr := <-readErrCh
	if processErr != nil {
		return processErr
	}

	if readErr != nil && !errors.Is(readErr, context.Canceled) {
		if inserted == 0 {
			w.WriteHeader(http.StatusBadRequest)
		}
		return readErr
	}

	if inserted == 0 {
		w.WriteHeader(http.StatusNoContent)
	}

	return nil
}
