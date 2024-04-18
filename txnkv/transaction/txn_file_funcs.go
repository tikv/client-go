// Copyright 2024 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transaction

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	zap "go.uber.org/zap"
)

type buildChunkResp struct {
	chunkId    uint64
	chunkRange txnChunkRange
	err        error
}

type buildChunkReq struct {
	data       []byte
	chunkRange txnChunkRange
	bo         *retry.Backoffer
	respCh     chan<- buildChunkResp
}

type chunkWorkerPool struct {
	reqCh chan buildChunkReq
}

func (p *chunkWorkerPool) BuildChunk(buf []byte, chunkRange txnChunkRange, bo *retry.Backoffer, respCh chan buildChunkResp, results []buildChunkResp) ([]buildChunkResp, error) {
	req := buildChunkReq{buf, chunkRange, bo, respCh}
Loop:
	for {
		select {
		case p.reqCh <- req:
			break Loop
		case res := <-respCh:
			if res.err != nil {
				return results, res.err
			}
			results = append(results, res)
		}
	}

	for {
		select {
		case res := <-respCh:
			if res.err != nil {
				return results, res.err
			}
			results = append(results, res)
		default:
			return results, nil
		}
	}
}

func newChunkWorkerPoolImpl() *chunkWorkerPool {
	cfg := config.GetGlobalConfig()
	concurrency := cfg.TiKVClient.TxnChunkWriterConcurrency

	input := make(chan buildChunkReq, concurrency)
	for i := 0; i < int(concurrency); i++ {
		go chunkWorkerLoop(input)
	}

	return &chunkWorkerPool{input}
}

func chunkWorkerLoop(input <-chan buildChunkReq) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("chunkWorkerLoop panic",
				zap.Any("r", r),
				zap.Stack("stack"))
			go chunkWorkerLoop(input)
		}
	}()

	var writer *chunkWriterClient
	for req := range input {
		var err error
		if writer == nil {
			if writer, err = newChunkWriterClient(); err != nil {
				req.respCh <- buildChunkResp{0, req.chunkRange, errors.Wrap(err, "txn file: create chunk writer client failed")}
				continue
			}
		}

		select {
		case <-req.bo.GetCtx().Done():
			err := req.bo.GetCtx().Err()
			logutil.Logger(req.bo.GetCtx()).Info("build txn file request dropped, err: %v",
				zap.Error(err), zap.Stringer("chunkRange", req.chunkRange))
			continue
		default:
		}

		chunkId, err := writer.buildTxnFile(req.bo, req.data)
		select {
		case req.respCh <- buildChunkResp{chunkId, req.chunkRange, err}:
		case <-req.bo.GetCtx().Done():
			err := req.bo.GetCtx().Err()
			logutil.Logger(req.bo.GetCtx()).Info("build txn file result dropped, err: %v",
				zap.Error(err), zap.Stringer("chunkRange", req.chunkRange))
		}
	}
}

var getChunkWorkerPool func() *chunkWorkerPool = sync.OnceValue[*chunkWorkerPool](newChunkWorkerPoolImpl)
