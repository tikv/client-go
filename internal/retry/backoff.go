// Copyright 2021 TiKV Authors
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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/retry/backoff.go
//

// Copyright 2016 PingCAP, Inc.
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

package retry

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Backoffer is a utility for retrying queries.
type Backoffer struct {
	ctx context.Context

	fn            map[string]backoffFn
	maxSleep      int
	totalSleep    int
	excludedSleep int

	vars *kv.Variables
	noop bool

	errors         []error
	configs        []*Config
	backoffSleepMS map[string]int
	backoffTimes   map[string]int
	parent         *Backoffer
}

type txnStartCtxKeyType struct{}

// TxnStartKey is a key for transaction start_ts info in context.Context.
var TxnStartKey interface{} = txnStartCtxKeyType{}

// NewBackoffer (Deprecated) creates a Backoffer with maximum sleep time(in ms).
func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer {
	return &Backoffer{
		ctx:      ctx,
		maxSleep: maxSleep,
		vars:     kv.DefaultVars,
	}
}

// NewBackofferWithVars creates a Backoffer with maximum sleep time(in ms) and kv.Variables.
func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer {
	return NewBackoffer(ctx, maxSleep).withVars(vars)
}

// NewNoopBackoff create a Backoffer do nothing just return error directly
func NewNoopBackoff(ctx context.Context) *Backoffer {
	return &Backoffer{ctx: ctx, noop: true}
}

// withVars sets the kv.Variables to the Backoffer and return it.
func (b *Backoffer) withVars(vars *kv.Variables) *Backoffer {
	if vars != nil {
		b.vars = vars
	}
	// maxSleep is the max sleep time in millisecond.
	// When it is multiplied by BackOffWeight, it should not be greater than MaxInt32.
	if b.maxSleep > 0 && math.MaxInt32/b.vars.BackOffWeight >= b.maxSleep {
		b.maxSleep *= b.vars.BackOffWeight
	}
	return b
}

// Backoff sleeps a while base on the Config and records the error message.
// It returns a retryable error if total sleep time exceeds maxSleep.
func (b *Backoffer) Backoff(cfg *Config, err error) error {
	if span := opentracing.SpanFromContext(b.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("tikv.backoff.%s", cfg), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(b.ctx, span1)
	}
	return b.BackoffWithCfgAndMaxSleep(cfg, -1, err)
}

// BackoffWithMaxSleepTxnLockFast sleeps a while base on the MaxSleepTxnLock and records the error message
// and never sleep more than maxSleepMs for each sleep.
func (b *Backoffer) BackoffWithMaxSleepTxnLockFast(maxSleepMs int, err error) error {
	cfg := BoTxnLockFast
	return b.BackoffWithCfgAndMaxSleep(cfg, maxSleepMs, err)
}

// BackoffWithCfgAndMaxSleep sleeps a while base on the Config and records the error message
// and never sleep more than maxSleepMs for each sleep.
func (b *Backoffer) BackoffWithCfgAndMaxSleep(cfg *Config, maxSleepMs int, err error) error {
	if strings.Contains(err.Error(), tikverr.MismatchClusterID) {
		logutil.Logger(b.ctx).Fatal("critical error", zap.Error(err))
	}
	select {
	case <-b.ctx.Done():
		return errors.WithStack(err)
	default:
	}
	if b.noop {
		return err
	}
	if b.maxSleep > 0 && (b.totalSleep-b.excludedSleep) >= b.maxSleep {
		longestSleepCfg, longestSleepTime := b.longestSleepCfg()
		errMsg := fmt.Sprintf("%s backoffer.maxSleep %dms is exceeded, errors:", cfg.String(), b.maxSleep)
		for i, err := range b.errors {
			// Print only last 3 errors for non-DEBUG log levels.
			if log.GetLevel() == zapcore.DebugLevel || i >= len(b.errors)-3 {
				errMsg += "\n" + err.Error()
			}
		}
		var backoffDetail bytes.Buffer
		totalTimes := 0
		for name, times := range b.backoffTimes {
			totalTimes += times
			if backoffDetail.Len() > 0 {
				backoffDetail.WriteString(", ")
			}
			backoffDetail.WriteString(name)
			backoffDetail.WriteString(":")
			backoffDetail.WriteString(strconv.Itoa(times))
		}
		errMsg += fmt.Sprintf("\ntotal-backoff-times: %v, backoff-detail: %v", totalTimes, backoffDetail.String())
		returnedErr := err
		if longestSleepCfg != nil {
			errMsg += fmt.Sprintf("\nlongest sleep type: %s, time: %dms", longestSleepCfg.String(), longestSleepTime)
			returnedErr = longestSleepCfg.err
		}
		logutil.Logger(b.ctx).Warn(errMsg)
		// Use the backoff type that contributes most to the timeout to generate a MySQL error.
		return errors.WithStack(returnedErr)
	}
	b.errors = append(b.errors, errors.Errorf("%s at %s", err.Error(), time.Now().Format(time.RFC3339Nano)))
	b.configs = append(b.configs, cfg)

	// Lazy initialize.
	if b.fn == nil {
		b.fn = make(map[string]backoffFn)
	}
	f, ok := b.fn[cfg.name]
	if !ok {
		f = cfg.createBackoffFn(b.vars)
		b.fn[cfg.name] = f
	}
	realSleep := f(b.ctx, maxSleepMs)
	if cfg.metric != nil {
		(*cfg.metric).Observe(float64(realSleep) / 1000)
	}

	b.totalSleep += realSleep
	if _, ok := isSleepExcluded[cfg.name]; ok {
		b.excludedSleep += realSleep
	}
	if b.backoffSleepMS == nil {
		b.backoffSleepMS = make(map[string]int)
	}
	b.backoffSleepMS[cfg.name] += realSleep
	if b.backoffTimes == nil {
		b.backoffTimes = make(map[string]int)
	}
	b.backoffTimes[cfg.name]++

	stmtExec := b.ctx.Value(util.ExecDetailsKey)
	if stmtExec != nil {
		detail := stmtExec.(*util.ExecDetails)
		atomic.AddInt64(&detail.BackoffDuration, int64(realSleep)*int64(time.Millisecond))
		atomic.AddInt64(&detail.BackoffCount, 1)
	}

	if b.vars != nil && b.vars.Killed != nil {
		if atomic.LoadUint32(b.vars.Killed) == 1 {
			return errors.WithStack(tikverr.ErrQueryInterrupted)
		}
	}

	var startTs interface{}
	if ts := b.ctx.Value(TxnStartKey); ts != nil {
		startTs = ts
	}
	logutil.Logger(b.ctx).Debug("retry later",
		zap.Error(err),
		zap.Int("totalSleep", b.totalSleep),
		zap.Int("excludedSleep", b.excludedSleep),
		zap.Int("maxSleep", b.maxSleep),
		zap.Stringer("type", cfg),
		zap.Reflect("txnStartTS", startTs))
	return nil
}

func (b *Backoffer) String() string {
	if b.totalSleep == 0 {
		return ""
	}
	return fmt.Sprintf(" backoff(%dms %v)", b.totalSleep, b.configs)
}

// copyMapWithoutRecursive is only used to deep copy map fields in the Backoffer type.
func copyMapWithoutRecursive(srcMap map[string]int) map[string]int {
	result := map[string]int{}
	for k, v := range srcMap {
		result[k] = v
	}
	return result
}

// Clone creates a new Backoffer which keeps current Backoffer's sleep time and errors, and shares
// current Backoffer's context.
// Some fields like `configs` and `vars` are concurrently used by all the backoffers in different threads,
// try not to modify the referenced content directly.
func (b *Backoffer) Clone() *Backoffer {
	return &Backoffer{
		ctx:            b.ctx,
		maxSleep:       b.maxSleep,
		totalSleep:     b.totalSleep,
		excludedSleep:  b.excludedSleep,
		vars:           b.vars,
		errors:         append([]error{}, b.errors...),
		configs:        append([]*Config{}, b.configs...),
		backoffSleepMS: copyMapWithoutRecursive(b.backoffSleepMS),
		backoffTimes:   copyMapWithoutRecursive(b.backoffTimes),
		parent:         b.parent,
	}
}

// Fork creates a new Backoffer which keeps current Backoffer's sleep time and errors, and holds
// a child context of current Backoffer's context.
// Some fields like `configs` and `vars` are concurrently used by all the backoffers in different threads,
// try not to modify the referenced content directly.
func (b *Backoffer) Fork() (*Backoffer, context.CancelFunc) {
	ctx, cancel := context.WithCancel(b.ctx)
	return &Backoffer{
		ctx:            ctx,
		maxSleep:       b.maxSleep,
		totalSleep:     b.totalSleep,
		excludedSleep:  b.excludedSleep,
		errors:         append([]error{}, b.errors...),
		configs:        append([]*Config{}, b.configs...),
		backoffSleepMS: copyMapWithoutRecursive(b.backoffSleepMS),
		backoffTimes:   copyMapWithoutRecursive(b.backoffTimes),
		vars:           b.vars,
		parent:         b,
	}, cancel
}

// GetVars returns the binded vars.
func (b *Backoffer) GetVars() *kv.Variables {
	return b.vars
}

// GetTotalSleep returns total sleep time.
func (b *Backoffer) GetTotalSleep() int {
	return b.totalSleep
}

// GetTypes returns type list of this backoff and all its ancestors.
func (b *Backoffer) GetTypes() []string {
	typs := make([]string, 0, len(b.configs))
	for b != nil {
		for _, cfg := range b.configs {
			typs = append(typs, cfg.String())
		}
		b = b.parent
	}
	return typs
}

// GetCtx returns the binded context.
func (b *Backoffer) GetCtx() context.Context {
	return b.ctx
}

// SetCtx sets the binded context to ctx.
func (b *Backoffer) SetCtx(ctx context.Context) {
	b.ctx = ctx
}

// GetBackoffTimes returns a map contains backoff time count by type.
func (b *Backoffer) GetBackoffTimes() map[string]int {
	return b.backoffTimes
}

// GetTotalBackoffTimes returns the total backoff times of the backoffer.
func (b *Backoffer) GetTotalBackoffTimes() int {
	total := 0
	for _, time := range b.backoffTimes {
		total += time
	}
	return total
}

// GetBackoffSleepMS returns a map contains backoff sleep time by type.
func (b *Backoffer) GetBackoffSleepMS() map[string]int {
	return b.backoffSleepMS
}

// ErrorsNum returns the number of errors.
func (b *Backoffer) ErrorsNum() int {
	return len(b.errors)
}

// Reset resets the sleep state of the backoffer, so that following backoff
// can sleep shorter. The reason why we don't create a new backoffer is that
// backoffer is similar to context and it records some metrics that we
// want to record for an entire process which is composed of serveral stages.
func (b *Backoffer) Reset() {
	b.fn = nil
	b.totalSleep = 0
	b.excludedSleep = 0
}

// ResetMaxSleep resets the sleep state and max sleep limit of the backoffer.
// It's used when switches to the next stage of the process.
func (b *Backoffer) ResetMaxSleep(maxSleep int) {
	b.Reset()
	b.maxSleep = maxSleep
	b.withVars(b.vars)
}

func (b *Backoffer) longestSleepCfg() (*Config, int) {
	candidate := ""
	maxSleep := 0
	for cfgName, sleepTime := range b.backoffSleepMS {
		if _, ok := isSleepExcluded[cfgName]; sleepTime > maxSleep && !ok {
			maxSleep = sleepTime
			candidate = cfgName
		}
	}
	for _, cfg := range b.configs {
		if cfg.name == candidate {
			return cfg, maxSleep
		}
	}
	return nil, 0
}
