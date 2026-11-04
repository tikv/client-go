// Copyright 2026 PingCAP, Inc.
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

// Package txnprotocol owns client-go's internal transaction-protocol selection
// policy. Its types intentionally stay below the public client-go API boundary.
package txnprotocol

import (
	"context"
	"fmt"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikvrpc"
)

const (
	legacyVersion     = uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_LEGACY)
	sharedLockVersion = uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
)

// Declaration is the transaction-protocol version selected for one physical
// RPC attempt. Its presence in a context distinguishes an explicit legacy
// declaration (version zero) from no selection.
type Declaration struct {
	Version uint32
}

type declarationContextKey struct{}

// WithDeclaration returns a child context carrying the selected declaration
// for a physical RPC attempt.
func WithDeclaration(ctx context.Context, declaration Declaration) context.Context {
	return context.WithValue(ctx, declarationContextKey{}, declaration)
}

// DeclarationFrom returns the selected declaration, when present.
func DeclarationFrom(ctx context.Context) (Declaration, bool) {
	declaration, ok := ctx.Value(declarationContextKey{}).(Declaration)
	return declaration, ok
}

// StoreRange is the immutable Store metadata snapshot used by the internal
// selector. Missing metadata is normalized to the conservative legacy range.
type StoreRange struct {
	Present bool
	Min     uint32
	Max     uint32
}

func (r StoreRange) bounds() (uint32, uint32) {
	if !r.Present {
		return legacyVersion, legacyVersion
	}
	return r.Min, r.Max
}

func (r StoreRange) String() string {
	if !r.Present {
		return "unknown"
	}
	return fmt.Sprintf("[%d, %d]", r.Min, r.Max)
}

// Selection is the internal result of selecting a declaration for one physical
// attempt.
type Selection struct {
	Protected      bool
	StoreRange     StoreRange
	Required       uint32
	ProcessDefault uint32
	Selected       uint32
}

// NeedStoreReload reports whether only a stale Store maximum could explain the
// local selection failure.
func (s Selection) NeedStoreReload() bool {
	if !s.Protected {
		return false
	}
	_, max := s.StoreRange.bounds()
	return s.Required > max && s.Required <= s.ProcessDefault
}

// requiredTxnProtocolVersion derives the current payload requirement. It is
// intentionally internal: callers cannot label an explicit shared-lock payload
// as legacy.
func requiredTxnProtocolVersion(req *tikvrpc.Request) (required uint32, protected bool) {
	if req == nil {
		return legacyVersion, false
	}
	switch req.Type {
	case tikvrpc.CmdGet, tikvrpc.CmdScan, tikvrpc.CmdBatchGet, tikvrpc.CmdScanLock, tikvrpc.CmdDeleteRange,
		tikvrpc.CmdPrewrite, tikvrpc.CmdPessimisticLock, tikvrpc.CmdPessimisticRollback, tikvrpc.CmdBatchRollback,
		tikvrpc.CmdResolveLock, tikvrpc.CmdCommit, tikvrpc.CmdCleanup, tikvrpc.CmdTxnHeartBeat, tikvrpc.CmdCheckTxnStatus,
		tikvrpc.CmdCheckSecondaryLocks, tikvrpc.CmdMvccGetByKey, tikvrpc.CmdMvccGetByStartTs, tikvrpc.CmdCop, tikvrpc.CmdCopStream:
		protected = true
	}
	if !protected {
		return legacyVersion, false
	}

	switch req.Type {
	case tikvrpc.CmdPrewrite:
		if r, ok := req.Req.(*kvrpcpb.PrewriteRequest); ok && r != nil {
			for _, mutation := range r.GetMutations() {
				if mutation.GetOp() == kvrpcpb.Op_SharedLock {
					return sharedLockVersion, true
				}
			}
		}
	case tikvrpc.CmdPessimisticLock:
		if r, ok := req.Req.(*kvrpcpb.PessimisticLockRequest); ok && r != nil {
			for _, mutation := range r.GetMutations() {
				if mutation.GetOp() == kvrpcpb.Op_SharedPessimisticLock {
					return sharedLockVersion, true
				}
			}
		}
	}
	return legacyVersion, true
}

// Prepare selects a declaration for one controlled physical attempt.
// storeID is diagnostic only; StoreRange must belong to the execution Store.
func Prepare(req *tikvrpc.Request, storeID uint64, storeRange StoreRange) (Selection, error) {
	required, protected := requiredTxnProtocolVersion(req)
	selection := Selection{
		Protected:      protected,
		Required:       required,
		ProcessDefault: uint32(tikvrpc.GetDefaultTxnProtocolVersion()),
		StoreRange:     storeRange,
	}
	if !selection.Protected {
		return selection, nil
	}

	min, max := storeRange.bounds()
	selected := selection.ProcessDefault
	if max < selected {
		selected = max
	}
	selection.Selected = selected
	if selected >= min && selected >= selection.Required {
		return selection, nil
	}
	return selection, newLocalSelectionError(storeID, storeRange, selection.ProcessDefault, selection.Required, selected)
}

// VersionForTransport validates the declaration in ctx against the current
// payload. A missing declaration is treated as the unknown [0, 0] Store range.
func VersionForTransport(ctx context.Context, req *tikvrpc.Request) (uint32, error) {
	required, protected := requiredTxnProtocolVersion(req)
	if !protected {
		return legacyVersion, nil
	}
	declaration, ok := DeclarationFrom(ctx)
	if !ok {
		if required == legacyVersion {
			return legacyVersion, nil
		}
		return legacyVersion, newLocalSelectionError(0, StoreRange{}, uint32(tikvrpc.GetDefaultTxnProtocolVersion()), required, legacyVersion)
	}
	if declaration.Version < required {
		return legacyVersion, newLocalSelectionError(0, StoreRange{}, uint32(tikvrpc.GetDefaultTxnProtocolVersion()), required, declaration.Version)
	}
	return declaration.Version, nil
}

func newLocalSelectionError(storeID uint64, storeRange StoreRange, processDefault, required, provided uint32) error {
	min, max := storeRange.bounds()
	return tikverr.NewErrIncompatibleRequest(&errorpb.IncompatibleRequest{
		Reason: errorpb.IncompatibleRequestReason_IncompatibleRequestReasonUnknown,
		Message: fmt.Sprintf(
			"cannot select txn protocol version locally: store %d cached range [%d, %d], process default %d, request required %d",
			storeID, min, max, processDefault, required,
		),
		ProvidedTxnProtocolVersion:      provided,
		MinCompatibleTxnProtocolVersion: min,
		MaxCompatibleTxnProtocolVersion: max,
	})
}
