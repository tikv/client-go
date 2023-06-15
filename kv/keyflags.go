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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/kv/keyflags.go
//

// Copyright 2021 PingCAP, Inc.

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

package kv

// KeyFlags are metadata associated with key.
// Notice that the highest bit is used by red black tree, do not set flags on it.
type KeyFlags uint16

// FlagBytes is the byte size of type KeyFlags
const FlagBytes = 2

const (
	flagPresumeKNE KeyFlags = 1 << iota
	flagKeyLocked
	flagNeedLocked
	flagKeyLockedValExist
	flagNeedCheckExists
	flagPrewriteOnly
	flagIgnoredIn2PC
	flagReadable
	flagNewlyInserted

	// The following are assertion related flags.
	// There are four choices of the two bits:
	// * 0: Assertion is not set and can be set later.
	// * flagAssertExists: We assert the key exists.
	// * flagAssertNotExists: We assert the key doesn't exist.
	// * flagAssertExists | flagAssertNotExists: Assertion cannot be made on this key (unknown).
	// Once either (or both) of the two flags is set, we say assertion is set (`HasAssertionFlags` becomes true), and
	// it's expected to be unchangeable within the current transaction.
	flagAssertExist
	flagAssertNotExist

	// It marks the conflict check of the key is postponed to prewrite. This is only used in pessimistic transactions.
	// When the key gets locked (and the existence is checked), the flag should be removed.
	// It should only be applied to keys with PresumeNotExist, so that an in-place constraint check becomes
	// (a conflict check + a constraint check) in prewrite.
	flagNeedConstraintCheckInPrewrite

	// A PresumeKNE that should not be unset. It's used by lazy uniqueness checks. So that PresumeKNE flags in previous
	// statement should not be mistakenly unset.
	// It's only set when a stmt that sets PresumeKNE finishes.
	// It's only read together with PresumeKNE
	// It doesn't have to be persistent, since the only place where it is used is in statement retry where it helps set
	// the correct assert_not_exist option. The life of the flag begins after stmt which sets the PresumeKNE flag, and
	// ends when the lock is acquired.
	flagPreviousPresumeKNE

	persistentFlags = flagKeyLocked | flagKeyLockedValExist | flagNeedConstraintCheckInPrewrite
)

// HasAssertExist returns whether the key need ensure exists in 2pc.
func (f KeyFlags) HasAssertExist() bool {
	return f&flagAssertExist != 0 && f&flagAssertNotExist == 0
}

// HasAssertNotExist returns whether the key need ensure non-exists in 2pc.
func (f KeyFlags) HasAssertNotExist() bool {
	return f&flagAssertNotExist != 0 && f&flagAssertExist == 0
}

// HasAssertUnknown returns whether the key is marked unable to do any assertion.
func (f KeyFlags) HasAssertUnknown() bool {
	return f&flagAssertExist != 0 && f&flagAssertNotExist != 0
}

// HasAssertionFlags returns whether the key's assertion is set.
func (f KeyFlags) HasAssertionFlags() bool {
	return f&flagAssertExist != 0 || f&flagAssertNotExist != 0
}

// HasPresumeKeyNotExists returns whether the associated key use lazy check.
func (f KeyFlags) HasPresumeKeyNotExists() bool {
	return f&(flagPresumeKNE|flagPreviousPresumeKNE) != 0
}

// HasLocked returns whether the associated key has acquired pessimistic lock.
func (f KeyFlags) HasLocked() bool {
	return f&flagKeyLocked != 0
}

// HasNeedLocked return whether the key needed to be locked
func (f KeyFlags) HasNeedLocked() bool {
	return f&flagNeedLocked != 0
}

// HasLockedValueExists returns whether the value exists when key locked.
func (f KeyFlags) HasLockedValueExists() bool {
	return f&flagKeyLockedValExist != 0
}

// HasNeedCheckExists returns whether the key need to check existence when it has been locked.
func (f KeyFlags) HasNeedCheckExists() bool {
	return f&flagNeedCheckExists != 0
}

// HasPrewriteOnly returns whether the key should be used in 2pc commit phase.
func (f KeyFlags) HasPrewriteOnly() bool {
	return f&flagPrewriteOnly != 0
}

// HasIgnoredIn2PC returns whether the key will be ignored in 2pc.
func (f KeyFlags) HasIgnoredIn2PC() bool {
	return f&flagIgnoredIn2PC != 0
}

// HasReadable returns whether the in-transaction operations is able to read the key.
func (f KeyFlags) HasReadable() bool {
	return f&flagReadable != 0
}

// HasNeedConstraintCheckInPrewrite returns whether the key needs to check conflict in prewrite.
func (f KeyFlags) HasNeedConstraintCheckInPrewrite() bool {
	return f&flagNeedConstraintCheckInPrewrite != 0
}

// AndPersistent returns the value of current flags&persistentFlags
func (f KeyFlags) AndPersistent() KeyFlags {
	return f & persistentFlags
}

// HasNewlyInserted returns whether the in-transaction key is generated by an "insert" operation.
func (f KeyFlags) HasNewlyInserted() bool {
	return f&flagNewlyInserted != 0
}

// ApplyFlagsOps applys flagspos to origin.
func ApplyFlagsOps(origin KeyFlags, ops ...FlagsOp) KeyFlags {
	for _, op := range ops {
		switch op {
		case SetPresumeKeyNotExists:
			origin |= flagPresumeKNE | flagNeedCheckExists
		case DelPresumeKeyNotExists:
			origin &= ^(flagPresumeKNE | flagNeedCheckExists)
		case SetKeyLocked:
			origin |= flagKeyLocked
		case DelKeyLocked:
			origin &= ^flagKeyLocked
		case SetNeedLocked:
			origin |= flagNeedLocked
		case DelNeedLocked:
			origin &= ^flagNeedLocked
		case SetKeyLockedValueExists:
			origin |= flagKeyLockedValExist
			origin &= ^flagNeedConstraintCheckInPrewrite
		case DelNeedCheckExists:
			origin &= ^flagNeedCheckExists
		case SetKeyLockedValueNotExists:
			origin &= ^flagKeyLockedValExist
			origin &= ^flagNeedConstraintCheckInPrewrite
		case SetPrewriteOnly:
			origin |= flagPrewriteOnly
		case SetIgnoredIn2PC:
			origin |= flagIgnoredIn2PC
		case SetReadable:
			origin |= flagReadable
		case SetNewlyInserted:
			origin |= flagNewlyInserted
		case SetAssertExist:
			origin &= ^flagAssertNotExist
			origin |= flagAssertExist
		case SetAssertNotExist:
			origin &= ^flagAssertExist
			origin |= flagAssertNotExist
		case SetAssertUnknown:
			origin |= flagAssertNotExist
			origin |= flagAssertExist
		case SetAssertNone:
			origin &= ^flagAssertExist
			origin &= ^flagAssertNotExist
		case SetNeedConstraintCheckInPrewrite:
			origin |= flagNeedConstraintCheckInPrewrite
		case DelNeedConstraintCheckInPrewrite:
			origin &= ^flagNeedConstraintCheckInPrewrite
		case SetPreviousPresumeKNE:
			origin |= flagPreviousPresumeKNE
		}
	}
	return origin
}

// FlagsOp describes KeyFlags modify operation.
type FlagsOp uint32

const (
	// SetPresumeKeyNotExists marks the existence of the associated key is checked lazily.
	// Implies KeyFlags.HasNeedCheckExists() == true.
	SetPresumeKeyNotExists FlagsOp = 1 << iota
	// DelPresumeKeyNotExists reverts SetPresumeKeyNotExists.
	DelPresumeKeyNotExists
	// SetKeyLocked marks the associated key has acquired lock.
	SetKeyLocked
	// DelKeyLocked reverts SetKeyLocked.
	DelKeyLocked
	// SetNeedLocked marks the associated key need to be acquired lock.
	SetNeedLocked
	// DelNeedLocked reverts SetKeyNeedLocked.
	DelNeedLocked
	// SetKeyLockedValueExists marks the value exists when key has been locked in Transaction.LockKeys.
	SetKeyLockedValueExists
	// SetKeyLockedValueNotExists marks the value doesn't exists when key has been locked in Transaction.LockKeys.
	SetKeyLockedValueNotExists
	// DelNeedCheckExists marks the key no need to be checked in Transaction.LockKeys.
	DelNeedCheckExists
	// SetPrewriteOnly marks the key shouldn't be used in 2pc commit phase.
	SetPrewriteOnly
	// SetIgnoredIn2PC marks the key will be ignored in 2pc.
	SetIgnoredIn2PC
	// SetReadable marks the key is readable by in-transaction read.
	SetReadable
	// SetNewlyInserted marks the key is newly inserted with value length greater than zero.
	SetNewlyInserted
	// SetAssertExist marks the key must exist.
	SetAssertExist
	// SetAssertNotExist marks the key must not exist.
	SetAssertNotExist
	// SetAssertUnknown mark the key maybe exists or not exists.
	SetAssertUnknown
	// SetAssertNone cleans up the key's assert.
	SetAssertNone
	// SetNeedConstraintCheckInPrewrite marks the key needs to check conflict in prewrite.
	SetNeedConstraintCheckInPrewrite
	// DelNeedConstraintCheckInPrewrite reverts SetNeedConstraintCheckInPrewrite. This is required when we decide to
	// make up the pessimistic lock.
	DelNeedConstraintCheckInPrewrite
	// SetPreviousPresumeKNE sets flagPreviousPresumeKNE.
	SetPreviousPresumeKNE
)
