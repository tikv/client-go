package config

import (
	"benchtool/utils"
	"fmt"
	"time"
)

const (
	WorkloadTypeTxnKV = "txnkv"
)

const (
	TxnKVCommandTypeWrite = "write"
	TxnKVCommandTypeRead  = "read"

	TxnKVCommandDefaultKey    = "txnkv_key"
	TxnKVCommandDefaultEndKey = "txnkv_key`"
	TxnKVCommandDefaultValue  = "txnkv_value"

	TxnKVModeDefault     = "2PC"
	TxnKVMode1PC         = "1PC"
	TxnKVModeAsyncCommit = "async-commit"
)

type TxnKVConfig struct {
	KeySize    int
	ValueSize  int
	ColumnSize int
	TxnSize    int

	PrepareRetryCount    int
	PrepareRetryInterval time.Duration
	ReadWriteRatio       *utils.ReadWriteRatio
	TxnMode              string
	LockTimeout          int

	Global *GlobalConfig
}

func (c *TxnKVConfig) Validate() error {
	if c.KeySize <= 0 || c.ValueSize <= 0 {
		return fmt.Errorf("key size or value size must be greater than 0")
	}
	if err := c.ReadWriteRatio.ParseRatio(); err != nil {
		return fmt.Errorf("parse read-write-ratio failed: %v", err)
	}
	return c.Global.ParsePdAddrs()
}
