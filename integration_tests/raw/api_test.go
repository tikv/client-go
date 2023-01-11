package raw_tikv_test

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc64"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

func TestApi(t *testing.T) {
	if !*withTiKV {
		t.Skip("skipping TestApi because with-tikv is not enabled")
	}
	suite.Run(t, new(apiTestSuite))
}

type apiTestSuite struct {
	suite.Suite
	client       *rawkv.Client
	clientForCas *rawkv.Client
	pdClient     pd.Client
	apiVersion   kvrpcpb.APIVersion
}

func getConfig(url string) (string, error) {
	transport := &http.Transport{}
	client := http.Client{
		Transport: transport,
	}
	defer transport.CloseIdleConnections()
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (s *apiTestSuite) getApiVersion(pdCli pd.Client) kvrpcpb.APIVersion {
	stores, err := pdCli.GetAllStores(context.Background())
	s.Nilf(err, "fail to get store, %v", err)

	for _, store := range stores {
		resp, err := getConfig(fmt.Sprintf("http://%s/config", store.StatusAddress))
		s.Nilf(err, "fail to get config of TiKV store %s: %v", store.StatusAddress, err)
		v := gjson.Get(resp, "storage.api-version")
		if v.Type == gjson.Null || v.Uint() != 2 {
			return kvrpcpb.APIVersion_V1
		}
	}
	return kvrpcpb.APIVersion_V2
}

func (s *apiTestSuite) newRawKVClient(pdCli pd.Client, addrs []string) *rawkv.Client {
	cli, err := rawkv.NewClientWithOpts(context.Background(), addrs, rawkv.WithAPIVersion(s.apiVersion))
	s.Nil(err)
	return cli
}

func (s *apiTestSuite) wrapPDClient(pdCli pd.Client, addrs []string) pd.Client {
	var err error
	if s.getApiVersion(pdCli) == kvrpcpb.APIVersion_V2 {
		pdCli, err = tikv.NewCodecPDClientWithKeyspace(tikv.ModeRaw, pdCli, tikv.DefaultKeyspaceName)
	}
	s.Nil(err)
	return pdCli
}

func (s *apiTestSuite) SetupTest() {
	addrs := strings.Split(*pdAddrs, ",")

	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	s.Nil(err)
	s.apiVersion = s.getApiVersion(pdClient)

	s.pdClient = s.wrapPDClient(pdClient, addrs)

	client := s.newRawKVClient(pdClient, addrs)
	s.client = client

	clientForCas := s.newRawKVClient(pdClient, addrs)
	clientForCas.SetAtomicForCAS(true)
	s.clientForCas = clientForCas
}

func withPrefix(prefix, key string) string {
	return prefix + key
}

func withPrefixes(prefix string, keys []string) []string {
	var result []string
	for i := range keys {
		result = append(result, withPrefix(prefix, keys[i]))
	}
	return result
}

func (s *apiTestSuite) cleanKeyPrefix(prefix string) {
	end := append([]byte(prefix), 127)
	err := s.client.DeleteRange(context.Background(), []byte(prefix), end)
	s.Nil(err)

	ks, _ := s.mustScan(prefix, "", "", 10240)
	s.Empty(ks)
}

func (s *apiTestSuite) mustPut(prefix string, key string, value string) {
	err := s.client.Put(context.Background(), []byte(withPrefix(prefix, key)), []byte(value))
	s.Nil(err)
}

func (s *apiTestSuite) mustGet(prefix string, key string) string {
	v, err := s.client.Get(context.Background(), []byte(withPrefix(prefix, key)))
	s.Nil(err)
	return string(v)
}

func (s *apiTestSuite) mustDelete(prefix string, key string) {
	err := s.client.Delete(context.Background(), []byte(withPrefix(prefix, key)))
	s.Nil(err)
}

func (s *apiTestSuite) mustScanBytes(prefix string, start string, end string, limit int) ([][]byte, [][]byte) {
	end = withPrefix(prefix, end)
	end += string([]byte{127})
	ks, vs, err := s.client.Scan(context.Background(), []byte(withPrefix(prefix, start)), []byte(end), limit)
	s.Nil(err)

	for i := range ks {
		ks[i] = bytes.TrimPrefix(ks[i], []byte(prefix))
	}
	return ks, vs
}

func (s *apiTestSuite) mustScan(prefix string, start string, end string, limit int) ([]string, []string) {
	ks, vs := s.mustScanBytes(prefix, start, end, limit)
	return toStrings(ks), toStrings(vs)
}

func (s *apiTestSuite) mustReverseScanBytes(prefix string, start string, end string, limit int) ([][]byte, [][]byte) {
	ks, vs, err := s.client.ReverseScan(context.Background(), []byte(withPrefix(prefix, start)), []byte(withPrefix(prefix, end)), limit)
	s.Nil(err)

	for i := range ks {
		ks[i] = bytes.TrimPrefix(ks[i], []byte(prefix))
	}
	return ks, vs
}

func (s *apiTestSuite) mustReverseScan(prefix string, start string, end string, limit int) ([]string, []string) {
	ks, vs := s.mustReverseScanBytes(prefix, start, end, limit)
	return toStrings(ks), toStrings(vs)
}

func (s *apiTestSuite) mustChecksum(prefix string, start string, end string) rawkv.RawChecksum {
	end = withPrefix(prefix, end)
	end += string([]byte{127})
	checksum, err := s.client.Checksum(context.Background(), []byte(withPrefix(prefix, start)), []byte(end))
	s.Nil(err)
	return checksum
}

func (s *apiTestSuite) mustDeleteRange(prefix string, start, end string) {
	if end == "" {
		end = prefix
	}
	end += string([]byte{127})
	err := s.client.DeleteRange(context.Background(), []byte(withPrefix(prefix, start)), []byte(withPrefix(prefix, end)))
	s.Nil(err)
}

func toStrings(data [][]byte) []string {
	var ss []string
	for _, b := range data {
		ss = append(ss, string(b))
	}
	return ss
}

func toBytes(data []string) [][]byte {
	var bs [][]byte
	for _, s := range data {
		bs = append(bs, []byte(s))
	}
	return bs
}

func (s *apiTestSuite) mustBatchPut(prefix string, keys []string, values []string) {
	s.Equal(len(keys), len(values))
	keys = withPrefixes(prefix, keys)
	err := s.client.BatchPut(context.Background(), toBytes(keys), toBytes(values))
	s.Nil(err)
}

func (s *apiTestSuite) mustBatchGetBytes(prefix string, keys []string) [][]byte {
	keys = withPrefixes(prefix, keys)
	vs, err := s.client.BatchGet(context.Background(), toBytes(keys))
	s.Nil(err)

	return vs
}

func (s *apiTestSuite) mustBatchGet(prefix string, keys []string) []string {
	return toStrings(s.mustBatchGetBytes(prefix, keys))
}

func (s *apiTestSuite) mustBatchDelete(prefix string, keys []string) {
	keys = withPrefixes(prefix, keys)
	err := s.client.BatchDelete(context.Background(), toBytes(keys))
	s.Nil(err)
}

func (s *apiTestSuite) mustCASBytes(prefix, key string, old, new []byte) (bool, []byte) {
	oldValue, success, err := s.clientForCas.CompareAndSwap(context.Background(), []byte(withPrefix(prefix, key)), old, new)
	s.Nil(err)
	return success, oldValue
}

// `old == ""` means "not exist"
func (s *apiTestSuite) mustCAS(prefix, key, old, new string) (bool, string) {
	var oldValue []byte
	if old != "" {
		oldValue = []byte(old)
	}
	success, oldValue := s.mustCASBytes(prefix, key, oldValue, []byte(new))
	return success, string(oldValue)
}

func (s *apiTestSuite) mustPutWithTTL(prefix, key, value string, ttl uint64) {
	err := s.client.PutWithTTL(context.Background(), []byte(withPrefix(prefix, key)), []byte(value), ttl)
	s.Nil(err)
}

func (s *apiTestSuite) mustGetKeyTTL(prefix, key string) *uint64 {
	ttl, err := s.client.GetKeyTTL(context.Background(), []byte(withPrefix(prefix, key)))
	s.Nil(err)
	return ttl
}

func (s *apiTestSuite) mustNotExist(prefix string, key string) {
	v, err := s.client.Get(context.Background(), []byte(withPrefix(prefix, key)))
	s.Nil(err)
	s.Nil(v)
}

func (s *apiTestSuite) mustExist(prefix string, key string) {
	v, err := s.client.Get(context.Background(), []byte(withPrefix(prefix, key)))
	s.Nil(err)
	s.NotNil(v)
}

func (s *apiTestSuite) mustSplitRegion(prefix string, splitKeys []string) {
	var keys [][]byte
	for i := range splitKeys {
		keys = append(keys, []byte(withPrefix(prefix, splitKeys[i])))
	}
	_, err := s.pdClient.SplitRegions(context.Background(), keys)
	if err != nil {
		s.T().Fatalf("failed to split regions: %v", err)
	}
	s.Nil(err)
}

func (s *apiTestSuite) TestSimple() {
	prefix := "test_simple"
	s.cleanKeyPrefix(prefix)

	s.mustNotExist(prefix, "key")

	s.mustPut(prefix, "key", "value")

	v := s.mustGet(prefix, "key")
	s.Equal("value", v)

	s.mustDelete(prefix, "key")
	s.mustNotExist(prefix, "key")
}

func (s *apiTestSuite) TestScan() {
	prefix := "test_scan"
	s.cleanKeyPrefix(prefix)

	var (
		keys   []string
		values []string
	)
	for i := 0; i < 20480; i++ {
		keys = append(keys, fmt.Sprintf("key@%v", i))
		values = append(values, fmt.Sprintf("value@%v", i))
	}
	s.mustBatchPut(prefix, keys, values)

	var splitKeys []string
	for i := 0; i < 20480; i += 1024 {
		splitKeys = append(splitKeys, fmt.Sprintf("key@%v", i))
	}

	s.mustSplitRegion(prefix, splitKeys)

	keys, values = s.mustScan(prefix, keys[0], "", 10240)
	s.Equal(10240, len(keys))
	s.Equal(10240, len(values))
	s.Equal(len(keys), len(values))
	for i := range keys {
		s.True(strings.HasPrefix(keys[i], "key@"))
		s.True(strings.HasPrefix(values[i], "value@"))
	}
}

func (s *apiTestSuite) TestReverseScan() {
	prefix := "test_reverse_scan"
	s.cleanKeyPrefix(prefix)

	for i := 0; i < 10; i++ {
		s.mustPut(prefix, fmt.Sprintf("key:%v", i), fmt.Sprintf("value:%v", i))
	}
	keys, values := s.mustReverseScan(prefix, "key:", "", 5)
	for i := range keys {
		s.Equal(fmt.Sprintf("key:%v", i), keys[len(keys)-1-i])
		s.Equal(fmt.Sprintf("value:%v", i), values[len(keys)-1-i])
	}
}

func (s *apiTestSuite) TestBatchOp() {
	prefix := "test_batch_op"
	ks := []string{"k1", "k2"}
	s.cleanKeyPrefix(prefix)
	s.mustBatchPut(prefix, ks, []string{"v1", "v2"})
	vs := s.mustBatchGet(prefix, ks)
	s.Equal("v1", vs[0])
	s.Equal("v2", vs[1])

	s.mustBatchDelete(prefix, ks)
	s.mustNotExist(prefix, ks[0])
	s.mustNotExist(prefix, ks[1])
}

func (s *apiTestSuite) TestCAS() {
	prefix := "test_cas"
	s.cleanKeyPrefix(prefix)

	success, old := s.mustCAS(prefix, "key", "", "hello world")
	s.True(success)
	s.Equal("", old)

	v := s.mustGet(prefix, "key")
	s.Equal("hello world", v)

	success, old = s.mustCAS(prefix, "key", "hello", "world")
	s.False(success)
	s.Equal("hello world", old)

	v = s.mustGet(prefix, "key")
	s.Equal("hello world", v)

	success, old = s.mustCAS(prefix, "key", "hello world", "world")
	s.True(success)
	s.Equal("hello world", old)

	v = s.mustGet(prefix, "key")
	s.Equal("world", v)
}

func (s *apiTestSuite) TestTTL() {
	prefix := "test_ttl"

	var ttl uint64 = 2
	s.mustPutWithTTL(prefix, "key", "value", ttl)
	time.Sleep(time.Second * time.Duration(ttl/2))

	rest := s.mustGetKeyTTL(prefix, "key")
	s.NotNil(rest)
	s.LessOrEqual(*rest, ttl/2)

	time.Sleep(time.Second * time.Duration(ttl/2))
	s.mustNotExist(prefix, "key")

	rest = s.mustGetKeyTTL(prefix, "key")
	s.Nil(rest)
}

func (s *apiTestSuite) TestDeleteRange() {
	prefix := "test_delete_range"

	s.cleanKeyPrefix(prefix)

	var (
		keys   []string
		values []string
	)
	for i := 0; i < 20480; i++ {
		keys = append(keys, fmt.Sprintf("key@%v", i))
		values = append(values, fmt.Sprintf("value@%v", i))
	}
	s.mustBatchPut(prefix, keys, values)

	s.mustSplitRegion(prefix, []string{"key@4096"})

	s.mustDeleteRange(prefix, "", "")
	s.mustNotExist(prefix, "key@0")
	s.mustNotExist(prefix, "key@1")
	s.mustNotExist(prefix, "key@2")
}

func (s *apiTestSuite) TestRawChecksum() {
	prefix := "test_checksum"

	s.cleanKeyPrefix(prefix)

	var (
		keys   []string
		values []string
	)
	expect := rawkv.RawChecksum{}
	digest := crc64.New(crc64.MakeTable(crc64.ECMA))
	for i := 0; i < 20480; i++ {
		key := fmt.Sprintf("key@%v", i)
		value := fmt.Sprintf("value@%v", i)
		keys = append(keys, key)
		values = append(values, value)

		digest.Reset()
		digest.Write([]byte(key))
		digest.Write([]byte(value))
		expect.Crc64Xor ^= digest.Sum64()
		expect.TotalKvs++
		expect.TotalBytes += (uint64)(len(prefix) + len(key) + len(value))
		if s.apiVersion == kvrpcpb.APIVersion_V2 {
			expect.TotalBytes += 4 // 4 bytes key prefix of API v2
		}
	}
	s.mustBatchPut(prefix, keys, values)
	checksum := s.mustChecksum(prefix, "", "")
	s.Equal(expect, checksum)
}

func (s *apiTestSuite) TestEmptyValue() {
	prefix := "test_empty_value"
	s.cleanKeyPrefix(prefix)

	s.mustNotExist(prefix, "key")

	verifyEmptyValue := func() {
		s.mustExist(prefix, "key")
		// get
		v := s.mustGet(prefix, "key")
		s.Empty(v)
		// batch_get
		vs := s.mustBatchGetBytes(prefix, []string{"key", "key1"})
		s.Equal([][]byte{{}, nil}, vs)
		// scan
		keys, values := s.mustScanBytes(prefix, "key", "keyz", 10)
		s.Equal([][]byte{[]byte("key")}, keys)
		s.Equal([][]byte{{}}, values)
		// reverse scan
		keys, values = s.mustReverseScanBytes(prefix, "keyz", "key", 10)
		s.Equal([][]byte{[]byte("key")}, keys)
		s.Equal([][]byte{{}}, values)
	}

	verifyNotExist := func() {
		s.mustNotExist(prefix, "key")
		// batch_get
		vs := s.mustBatchGetBytes(prefix, []string{"key", "key1"})
		s.Equal([][]byte{nil, nil}, vs)
		// scan
		keys, values := s.mustScanBytes(prefix, "key", "keyz", 10)
		s.Nil(keys)
		s.Nil(values)
		// reverse scan
		keys, values = s.mustReverseScanBytes(prefix, "keyz", "key", 10)
		s.Nil(keys)
		s.Nil(values)
	}

	// put
	s.mustPut(prefix, "key", "")
	verifyEmptyValue()

	// delete
	s.mustDelete(prefix, "key")
	verifyNotExist()

	// batch_put
	s.mustBatchPut(prefix, []string{"key"}, []string{""})
	verifyEmptyValue()

	// compare_and_swap, nil -> ""
	s.mustDelete(prefix, "key")
	ok, oldVal := s.mustCASBytes(prefix, "key", nil, []byte(""))
	s.True(ok)
	s.Nil(oldVal)
	verifyEmptyValue()

	// compare_and_swap, "" -> "val"
	ok, oldVal = s.mustCASBytes(prefix, "key", []byte(""), []byte("val"))
	s.True(ok)
	s.Equal([]byte{}, oldVal)
}

func (s *apiTestSuite) TearDownTest() {
	if s.client != nil {
		_ = s.client.Close()
	}
	if s.clientForCas != nil {
		_ = s.clientForCas.Close()
	}
	if s.pdClient != nil {
		s.pdClient.Close()
	}
}
