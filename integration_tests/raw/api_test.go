package raw_tikv_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
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
	version      atomic.Uint64
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

func isV2Enabled(t *testing.T, ctx context.Context, addrs []string) bool {
	pdCli, err := pd.NewClientWithContext(ctx, addrs, pd.SecurityOption{})
	defer pdCli.Close()
	require.Nil(t, err)
	stores, err := pdCli.GetAllStores(ctx)
	require.Nilf(t, err, "fail to get store, %v", err)

	for _, store := range stores {
		resp, err := getConfig(fmt.Sprintf("http://%s/config", store.StatusAddress))
		require.Nilf(t, err, "fail to get config of TiKV store %s: %v", store.StatusAddress, err)
		v := gjson.Get(resp, "storage.api-version")
		if v.Type == gjson.Null || v.Uint() != 2 {
			return false
		}
	}
	return true
}

func newRawKVClient(t *testing.T, ctx context.Context, addrs []string) *rawkv.Client {
	var clientBuilder func(ctx context.Context, pdAddrs []string, security config.Security, opts ...pd.ClientOption) (*rawkv.Client, error)
	if isV2Enabled(t, ctx, addrs) {
		clientBuilder = rawkv.NewClientV2
	} else {
		clientBuilder = rawkv.NewClient
	}
	cli, err := clientBuilder(ctx, addrs, config.DefaultConfig().Security)
	require.Nil(t, err)
	return cli
}

func (s *apiTestSuite) SetupTest() {
	ctx := context.Background()
	addrs := strings.Split(*pdAddrs, ",")

	if !isV2Enabled(s.T(), ctx, addrs) {
		s.T().Skip("skipping because the TiKV cluster is not enable ApiV2")
	}

	client, err := rawkv.NewClientV2(ctx, addrs, config.DefaultConfig().Security)
	s.Nil(err)
	s.client = client

	clientForCas, err := rawkv.NewClientV2(ctx, addrs, config.DefaultConfig().Security)
	s.Nil(err)

	clientForCas.SetAtomicForCAS(true)
	s.clientForCas = clientForCas
}

func withPrefix(prefix, key string) string {
	return fmt.Sprintf("%s:%s", prefix, key)
}

func withPrefixes(prefix string, keys []string) []string {
	var result []string
	for i := range keys {
		result = append(result, withPrefix(prefix, keys[i]))
	}
	return keys
}

func (s *apiTestSuite) cleanKeyPrefix(prefix string) {
	end := append([]byte(prefix), 128)
	err := s.client.DeleteRange(context.Background(), []byte(prefix), end)
	s.Nil(err)
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

func (s *apiTestSuite) mustScan(prefix string, start string, end string, limit int) ([]string, []string) {
	ks, vs, err := s.client.Scan(context.Background(), []byte(withPrefix(prefix, start)), []byte(withPrefix(prefix, end)), limit)
	s.Nil(err)
	return toStrings(ks), toStrings(vs)
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

func (s *apiTestSuite) mustBatchGet(prefix string, keys []string) []string {
	keys = withPrefixes(prefix, keys)
	vs, err := s.client.BatchGet(context.Background(), toBytes(keys))
	s.Nil(err)
	return toStrings(vs)
}

func (s *apiTestSuite) mustBatchDelete(prefix string, keys []string) {
	keys = withPrefixes(prefix, keys)
	err := s.client.BatchDelete(context.Background(), toBytes(keys))
	s.Nil(err)
}

func (s *apiTestSuite) mustCAS(prefix, key, old, new string) (bool, string) {
	var oldValue []byte
	if old != "" {
		oldValue = []byte(old)
	}
	oldValue, success, err := s.clientForCas.CompareAndSwap(context.Background(), []byte(withPrefix(prefix, key)), oldValue, []byte(new))
	s.Nil(err)
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
	v := s.mustGet(prefix, key)
	s.Empty(v)
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

	for i := 0; i < 10; i++ {
		s.mustPut(prefix, fmt.Sprintf("key:%v", i), fmt.Sprintf("value:%v", i))
	}
	keys, values := s.mustScan(prefix, "key:", "", 5)
	for i := range keys {
		s.Equal(fmt.Sprintf("key:%v", i), keys[i])
		s.Equal(fmt.Sprintf("value:%v", i), values[i])
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

func (s *apiTestSuite) TearDownTest() {
	if s.client != nil {
		_ = s.client.Close()
	}
	if s.clientForCas != nil {
		_ = s.clientForCas.Close()
	}
}
