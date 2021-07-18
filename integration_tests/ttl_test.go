package tikv_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

func TestTTL(t *testing.T) {
	suite.Run(t, new(ttlTestSuite))
}

type ttlTestSuite struct {
	suite.Suite
	client *rawkv.Client
}

func (s *ttlTestSuite) SetupTest() {
	t := s.T()
	if !*withTiKV {
		t.Skip("skipping TestTTL because with-tikv is not enabled")
	}
	addrs := strings.Split(*pdAddrs, ",")
	client, err := rawkv.NewClient(context.TODO(), addrs, config.DefaultConfig().Security)
	require.Nil(s.T(), err)
	s.client = client
}

func (s *ttlTestSuite) mustPutWithTTL(key, value []byte, ttl uint64) {
	err := s.client.PutWithTTL(context.TODO(), key, value, ttl)
	s.Nil(err)
}

func (s *ttlTestSuite) mustNotExist(key []byte) {
	v, err := s.client.Get(context.TODO(), key)
	s.Nil(err)
	s.Nil(v)
}

func (s *ttlTestSuite) mustGetKeyTTL(key []byte) *uint64 {
	ttl, err := s.client.GetKeyTTL(context.TODO(), key)
	s.Nil(err)
	return ttl
}

// TODO: It's an unstable test, we may mock this feature in unistore.
func (s *ttlTestSuite) TestPutWithTTL() {
	key := []byte("test-put-with-ttl")
	value := []byte("value")
	var ttl uint64 = 1
	s.mustPutWithTTL(key, value, ttl)
	time.Sleep(time.Second * time.Duration(ttl*2))
	s.mustNotExist(key)
}

func (s *ttlTestSuite) TestGetKeyTTL() {
	key := []byte("test-get-key-ttl")
	value := []byte("value")
	var ttl uint64 = 2
	s.mustPutWithTTL(key, value, ttl)
	time.Sleep(time.Second * time.Duration(ttl/2))

	rest := s.mustGetKeyTTL(key)
	s.NotNil(rest)
	s.LessOrEqual(*rest, ttl/2)

	time.Sleep(time.Second * time.Duration(ttl/2))
	s.mustNotExist(key)

	rest = s.mustGetKeyTTL(key)
	s.Nil(rest)
}

func (s *ttlTestSuite) TearDownTest() {
	s.client.Close()
}
