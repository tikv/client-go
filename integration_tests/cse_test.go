package tikv_test

import (
	"context"
	"encoding/hex"
	"log"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

// TODO(iosmanthus): refactor this suite as a unit test.
func TestCSE(t *testing.T) {
	if !*withCSE {
		t.Skip("skipping test; use -with-cse flag to run this test")
	}
	suite.Run(t, new(cseSuite))
}

type cseSuite struct {
	suite.Suite
	pdCli pd.Client
}

func (s *cseSuite) SetupTest() {
	pdCli, err := pd.NewClient(strings.Split(*pdAddrs, ","), pd.SecurityOption{})
	s.Nil(err)
	pdCli, err = tikv.NewCSEClient(pdCli, nil)
	s.Nil(err)
	s.pdCli = tikv.NewCodecPDClient(tikv.ModeTxn, pdCli)
}

func (s *cseSuite) TearDownTest() {
	s.pdCli.Close()
}

func (s *cseSuite) TestGetRegion() {
	key, err := hex.DecodeString("780000016d44444c5461626c65ff56657273696f6e00fe0000000000000073")
	s.Nil(err)
	currentRegion, err := s.pdCli.GetRegion(context.Background(), key)
	s.Nil(err)
	s.NotNil(currentRegion)
	s.LessOrEqual(currentRegion.Meta.StartKey, key)
	s.Less(key, currentRegion.Meta.EndKey)
	prevRegion, err := s.pdCli.GetPrevRegion(context.Background(), key)
	s.Nil(err)
	s.Equal(prevRegion.Meta.EndKey, currentRegion.Meta.StartKey)
}

func BenchmarkGetRegionByPD(b *testing.B) {
	pdCli, err := pd.NewClient(strings.Split(*pdAddrs, ","), pd.SecurityOption{})
	if err != nil {
		b.Fatal(err)
	}
	pdCli = tikv.NewCodecPDClient(tikv.ModeTxn, pdCli)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region, err := pdCli.GetRegion(context.Background(), []byte("780000016d44444c5461626c65ff56657273696f6e00fe0000000000000073"))
		if err != nil {
			b.Fatal(err)
		}
		if region == nil {
			log.Fatalln("region is nil")
		}
	}
	b.StopTimer()
	pdCli.Close()
}

func BenchmarkGetRegionByCSE(b *testing.B) {
	pdCli, err := pd.NewClient(strings.Split(*pdAddrs, ","), pd.SecurityOption{})
	if err != nil {
		b.Fatal(err)
	}
	pdCli, err = tikv.NewCSEClient(pdCli, nil)
	if err != nil {
		b.Fatal(err)
	}
	pdCli = tikv.NewCodecPDClient(tikv.ModeTxn, pdCli)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region, err := pdCli.GetRegion(context.Background(), []byte("780000016d44444c5461626c65ff56657273696f6e00fe0000000000000073"))
		if err != nil {
			b.Fatal(err)
		}
		if region == nil {
			log.Fatalln("region is nil")
		}
	}
	b.StopTimer()
	pdCli.Close()
}
