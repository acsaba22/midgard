package event

import (
	"testing"

	"github.com/tendermint/tendermint/libs/kv"
)

var GoldenAssets = []struct{ Asset, Chain, Ticker, ID string }{
	{"BTC.BTC", "BTC", "BTC", ""},
	{"ETH.ETH", "ETH", "ETH", ""},
	{"ETH.USDT-0xdac17f958d2ee523a2206206994597c13d831ec7", "ETH", "USDT", "0xdac17f958d2ee523a2206206994597c13d831ec7"},
	{"BNB.BNB", "BNB", "BNB", ""},
	{"BNB.RUNE-B1A", "BNB", "RUNE", "B1A"},
	{"THOR.RUNE", "THOR", "RUNE", ""},
	{"", "", "", ""},
	{".", "", "", ""},
	{"-", "", "", ""},
	{".-", "", "", ""},
	{"1.", "1", "", ""},
	{"2.-", "2", "", ""},
	{"A", "", "A", ""},
	{".B", "", "B", ""},
	{"C-", "", "C", ""},
	{".D-", "", "D", ""},
	{"-a", "", "", "a"},
	{".-b", "", "", "b"},
}

func TestParseAsset(t *testing.T) {
	for _, gold := range GoldenAssets {
		chain, ticker, ID := ParseAsset([]byte(gold.Asset))
		if string(chain) != gold.Chain || string(ticker) != gold.Ticker || string(ID) != gold.ID {
			t.Errorf("%q got [%q %q %q], want [%q %q %q]", gold.Asset, chain, ticker, ID, gold.Chain, gold.Ticker, gold.ID)
		}
	}
}

func TestOutbound(t *testing.T) {
	var event Outbound
	err := event.LoadTendermint(toAttrs(map[string]string{
		"chain":    "BTC",
		"coin":     "23282731 BTC.BTC",
		"from":     "bcrt1q53nknrl2d2nmvguhhvacd4dfsm4jlv8c46ed3y",
		"id":       "0000000000000000000000000000000000000000000000000000000000000000",
		"in_tx_id": "04FFE1117647700F48F678DF53372D503F31C745D6DDE3599D9CB6381188620E",
		"memo":     "OUTBOUND:04FFE1117647700F48F678DF53372D503F31C745D6DDE3599D9CB6381188620E",
		"to":       "bcrt1q0s4mg25tu6termrk8egltfyme4q7sg3h8kkydt",
	}))
	if err != nil {
		t.Fatal(err)
	}

	if event.Tx != nil {
		t.Errorf("got tx %#x, want nil for zeros only", event.Tx)
	}
}

func TestSwap(t *testing.T) {
	var event Swap
	err := event.LoadTendermint(toAttrs(map[string]string{
		"chain":                 "BNB",
		"coin":                  "500000 BNB.BNB",
		"from":                  "tbnb157dxmw9jz5emuf0apj4d6p3ee42ck0uwksxfff",
		"id":                    "0F1DE3EC877075636F21AF1E7399AA9B9C710A4989E61A9F5942A78B9FA96621",
		"liquidity_fee":         "259372",
		"liquidity_fee_in_rune": "259372",
		"memo":                  "SWAP:BTC.BTC:bcrt1qqqnde7kqe5sf96j6zf8jpzwr44dh4gkd3ehaqh",
		"pool":                  "BNB.BNB",
		"price_target":          "1",
		"to":                    "tbnb153nknrl2d2nmvguhhvacd4dfsm4jlv8c87nscv",
		"trade_slip":            "33",
	}))
	if err != nil {
		t.Fatal(err)
	}

	if event.FromE8 != 500000 || string(event.FromAsset) != "BNB.BNB" {
		t.Errorf(`got from %d %q with "coin": "500000 BNB.BNB"`, event.FromE8, event.FromAsset)
	}
	if got := event.DoubleAsset(); string(got) != "BTC.BTC" {
		t.Errorf("got asset %q, want BitCoin", got)
	}
}

func TestRefund(t *testing.T) {
	var event Refund
	err := event.LoadTendermint(toAttrs(map[string]string{
		"chain":  "BNB",
		"code":   "105",
		"coin":   "150000000 BNB.BNB, 50000000000 BNB.RUNE-67C",
		"from":   "tbnb189az9plcke2c00vns0zfmllfpfdw67dtv25kgx",
		"id":     "98C1864036571E805BB0E0CCBAFF0F8D80F69BDEA32D5B26E0DDB95301C74D0C",
		"memo":   "",
		"reason": "memo can't be empty",
		"to":     "tbnb153nknrl2d2nmvguhhvacd4dfsm4jlv8c87nscv",
	}))
	if err != nil {
		t.Fatal(err)
	}

	if event.AssetE8 != 150000000 || string(event.Asset) != "BNB.BNB" || event.Asset2ndE8 != 50000000000 || string(event.Asset2nd) != "BNB.RUNE-67C" {
		t.Errorf(`got %d %q and %d %q with "coin": "150000000 BNB.BNB, 50000000000 BNB.RUNE-67C"`, event.AssetE8, event.Asset, event.Asset2ndE8, event.Asset2nd)
	}
}

func toAttrs(m map[string]string) []kv.Pair {
	a := make([]kv.Pair, 0, len(m))
	for k, v := range m {
		a = append(a, kv.Pair{Key: []byte(k), Value: []byte(v)})
	}
	return a
}
