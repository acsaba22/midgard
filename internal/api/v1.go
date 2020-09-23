package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"gitlab.com/thorchain/midgard/chain/notinchain"
	"gitlab.com/thorchain/midgard/internal/timeseries"
	"gitlab.com/thorchain/midgard/internal/timeseries/stat"
)

// Version 1 compatibility is a minimal effort attempt to provide smooth migration.

// InSync returns whether the entire blockchain is processed.
var InSync func() bool

func serveV1Assets(w http.ResponseWriter, r *http.Request) {
	assets, err := assetParam(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	assetE8DepthPerPool, runeE8DepthPerPool, timestamp := timeseries.AssetAndRuneDepths()
	window := stat.Window{Since: time.Unix(0, 0), Until: timestamp}

	array := make([]interface{}, len(assets))
	for i, asset := range assets {
		stakes, err := stat.PoolStakesLookup(r.Context(), asset, window)
		if err != nil {
			respError(w, r, err)
			return
		}
		m := map[string]interface{}{
			"asset":       asset,
			"dateCreated": stakes.First.Unix(),
		}
		if assetDepth := assetE8DepthPerPool[asset]; assetDepth != 0 {
			m["priceRune"] = strconv.FormatFloat(float64(runeE8DepthPerPool[asset])/float64(assetDepth), 'f', -1, 64)
		}
		array[i] = m
	}

	respJSON(w, array)
}

func serveV1Health(w http.ResponseWriter, r *http.Request) {
	height, _, _ := timeseries.LastBlock()
	respJSON(w, map[string]interface{}{
		"database":      true,
		"scannerHeight": height + 1,
		"catching_up":   !InSync(),
	})
}

func serveV1Network(w http.ResponseWriter, r *http.Request) {
	_, runeE8DepthPerPool, _ := timeseries.AssetAndRuneDepths()

	var runeDepth int64
	for _, depth := range runeE8DepthPerPool {
		runeDepth += depth
	}

	activeNodes := make(map[string]struct{})
	standbyNodes := make(map[string]struct{})
	var activeBonds, standbyBonds sortedBonds
	nodes, err := notinchain.NodeAccountsLookup()
	if err != nil {
		respError(w, r, err)
		return
	}
	for _, node := range nodes {
		switch node.Status {
		case "active":
			activeNodes[node.NodeAddr] = struct{}{}
			activeBonds = append(activeBonds, node.Bond)
		case "standby":
			standbyNodes[node.NodeAddr] = struct{}{}
			standbyBonds = append(standbyBonds, node.Bond)
		}
	}
	sort.Sort(activeBonds)
	sort.Sort(standbyBonds)

	respJSON(w, map[string]interface{}{
		"activeBonds":      intArrayStrs([]int64(activeBonds)),
		"activeNodeCount":  strconv.Itoa(len(activeNodes)),
		"bondMetrics":      activeAndStandbyBondMetrics(activeBonds, standbyBonds),
		"totalStaked":      intStr(runeDepth),
		"standbyBonds":     intArrayStrs([]int64(standbyBonds)),
		"standbyNodeCount": strconv.Itoa(len(standbyNodes)),
	})

	/* TODO(pascaldekloe): Apply bond logic from usecase.go in main branch.
	   {
	     "blockRewards":{
	       "blockReward":"64760607",
	       "bondReward":"35799633",
	       "stakeReward":"28960974"
	     },
	     "bondingROI":"0.4633353645582847",
	     "nextChurnHeight":"345405",
	     "poolActivationCountdown":15889,
	     "poolShareFactor":"0.44720047711597544",
	     "stakingROI":"0.9812757721632637",
	     "totalReserve":"408729453693315",
	   }
	*/
}

type sortedBonds []int64

func (b sortedBonds) Len() int           { return len(b) }
func (b sortedBonds) Less(i, j int) bool { return b[i] < b[j] }
func (b sortedBonds) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

func activeAndStandbyBondMetrics(active, standby sortedBonds) map[string]interface{} {
	m := make(map[string]interface{})
	if len(active) != 0 {
		var total int64
		for _, n := range active {
			total += n
		}
		m["totalActiveBond"] = total
		m["minimumActiveBond"] = active[0]
		m["maximumActiveBond"] = active[len(active)-1]
		m["averageActiveBond"] = ratFloatStr(big.NewRat(total, int64(len(active))))
		m["medianActiveBond"] = active[len(active)/2]
	}
	if len(standby) != 0 {
		var total int64
		for _, n := range standby {
			total += n
		}
		m["totalStandbyBond"] = total
		m["minimumStandbyBond"] = standby[0]
		m["maximumStandbyBond"] = standby[len(standby)-1]
		m["averageStandbyBond"] = ratFloatStr(big.NewRat(total, int64(len(standby))))
		m["medianStandbyBond"] = standby[len(standby)/2]
	}
	return m
}

func serveV1Nodes(w http.ResponseWriter, r *http.Request) {
	secpAddrs, edAddrs, err := timeseries.NodesSecpAndEd(r.Context(), time.Now())
	if err != nil {
		respError(w, r, err)
		return
	}

	m := make(map[string]struct {
		Secp string `json:"secp256k1"`
		Ed   string `json:"ed25519"`
	}, len(secpAddrs))
	for key, addr := range secpAddrs {
		e := m[addr]
		e.Secp = key
		m[addr] = e
	}
	for key, addr := range edAddrs {
		e := m[addr]
		e.Ed = key
		m[addr] = e
	}

	array := make([]interface{}, 0, len(m))
	for _, e := range m {
		array = append(array, e)
	}
	respJSON(w, array)
}

func serveV1Pools(w http.ResponseWriter, r *http.Request) {
	pools, err := timeseries.Pools(r.Context(), time.Time{})
	if err != nil {
		respError(w, r, err)
		return
	}
	respJSON(w, pools)
}

func serveV1PoolsAsset(w http.ResponseWriter, r *http.Request) {
	asset := path.Base(r.URL.Path)
	if asset == "detail" {
		serveV1PoolsDetail(w, r)
		return
	}

	assetE8DepthPerPool, runeE8DepthPerPool, timestamp := timeseries.AssetAndRuneDepths()
	window := stat.Window{Since: time.Unix(0, 0), Until: timestamp}

	// TODO(acsaba): this is not final. Either change the function signature,
	// or provide a sane height here.
	m, err := poolsAsset(r.Context(), asset, -1, assetE8DepthPerPool, runeE8DepthPerPool, window)
	if err != nil {
		respError(w, r, err)
		return
	}

	respJSON(w, m)
}

// compatibility layer
func serveV1PoolsDetail(w http.ResponseWriter, r *http.Request) {
	// TODO(acsaba): remove log
	log.Print("Detail request: ", r.URL.RequestURI())

	height, err := heightParam(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// TODO(acsaba): remove log
	log.Print("returning depths for height: ", height)

	assetE8DepthPerPool, runeE8DepthPerPool, timestamp := timeseries.AssetAndRuneDepthsAtHeight(height)
	window := stat.Window{Since: time.Unix(0, 0), Until: timestamp}

	assets, err := assetParam(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	array := make([]interface{}, len(assets))
	for i, asset := range assets {
		m, err := poolsAsset(r.Context(), asset, height, assetE8DepthPerPool, runeE8DepthPerPool, window)
		if err != nil {
			respError(w, r, err)
			return
		}
		array[i] = m
	}

	respJSON(w, array)
}

func poolsAsset(ctx context.Context, asset string, height int64, assetE8DepthPerPool, runeE8DepthPerPool map[string]int64, window stat.Window) (map[string]interface{}, error) {
	status, err := timeseries.PoolStatus(ctx, asset, window.Until)
	if err != nil {
		return nil, err
	}
	stakeAddrs, err := timeseries.StakeAddrs(ctx, window.Until)
	if err != nil {
		return nil, err
	}
	stakes, err := stat.PoolStakesLookup(ctx, asset, window)
	if err != nil {
		return nil, err
	}
	unstakes, err := stat.PoolUnstakesLookup(ctx, asset, window)
	if err != nil {
		return nil, err
	}
	swapsFromRune, err := stat.PoolSwapsFromRuneLookup(ctx, asset, window)
	if err != nil {
		return nil, err
	}
	swapsToRune, err := stat.PoolSwapsToRuneLookup(ctx, asset, window)
	if err != nil {
		return nil, err
	}

	assetDepth := assetE8DepthPerPool[asset]
	runeDepth := runeE8DepthPerPool[asset]

	m := map[string]interface{}{
		"height":           intStr(height),
		"asset":            asset,
		"assetDepth":       intStr(assetDepth),
		"assetStakedTotal": intStr(stakes.AssetE8Total),
		"buyAssetCount":    intStr(swapsFromRune.TxCount),
		"buyFeesTotal":     intStr(swapsFromRune.LiqFeeE8Total),
		"poolDepth":        intStr(2 * runeDepth),
		"poolFeesTotal":    intStr(swapsFromRune.LiqFeeE8Total + swapsToRune.LiqFeeE8Total),
		"poolUnits":        intStr(stakes.StakeUnitsTotal - unstakes.StakeUnitsTotal),
		"runeDepth":        intStr(runeDepth),
		"runeStakedTotal":  intStr(stakes.RuneE8Total - unstakes.RuneE8Total),
		"sellAssetCount":   intStr(swapsToRune.TxCount),
		"sellFeesTotal":    intStr(swapsToRune.LiqFeeE8Total),
		"stakeTxCount":     intStr(stakes.TxCount),
		"stakersCount":     strconv.Itoa(len(stakeAddrs)),
		"stakingTxCount":   intStr(stakes.TxCount + unstakes.TxCount),
		"status":           status,
		"swappingTxCount":  intStr(swapsFromRune.TxCount + swapsToRune.TxCount),
		"withdrawTxCount":  intStr(unstakes.TxCount),
	}

	if assetDepth != 0 {
		priceInRune := big.NewRat(runeDepth, assetDepth)
		m["price"] = ratFloatStr(priceInRune)

		poolStakedTotal := big.NewRat(stakes.AssetE8Total-unstakes.AssetE8Total, 1)
		poolStakedTotal.Mul(poolStakedTotal, priceInRune)
		poolStakedTotal.Add(poolStakedTotal, big.NewRat(stakes.RuneE8Total-unstakes.RuneE8Total, 1))
		m["poolStakedTotal"] = ratIntStr(poolStakedTotal)

		buyVolume := big.NewRat(swapsFromRune.AssetE8Total, 1)
		buyVolume.Mul(buyVolume, priceInRune)
		m["buyVolume"] = ratIntStr(buyVolume)

		sellVolume := big.NewRat(swapsToRune.AssetE8Total, 1)
		sellVolume.Mul(sellVolume, priceInRune)
		m["sellVolume"] = ratIntStr(sellVolume)

		poolVolume := big.NewRat(swapsFromRune.AssetE8Total+swapsToRune.AssetE8Total, 1)
		poolVolume.Mul(poolVolume, priceInRune)
		m["poolVolume"] = ratIntStr(poolVolume)

		if n := swapsFromRune.TxCount; n != 0 {
			r := big.NewRat(n, 1)
			r.Quo(buyVolume, r)
			m["buyTxAverage"] = ratFloatStr(r)
		}
		if n := swapsToRune.TxCount; n != 0 {
			r := big.NewRat(n, 1)
			r.Quo(sellVolume, r)
			m["sellTxAverage"] = ratFloatStr(r)
		}
		if n := swapsFromRune.TxCount + swapsToRune.TxCount; n != 0 {
			r := big.NewRat(n, 1)
			r.Quo(poolVolume, r)
			m["poolTxAverage"] = ratFloatStr(r)
		}
	}

	var assetROI, runeROI *big.Rat
	if staked := stakes.AssetE8Total - unstakes.AssetE8Total; staked != 0 {
		assetROI = big.NewRat(assetDepth-staked, staked)
		m["assetROI"] = ratFloatStr(assetROI)
	}
	if staked := stakes.RuneE8Total - unstakes.RuneE8Total; staked != 0 {
		runeROI = big.NewRat(runeDepth-staked, staked)
		m["runeROI"] = ratFloatStr(runeROI)
	}
	if assetROI != nil || runeROI != nil {
		// why an average?
		avg := new(big.Rat)
		avg.Add(assetROI, runeROI)
		avg.Mul(avg, big.NewRat(1, 2))
		m["poolROI"] = ratFloatStr(avg)
	}

	if n := swapsFromRune.TxCount; n != 0 {
		m["buyFeeAverage"] = ratFloatStr(big.NewRat(swapsFromRune.LiqFeeE8Total, n))
	}
	if n := swapsToRune.TxCount; n != 0 {
		m["sellFeeAverage"] = ratFloatStr(big.NewRat(swapsToRune.LiqFeeE8Total, n))
	}
	if n := swapsFromRune.TxCount + swapsToRune.TxCount; n != 0 {
		m["poolFeeAverage"] = ratFloatStr(big.NewRat(swapsFromRune.LiqFeeE8Total+swapsToRune.LiqFeeE8Total, n))
	}

	if n := swapsFromRune.TxCount; n != 0 {
		r := big.NewRat(swapsFromRune.TradeSlipBPTotal, n)
		r.Quo(r, big.NewRat(10000, 1))
		m["buySlipAverage"] = ratFloatStr(r)
	}
	if n := swapsToRune.TxCount; n != 0 {
		r := big.NewRat(swapsToRune.TradeSlipBPTotal, n)
		r.Quo(r, big.NewRat(10000, 1))
		m["sellSlipAverage"] = ratFloatStr(r)
	}
	if n := swapsFromRune.TxCount + swapsToRune.TxCount; n != 0 {
		r := big.NewRat(swapsFromRune.TradeSlipBPTotal+swapsToRune.TradeSlipBPTotal, n)
		r.Quo(r, big.NewRat(10000, 1))
		m["poolSlipAverage"] = ratFloatStr(r)
	}

	/* TODO:
	PoolROI12        float64
	PoolVolume24hr   uint64
	SwappersCount    uint64
	*/

	return m, nil
}

func serveV1Stakers(w http.ResponseWriter, r *http.Request) {
	addrs, err := timeseries.StakeAddrs(r.Context(), time.Time{})
	if err != nil {
		respError(w, r, err)
		return
	}
	respJSON(w, addrs)
}

func serveV1StakersAddr(w http.ResponseWriter, r *http.Request) {
	addr := path.Base(r.URL.Path)
	pools, err := stat.AllPoolStakesAddrLookup(r.Context(), addr, stat.Window{Until: time.Now()})
	if err != nil {
		respError(w, r, err)
		return
	}

	var runeE8Total int64
	assets := make([]string, len(pools))
	for i := range pools {
		assets[i] = pools[i].Asset
		runeE8Total += pools[i].RuneE8Total
	}

	// TODO(pascaldekloe): unstakes

	respJSON(w, map[string]interface{}{
		// TODO(pascaldekloe)
		//“totalEarned” : “123123123”,
		//“totalROI” : “0.20”
		"stakeArray":  assets,
		"totalStaked": intStr(runeE8Total),
	})
}

func serveV1Stats(w http.ResponseWriter, r *http.Request) {
	_, runeE8DepthPerPool, timestamp := timeseries.AssetAndRuneDepths()
	window := stat.Window{Since: time.Unix(0, 0), Until: timestamp}

	stakes, err := stat.StakesLookup(r.Context(), window)
	if err != nil {
		respError(w, r, err)
		return
	}
	unstakes, err := stat.UnstakesLookup(r.Context(), window)
	if err != nil {
		respError(w, r, err)
		return
	}
	swapsFromRune, err := stat.SwapsFromRuneLookup(r.Context(), window)
	if err != nil {
		respError(w, r, err)
		return
	}
	swapsToRune, err := stat.SwapsToRuneLookup(r.Context(), window)
	if err != nil {
		respError(w, r, err)
		return
	}
	dailySwapsFromRune, err := stat.SwapsFromRuneLookup(r.Context(), stat.Window{Since: timestamp.Add(-24 * time.Hour), Until: timestamp})
	if err != nil {
		respError(w, r, err)
		return
	}
	dailySwapsToRune, err := stat.SwapsToRuneLookup(r.Context(), stat.Window{Since: timestamp.Add(-24 * time.Hour), Until: timestamp})
	if err != nil {
		respError(w, r, err)
		return
	}
	monthlySwapsFromRune, err := stat.SwapsFromRuneLookup(r.Context(), stat.Window{Since: timestamp.Add(-30 * 24 * time.Hour), Until: timestamp})
	if err != nil {
		respError(w, r, err)
		return
	}
	monthlySwapsToRune, err := stat.SwapsToRuneLookup(r.Context(), stat.Window{Since: timestamp.Add(-30 * 24 * time.Hour), Until: timestamp})
	if err != nil {
		respError(w, r, err)
		return
	}

	var runeDepth int64
	for _, depth := range runeE8DepthPerPool {
		runeDepth += depth
	}

	respJSON(w, map[string]interface{}{
		"dailyActiveUsers":   intStr(dailySwapsFromRune.RuneAddrCount + dailySwapsToRune.RuneAddrCount),
		"dailyTx":            intStr(dailySwapsFromRune.TxCount + dailySwapsToRune.TxCount),
		"monthlyActiveUsers": intStr(monthlySwapsFromRune.RuneAddrCount + monthlySwapsToRune.RuneAddrCount),
		"monthlyTx":          intStr(monthlySwapsFromRune.TxCount + monthlySwapsToRune.TxCount),
		"totalAssetBuys":     intStr(swapsFromRune.TxCount),
		"totalAssetSells":    intStr(swapsToRune.TxCount),
		"totalDepth":         intStr(runeDepth),
		"totalUsers":         intStr(swapsFromRune.RuneAddrCount + swapsToRune.RuneAddrCount),
		"totalStakeTx":       intStr(stakes.TxCount + unstakes.TxCount),
		"totalStaked":        intStr(stakes.RuneE8Total - unstakes.RuneE8Total),
		"totalTx":            intStr(swapsFromRune.TxCount + swapsToRune.TxCount + stakes.TxCount + unstakes.TxCount),
		"totalVolume":        intStr(swapsFromRune.RuneE8Total + swapsToRune.RuneE8Total),
		"totalWithdrawTx":    intStr(unstakes.RuneE8Total),
	})
	/* TODO(pascaldekloe)
	   "poolCount":"20",
	   "totalEarned":"1827445688454",
	   "totalVolume24hr":"37756279870656",
	*/
}

const assetListMax = 10

func assetParam(r *http.Request) ([]string, error) {
	list := strings.Join(r.URL.Query()["asset"], ",")
	if list == "" {
		return nil, errors.New("asset query parameter required")
	}
	assets := strings.SplitN(list, ",", assetListMax+1)
	if len(assets) > assetListMax {
		return nil, errors.New("too many entries in asset query parameter")
	}
	return assets, nil
}

// Return the value of the height url parameter.
// If height parameter is missing or it's -1 it returns the height of the latest block.
func heightParam(r *http.Request) (int64, error) {
	lastHeight, _, _ := timeseries.LastBlock()
	heightParams := r.URL.Query()["height"]
	if len(heightParams) == 0 {
		return lastHeight, nil
	} else if 1 < len(heightParams) {
		return -1, errors.New("too many height parameters")
	}
	height, err := strconv.ParseInt(heightParams[0], 10, 64)
	if err != nil {
		return -1, fmt.Errorf("couldn't parse block parameter as int: %w", err)
	}
	if height == -1 {
		return lastHeight, nil
	}
	if height <= 0 || lastHeight < height {
		return -1, fmt.Errorf("block parameter is out of bounds: %d", height)
	}
	return height, nil
}

func respJSON(w http.ResponseWriter, body interface{}) {
	w.Header().Set("Content-Type", "application/json")

	e := json.NewEncoder(w)
	e.SetIndent("", "\t")
	e.Encode(body)
}

func respError(w http.ResponseWriter, r *http.Request, err error) {
	log.Printf("HTTP %q %q: %s", r.Method, r.URL.Path, err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// IntStr returns the value as a decimal string.
// JSON numbers are double-precision floating-points.
// We don't want any unexpected rounding due to the 57-bit limit.
func intStr(v int64) string {
	return strconv.FormatInt(v, 10)
}

func intArrayStrs(a []int64) []string {
	b := make([]string, len(a))
	for i, v := range a {
		b[i] = intStr(v)
	}
	return b
}

// RatIntStr returs the (rounded) integer value as a decimal string.
// We don't want any unexpected rounding due to the 57-bit limit.
func ratIntStr(v *big.Rat) string {
	return new(big.Int).Div(v.Num(), v.Denom()).String()
}

// RatFloat transforms the rational value, possibly with loss of precision.
func ratFloatStr(r *big.Rat) string {
	f, _ := r.Float64()
	return strconv.FormatFloat(f, 'f', -1, 64)
}
