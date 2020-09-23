package timeseries

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// DBQuery is the SQL client.
var DBQuery func(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

// DBExec is the SQL client.
var DBExec func(query string, args ...interface{}) (sql.Result, error)

// OutboundTimeout is an upperboundary for the amount of time for a followup on outbound events.
const OutboundTimeout = time.Hour

// LastBlockTrack is an in-memory copy of the write state.
var lastBlockTrack atomic.Value

// BlockTrack is a write state.
type blockTrack struct {
	Height    int64
	Timestamp time.Time
	Hash      []byte
	aggTrack
}

// AggTrack has a snapshot of runningTotals.
type aggTrack struct {
	AssetE8DepthPerPool map[string]int64
	RuneE8DepthPerPool  map[string]int64
}

// Read state from database.
// If height == -1 reads last height from database.

func loadBlockFromDB(height int64) (*blockTrack, error) {
	var restriction string
	if 0 < height {
		restriction = fmt.Sprintf("WHERE height = %d", height)
	} else {
		restriction = "ORDER BY height DESC LIMIT 1"
	}
	q := "SELECT height, timestamp, hash, agg_state FROM block_log " + restriction

	log.Printf("Running query on DB %s", q)

	rows, err := DBQuery(context.Background(), q)
	if err != nil {
		return nil, fmt.Errorf("last block lookup: %w", err)
	}
	defer rows.Close()

	var track blockTrack
	if rows.Next() {
		var ns int64
		var aggSerial []byte
		rows.Scan(&track.Height, &ns, &track.Hash, &aggSerial)
		track.Timestamp = time.Unix(0, ns)
		if err := gob.NewDecoder(bytes.NewReader(aggSerial)).Decode(&track.aggTrack); err != nil {
			return nil, fmt.Errorf("restore with malformed aggregation state denied on %w", err)
		}
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return &track, nil
}

// Setup initializes the package. The previous state is restored (if there was any).
func Setup() (lastBlockHeight int64, lastBlockTimestamp time.Time, lastBlockHash []byte, err error) {
	track, err := loadBlockFromDB(-1)
	if err != nil {
		return 0, time.Time{}, nil, err
	}

	// sync in-memory tracker
	lastBlockTrack.Store(track)

	// apply aggregation state to recorder
	for pool, E8 := range track.AssetE8DepthPerPool {
		v := E8 // copy
		recorder.assetE8DepthPerPool[pool] = &v
	}
	for pool, E8 := range track.RuneE8DepthPerPool {
		v := E8 // copy
		recorder.runeE8DepthPerPool[pool] = &v
	}

	return track.Height, track.Timestamp, track.Hash, nil
}

// CommitBlock marks the given height as done.
// Invokation of EventListener during CommitBlock causes race conditions!
func CommitBlock(height int64, timestamp time.Time, hash []byte) error {
	// in-memory snapshot
	track := blockTrack{
		Height:    height,
		Timestamp: timestamp,
		Hash:      make([]byte, len(hash)),
		aggTrack: aggTrack{
			AssetE8DepthPerPool: recorder.AssetE8DepthPerPool(),
			RuneE8DepthPerPool:  recorder.RuneE8DepthPerPool(),
		},
	}
	copy(track.Hash, hash)

	// persist to database
	var aggSerial bytes.Buffer
	if err := gob.NewEncoder(&aggSerial).Encode(&track.aggTrack); err != nil {
		// won't bing the service down, but prevents state recovery
		log.Print("aggregation state ommited from persistence:", err)
	}
	const q = "INSERT INTO block_log (height, timestamp, hash, agg_state) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING"
	result, err := DBExec(q, height, timestamp.UnixNano(), hash, aggSerial.Bytes())
	if err != nil {
		return fmt.Errorf("persist block height %d: %w", height, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("persist block height %d result: %w", height, err)
	}
	if n == 0 {
		log.Printf("block height %d already committed", height)
	}

	// calculate & reset
	recorder.linkedEvents.ApplyOutboundQ(&recorder.runningTotals, height, timestamp)
	recorder.linkedEvents.ApplyFeeQ(&recorder.runningTotals, height, timestamp)

	// commit in-memory state
	lastBlockTrack.Store(&track)

	return nil
}

// LastBlock gets the most recent commit.
func LastBlock() (height int64, timestamp time.Time, hash []byte) {
	track := lastBlockTrack.Load().(*blockTrack)
	return track.Height, track.Timestamp, track.Hash
}

// AssetAndRuneDepths gets the current snapshot handle.
// The asset price is the asset depth divided by the RUNE depth.
func AssetAndRuneDepths() (assetE8PerPool, runeE8PerPool map[string]int64, timestamp time.Time) {
	track := lastBlockTrack.Load().(*blockTrack)
	return track.aggTrack.AssetE8DepthPerPool, track.aggTrack.RuneE8DepthPerPool, track.Timestamp
}

// Same as AsAssetAndRuneDepths but for specific height
func AssetAndRuneDepthsAtHeight(height int64) (assetE8PerPool, runeE8PerPool map[string]int64, timestamp time.Time) {
	track, err := loadBlockFromDB(height)
	if err != nil {
		log.Print("Error on db query: ", err)
		empty := map[string]int64{}
		return empty, empty, time.Time{}
	}
	log.Print("track at hight: ", *track)
	return track.aggTrack.AssetE8DepthPerPool, track.aggTrack.RuneE8DepthPerPool, track.Timestamp
}
