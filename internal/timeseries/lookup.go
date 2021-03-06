package timeseries

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

// ErrBeyondLast denies a request into the future. 💫
var errBeyondLast = errors.New("cannot resolve beyond the last block (timestamp)")

// Pools gets all asset identifiers for a given point in time.
// A zero moment defaults to the latest available.
// Requests beyond the last block cause an error.
func Pools(ctx context.Context, moment time.Time) ([]string, error) {
	_, timestamp, _ := LastBlock()
	if moment.IsZero() {
		moment = timestamp
	} else if timestamp.Before(moment) {
		return nil, errBeyondLast
	}

	const q = "SELECT pool FROM stake_events WHERE block_timestamp <= $1 GROUP BY pool"
	rows, err := DBQuery(ctx, q, moment.UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pools []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return pools, err
		}
		pools = append(pools, s)
	}
	return pools, rows.Err()
}

// PoolStatus gets the label for a given point in time.
// A zero moment defaults to the latest available.
// Requests beyond the last block cause an error.
func PoolStatus(ctx context.Context, pool string, moment time.Time) (string, error) {
	_, timestamp, _ := LastBlock()
	if moment.IsZero() {
		moment = timestamp
	} else if timestamp.Before(moment) {
		return "", errBeyondLast
	}

	const q = "SELECT COALESCE(last(status, block_timestamp), '') FROM pool_events WHERE block_timestamp <= $2 AND asset = $1"
	rows, err := DBQuery(ctx, q, pool, moment.UnixNano())
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var status string
	if rows.Next() {
		if err := rows.Scan(&status); err != nil {
			return "", err
		}
	}
	return status, rows.Err()
}

// StakeAddrs gets all known addresses for a given point in time.
// A zero moment defaults to the latest available.
// Requests beyond the last block cause an error.
func StakeAddrs(ctx context.Context, moment time.Time) (addrs []string, err error) {
	_, timestamp, _ := LastBlock()
	if moment.IsZero() {
		moment = timestamp
	} else if timestamp.Before(moment) {
		return nil, errBeyondLast
	}

	const q = "SELECT rune_addr FROM stake_events WHERE block_timestamp <= $1 GROUP BY rune_addr"
	rows, err := DBQuery(ctx, q, moment.UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	addrs = make([]string, 0, 1024)
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return addrs, err
		}
		addrs = append(addrs, s)
	}
	return addrs, rows.Err()
}

// Mimir gets all values for a given point in time.
// A zero moment defaults to the latest available.
// Requests beyond the last block cause an error.
func Mimir(ctx context.Context, moment time.Time) (map[string]string, error) {
	_, timestamp, _ := LastBlock()
	if moment.IsZero() {
		moment = timestamp
	} else if timestamp.Before(moment) {
		return nil, errBeyondLast
	}

	// could optimise by only fetching latest
	const q = "SELECT name, value FROM set_mimir_event_entries WHERE block_timestamp <= $1"
	rows, err := DBQuery(ctx, q, moment.UnixNano())
	if err != nil {
		return nil, fmt.Errorf("mimir lookup: %w", err)
	}
	defer rows.Close()

	m := make(map[string]string)
	for rows.Next() {
		var name, value string
		err := rows.Scan(&name, &value)
		if err != nil {
			return m, fmt.Errorf("mimir retrieve: %w", err)
		}
		m[name] = value
	}
	return m, rows.Err()
}

// StatusPerNode gets the labels for a given point in time.
// New nodes have the empty string (for no confirmed status).
// A zero moment defaults to the latest available.
// Requests beyond the last block cause an error.
func StatusPerNode(ctx context.Context, moment time.Time) (map[string]string, error) {
	_, timestamp, _ := LastBlock()
	if moment.IsZero() {
		moment = timestamp
	} else if timestamp.Before(moment) {
		return nil, errBeyondLast
	}

	m, err := newNodes(ctx, moment)
	if err != nil {
		return nil, err
	}

	// could optimise by only fetching latest
	const q = "SELECT node_addr, current FROM update_node_account_status_events WHERE block_timestamp <= $1"
	rows, err := DBQuery(ctx, q, moment.UnixNano())
	if err != nil {
		return nil, fmt.Errorf("status per node lookup: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var node, status string
		err := rows.Scan(&node, &status)
		if err != nil {
			return m, fmt.Errorf("status per node retrieve: %w", err)
		}
		m[node] = status
	}
	return m, rows.Err()
}

func newNodes(ctx context.Context, moment time.Time) (map[string]string, error) {
	// could optimise by only fetching latest
	const q = "SELECT node_addr FROM new_node_events WHERE block_timestamp <= $1"
	rows, err := DBQuery(ctx, q, moment.UnixNano())
	if err != nil {
		return nil, fmt.Errorf("new node lookup: %w", err)
	}
	defer rows.Close()

	m := make(map[string]string)
	for rows.Next() {
		var node string
		err := rows.Scan(&node)
		if err != nil {
			return m, fmt.Errorf("new node retrieve: %w", err)
		}
		m[node] = ""
	}
	return m, rows.Err()
}

// NodesSecpAndEd returs the public keys mapped to their respective addresses.
func NodesSecpAndEd(ctx context.Context, t time.Time) (secp256k1Addrs, ed25519Addrs map[string]string, err error) {
	const q = `SELECT node_addr, secp256k1, ed25519
FROM set_node_keys_events
WHERE block_timestamp <= $1`

	rows, err := DBQuery(ctx, q, t.UnixNano())
	if err != nil {
		return nil, nil, fmt.Errorf("node addr lookup: %w", err)
	}
	defer rows.Close()

	secp256k1Addrs = make(map[string]string)
	ed25519Addrs = make(map[string]string)
	for rows.Next() {
		var addr, secp, ed string
		if err := rows.Scan(&addr, &secp, &ed); err != nil {
			return nil, nil, fmt.Errorf("node addr resolve: %w", err)
		}
		if current, ok := secp256k1Addrs[secp]; ok && current != addr {
			log.Printf("secp256k1 key %q used by node address %q and %q", secp, current, addr)
		}
		secp256k1Addrs[secp] = addr
		if current, ok := ed25519Addrs[ed]; ok && current != addr {
			log.Printf("Ed25519 key %q used by node address %q and %q", ed, current, addr)
		}
		ed25519Addrs[secp] = addr
	}
	return
}
