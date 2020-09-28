// MapDiff helps to get differences between snapshots of a map.
//
// SnapshotManager creates queries to insert depth information into the aggregate_table.
package timeseries

import (
	"fmt"
	"log"
	"strings"
)

type mapStrInt map[string]int64
type mapStrIntPtr map[string]*int64

type mapDiff struct {
	snapshot mapStrInt
}

func (md *mapDiff) save(newMap map[string]int64) {
	md.snapshot = mapStrInt{}
	for k, v := range newMap {
		md.snapshot[k] = v
	}
}

// Returns values from newMap where they differ from snapshot.
// If a value is missing from newMap, returns 0 for that key.
func (md *mapDiff) diff(newMap map[string]*int64) map[string]int64 {
	result := mapStrInt{}

	for k, v := range newMap {
		oldv, ok := md.snapshot[k]
		if !ok || oldv != *v {
			result[k] = *v
		}
	}

	// Return 0 for deleted values.
	// TODO(acsaba): document why we don't write out 0s.
	for k, oldv := range md.snapshot {
		_, ok := newMap[k]
		if !ok && oldv != 0 {
			result[k] = 0
		}
	}

	return result
}

func (md *mapDiff) diffAtKey(pool string, newMap map[string]int64) (hasDiff bool, newValue int64) {
	oldV, hasOld := md.snapshot[pool]
	newV, hasNew := newMap[pool]
	if hasNew == true {
		return !hasOld || oldV != newV, newV
	} else {
		return hasOld, 0
	}
}

// func diffAtKey(new

type poolIdMap map[string]int

func (pim *poolIdMap) getId(poolName string) int {
	id, ok := (*pim)[poolName]
	if ok {
		return id
	}
	log.Printf("New pool name: |%s|", poolName)
	newId := len(*pim)
	(*pim)[poolName] = newId
	return newId
}

var poolIdMapperr poolIdMap = poolIdMap{}

type snapshotManager struct {
	assetE8DepthSnapshot mapDiff
	runeE8DepthSnapshot  mapDiff
	snapshotHeight       int64
}

var depthSnapshot snapshotManager

// Returns query which will insert.
func (sm *snapshotManager) update(height int64, assetE8DepthPerPool, runeE8DepthPerPool map[string]int64) error {
	dolog := height%10000 == 0 // || len(assetE8DepthPerPool) != 0

	if dolog {
		log.Printf("snapshotting at height %d (%v): %v", height, dolog, assetE8DepthPerPool)
	}

	// TODO_COMMIT: add back height check, or make it roboust
	// if sm.snapshotHeight+1 != height {
	// 	return fmt.Errorf("Snapshot height doesn't follow previous snapshot height (%d, %d)", height, sm.snapshotHeight)
	// }
	sm.snapshotHeight = height

	// type row struct {
	// 	pool         string
	// 	assetE8Depth int64
	// 	runeE8Depth  int64
	// }
	// newRows := []row{}

	// we need to iterate over all 4 maps (old, new; snapshot, new)
	poolNames := map[string]bool{}
	accumulatePoolNames := func(m map[string]int64) {
		for pool := range m {
			poolNames[pool] = true
		}
	}
	accumulatePoolNames(assetE8DepthPerPool)
	accumulatePoolNames(runeE8DepthPerPool)
	accumulatePoolNames(sm.assetE8DepthSnapshot.snapshot)
	accumulatePoolNames(sm.runeE8DepthSnapshot.snapshot)

	// TODO_BEFORE_COMIT check if there is a small limit on query size. should we add rows separately?
	queryFront := "INSERT INTO aggregate_states (height, pool, asset_e8, rune_e8) VALUES "
	queryEnd := " ON CONFLICT DO NOTHING;"
	rowFormat := "($%d, $%d, $%d, $%d)"
	rowStrs := []string{}
	values := []interface{}{}

	queryFront2 := "INSERT INTO aggregate_id_states (height, pool_id, asset_e8, rune_e8) VALUES "
	queryEnd2 := " ON CONFLICT DO NOTHING;"
	rowFormat2 := "($%d, $%d, $%d, $%d)"
	rowStrs2 := []string{}
	values2 := []interface{}{}

	if dolog {
		log.Printf("pool names: %v", poolNames)
	}
	for pool := range poolNames {
		assetDiff, assetValue := sm.assetE8DepthSnapshot.diffAtKey(pool, assetE8DepthPerPool)
		runeDiff, runeValue := sm.runeE8DepthSnapshot.diffAtKey(pool, runeE8DepthPerPool)
		if assetDiff || runeDiff {
			// dolog = true
			// newRows = append(newRows, row{pool, assetValue, runeValue})
			p := len(values)
			rowStrs = append(rowStrs, fmt.Sprintf(rowFormat, p+1, p+2, p+3, p+4))
			values = append(values, height, pool, assetValue, runeValue)

			poolId := poolIdMapperr.getId(pool)
			rowStrs2 = append(rowStrs2, fmt.Sprintf(rowFormat2, p+1, p+2, p+3, p+4))
			values2 = append(values2, height, poolId, assetValue, runeValue)
		}
	}
	sm.assetE8DepthSnapshot.save(assetE8DepthPerPool)
	sm.runeE8DepthSnapshot.save(runeE8DepthPerPool)

	diffNum := len(rowStrs)

	if 0 == diffNum {
		// log.Printf("Height doesn't have depth changes %d", height)
		return nil
	}

	query := queryFront + strings.Join(rowStrs, ", ") + queryEnd
	query2 := queryFront2 + strings.Join(rowStrs2, ", ") + queryEnd2
	if dolog {
		log.Printf("Saving query: %s | values: %v", query, values)
		log.Printf("Saving query2: %s | values: %v", query2, values2)
	}
	// time.Sleep(100 * time.Millisecond)
	{
		result, err := DBExec(query, values...)
		if err != nil {
			return fmt.Errorf("Error saving depths %d: %w", height, err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("Error saving depths %d results: %w", height, err)
		}
		if n != int64(diffNum) {
			return fmt.Errorf("Not all depths were saved at height %d (expected: %d, actual: %d)", height, n, diffNum)
		}
	}
	{
		result, err := DBExec(query2, values2...)
		if err != nil {
			return fmt.Errorf("Error 2 saving depths %d: %w", height, err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("Error 2 saving depths %d results: %w", height, err)
		}
		if n != int64(diffNum) {

			log.Printf("Saving query: %s | values: %v", query, values)
			log.Printf("Saving query2: %s | values: %v", query2, values2)
			log.Printf("Mapper %v", poolIdMapperr)
			for i, v := range values {
				if i%4 == 1 {
					log.Printf("%v -> %v", v, poolIdMapperr.getId(v.(string)))

				}
			}
			return fmt.Errorf("2 Not all depths were saved at height %d (expected: %d, actual: %d)", height, diffNum, n)
		}
	}
	return nil
}
