package event

import (
	"errors"
	"log"
	"time"

	// Tendermint is all about types? 🤔
	abci "github.com/tendermint/tendermint/abci/types"
	rpc "github.com/tendermint/tendermint/rpc/core/types"
	tendermint "github.com/tendermint/tendermint/types"
)

// Metadata has metadata for a block (from the chain).
type Metadata struct {
	BlockTimestamp time.Time // official acceptance moment
}

// Listener defines an event callback.
type Listener interface {
	OnAdd(*Add, *Metadata)
	OnFee(*Fee, *Metadata)
	OnMessage(*Message, *Metadata)
	OnOutbound(*Outbound, *Metadata)
	OnPool(*Pool, *Metadata)
	OnRefund(*Refund, *Metadata)
	OnReserve(*Reserve, *Metadata)
	OnStake(*Stake, *Metadata)
	OnSwap(*Swap, *Metadata)
	OnUnstake(*Unstake, *Metadata)
}

// Demux is a demultiplexer for events from the blockchain.
type Demux struct {
	Listener // destination

	// prevent memory allocation
	reuse struct {
		Add
		Fee
		Message
		Outbound
		Pool
		Refund
		Reserve
		Stake
		Swap
		Unstake
	}
}

// Block invokes Listener for each transaction event in block.
func (d *Demux) Block(block *rpc.ResultBlockResults, meta *tendermint.BlockMeta) {
	m := Metadata{BlockTimestamp: meta.Header.Time}

	for txIndex, tx := range block.TxsResults {
		for eventIndex, event := range tx.Events {
			if err := d.event(event, &m); err != nil {
				// TODO(pascaldekloe): Find best way to ID an event.
				log.Printf("block %s tx %d event %d type %q skipped: %s",
					meta.BlockID.String(), txIndex, eventIndex, event.Type, err)
			}
		}
	}
}

var errEventType = errors.New("unknown event type")

// Block notifies Listener for the transaction event.
// Errors do not include the event type in the message.
func (d *Demux) event(event abci.Event, meta *Metadata) error {
	attrs := event.Attributes

	switch event.Type {
	case "add":
		if err := d.reuse.Add.LoadTendermint(attrs); err != nil {
			return err
		}
		d.Listener.OnAdd(&d.reuse.Add, meta)
	case "fee":
		if err := d.reuse.Fee.LoadTendermint(attrs); err != nil {
			return err
		}
		d.Listener.OnFee(&d.reuse.Fee, meta)
	case "message":
		if err := d.reuse.Message.LoadTendermint(attrs); err != nil {
			return err
		}
		d.Listener.OnMessage(&d.reuse.Message, meta)
	case "outbound":
		if err := d.reuse.Outbound.LoadTendermint(attrs); err != nil {
			return err
		}
		d.Listener.OnOutbound(&d.reuse.Outbound, meta)
	case "pool":
		if err := d.reuse.Pool.LoadTendermint(attrs); err != nil {
			return err
		}
		d.Listener.OnPool(&d.reuse.Pool, meta)
	case "refund":
		if err := d.reuse.Refund.LoadTendermint(attrs); err != nil {
			return err
		}
		d.Listener.OnRefund(&d.reuse.Refund, meta)
	case "reserve":
		if err := d.reuse.Reserve.LoadTendermint(attrs); err != nil {
			return err
		}
		d.Listener.OnReserve(&d.reuse.Reserve, meta)
	case "stake":
		if err := d.reuse.Stake.LoadTendermint(attrs); err != nil {
			return err
		}
		d.Listener.OnStake(&d.reuse.Stake, meta)
	case "swap":
		if err := d.reuse.Swap.LoadTendermint(attrs); err != nil {
			return err
		}
		d.Listener.OnSwap(&d.reuse.Swap, meta)
	case "unstake":
		if err := d.reuse.Unstake.LoadTendermint(attrs); err != nil {
			return err
		}
		d.Listener.OnUnstake(&d.reuse.Unstake, meta)
	default:
		return errEventType
	}

	return nil
}