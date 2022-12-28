package types

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/log"
	tmpubsub "github.com/tendermint/tendermint/libs/pubsub"
	"github.com/tendermint/tendermint/libs/service"
)

const defaultCapacity = 0
const SockAddr = "/tmp/osmosisd.sock"

var connection net.Conn

type EventBusSubscriber interface {
	Subscribe(ctx context.Context, subscriber string, query tmpubsub.Query, outCapacity ...int) (Subscription, error)
	Unsubscribe(ctx context.Context, subscriber string, query tmpubsub.Query) error
	UnsubscribeAll(ctx context.Context, subscriber string) error

	NumClients() int
	NumClientSubscriptions(clientID string) int
}

type Subscription interface {
	Out() <-chan tmpubsub.Message
	Canceled() <-chan struct{}
	Err() error
}

// EventBus is a common bus for all events going through the system. All calls
// are proxied to underlying pubsub server. All events must be published using
// EventBus to ensure correct data types.
type EventBus struct {
	service.BaseService
	pubsub *tmpubsub.Server
}

type UnixData struct {
	Event string `json:"event"`
	Data  any    `json:"data"`
	Hash  any    `json:"hash"`
}

// NewEventBus returns a new event bus.
func NewEventBus() *EventBus {
	return NewEventBusWithBufferCapacity(defaultCapacity)
}

// NewEventBusWithBufferCapacity returns a new event bus with the given buffer capacity.
func NewEventBusWithBufferCapacity(cap int) *EventBus {
	go CreateUnixSocket()
	pubsub := tmpubsub.NewServer(tmpubsub.BufferCapacity(cap))
	b := &EventBus{pubsub: pubsub}
	b.BaseService = *service.NewBaseService(nil, "EventBus", b)
	return b
}

func CreateUnixSocket() {
	if err := os.RemoveAll(SockAddr); err != nil {
		panic(err)
	}

	listener, err := net.Listen("unix", SockAddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("Server is listening...")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		connection = conn
	}
}

func (b *EventBus) SetLogger(l log.Logger) {
	b.BaseService.SetLogger(l)
	b.pubsub.SetLogger(l.With("module", "pubsub"))
}

func (b *EventBus) OnStart() error {
	return b.pubsub.Start()
}

func (b *EventBus) OnStop() {
	if err := b.pubsub.Stop(); err != nil {
		b.pubsub.Logger.Error("error trying to stop eventBus", "error", err)
	}
}

func (b *EventBus) NumClients() int {
	return b.pubsub.NumClients()
}

func (b *EventBus) NumClientSubscriptions(clientID string) int {
	return b.pubsub.NumClientSubscriptions(clientID)
}

func (b *EventBus) Subscribe(
	ctx context.Context,
	subscriber string,
	query tmpubsub.Query,
	outCapacity ...int,
) (Subscription, error) {
	return b.pubsub.Subscribe(ctx, subscriber, query, outCapacity...)
}

// This method can be used for a local consensus explorer and synchronous
// testing. Do not use for for public facing / untrusted subscriptions!
func (b *EventBus) SubscribeUnbuffered(
	ctx context.Context,
	subscriber string,
	query tmpubsub.Query,
) (Subscription, error) {
	return b.pubsub.SubscribeUnbuffered(ctx, subscriber, query)
}

func (b *EventBus) Unsubscribe(ctx context.Context, subscriber string, query tmpubsub.Query) error {
	return b.pubsub.Unsubscribe(ctx, subscriber, query)
}

func (b *EventBus) UnsubscribeAll(ctx context.Context, subscriber string) error {
	return b.pubsub.UnsubscribeAll(ctx, subscriber)
}

func (b *EventBus) Publish(eventType string, eventData TMEventData) error {
	// no explicit deadline for publishing events
	ctx := context.Background()
	return b.pubsub.PublishWithEvents(ctx, eventData, map[string][]string{EventTypeKey: {eventType}})
}

// validateAndStringifyEvents takes a slice of event objects and creates a
// map of stringified events where each key is composed of the event
// type and each of the event's attributes keys in the form of
// "{event.Type}.{attribute.Key}" and the value is each attribute's value.
func (b *EventBus) validateAndStringifyEvents(events []types.Event, logger log.Logger) map[string][]string {
	result := make(map[string][]string)
	for _, event := range events {
		if len(event.Type) == 0 {
			logger.Debug("Got an event with an empty type (skipping)", "event", event)
			continue
		}

		for _, attr := range event.Attributes {
			if len(attr.Key) == 0 {
				logger.Debug("Got an event attribute with an empty key(skipping)", "event", event)
				continue
			}

			compositeTag := fmt.Sprintf("%s.%s", event.Type, attr.Key)
			result[compositeTag] = append(result[compositeTag], attr.Value)
		}
	}

	return result
}

func PublishSocketEvent(event string, data any, hash any) {
	if connection != nil {
		jsonData, err := json.Marshal(&UnixData{Event: event, Data: data, Hash: hash})
		if err != nil {
			panic(err)
		}
		outString := string(jsonData)
		connection.Write([]byte(outString))
	}
}

func (b *EventBus) PublishEventNewBlock(data EventDataNewBlock) error {
	PublishSocketEvent("block", data.Block, data.Block.Hash())

	// no explicit deadline for publishing events
	ctx := context.Background()

	resultEvents := append(data.ResultBeginBlock.Events, data.ResultEndBlock.Events...)
	events := b.validateAndStringifyEvents(resultEvents, b.Logger.With("block", data.Block.StringShort()))

	// add predefined new block event
	events[EventTypeKey] = append(events[EventTypeKey], EventNewBlock)

	return b.pubsub.PublishWithEvents(ctx, data, events)
}

func (b *EventBus) PublishEventNewBlockHeader(data EventDataNewBlockHeader) error {
	// no explicit deadline for publishing events
	ctx := context.Background()

	resultTags := append(data.ResultBeginBlock.Events, data.ResultEndBlock.Events...)
	// TODO: Create StringShort method for Header and use it in logger.
	events := b.validateAndStringifyEvents(resultTags, b.Logger.With("header", data.Header))

	// add predefined new block header event
	events[EventTypeKey] = append(events[EventTypeKey], EventNewBlockHeader)

	return b.pubsub.PublishWithEvents(ctx, data, events)
}

func (b *EventBus) PublishEventNewEvidence(evidence EventDataNewEvidence) error {
	return b.Publish(EventNewEvidence, evidence)
}

func (b *EventBus) PublishEventVote(data EventDataVote) error {
	return b.Publish(EventVote, data)
}

func (b *EventBus) PublishEventValidBlock(data EventDataRoundState) error {
	return b.Publish(EventValidBlock, data)
}

// PublishEventTx publishes tx event with events from Result. Note it will add
// predefined keys (EventTypeKey, TxHashKey). Existing events with the same keys
// will be overwritten.
func (b *EventBus) PublishEventTx(data EventDataTx) error {
	PublishSocketEvent("tx", data.Tx, fmt.Sprintf("%X", Tx(data.Tx).Hash()))

	// no explicit deadline for publishing events
	ctx := context.Background()

	events := b.validateAndStringifyEvents(data.Result.Events, b.Logger.With("tx", data.Tx))

	// add predefined compositeKeys
	events[EventTypeKey] = append(events[EventTypeKey], EventTx)
	events[TxHashKey] = append(events[TxHashKey], fmt.Sprintf("%X", Tx(data.Tx).Hash()))
	events[TxHeightKey] = append(events[TxHeightKey], fmt.Sprintf("%d", data.Height))

	return b.pubsub.PublishWithEvents(ctx, data, events)
}

// PublishEventMemTx
func (b *EventBus) PublishEventMemTx(memTx []byte) error {
	PublishSocketEvent("memtx", memTx, fmt.Sprintf("%X", Tx(memTx).Hash()))
	return nil
}

func (b *EventBus) PublishEventNewRoundStep(data EventDataRoundState) error {
	return b.Publish(EventNewRoundStep, data)
}

func (b *EventBus) PublishEventTimeoutPropose(data EventDataRoundState) error {
	return b.Publish(EventTimeoutPropose, data)
}

func (b *EventBus) PublishEventTimeoutWait(data EventDataRoundState) error {
	return b.Publish(EventTimeoutWait, data)
}

func (b *EventBus) PublishEventNewRound(data EventDataNewRound) error {
	return b.Publish(EventNewRound, data)
}

func (b *EventBus) PublishEventCompleteProposal(data EventDataCompleteProposal) error {
	return b.Publish(EventCompleteProposal, data)
}

func (b *EventBus) PublishEventPolka(data EventDataRoundState) error {
	return b.Publish(EventPolka, data)
}

func (b *EventBus) PublishEventUnlock(data EventDataRoundState) error {
	return b.Publish(EventUnlock, data)
}

func (b *EventBus) PublishEventRelock(data EventDataRoundState) error {
	return b.Publish(EventRelock, data)
}

func (b *EventBus) PublishEventLock(data EventDataRoundState) error {
	return b.Publish(EventLock, data)
}

func (b *EventBus) PublishEventValidatorSetUpdates(data EventDataValidatorSetUpdates) error {
	return b.Publish(EventValidatorSetUpdates, data)
}

// -----------------------------------------------------------------------------
type NopEventBus struct{}

func (NopEventBus) Subscribe(
	ctx context.Context,
	subscriber string,
	query tmpubsub.Query,
	out chan<- interface{},
) error {
	return nil
}

func (NopEventBus) Unsubscribe(ctx context.Context, subscriber string, query tmpubsub.Query) error {
	return nil
}

func (NopEventBus) UnsubscribeAll(ctx context.Context, subscriber string) error {
	return nil
}

func (NopEventBus) PublishEventNewBlock(data EventDataNewBlock) error {
	return nil
}

func (NopEventBus) PublishEventNewBlockHeader(data EventDataNewBlockHeader) error {
	return nil
}

func (NopEventBus) PublishEventNewEvidence(evidence EventDataNewEvidence) error {
	return nil
}

func (NopEventBus) PublishEventVote(data EventDataVote) error {
	return nil
}

func (NopEventBus) PublishEventTx(data EventDataTx) error {
	return nil
}

func (NopEventBus) PublishEventNewRoundStep(data EventDataRoundState) error {
	return nil
}

func (NopEventBus) PublishEventTimeoutPropose(data EventDataRoundState) error {
	return nil
}

func (NopEventBus) PublishEventTimeoutWait(data EventDataRoundState) error {
	return nil
}

func (NopEventBus) PublishEventNewRound(data EventDataRoundState) error {
	return nil
}

func (NopEventBus) PublishEventCompleteProposal(data EventDataRoundState) error {
	return nil
}

func (NopEventBus) PublishEventPolka(data EventDataRoundState) error {
	return nil
}

func (NopEventBus) PublishEventUnlock(data EventDataRoundState) error {
	return nil
}

func (NopEventBus) PublishEventRelock(data EventDataRoundState) error {
	return nil
}

func (NopEventBus) PublishEventLock(data EventDataRoundState) error {
	return nil
}

func (NopEventBus) PublishEventValidatorSetUpdates(data EventDataValidatorSetUpdates) error {
	return nil
}
