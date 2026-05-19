package pubsub

import "sync"

// Message represents a published message delivered to a subscriber.
type Message struct {
	Channel string
	Payload string
}

// Subscriber represents a connected client's message inbox.
// Each SUBSCRIBE command creates one Subscriber per connection.
type Subscriber struct {
	Ch chan Message // buffered channel — broker sends here, client goroutine reads
}

// NewSubscriber creates a Subscriber with a buffered inbox.
func NewSubscriber() *Subscriber {
	return &Subscriber{
		Ch: make(chan Message, 64),
	}
}

// Broker is the central pub/sub registry.
// It maps channel names → list of subscribers.
type Broker struct {
	mu       sync.RWMutex
	channels map[string][]*Subscriber
}

// NewBroker creates a new Broker.
func NewBroker() *Broker {
	return &Broker{
		channels: make(map[string][]*Subscriber),
	}
}

// Subscribe registers a subscriber for one or more channels.
func (b *Broker) Subscribe(sub *Subscriber, channels ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range channels {
		b.channels[ch] = append(b.channels[ch], sub)
	}
}

// Unsubscribe removes a subscriber from one or more channels.
func (b *Broker) Unsubscribe(sub *Subscriber, channels ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range channels {
		subs := b.channels[ch]
		for i, s := range subs {
			if s == sub {
				b.channels[ch] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
	}
}

// Publish sends a message to all subscribers of a channel.
// Returns the number of clients that received the message.
func (b *Broker) Publish(channel, payload string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	subs := b.channels[channel]
	for _, s := range subs {
		select {
		case s.Ch <- Message{Channel: channel, Payload: payload}:
		default: // skip slow/full subscribers, never block
		}
	}
	return len(subs)
}
