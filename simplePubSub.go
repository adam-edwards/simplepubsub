package simplepubsub

import (
  "sync"
  "context"
  "fmt"

  "github.com/google/uuid"
)

// A Subscriber provides a means to send it data.
type Subscriber interface {
  Data() <-chan interface{}
  SetSubscriptionKey(string)
}

// PubSub implements a simple pub/sub message broker to allow
// thread-safe fan-in or fan-out message passing between goroutines.
type PubSub struct {
  sync.RWMutex
  topics map[string][]string
  subs map[string]Subscriber
}

// NewPubSub returns an initialized PubSub.
func NewPubSub() *PubSub {
  return &PubSub{}
}

// Publish publishes the given data to all subscribers to the
// given topic. If there are no subscribers, or if the topic does
// not exist, an error is returned.
func (broker *PubSub) Publish(topic string, data interface{}) error {
  broker.RLock()
  defer broker.RUnlock()

  subKeys, ok := broker.topics[topic]
  if !ok || len(subKeys) == 0 {
    return fmt.Errorf("No subscribers for topic: %s", topic)
  }

  for _, subKey := range subKeys {

    go func(broker *PubSub, subKey string, data interface{}) {

      broker.RLock()
      sub, ok := broker.subs[subKey]
      broker.RUnlock()

      if !ok {
        panic("Subscriber ID does not exist.")
      }

      sub.Data()<- data

    }(broker, subKey, data)

  }

  return nil
}

// Subscribe subscribes to the given topic. The subscriber will
// not see messages published prior to subscription.
func (broker *PubSub) Subscribe(topic string, sub Subscriber) {
  broker.Lock()
  defer broker.Unlock()

  subKey := uuid.New().String()
  broker.subs[subKey] = sub
  append(broker.topics[topic], subKey)
  sub.SetSubscriptionKey(subKey)
}

// Unsubscribe removes the subscriber from the given topic.
func (broker *PubSub) Unsubscribe(topic, subKey string) {
  broker.Lock()
  defer broker.Unlock()

  for i, k := range broker.topics[topic] {
    if k == subKey {
      // Fast delete from slice without preserving order
      broker.topics[topic][i] = broker.topics[topic][len(broker.topics[topic])-1]
      broker.topics[topic] = broker.topics[topic][:len(broker.topics[topic])-1]
      break
    }
  }
  delete(broker.subs, subKey)
}
