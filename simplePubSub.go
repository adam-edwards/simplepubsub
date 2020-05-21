package simplepubsub

import (
  "sync"
  "context"
)

type Subscriber interface {
  Data() <-chan interface{}
}

type PubSub struct {
  sync.RWMutex
  subs map[string][]Subscriber
}

func (broker *PubSub) Publish(topic string, data interface{}) error {
  broker.RLock()
  defer broker.RUnlock()

  if subs, ok := broker.subs[topic]; ok {
    ctx := context.Background()

    for _, s := range subs {
      s.Data()<- data
    }
  }

  return fmt.Errorf("No subscribers for topic: %s", topic)
}

func (broker *PubSub) Subscribe(topic string, sub Subscriber) {
  broker.Lock()
  defer broker.Unlock()

  append(broker.subs[topic], sub)
}
