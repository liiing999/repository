package repl

import "sync"

// broadcaster 是多订阅者的事件广播器：每次 Notify 向所有订阅者的
// 缓冲为 1 的通道发一个信号；满了就算“已有待处理信号”，不阻塞。
// 用来替代多协程共享单 channel 造成的信号丢失。
type broadcaster struct {
	mu   sync.Mutex
	subs map[int64]chan struct{}
	seq  int64
}

func newBroadcaster() *broadcaster {
	return &broadcaster{subs: make(map[int64]chan struct{})}
}

func (b *broadcaster) subscribe() (int64, <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	id := b.seq
	ch := make(chan struct{}, 1)
	b.subs[id] = ch
	return id, ch
}

func (b *broadcaster) unsubscribe(id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs, id)
}

func (b *broadcaster) notify() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
