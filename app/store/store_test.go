package store

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jaipreethtiruvaipati/redis-clone/app/stream"
)

func TestStringsTTLAndTypes(t *testing.T) {
	s := New()
	if err := s.SetChecked("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.GetChecked("foo"); err != nil || !ok || got != "bar" {
		t.Fatalf("GetChecked = %q,%v,%v", got, ok, err)
	}
	if _, err := s.RPushChecked("foo", "x"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("RPUSH on string error = %v", err)
	}
	if s.Type("foo") != TypeString {
		t.Fatalf("TYPE foo = %q", s.Type("foo"))
	}
	if err := s.SetWithExpiryChecked("exp", "v", 15*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	if _, ok, err := s.GetChecked("exp"); err != nil || ok {
		t.Fatalf("expired GET = %v,%v", ok, err)
	}
	if s.Type("exp") != TypeNone {
		t.Fatalf("expired TYPE = %q", s.Type("exp"))
	}
}

func TestLists(t *testing.T) {
	s := New()
	if n, err := s.RPushChecked("l", "b", "c"); err != nil || n != 2 {
		t.Fatalf("RPUSH = %d,%v", n, err)
	}
	if n, err := s.LPushChecked("l", "x", "a"); err != nil || n != 4 {
		t.Fatalf("LPUSH = %d,%v", n, err)
	}
	if got, err := s.LRangeChecked("l", 0, -1); err != nil || len(got) != 4 || got[0] != "a" || got[3] != "c" {
		t.Fatalf("LRANGE = %#v,%v", got, err)
	}
	if got, err := s.LPopNChecked("l", 2); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "x" {
		t.Fatalf("LPOP count = %#v,%v", got, err)
	}
	if n, err := s.LLenChecked("l"); err != nil || n != 2 {
		t.Fatalf("LLEN = %d,%v", n, err)
	}
	if _, err := s.ZAddChecked("l", 1, "m"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ZADD on list error = %v", err)
	}
}

func TestBLPopAndCancellation(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan struct {
		value string
		ok    bool
		err   error
	}, 1)
	go func() {
		v, ok, err := s.BLPopContext(ctx, "l", 0)
		result <- struct {
			value string
			ok    bool
			err   error
		}{v, ok, err}
	}()
	time.Sleep(5 * time.Millisecond)
	if _, err := s.RPushChecked("l", "value"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if !got.ok || got.value != "value" || got.err != nil {
			t.Fatalf("BLPOP = %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("BLPOP did not wake")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if _, _, err := s.BLPopContext(ctx2, "other", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled BLPOP error = %v", err)
	}
	cancel()
}

func TestBLPopTimeoutNotificationNeverLosesItem(t *testing.T) {
	for i := 0; i < 100; i++ {
		s := New()
		result := make(chan bool, 1)
		go func() {
			_, ok := s.BLPop("race", 2*time.Millisecond)
			result <- ok
		}()
		deadline := time.Now().Add(time.Second)
		for {
			s.mu.RLock()
			registered := len(s.waiters["race"]) == 1
			s.mu.RUnlock()
			if registered {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("waiter was not registered")
			}
		}
		time.Sleep(time.Millisecond)
		if _, err := s.RPushChecked("race", "value"); err != nil {
			t.Fatal(err)
		}
		if ok := <-result; !ok {
			if value, exists := s.LPop("race"); !exists || value != "value" {
				t.Fatalf("iteration %d lost item during timeout/notification race", i)
			}
		}
	}
}

func TestBLPopWaitersAreFIFO(t *testing.T) {
	s := New()
	results := make([]chan string, 3)
	for i := range results {
		results[i] = make(chan string, 1)
		go func(ch chan<- string) {
			value, ok := s.BLPop("fifo", time.Second)
			if !ok {
				ch <- "timeout"
				return
			}
			ch <- value
		}(results[i])
		deadline := time.Now().Add(time.Second)
		for {
			s.mu.RLock()
			registered := len(s.waiters["fifo"]) == i+1
			s.mu.RUnlock()
			if registered {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("waiter was not registered")
			}
		}
	}
	if _, err := s.RPushChecked("fifo", "a", "b", "c"); err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"a", "b", "c"} {
		if got := <-results[i]; got != want {
			t.Fatalf("waiter %d got %q, want %q", i, got, want)
		}
	}
}

func TestBLPopMultiKeyWakesOnAnyKey(t *testing.T) {
	s := New()
	result := make(chan struct {
		key, value string
		ok         bool
		err        error
	}, 1)
	go func() {
		key, value, ok, err := s.BLPopMultiContext(context.Background(), []string{"first", "second"}, time.Second)
		result <- struct {
			key, value string
			ok         bool
			err        error
		}{key, value, ok, err}
	}()
	time.Sleep(10 * time.Millisecond)
	if _, err := s.RPushChecked("second", "value"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || !got.ok || got.key != "second" || got.value != "value" {
			t.Fatalf("multi-key BLPOP = %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("multi-key BLPOP did not wake")
	}
}

func TestSortedSetsAndStreams(t *testing.T) {
	s := New()
	if n, err := s.ZAddChecked("z", 2, "b"); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if n, err := s.ZAddChecked("z", 1, "a"); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if got, err := s.ZRangeChecked("z", 0, -1); err != nil || len(got) != 2 || got[0] != "a" {
		t.Fatalf("ZRANGE = %#v,%v", got, err)
	}
	if rank, ok, err := s.ZRankChecked("z", "b"); err != nil || !ok || rank != 1 {
		t.Fatalf("ZRANK = %d,%v,%v", rank, ok, err)
	}
	if score, ok, err := s.ZScoreChecked("z", "a"); err != nil || !ok || score != 1 {
		t.Fatalf("ZSCORE = %v,%v,%v", score, ok, err)
	}
	if n, err := s.ZRemChecked("z", "a"); err != nil || n != 1 || s.ZCard("z") != 1 {
		t.Fatalf("ZREM = %d,%v", n, err)
	}
	if _, err := s.XAdd("z", "1-0", []string{"f", "v"}); !errors.Is(err, ErrWrongType) {
		t.Fatalf("XADD on zset error = %v", err)
	}
	if id, err := s.XAdd("stream", "1-0", []string{"f", "v"}); err != nil || id != "1-0" {
		t.Fatal(id, err)
	}
	if id, err := s.XAdd("stream", "2-0", []string{"g", "w"}); err != nil || id != "2-0" {
		t.Fatal(id, err)
	}
	rangeEntries, err := s.XRangeChecked("stream", stream.EntryID{Milliseconds: 0}, stream.EntryID{Milliseconds: 3, Seq: ^uint64(0)})
	if err != nil || len(rangeEntries) != 2 {
		t.Fatalf("XRANGE = %#v,%v", rangeEntries, err)
	}
	results, err := s.XReadChecked([]string{"stream"}, []stream.EntryID{{Milliseconds: 1}})
	if err != nil || len(results) != 1 || len(results[0].Entries) != 1 || results[0].Entries[0].ID.String() != "2-0" {
		t.Fatalf("XREAD = %#v,%v", results, err)
	}
	if s.Type("z") != TypeZSet || s.Type("stream") != TypeStream {
		t.Fatalf("types: z=%q stream=%q", s.Type("z"), s.Type("stream"))
	}
}

func TestConcurrentIncr(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := s.Incr("counter"); err != nil {
					t.Errorf("INCR: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	if got, ok := s.Get("counter"); !ok || got != "1000" {
		t.Fatalf("counter = %q,%v", got, ok)
	}
}

func TestBlockingXReadMultiStream(t *testing.T) {
	s := New()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan struct {
		results []stream.ReadResult
		ok      bool
		err     error
	}, 1)
	go func() {
		got, ok, err := s.BXReadMultiContext(ctx, []string{"s1", "s2"}, []stream.EntryID{{}, {}}, 0)
		result <- struct {
			results []stream.ReadResult
			ok      bool
			err     error
		}{got, ok, err}
	}()
	time.Sleep(10 * time.Millisecond)
	if _, err := s.XAdd("s2", "1-0", []string{"f", "v"}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || !got.ok || len(got.results) != 1 || got.results[0].Key != "s2" {
			t.Fatalf("multi-stream XREAD = %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("multi-stream XREAD did not wake")
	}
}

func TestConcurrentListProducersConsumers(t *testing.T) {
	s := New()
	const producers, consumers, each = 4, 4, 100
	var producersWG sync.WaitGroup
	for i := 0; i < producers; i++ {
		producersWG.Add(1)
		go func(id int) {
			defer producersWG.Done()
			for j := 0; j < each; j++ {
				if _, err := s.RPushChecked("jobs", strconv.Itoa(id*each+j)); err != nil {
					t.Errorf("RPUSH: %v", err)
				}
			}
		}(i)
	}
	consumed := make(chan string, producers*each)
	var consumersWG sync.WaitGroup
	for i := 0; i < consumers; i++ {
		consumersWG.Add(1)
		go func() {
			defer consumersWG.Done()
			for j := 0; j < each; j++ {
				v, ok := s.BLPop("jobs", time.Second)
				if !ok {
					t.Errorf("BLPOP timed out")
					return
				}
				consumed <- v
			}
		}()
	}
	producersWG.Wait()
	consumersWG.Wait()
	close(consumed)
	if got := len(consumed); got != producers*each || s.LLen("jobs") != 0 {
		t.Fatalf("consumed=%d remaining=%d", got, s.LLen("jobs"))
	}
}

func TestConcurrentSortedSetAndStreams(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				member := string(rune('a'+(id*50+j)%26)) + string(rune('0'+id))
				if _, err := s.ZAddChecked("z", float64(id*50+j), member); err != nil {
					t.Errorf("ZADD: %v", err)
				}
				_, _, _ = s.ZRankChecked("z", member)
				_, _ = s.ZRangeChecked("z", 0, 10)
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = s.ZRemChecked("z", "missing")
				_, _ = s.ZCardChecked("z")
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				if _, err := s.XAdd("events", "*", []string{"n", "v"}); err != nil {
					t.Errorf("XADD: %v", err)
				}
				_, _ = s.XReadChecked([]string{"events"}, []stream.EntryID{{}})
			}
		}()
	}
	wg.Wait()
	if s.ZCard("z") == 0 || s.Type("events") != TypeStream {
		t.Fatalf("concurrent structures lost data: zcard=%d type=%s", s.ZCard("z"), s.Type("events"))
	}
}

func TestConcurrentStreamIDsRemainMonotonic(t *testing.T) {
	s := New()
	const writers = 8
	const each = 50
	ids := make(chan stream.EntryID, writers*each)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				id, err := s.XAdd("monotonic", "*", []string{"f", "v"})
				if err != nil {
					t.Errorf("XADD: %v", err)
					return
				}
				parsed, err := stream.Parse(id)
				if err != nil {
					t.Errorf("parse generated ID: %v", err)
					return
				}
				ids <- parsed
			}
		}()
	}
	wg.Wait()
	close(ids)
	all := make([]stream.EntryID, 0, writers*each)
	for id := range ids {
		all = append(all, id)
	}
	if len(all) != writers*each {
		t.Fatalf("generated %d IDs, want %d", len(all), writers*each)
	}
	entries := s.XRange("monotonic", stream.EntryID{}, stream.EntryID{Milliseconds: 1 << 62, Seq: ^uint64(0)})
	if len(entries) != len(all) {
		t.Fatalf("stream contains %d entries, want %d", len(entries), len(all))
	}
	for i := 1; i < len(entries); i++ {
		if !entries[i-1].ID.LessThan(entries[i].ID) {
			t.Fatalf("non-monotonic IDs at %d: %s then %s", i, entries[i-1].ID, entries[i].ID)
		}
	}
}
