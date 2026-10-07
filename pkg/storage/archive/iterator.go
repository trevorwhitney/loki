package archive

import (
	"context"

	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
)

// partitionLoader decodes one partition's objects into an iterator sorted in
// the query direction.
type partitionLoader func(ctx context.Context, paths []string) (iter.EntryIterator, error)

type loadedPartition struct {
	it  iter.EntryIterator
	err error
}

// lazyEntryIterator concatenates partitions in query order, decoding each
// one only when the previous is exhausted and keeping a bounded number
// decoded ahead. Partitions are disjoint in event time, so concatenating
// their sorted iterators yields a sorted stream; a log query with a limit
// stops the iterator early and the partitions it never reached are never
// fetched.
type lazyEntryIterator struct {
	cancel  context.CancelFunc
	results chan loadedPartition
	cur     iter.EntryIterator
	err     error
}

func newLazyEntryIterator(ctx context.Context, partitions [][]string, prefetch int, load partitionLoader) iter.EntryIterator {
	if len(partitions) == 0 {
		return iter.NoopEntryIterator
	}
	ctx, cancel := context.WithCancel(ctx)
	l := &lazyEntryIterator{
		cancel:  cancel,
		results: make(chan loadedPartition, max(prefetch, 1)),
	}
	go func() {
		defer close(l.results)
		for _, paths := range partitions {
			it, err := load(ctx, paths)
			select {
			case l.results <- loadedPartition{it: it, err: err}:
			case <-ctx.Done():
				if it != nil {
					_ = it.Close()
				}
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return l
}

func (l *lazyEntryIterator) Next() bool {
	for {
		if l.cur != nil {
			if l.cur.Next() {
				return true
			}
			if err := l.cur.Err(); err != nil {
				l.err = err
				return false
			}
			_ = l.cur.Close()
			l.cur = nil
		}
		r, ok := <-l.results
		if !ok {
			return false
		}
		if r.err != nil {
			l.err = r.err
			return false
		}
		l.cur = r.it
	}
}

func (l *lazyEntryIterator) At() logproto.Entry {
	if l.cur == nil {
		return logproto.Entry{}
	}
	return l.cur.At()
}

func (l *lazyEntryIterator) Labels() string {
	if l.cur == nil {
		return ""
	}
	return l.cur.Labels()
}

func (l *lazyEntryIterator) StreamHash() uint64 {
	if l.cur == nil {
		return 0
	}
	return l.cur.StreamHash()
}

func (l *lazyEntryIterator) Err() error { return l.err }

func (l *lazyEntryIterator) Close() error {
	l.cancel()
	if l.cur != nil {
		_ = l.cur.Close()
		l.cur = nil
	}
	// Drain whatever the loader had already produced.
	for r := range l.results {
		if r.it != nil {
			_ = r.it.Close()
		}
	}
	return nil
}
