package gocassa

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

type concurrentMultiOp struct {
	multiOp
	opts        Options
	concurrency int
}

func NoopWithConcurrentExecution(concurrency int) Op {
	return &concurrentMultiOp{
		multiOp:     multiOp(nil),
		concurrency: concurrency,
	}
}

func (cmo *concurrentMultiOp) RunWithContext(ctx context.Context) error {
	return cmo.WithOptions(Options{Context: ctx}).Run()
}
func (cmo *concurrentMultiOp) WithOptions(opts Options) Op {
	multiOpCopy := make(multiOp, len(cmo.multiOp))
	copy(multiOpCopy, cmo.multiOp)
	return &concurrentMultiOp{
		multiOp:     multiOpCopy,
		concurrency: cmo.concurrency,
		opts:        cmo.opts.Merge(opts),
	}
}

func (cmo *concurrentMultiOp) Run() error {
	if err := cmo.Preflight(); err != nil {
		return err
	}
	var g *errgroup.Group
	if cmo.opts.Context != nil {
		g, cmo.opts.Context = errgroup.WithContext(cmo.opts.Context)
	} else {
		g = &errgroup.Group{}
	}
	g.SetLimit(cmo.concurrency)

	for _, op := range cmo.multiOp {
		op := op.WithOptions(cmo.opts)
		fn := func() (err error) {
			defer func() {
				if v := recover(); v != nil {
					err = fmt.Errorf("concurrent multiop panic: %v", v)
				}
			}()
			err = op.Run()
			return
		}
		g.Go(fn)
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
