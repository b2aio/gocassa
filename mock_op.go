package gocassa

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type mockOp struct {
	mock.Mock

	options      Options
	funcs        []func(mockOp) error
	preflightErr error
}

func newOp(f func(mockOp) error) mockOp {
	return mockOp{
		funcs: []func(mockOp) error{f},
	}
}

func (m mockOp) Add(ops ...Op) Op {
	return mockMultiOp{m}.Add(ops...)
}

func (m mockOp) Run() (err error) {
	defer func() {
		if r := recover(); r != nil {
			for _, f := range m.funcs {
				err = f(m)
				if err != nil {
					return 
				}
			}
			return
		}
	}()

	returnArgs := m.Called()
	if returnFunc, ok := returnArgs.Get(0).(func() error); ok {
		err = returnFunc()
	} else {
		err = returnArgs.Get(0).(error)
	}

	return
}

func (m mockOp) RunWithContext(ctx context.Context) error {
	return m.WithOptions(Options{Context: ctx}).Run()
}

func (m mockOp) Options() Options {
	return m.options
}

func (m mockOp) WithOptions(opt Options) Op {
	m.options = m.options.Merge(opt)
	return m
}

func (m mockOp) RunAtomically() error {
	return m.Run()
}

func (m mockOp) RunLoggedBatchWithContext(ctx context.Context) error {
	return m.WithOptions(Options{Context: ctx}).Run()
}

func (m mockOp) RunAtomicallyWithContext(ctx context.Context) error {
	return m.RunLoggedBatchWithContext(ctx)
}

func (m mockOp) GenerateStatement() Statement {
	return noOpStatement{}
}

func (m mockOp) QueryExecutor() QueryExecutor {
	return nil
}

func (m mockOp) Preflight() error {
	return m.preflightErr
}

type mockMultiOp []Op

func (mo mockMultiOp) Run() error {
	if err := mo.Preflight(); err != nil {
		return err
	}
	for _, op := range mo {
		if err := op.Run(); err != nil {
			return err
		}
	}
	return nil
}

func (mo mockMultiOp) RunWithContext(ctx context.Context) error {
	return mo.WithOptions(Options{Context: ctx}).Run()
}

func (mo mockMultiOp) RunAtomically() error {
	return mo.Run()
}

func (mo mockMultiOp) RunLoggedBatchWithContext(ctx context.Context) error {
	return mo.WithOptions(Options{Context: ctx}).Run()
}

func (mo mockMultiOp) RunAtomicallyWithContext(ctx context.Context) error {
	return mo.RunLoggedBatchWithContext(ctx)
}

func (mo mockMultiOp) GenerateStatement() Statement {
	return noOpStatement{}
}

func (mo mockMultiOp) QueryExecutor() QueryExecutor {
	return nil
}

func (mo mockMultiOp) Add(inOps ...Op) Op {
	ops := make(mockMultiOp, 0, len(inOps))
	for _, op := range inOps {
		// If any multiOps were passed, flatten them out
		switch op := op.(type) {
		case mockMultiOp:
			ops = append(ops, op...)
		case mockOp:
			ops = append(ops, op)
		default:
			panic("can't Add non-mock ops to mockMultiOp")
		}
	}
	return append(mo, ops...)
}

func (mo mockMultiOp) Options() Options {
	var opts Options
	for _, op := range mo {
		opts = opts.Merge(op.Options())
	}
	return opts
}

func (mo mockMultiOp) WithOptions(opts Options) Op {
	result := make(mockMultiOp, len(mo))
	for i, op := range mo {
		result[i] = op.WithOptions(opts)
	}
	return result
}

func (mo mockMultiOp) Preflight() error {
	for _, op := range mo {
		if err := op.Preflight(); err != nil {
			return err
		}
	}
	return nil
}

