package sqlite

import (
	"context"
	"fmt"
)

type Hooks[T any] struct {
	BeforePut []Hook[T]
	AfterPut  []Hook[T]
}

type Hook[T any] func(ctx context.Context, model *T) error

type HookBeforePut[T any] interface {
	BeforePut(ctx context.Context, tx *Tx[T]) error
}

func runBeforePut[T any](ctx context.Context, tx *Tx[T], hooks Hooks[T], model *T) error {
	if hook, ok := any(model).(HookBeforePut[T]); ok {
		if err := hook.BeforePut(ctx, tx); err != nil {
			return fmt.Errorf("before put hook: %w", err)
		}
	}

	for _, hook := range hooks.BeforePut {
		if err := hook(ctx, model); err != nil {
			return fmt.Errorf("global before put hook: %w", err)
		}
	}

	return nil
}

type HookAfterPut interface {
	AfterPut(ctx context.Context) error
}

func runAfterPut[T any](ctx context.Context, hooks Hooks[T], model *T) error {
	if hook, ok := any(model).(HookAfterPut); ok {
		if err := hook.AfterPut(ctx); err != nil {
			return fmt.Errorf("after put hook: %w", err)
		}
	}
	for _, hook := range hooks.AfterPut {
		if err := hook(ctx, model); err != nil {
			return fmt.Errorf("global after put hook: %w", err)
		}
	}
	return nil
}
