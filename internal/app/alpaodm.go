package app

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/New-Earth-Lab/asdk-go/asdk"
	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"golang.org/x/sync/errgroup"
)

type AlpaoDM struct {
	sdk          *asdk.Asdk
	subscription *aeron.Subscription
}

func NewAlpaoDM(serialName string, subscription *aeron.Subscription) (*AlpaoDM, error) {

	sdk, err := asdk.Init(serialName)
	if err != nil {
		return nil, fmt.Errorf("alpao: Unable to open %s", serialName)
	}

	return &AlpaoDM{
		sdk:          sdk,
		subscription: subscription,
	}, nil
}

func (a *AlpaoDM) Shutdown() error {
	return a.sdk.Release()
}

func (a *AlpaoDM) Run(subscription *aeron.Subscription, ctx context.Context) error {
	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		handler := aeron.NewFragmentAssembler(
			func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
				commandBuf := unsafe.Add(buffer.Ptr(), offset)
				_ = a.sdk.SendUnsafe(commandBuf)
				// sl := unsafe.Slice((*float64)(commandBuf), length/8)
				// err = dm.Sdk.Send(sl)
			}, aeron.DefaultFragmentAssemblyBufferLength)

		idleStrategy := idlestrategy.Sleeping{SleepFor: time.Millisecond}

		for {
			fragmentsRead := subscription.Poll(handler.OnFragment, 10)
			select {
			case <-ctx.Done():
				return nil
			default:
				idleStrategy.Idle(fragmentsRead)
			}
		}
	})
	return wg.Wait()
}
