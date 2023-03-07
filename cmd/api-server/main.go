package main

import (
	"context"
	"flag"
	"net/http"

	"github.com/go-faster/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/New-Earth-Lab/alpaodmservice/internal/api"
	"github.com/New-Earth-Lab/alpaodmservice/internal/app"
	"github.com/New-Earth-Lab/alpaodmservice/internal/oas"
	"github.com/lirm/aeron-go/aeron"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger) error {
		var arg struct {
			Addr                string
			MetricsAddr         string
			AeronUri            string
			AeronStreamId       int
			AlpaoDmSerialNumber string
		}
		flag.StringVar(&arg.Addr, "addr", "127.0.0.1:8080", "listen address")
		flag.StringVar(&arg.MetricsAddr, "metrics.addr", "127.0.0.1:9090", "metrics listen address")
		flag.StringVar(&arg.AeronUri, "aeron.Uri", "aeron:ipc", "Aeron channel URI")
		flag.IntVar(&arg.AeronStreamId, "aeron.StreamId", 1002, "Aeron stream ID")
		flag.StringVar(&arg.AlpaoDmSerialNumber, "alpaoDmSerialNumber", "BAX307", "ALPAO DM serial number")
		flag.Parse()

		lg.Info("Initializing",
			zap.String("http.addr", arg.Addr),
			zap.String("metrics.addr", arg.MetricsAddr),
			zap.String("aeron.Uri", arg.AeronUri),
			zap.Int("aeron.streamId", arg.AeronStreamId),
			zap.String("alpao.dmSerialNumber", arg.AlpaoDmSerialNumber),
		)

		m, err := app.NewMetrics(lg, app.Config{
			Addr: arg.MetricsAddr,
			Name: "api",
		})
		if err != nil {
			return errors.Wrap(err, "metrics")
		}

		oasServer, err := oas.NewServer(api.Handler{},
			oas.WithTracerProvider(m.TracerProvider()),
			oas.WithMeterProvider(m.MeterProvider()),
		)
		if err != nil {
			return errors.Wrap(err, "server init")
		}
		httpServer := http.Server{
			Addr:    arg.Addr,
			Handler: oasServer,
		}

		aeronContext := aeron.NewContext()

		a, err := aeron.Connect(aeronContext)
		if err != nil {
			return errors.Wrap(err, "aeron connect")
		}
		defer a.Close()

		subscription, err := a.AddSubscription(arg.AeronUri, int32(arg.AeronStreamId))
		if err != nil {
			return errors.Wrap(err, "aeron AddPublication")
		}
		defer subscription.Close()

		dm, err := app.NewAlpaoDM(arg.AlpaoDmSerialNumber, subscription)
		if err != nil {
			return errors.Wrap(err, "alpaodm")
		}

		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return m.Run(ctx)
		})
		g.Go(func() error {
			<-ctx.Done()
			if err := httpServer.Shutdown(ctx); err != nil {
				return errors.Wrap(err, "http")
			}
			if err := dm.Shutdown(); err != nil {
				return errors.Wrap(err, "alpaodm")
			}
			return nil
		})
		g.Go(func() error {
			defer lg.Info("HTTP server stopped")
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				return errors.Wrap(err, "http")
			}
			return nil
		})
		g.Go(func() error {
			return dm.Run(subscription, ctx)
		})

		return g.Wait()
	})
}
