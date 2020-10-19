// Copyright 2020 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package traceidprocessor

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/traceidprocessor/cache"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

var (
	errNoMissingCacheEndpoint = errors.New("the CacheEndpoint property is empty")
)

type traceidprocessor struct {
	logger *zap.Logger
	config Config
	cche   cache.Cache
}

// Create new processor
func newProcessor(logger *zap.Logger, cfg configmodels.Processor) (*traceidprocessor, error) {
	logger.Info("building processor")

	oCfg := cfg.(*Config)
	// we need a "CacheEndpoint" value
	if len(oCfg.CacheEndpoint) == 0 {
		return nil, fmt.Errorf("invalid cache endpoint: %w", errNoMissingCacheEndpoint)
	}

	cacheProvider := cache.NewProvider()
	cche, err := cacheProvider(oCfg.CacheEndpoint, oCfg.TTL)
	if err != nil {
		return nil, err
	}

	return &traceidprocessor{
		logger: logger,
		config: *oCfg,
		cche:   cche,
	}, nil
}

func (tp *traceidprocessor) Start(_ context.Context, _ component.Host) error {
	return tp.cche.Start()
}

func (tp *traceidprocessor) Shutdown(context.Context) error {
	return tp.cche.Stop()
}

func (tp *traceidprocessor) ProcessTraces(ctx context.Context, td pdata.Traces) (pdata.Traces, error) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		if rs.IsNil() {
			continue
		}

		for j := 0; j < rs.InstrumentationLibrarySpans().Len(); j++ {
			ispans := rs.InstrumentationLibrarySpans().At(j)
			if ispans.IsNil() {
				continue
			}
			spans := ispans.Spans()
			for k := 0; k < spans.Len(); k++ {
				tp.processTraceID(ctx, spans.At(k))
			}
		}
	}

	return td, nil
}

func (tp *traceidprocessor) processTraceID(ctx context.Context, span pdata.Span) {
	if span.IsNil() {
		return
	}
	traceID := span.TraceID()
	start := int64(span.StartTime())

	var arg string
	if start == 0 {
		arg = fmt.Sprintf("%x", time.Now().Unix())
	} else {
		arg = fmt.Sprintf("%x", (start / int64(time.Second)))
	}

	epoch, err := tp.cche.GetOrSet(traceID.HexString(), arg)
	if err != nil {
		return
	}

	b, err := hex.DecodeString(epoch)
	if err != nil {
		return
	}

	traceIDBytes := traceID.Bytes()
	span.SetTraceID(pdata.NewTraceID(append(append(b[:4], traceIDBytes[4:]...))))
}

func (tp *traceidprocessor) cacheTTL() int {
	return tp.cche.TTL()
}
