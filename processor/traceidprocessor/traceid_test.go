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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"go.uber.org/zap"
)

func newTraceProcessor(cfg configmodels.Processor, nextTraceConsumer consumer.TraceConsumer) (component.TraceProcessor, error) {

	return createTraceProcessor(
		context.Background(),
		component.ProcessorCreateParams{Logger: zap.NewNop()},
		cfg,
		nextTraceConsumer,
	)
}

type multiTest struct {
	t *testing.T

	tp component.TraceProcessor

	nextTrace *exportertest.SinkTraceExporter
}

func newMultiTest(
	t *testing.T,
	cfg configmodels.Processor,
	errFunc func(err error),
) *multiTest {
	m := &multiTest{
		t:         t,
		nextTrace: &exportertest.SinkTraceExporter{},
	}

	tp, err := newTraceProcessor(cfg, m.nextTrace)
	if errFunc == nil {
		assert.NotNil(t, tp)
		require.NoError(t, err)
	} else {
		assert.Nil(t, tp)
		errFunc(err)
	}

	m.tp = tp
	return m
}

func (m *multiTest) testConsume(
	ctx context.Context,
	traces pdata.Traces,
	errFunc func(err error),
) pdata.Traces {
	errs := []error{
		m.tp.ConsumeTraces(ctx, traces),
	}

	for _, err := range errs {
		if errFunc != nil {
			errFunc(err)
		}
	}
	return traces
}

func (m *multiTest) assertTraceIDs(td pdata.Traces, traceIDs []string) {
	var gotTraceIDs []string
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
				span := spans.At(k)
				if span.IsNil() {
					continue
				}
				gotTraceIDs = append(gotTraceIDs, span.TraceID().HexString())
			}
		}
	}
	assert.Equal(m.t, gotTraceIDs, traceIDs)
}

func TestNewProcessor(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig()

	newMultiTest(t, cfg, nil)
}

func TestProcessorCacheEndpointMissing(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig()
	oCfg := cfg.(*Config)
	oCfg.CacheEndpoint = ""

	newMultiTest(t, cfg, func(err error) {
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "invalid cache endpoint: the CacheEndpoint property is empty")
	})
}

func TestProcessorTTLMissing(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig()
	oCfg := cfg.(*Config)
	oCfg.TTL = 0

	newMultiTest(t, NewFactory().CreateDefaultConfig(), nil)
}

func generateTraces(traceID string, startTime int64) (pdata.Traces, error) {
	t := pdata.NewTraces()
	rs := t.ResourceSpans()
	rs.Resize(1)
	rs.At(0).InitEmpty()
	rs.At(0).InstrumentationLibrarySpans().Resize(1)
	rs.At(0).InstrumentationLibrarySpans().At(0).Spans().Resize(1)
	span := rs.At(0).InstrumentationLibrarySpans().At(0).Spans().At(0)
	span.SetName("TestSpan")
	b, err := hex.DecodeString(traceID)
	if err != nil {
		return t, err
	}
	span.SetStartTime(pdata.TimestampUnixNano(startTime))
	span.SetTraceID(pdata.NewTraceID(b))
	return t, nil
}

func TestTraceIDNoReplace(t *testing.T) {
	m := newMultiTest(t, NewFactory().CreateDefaultConfig(), nil)

	ctx := context.Background()
	tsNano := time.Now().UnixNano()
	expectedTraceID := fmt.Sprintf("%xffffffffffffffffffffffff", (tsNano / int64(time.Second)))
	incomingTraces, err := generateTraces(expectedTraceID, tsNano)
	require.NoError(t, err)

	traces := m.testConsume(
		ctx,
		incomingTraces,
		func(err error) {
			assert.NoError(t, err)
		})
	m.assertTraceIDs(traces, []string{expectedTraceID})
}

func TestTraceIDReplace(t *testing.T) {
	m := newMultiTest(t, NewFactory().CreateDefaultConfig(), nil)

	ctx := context.Background()
	tsNano := time.Now().UnixNano()
	expectedTraceID := fmt.Sprintf("%xffffffffffffffffffffffff", (tsNano / int64(time.Second)))
	incomingTraces, err := generateTraces("ffffffffffffffffffffffffffffffff", tsNano)
	require.NoError(t, err)

	traces := m.testConsume(
		ctx,
		incomingTraces,
		func(err error) {
			assert.NoError(t, err)
		})
	m.assertTraceIDs(traces, []string{expectedTraceID})
}

func TestCapabilities(t *testing.T) {
	p, err := newTraceProcessor(
		NewFactory().CreateDefaultConfig(),
		exportertest.NewNopTraceExporter(),
	)
	assert.NoError(t, err)
	caps := p.GetCapabilities()
	assert.True(t, caps.MutatesConsumedData)
}
