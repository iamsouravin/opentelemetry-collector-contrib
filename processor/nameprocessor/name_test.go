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

package nameprocessor

import (
	"context"
	"testing"

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

func (m *multiTest) assertSpanNames(td pdata.Traces, spanNames []string) {
	var gotSpanNames []string
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
				gotSpanNames = append(gotSpanNames, span.Name())
			}
		}
	}
	assert.Equal(m.t, gotSpanNames, spanNames)
}

func TestNewProcessor(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig()

	newMultiTest(t, cfg, nil)
}

func TestProcessorFromCharMissing(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig()
	oCfg := cfg.(*Config)
	oCfg.FromChar = ""

	newMultiTest(t, cfg, func(err error) {
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "invalid character to replace: the FromChar property is empty")
	})
}

func TestProcessorToCharMissing(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig()
	oCfg := cfg.(*Config)
	oCfg.ToChar = ""

	newMultiTest(t, cfg, func(err error) {
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "invalid substitution character: the ToChar property is empty")
	})
}

func generateTraces(spanName string) pdata.Traces {
	t := pdata.NewTraces()
	rs := t.ResourceSpans()
	rs.Resize(1)
	rs.At(0).InitEmpty()
	rs.At(0).InstrumentationLibrarySpans().Resize(1)
	rs.At(0).InstrumentationLibrarySpans().At(0).Spans().Resize(1)
	span := rs.At(0).InstrumentationLibrarySpans().At(0).Spans().At(0)
	span.SetName(spanName)
	return t
}

func TestSpanNameNoReplace(t *testing.T) {
	m := newMultiTest(t, NewFactory().CreateDefaultConfig(), nil)

	ctx := context.Background()
	traces := m.testConsume(
		ctx,
		generateTraces("name"),
		func(err error) {
			assert.NoError(t, err)
		})
	m.assertSpanNames(traces, []string{"name"})
}

func TestSpanNameReplace(t *testing.T) {
	m := newMultiTest(t, NewFactory().CreateDefaultConfig(), nil)

	ctx := context.Background()
	traces := m.testConsume(
		ctx,
		generateTraces("name*"),
		func(err error) {
			assert.NoError(t, err)
		})
	m.assertSpanNames(traces, []string{"name+"})
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
