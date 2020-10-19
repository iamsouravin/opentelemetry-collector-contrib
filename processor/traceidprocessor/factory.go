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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

const (
	// The value of "type" key in configuration.
	typeStr = "traceid"
)

var processorCapabilities = component.ProcessorCapabilities{MutatesConsumedData: true}

// NewFactory creates a factory for the name processor.
func NewFactory() component.ProcessorFactory {
	return processorhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		processorhelper.WithTraces(createTraceProcessor),
	)
}

func createDefaultConfig() configmodels.Processor {
	return &Config{
		ProcessorSettings: configmodels.ProcessorSettings{
			TypeVal: typeStr,
			NameVal: typeStr,
		},
		CacheEndpoint: "local://",
		TTL:           604800,
	}
}

func createTraceProcessor(_ context.Context, params component.ProcessorCreateParams, cfg configmodels.Processor, nextConsumer consumer.TraceConsumer) (component.TraceProcessor, error) {
	tp, err := newProcessor(params.Logger, cfg)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewTraceProcessor(
		cfg,
		nextConsumer,
		tp,
		processorhelper.WithCapabilities(processorCapabilities),
		processorhelper.WithStart(tp.Start),
		processorhelper.WithShutdown(tp.Shutdown),
	)
}
