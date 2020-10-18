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
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

var (
	errNoMissingFromChar = errors.New("the FromChar property is empty")
	errNoMissingToChar   = errors.New("the ToChar property is empty")
)

type nameprocessor struct {
	logger *zap.Logger
	config Config
}

// Create new processor
func newProcessor(logger *zap.Logger, cfg configmodels.Processor) (*nameprocessor, error) {
	logger.Info("building processor")

	oCfg := cfg.(*Config)
	// we need a "FromChar" value
	if len(oCfg.FromChar) == 0 {
		return nil, fmt.Errorf("invalid character to replace: %w", errNoMissingFromChar)
	}

	// we need a "ToChar" value
	if len(oCfg.ToChar) == 0 {
		return nil, fmt.Errorf("invalid substitution character: %w", errNoMissingToChar)
	}

	return &nameprocessor{
		logger: logger,
		config: *oCfg,
	}, nil
}

func (np *nameprocessor) ProcessTraces(ctx context.Context, td pdata.Traces) (pdata.Traces, error) {
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
				np.processName(ctx, spans.At(k))
			}
		}
	}

	return td, nil
}

func (np *nameprocessor) processName(ctx context.Context, span pdata.Span) {
	if span.IsNil() {
		return
	}
	span.SetName(strings.Replace(span.Name(), np.config.FromChar, np.config.ToChar, -1))
}
