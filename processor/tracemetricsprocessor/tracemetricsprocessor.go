// Copyright  The OpenTelemetry Authors
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

package tracemetricsprocessor

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
)

type metricKey string

type traceMetricsProcessor struct {
	logger *zap.Logger
	config Config

	// The starting time of the data points.
	startTime time.Time

	lock            sync.RWMutex
	metricsExporter component.MetricsExporter
	nextConsumer    consumer.Traces

	// Call & Error counts.
	callSum map[metricKey]int64
}

func newProcessor(logger *zap.Logger, config config.Exporter, nextConsumer consumer.Traces) *traceMetricsProcessor {
	logger.Info("Building traceMetricsProcessor")
	pConfig := config.(*Config)

	return &traceMetricsProcessor{
		logger:       logger,
		config:       *pConfig,
		callSum:      make(map[metricKey]int64),
		startTime:    time.Now(),
		nextConsumer: nextConsumer,
	}
}

// GetCapabilities implements the component.Processor interface.
func (tp *traceMetricsProcessor) GetCapabilities() component.ProcessorCapabilities {
	tp.logger.Info("GetCapabilities for traceMetricsProcessor")
	return component.ProcessorCapabilities{MutatesConsumedData: false}
}

// Shutdown implements the component.Component interface.
func (tp *traceMetricsProcessor) Shutdown(ctx context.Context) error {
	tp.logger.Info("Shutting down traceMetricsProcessor")
	return nil
}

func (tp *traceMetricsProcessor) Start(ctx context.Context, host component.Host) error {
	tp.logger.Info("Starting traceMetricsProcessor")
	exporters := host.GetExporters()

	var availableMetricsExporters []string

	// The available list of exporters come from any configured metrics pipelines' exporters.
	for k, exp := range exporters[config.MetricsDataType] {
		metricsExp, ok := exp.(component.MetricsExporter)
		if !ok {
			return fmt.Errorf("the exporter %q isn't a metrics exporter", k.Name())
		}

		availableMetricsExporters = append(availableMetricsExporters, k.Name())

		tp.logger.Debug("Looking for traceMetricsProcessor exporter from available exporters",
			zap.String("traceMetricsProcessor-exporter", tp.config.MetricsExporter),
			zap.Any("available-exporters", availableMetricsExporters),
		)
		if k.Name() == tp.config.MetricsExporter {
			tp.metricsExporter = metricsExp
			tp.logger.Info("Found exporter", zap.String("traceMetricsProcessor-exporter", tp.config.MetricsExporter))
			break
		}
	}
	tp.logger.Info("Started traceMetricsProcessor")
	return nil
}

func (tp *traceMetricsProcessor) ConsumeTraces(ctx context.Context, traces pdata.Traces) error {
	tp.aggregateMetrics(traces)
	m := tp.buildMetrics()
	if err := tp.metricsExporter.ConsumeMetrics(ctx, *m); err != nil {
		return err
	}
	// Forward trace data unmodified.
	if err := tp.nextConsumer.ConsumeTraces(ctx, traces); err != nil {
		return err
	}
	return nil
}

func (tp *traceMetricsProcessor) ProcessTraces(ctx context.Context, td pdata.Traces) (pdata.Traces, error) {

	tp.aggregateMetrics(td)
	m := tp.buildMetrics()

	if err := tp.metricsExporter.ConsumeMetrics(ctx, *m); err != nil {
		return td, err
	}

	return td, nil
}

func (tp *traceMetricsProcessor) buildMetrics() *pdata.Metrics {
	m := pdata.NewMetrics()
	ilm := pdata.NewInstrumentationLibraryMetrics()
	rm := pdata.NewResourceMetrics()
	rm.Resource().Attributes().Insert("traceMetricsProcessorKey", pdata.NewAttributeValueString("traceMetricsProcessorVal"))
	m.ResourceMetrics().Append(rm)
	rm.InstrumentationLibraryMetrics().Append(ilm)
	ilm.InstrumentationLibrary().SetName("traceMetricsProcessor")
	tp.lock.RLock()
	tp.collectCallMetrics(ilm)
	tp.lock.RUnlock()
	return &m
}

// collectCallMetrics collects the raw call count metrics, writing the data
// into the given instrumentation library metrics.
func (tp *traceMetricsProcessor) collectCallMetrics(ilm pdata.InstrumentationLibraryMetrics) {
	for key := range tp.callSum {
		mCalls := pdata.NewMetric()
		ilm.Metrics().Append(mCalls)
		mCalls.SetDataType(pdata.MetricDataTypeIntSum)
		mCalls.SetName("bt_cpm_" + string(key))
		mCalls.IntSum().SetIsMonotonic(true)
		mCalls.IntSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)

		dpCalls := pdata.NewIntDataPoint()
		mCalls.IntSum().DataPoints().Append(dpCalls)
		dpCalls.SetStartTime(pdata.TimestampFromTime(tp.startTime))
		dpCalls.SetTimestamp(pdata.TimestampFromTime(time.Now()))
		dpCalls.SetValue(tp.callSum[key])
		dpCalls.LabelsMap().Insert("spanName", string(key))
	}
	tp.logger.Info("Collected metrics of size " + strconv.Itoa(ilm.Metrics().Len()))
}

func (tp *traceMetricsProcessor) aggregateMetrics(traces pdata.Traces) {
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rs := traces.ResourceSpans().At(i)
		tp.aggregateMetricsForServiceSpans(rs)
	}
}

func (tp *traceMetricsProcessor) aggregateMetricsForServiceSpans(rs pdata.ResourceSpans) {
	ilsSlice := rs.InstrumentationLibrarySpans()
	for j := 0; j < ilsSlice.Len(); j++ {
		ils := ilsSlice.At(j)
		spans := ils.Spans()
		for k := 0; k < spans.Len(); k++ {
			span := spans.At(k)
			if span.ParentSpanID().IsEmpty() {
				tp.aggregateMetricsForSpan(span)
			}
		}
	}
}

func (tp *traceMetricsProcessor) aggregateMetricsForSpan(span pdata.Span) {
	metricName := span.Name()
	tp.callSum[metricKey(metricName)]++
}
