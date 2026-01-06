#ifndef METRICS_EXPORTER_HPP
#define METRICS_EXPORTER_HPP
#include <string>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/meter_provider.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/exporters/ostream/metric_exporter_factory.h>
#include <opentelemetry/exporters/prometheus/exporter_factory.h>
#include <opentelemetry/exporters/prometheus/exporter_options.h>
#include <opentelemetry/sdk/metrics/meter_context.h>
#include <opentelemetry/sdk/metrics/meter_context_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/provider.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/meter_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/view_factory.h>

namespace
{
void initializeMetrics(const std::string &prometheusURL)
{
    const std::string VERSION{"1.2.0"};
    const std::string SCHEMA{"https://opentelemetry.io/schemas/1.2.0"};

    opentelemetry::exporter::metrics::PrometheusExporterOptions
        prometheusOptions;
    prometheusOptions.url = prometheusURL;
    auto prometheusExporter
        = opentelemetry::exporter::metrics::PrometheusExporterFactory::Create(
              prometheusOptions);
    // Initialize and set the global MeterProvider
    auto providerInstance 
        = opentelemetry::sdk::metrics::MeterProviderFactory::Create();
    auto *meterProvider
        = static_cast<opentelemetry::sdk::metrics::MeterProvider *>
          (providerInstance.get());
    meterProvider->AddMetricReader(std::move(prometheusExporter));

    // Histogram config
    auto histogramInstrumentSelector
        = opentelemetry::sdk::metrics::InstrumentSelectorFactory::Create(
             opentelemetry::sdk::metrics::InstrumentType::kHistogram,
             "database_write_time_histogram",
             "s");  
    auto histogramMeterSelector
        = opentelemetry::sdk::metrics::MeterSelectorFactory::Create(
             "database_write_time", VERSION, SCHEMA);
    auto histogramAggregationConfig
        = std::make_shared<opentelemetry::sdk::metrics::HistogramAggregationConfig> (); 
    histogramAggregationConfig->boundaries_
        = std::vector<double> {0.0,
                               0.0001,
                               0.0005,
                               0.0010, 
                               0.0050,
                               0.0100,
                               0.5000,
                               1.0000,
                               1000.0};
    auto histogramView 
        = opentelemetry::sdk::metrics::ViewFactory::Create(
             "database_write_time",
             "Time required to write packet to the databaes",
             opentelemetry::sdk::metrics::AggregationType::kHistogram,
             histogramAggregationConfig);
    providerInstance->AddView(std::move(histogramInstrumentSelector),
                      std::move(histogramMeterSelector),
                      std::move(histogramView));

    std::shared_ptr<opentelemetry::metrics::MeterProvider>
        provider(std::move(providerInstance));
    opentelemetry::sdk::metrics::Provider::SetMeterProvider(provider);
}

void cleanupMetrics()
{
     std::shared_ptr<opentelemetry::metrics::MeterProvider> none;
     opentelemetry::sdk::metrics::Provider::SetMeterProvider(none);
}
}

#endif
