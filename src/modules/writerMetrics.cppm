module;

#include <iostream>
#include <atomic>
#include <string>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/meter_provider.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/exporters/otlp/otlp_http.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h>
#ifdef WITH_OTLP_GRPC
#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_options.h>
#endif
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h>
#include <opentelemetry/sdk/metrics/meter_context.h>
#include <opentelemetry/sdk/metrics/meter_context_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/provider.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/meter_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/view_factory.h>

export module WriterMetrics;
import OTelOptions;

namespace
{

void createDatabaseWriterHistogram(
    opentelemetry::sdk::metrics::MeterProvider *metricsProvider)
{
    // Histogram config
    auto histogramInstrumentSelector
        = opentelemetry::sdk::metrics::InstrumentSelectorFactory::Create(
             opentelemetry::sdk::metrics::InstrumentType::kHistogram,
             "database_write_duration_histogram",
             "{s}");
    auto histogramMeterSelector
        = opentelemetry::sdk::metrics::MeterSelectorFactory::Create(
             "database_write_duration",
             "https://opentelemetry.io/schemas/1.2.0",
             "1.2.0");
    auto histogramAggregationConfig
        = std::make_shared
          <
             opentelemetry::sdk::metrics::HistogramAggregationConfig
          > ();
    histogramAggregationConfig->boundaries_
        = std::vector<double> {0.0,
                               0.0005,
                               0.0010,
                               0.0050,
                               0.0100,
                               0.5000,
                               1.0000,
                               1000.0};
    auto histogramView
        = opentelemetry::sdk::metrics::ViewFactory::Create(
             "database_write_duration",
              "Time required to write packet to the database.",
              opentelemetry::sdk::metrics::AggregationType::kHistogram,
              histogramAggregationConfig);
    metricsProvider->AddView(std::move(histogramInstrumentSelector),
                             std::move(histogramMeterSelector),
                             std::move(histogramView));
}

}

namespace UWaveServer::Metrics
{

bool metricsInitialized{false};

export 
void initialize(
    const bool exportMetrics,
    const UWaveServer::OTelHTTPMetricsOptions &otelHTTPMetricsOptions)
{
    if (!exportMetrics){return;}
    namespace otel = opentelemetry;
    otel::exporter::otlp::OtlpHttpMetricExporterOptions exporterOptions;
    exporterOptions.url = otelHTTPMetricsOptions.url
                        + otelHTTPMetricsOptions.suffix;
    //exporterOptions.console_debug = debug != "" && debug != "0" && debug != "no";
    exporterOptions.content_type
        = otel::exporter::otlp::HttpRequestContentType::kBinary;

    auto exporter
        = otel::exporter::otlp::OtlpHttpMetricExporterFactory::Create(
             exporterOptions);
    // Initialize and set the global MeterProvider
    otel::sdk::metrics::PeriodicExportingMetricReaderOptions readerOptions;
    readerOptions.export_interval_millis
        = otelHTTPMetricsOptions.exportInterval;
    readerOptions.export_timeout_millis
        = otelHTTPMetricsOptions.exportTimeOut;

    auto reader
        = otel::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(
             std::move(exporter),
             readerOptions);

    auto context = otel::sdk::metrics::MeterContextFactory::Create();
    context->AddMetricReader(std::move(reader));

    auto metricsProvider
        = otel::sdk::metrics::MeterProviderFactory::Create(
             std::move(context));

    // Histogram config
    createDatabaseWriterHistogram(metricsProvider.get());
    /*
    auto histogramInstrumentSelector
        = otel::sdk::metrics::InstrumentSelectorFactory::Create(
             opentelemetry::sdk::metrics::InstrumentType::kHistogram,
             "database_write_duration_histogram",
             "{s}");  
    auto histogramMeterSelector
        = otel::sdk::metrics::MeterSelectorFactory::Create(
             "database_write_duration",
             "https://opentelemetry.io/schemas/1.2.0",
             "1.2.0");
    auto histogramAggregationConfig
        = std::make_shared<otel::sdk::metrics::HistogramAggregationConfig> ();  
    histogramAggregationConfig->boundaries_
        = std::vector<double> {0.0,
                               0.0005,
                               0.0010, 
                               0.0050,
                               0.0100,
                               0.5000,
                               1.0000,
                               1000.0};
    auto histogramView 
        = opentelemetry::sdk::metrics::ViewFactory::Create(
             "database_write_duration",
              "Time required to write packet to the database.",
              opentelemetry::sdk::metrics::AggregationType::kHistogram,
              histogramAggregationConfig);
    metricsProvider->AddView(std::move(histogramInstrumentSelector),
                             std::move(histogramMeterSelector),
                             std::move(histogramView));
    */

    std::shared_ptr<otel::metrics::MeterProvider>
        provider(std::move(metricsProvider));
    otel::sdk::metrics::Provider::SetMeterProvider(provider);
    metricsInitialized = true;
}

#ifdef WITH_OTLP_GRPC
export
void initialize(
    const bool exportMetrics,
    const UWaveServer::OTelGRPCMetricsOptions &otelGRPCMetricsOptions)
{
    if (!exportMetrics){return;}
    namespace otel = opentelemetry;
    otel::exporter::otlp::OtlpGrpcMetricExporterOptions exporterOptions;
    exporterOptions.endpoint = otelGRPCMetricsOptions.url;
    exporterOptions.use_ssl_credentials = false;
    if (!otelGRPCMetricsOptions.certificatePath.empty())
    {   
        exporterOptions.use_ssl_credentials = true;
        exporterOptions.ssl_credentials_cacert_path
           = otelGRPCMetricsOptions.certificatePath;
    }   
    auto exporter
        = otel::exporter::otlp::OtlpGrpcMetricExporterFactory::Create(
             exporterOptions);

    // Initialize and set the global MeterProvider
    otel::sdk::metrics::PeriodicExportingMetricReaderOptions readerOptions;
    readerOptions.export_interval_millis
        = otelGRPCMetricsOptions.exportInterval;
    readerOptions.export_timeout_millis
        = otelGRPCMetricsOptions.exportTimeOut;

    auto reader
        = otel::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(
             std::move(exporter),
             readerOptions);

    auto context = otel::sdk::metrics::MeterContextFactory::Create();
    context->AddMetricReader(std::move(reader));

    auto metricsProvider
        = otel::sdk::metrics::MeterProviderFactory::Create(
             std::move(context));

    // Histogram config
    createDatabaseWriterHistogram(metricsProvider.get());

    std::shared_ptr<otel::metrics::MeterProvider>
        provider(std::move(metricsProvider));
    otel::sdk::metrics::Provider::SetMeterProvider(provider);
    metricsInitialized = true;
}

#endif

export void cleanup()
{
    if (metricsInitialized)
    {
        std::shared_ptr<opentelemetry::metrics::MeterProvider> none;
        opentelemetry::sdk::metrics::Provider::SetMeterProvider(none);
    }
    metricsInitialized = false;
}

export class MetricsSingleton
{
public:
    enum class Reason
    {
        Duplicate,
        Expired,
        Future
    };
    static MetricsSingleton &getInstance()
    {   
        std::mutex mutex;
        std::scoped_lock lock{mutex};
        static MetricsSingleton instance;
        return instance;
    }   
    void incrementReceivedPacketsCounter() noexcept
    {   
        mReceivedPacketsCounter.fetch_add(1, std::memory_order_relaxed);
    }   
    [[nodiscard]] int64_t getReceivedPacketsCount() const noexcept
    {   
        return mReceivedPacketsCounter.load();
    }   
    void incrementWrittenPacketsCounter() noexcept
    {
        mWrittenPacketsCounter.fetch_add(1, std::memory_order_relaxed);
    }
    [[nodiscard]] int64_t getWrittenPacketsCount() const noexcept
    {   
        return mWrittenPacketsCounter.load();
    }   
    void incrementNotWrittenPacketsCounter() noexcept
    {
        mWrittenPacketsCounter.fetch_add(1, std::memory_order_relaxed);
    }
    [[nodiscard]] int64_t getNotWrittenPacketsCount() const noexcept
    {
        return mWrittenPacketsCounter.load();
    }
    void incrementRejectedPacketsCounter(const Reason reason) noexcept
    {   
        mRejectedPacketsCounter.fetch_add(1, std::memory_order_relaxed);
        if (reason == Reason::Duplicate)
        {
        }
        else if (reason == Reason::Expired)
        {
        }
        else if (reason == Reason::Future)
        {
        }
    }   
    [[nodiscard]] int64_t getRejectedPacketsCount() const noexcept
    {
        return mRejectedPacketsCounter.load();
    }
    void resetCounters()
    {   
        mReceivedPacketsCounter.store(0);
        mWrittenPacketsCounter.store(0);
        mNotWrittenPacketsCounter.store(0);
        mRejectedPacketsCounter.store(0);
        mFuturePacketsCounter.store(0);
        mExpiredPacketsCounter.store(0);
        mDuplicatePacketsCounter.store(0);
    }   
private:
    MetricsSingleton() = default;
    ~MetricsSingleton() = default;
    std::atomic<int64_t> mReceivedPacketsCounter{0};
    std::atomic<int64_t> mWrittenPacketsCounter{0};
    std::atomic<int64_t> mNotWrittenPacketsCounter{0};
    std::atomic<int64_t> mRejectedPacketsCounter{0};
    std::atomic<int64_t> mFuturePacketsCounter{0};
    std::atomic<int64_t> mExpiredPacketsCounter{0};
    std::atomic<int64_t> mDuplicatePacketsCounter{0};
};

export void initializeMetricsSingleton()
{
    MetricsSingleton::getInstance();
}

export void observeNumberOfPacketsReceived(
    opentelemetry::metrics::ObserverResult observerResult,
    void *)
{
    if (opentelemetry::nostd::holds_alternative
        <
            opentelemetry::nostd::shared_ptr
            <
                opentelemetry::metrics::ObserverResultT<int64_t>
            >
        > (observerResult))
    {
        auto observer = opentelemetry::nostd::get
        <
            opentelemetry::nostd::shared_ptr
            <
               opentelemetry::metrics::ObserverResultT<int64_t>
            >   
        > (observerResult);
        try
        {
            auto &instance = MetricsSingleton::getInstance();
            auto value = instance.getReceivedPacketsCount();
            observer->Observe(value);
        }
        catch (const std::exception &e) 
        {

        }
    }
}

export void observeNumberOfPacketsWritten(
    opentelemetry::metrics::ObserverResult observerResult,
    void *)
{
    if (opentelemetry::nostd::holds_alternative
        <
            opentelemetry::nostd::shared_ptr
            <
                opentelemetry::metrics::ObserverResultT<int64_t>
            >
        > (observerResult))
    {
        auto observer = opentelemetry::nostd::get
        <
            opentelemetry::nostd::shared_ptr
            <
               opentelemetry::metrics::ObserverResultT<int64_t>
            >   
        > (observerResult);
        try
        {
            auto &instance = MetricsSingleton::getInstance();
            auto value = instance.getWrittenPacketsCount();
            observer->Observe(value);
        }
        catch (const std::exception &e) 
        {

        }
    }
}

export void observeNumberOfPacketsNotWritten(
    opentelemetry::metrics::ObserverResult observerResult,
    void *)
{
    if (opentelemetry::nostd::holds_alternative
        <
            opentelemetry::nostd::shared_ptr
            <
                opentelemetry::metrics::ObserverResultT<int64_t>
            >
        > (observerResult))
    {   
        auto observer = opentelemetry::nostd::get
        <
            opentelemetry::nostd::shared_ptr
            <
               opentelemetry::metrics::ObserverResultT<int64_t>
            >
        > (observerResult);
        try
        {
            auto &instance = MetricsSingleton::getInstance();
            auto value = instance.getNotWrittenPacketsCount();
            observer->Observe(value);
        }
        catch (const std::exception &e) 
        {

        }
    }   
}


}
