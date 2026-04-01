module;
#include <string>
#ifndef NDEBUG
#include <cassert>
#endif
#include <filesystem>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter_factory.h>
#ifdef WITH_OTLP_GRPC
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_factory.h>
#endif
#include <opentelemetry/logs/provider.h>
#include <opentelemetry/sdk/logs/logger_provider_factory.h>
#include <opentelemetry/sdk/logs/simple_log_record_processor_factory.h>

export module Logger;
import OTelOptions;
import OTelSpdLog;

namespace
{

std::shared_ptr<opentelemetry::sdk::logs::LoggerProvider> loggerProvider{nullptr};

void setVerbosityForSPDLOG(const int verbosity,
                           spdlog::logger *logger)
{
#ifndef NDEBUG
    assert(logger != nullptr);
#endif
    if (verbosity <= 1)
    {
        logger->set_level(spdlog::level::critical);
    }
    if (verbosity == 2){logger->set_level(spdlog::level::warn);}
    if (verbosity == 3){logger->set_level(spdlog::level::info);}
    if (verbosity >= 4){logger->set_level(spdlog::level::debug);}
}

}

namespace UWaveServer::Logger
{   


export std::shared_ptr<spdlog::logger>
    initialize(const int verbosity,
               const bool exportLogs,
               const OTelHTTPLogOptions &otelHTTPLogOptions)
{
    std::shared_ptr<spdlog::logger> logger{nullptr};
    auto consoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt> ();
    if (exportLogs)
    {
        namespace otel = opentelemetry;
        otel::exporter::otlp::OtlpHttpLogRecordExporterOptions httpOptions;
        httpOptions.url = otelHTTPLogOptions.url
                        + otelHTTPLogOptions.suffix;
        auto exporter
            = otel::exporter::otlp::OtlpHttpLogRecordExporterFactory::Create(httpOptions);
        auto processor
            = otel::sdk::logs::SimpleLogRecordProcessorFactory::Create(
                 std::move(exporter));
        loggerProvider
            = otel::sdk::logs::LoggerProviderFactory::Create(
                std::move(processor));
        std::shared_ptr<opentelemetry::logs::LoggerProvider> apiProvider = loggerProvider;
        otel::logs::Provider::SetLoggerProvider(apiProvider);

        auto otelLogger
            = std::make_shared<spdlog::sinks::OpenTelemetrySink<std::mutex>> ();

        logger
            = std::make_shared<spdlog::logger>
              (spdlog::logger ("OTelLogger", {otelLogger, consoleSink}));
    }
    else
    {
        logger
            = std::make_shared<spdlog::logger>
              (spdlog::logger ("", {consoleSink}));
    }
    // Verbosity
    ::setVerbosityForSPDLOG(verbosity, &*logger);
    return logger;
}

#ifdef WITH_OTLP_GRPC
export std::shared_ptr<spdlog::logger>
    initialize(const int verbosity,
               const bool exportLogs,
               const OTelGRPCLogOptions &otelGRPCLogOptions)
{
    std::shared_ptr<spdlog::logger> logger{nullptr};
    auto consoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt> (); 
    if (exportLogs)
    {   
        namespace otel = opentelemetry;
        otel::exporter::otlp::OtlpGrpcLogRecordExporterOptions grpcOptions;
        grpcOptions.endpoint = otelGRPCLogOptions.url;
        //grpcOptions.timeout = duration?
        grpcOptions.use_ssl_credentials = false;
        if (!otelGRPCLogOptions.certificatePath.empty() &&
            std::filesystem::exists(otelGRPCLogOptions.certificatePath))
        {
            grpcOptions.use_ssl_credentials = true;
            grpcOptions.ssl_credentials_cacert_path
                = otelGRPCLogOptions.certificatePath;
        }
        auto exporter
            = otel::exporter::otlp::OtlpGrpcLogRecordExporterFactory::Create(grpcOptions);
        auto processor
            = otel::sdk::logs::SimpleLogRecordProcessorFactory::Create(
                 std::move(exporter));
        loggerProvider
            = otel::sdk::logs::LoggerProviderFactory::Create(
                std::move(processor));
        std::shared_ptr<opentelemetry::logs::LoggerProvider> apiProvider = loggerProvider;
        otel::logs::Provider::SetLoggerProvider(apiProvider);

        auto otelLogger
            = std::make_shared<spdlog::sinks::OpenTelemetrySink<std::mutex>> ();

        logger
            = std::make_shared<spdlog::logger>
              (spdlog::logger ("OTelLogger", {otelLogger, consoleSink}));
    }
    else
    {
        logger
            = std::make_shared<spdlog::logger>
              (spdlog::logger ("", {consoleSink}));
    }
    // Verbosity
    ::setVerbosityForSPDLOG(verbosity, &*logger);
    return logger;
}
#endif

export void cleanup()
{
    if (loggerProvider)
    {   
        loggerProvider->ForceFlush();
        loggerProvider.reset();
        std::shared_ptr<opentelemetry::logs::LoggerProvider> none;
        opentelemetry::logs::Provider::SetLoggerProvider(none);
    }
    loggerProvider = nullptr;         
}

}
