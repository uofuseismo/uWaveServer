#ifndef SERVER_METRICS_HPP
#define SERVER_METRICS_HPP
#include <chrono>
#include <atomic>
#include <map>
#include <spdlog/spdlog.h>
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/meter_provider.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/view/view_factory.h>
//#include "uSEEDLinkToRingServer/packet.hpp"
//#include "uSEEDLinkToRingServer/streamIdentifier.hpp"
//#include "getNow.hpp"

#define VERSION "1.2.0"
#define SCHEMA "https://opentelemetry.io/schemas/1.2.0"

namespace
{

opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    mPacketsWrittenCounter;
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    mPacketsNotWrittenCounter;
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    mPacketsReceivedCounter;
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    mExpiredPacketsCounter;
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    mFuturePacketsCounter;
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    mInvalidPacketsCounter;
opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
    mWriteHistogram{nullptr};

template<typename T>
class ObservableMap
{
public:
    /// If the key, value pair is present then value will be added to it
    /// otherwise the key, value pair is initialized to the given value.
    void add_or_assign(const std::string &key, const T value)
    {
        std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mMap.find(key);
        if (idx != mMap.end())
        {
            idx->second = idx->second + value;
        }
        else
        {
            mMap.insert( std::pair {key, value} );
        }
    }
    [[nodiscard]] std::set<std::string> keys() const noexcept
    {   
        std::set<std::string> result;
        std::lock_guard<std::mutex> lock(mMutex);
        for (const auto &item : mMap)
        {
            result.insert(item.first);
        }
        return result;
    } 
    [[nodiscard]] std::optional<T> operator[](const std::string &key) const
    {   
        std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mMap.find(key);
        if (idx != mMap.end())
        {
            return std::make_optional(idx->second);
        }
        return std::nullopt;
    }   
    [[nodiscard]] std::optional<T> at(const std::string &key) const
    {   
        std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mMap.find(key);
        if (idx != mMap.end())
        {
            return std::make_optional (idx->second);
        }
        return std::nullopt;
    }   
    [[nodiscard]] size_t size() const noexcept
    {   
        std::lock_guard<std::mutex> lock(mMutex);
        return mMap.size();
    }   
    mutable std::mutex mMutex;
    std::map<std::string, T> mMap;
};

::ObservableMap<int64_t> mObservablePacketsWritten;
::ObservableMap<int64_t> mObservablePacketsNotWritten;
::ObservableMap<int64_t> mObservablePacketsReceived;
::ObservableMap<int64_t> mObservableInvalidPackets;
::ObservableMap<int64_t> mObservableExpiredPackets;
::ObservableMap<int64_t> mObservableFuturePackets;

void observePacketsWritten(
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
        auto keys = mObservablePacketsWritten.keys();
        for (const auto &key : keys)
        {
            try
            {
                auto value = mObservablePacketsWritten[key];
                if (value)
                {
                    std::map<std::string, std::string>
                       attribute{ {"database", key} };
                    observer->Observe(*value, attribute);
                }
                else
                {
                    throw std::runtime_error("Could not find " + key 
                                           + " in map");
                }
            }
            catch (const std::exception &e) 
            {
                spdlog::warn(e.what());
            }
        }
    }   
}

void observePacketsNotWritten(
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
        auto keys = mObservablePacketsNotWritten.keys();
        for (const auto &key : keys)
        {
            try
            {
                auto value = mObservablePacketsNotWritten[key];
                if (value)
                {
                    std::map<std::string, std::string>
                       attribute{ {"database", key} };
                    observer->Observe(*value, attribute);
                }
                else
                {
                    throw std::runtime_error("Could not find " + key 
                                           + " in map");
                }
            }
            catch (const std::exception &e) 
            {
                spdlog::warn(e.what());
            }
        }
    }   
}
/*

void observeClientErrorResponses(
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
        auto keys = mObservableClientErrorResponses.keys();
        for (const auto &key : keys)
        {
            try
            {
                auto value = mObservableClientErrorResponses[key];
                if (value)
                {
                    std::map<std::string, std::string>
                       attribute{ {"route", key} };
                    observer->Observe(*value, attribute);
                }
                else
                {
                    throw std::runtime_error("Could not find " + key 
                                           + " in map");
                }
            }
            catch (const std::exception &e) 
            {
                spdlog::warn(e.what());
            }
        }
    }   
}
*/

void initializeWriterMetrics(const std::string &applicationName)
{
    // Need a provider from which to get a meter.  This is initialized
    // once and should last the duration of the application.
    auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
 
    // Meter will be bound to application (library, module, class, etc.)
    // so as to identify who is genreating these metrics.
    auto meter = provider->GetMeter(applicationName, "1.2.0");
    
    // Create the metric instruments (instruments are used to report
    // measurements)

    // Succesful responses
    mPacketsWrittenCounter
        = meter->CreateInt64ObservableCounter(
             "packets_written_to_database_counter",
             "Number of packets successfully written to the database",
             "packets");
    mPacketsWrittenCounter->AddCallback(observePacketsWritten, nullptr);

    // Server error responses
    mPacketsNotWrittenCounter
        = meter->CreateInt64ObservableCounter(
             "packets_not_written_to_database_counter",
             "Number of packets unsuccessfully written to the database",
             "packets");
    mPacketsNotWrittenCounter->AddCallback(observePacketsNotWritten, nullptr);


/*
    // Client error responses
    mClientErrorResponseCounter
        = meter->CreateInt64ObservableCounter(
             "client_error_responses_counter",
             "Number of client error (400) responses",
             "responses");
    mClientErrorResponseCounter->AddCallback(observeClientErrorResponses, nullptr);
*/
/*
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
        = std::vector<double> {0.0, 0.005, 0.01, 0.5, 1.0, 5.0, 10.0, 300.0};
    auto histogramView 
        = opentelemetry::sdk::metrics::ViewFactory::Create(
             "database_write_time",
             "Time required to write packet to the databaes",
             opentelemetry::sdk::metrics::AggregationType::kHistogram,
             histogramAggregationConfig);
*/
/*
    provider->AddView(std::move(histogramInstrumentSelector),
                      std::move(histogramMeterSelector),
                      std::move(histogramView));
 */    
    auto histogramMeter = provider->GetMeter("database_write_time", VERSION); 
    mWriteHistogram
       = histogramMeter->CreateDoubleHistogram(
            "database_write_time_histogram",
            "Time required to write packet to the database",
            "s");
/*
    mWriteHistogram
       = meter->CreateDoubleHistogram(
            "database_write_time_histogram",
            "Time required to write packet to the database",
            "s");
*/
}

}

#endif
