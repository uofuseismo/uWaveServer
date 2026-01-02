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

namespace
{

opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    mSuccessResponseCounter;
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    mServerErrorResponseCounter;
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    mClientErrorResponseCounter;

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

::ObservableMap<int64_t> mObservableSuccessResponses;
::ObservableMap<int64_t> mObservableServerErrorResponses;
::ObservableMap<int64_t> mObservableClientErrorResponses;

void observeSuccessfulResponses(
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
        auto keys = mObservableSuccessResponses.keys();
        for (const auto &key : keys)
        {
            try
            {
                auto value = mObservableSuccessResponses[key];
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

void observeServerErrorResponses(
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
        auto keys = mObservableServerErrorResponses.keys();
        for (const auto &key : keys)
        {
            try
            {
                auto value = mObservableServerErrorResponses[key];
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

void initializeImportMetrics(const std::string &applicationName)
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
    mSuccessResponseCounter
        = meter->CreateInt64ObservableCounter(
             "success_responses_counter",
             "Number of success (200) responses",
             "responses");
    mSuccessResponseCounter->AddCallback(observeSuccessfulResponses, nullptr);

    // Server error responses
    mServerErrorResponseCounter
        = meter->CreateInt64ObservableCounter(
             "server_error_responses_counter",
             "Number of server error (500) responses",
             "responses");
    mServerErrorResponseCounter->AddCallback(observeServerErrorResponses, nullptr);

    // Client error responses
    mClientErrorResponseCounter
        = meter->CreateInt64ObservableCounter(
             "client_error_responses_counter",
             "Number of client error (400) responses",
             "responses");
    mClientErrorResponseCounter->AddCallback(observeClientErrorResponses, nullptr);
}

}

#endif
