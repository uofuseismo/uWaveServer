#include <string>
#include <chrono>
#include "uWaveServer/packetSanitizerOptions.hpp"

using namespace UWaveServer;

class PacketSanitizerOptions::PacketSanitizerOptionsImpl
{
public:
    std::chrono::seconds mMaxFutureTime{0};
    std::chrono::seconds mMaxLatency{500};
    std::chrono::seconds mLogBadDataInterval{3600};
    std::chrono::seconds mCircularBufferDuration{60};
    bool mLogBadData{true};
};

/// Constructor
PacketSanitizerOptions::PacketSanitizerOptions() :
    pImpl(std::make_unique<PacketSanitizerOptionsImpl> ())
{
}

/// Copy constructor
PacketSanitizerOptions::PacketSanitizerOptions(
    const PacketSanitizerOptions &options)
{
    *this = options;
}

/// Move constructor
PacketSanitizerOptions::PacketSanitizerOptions(
    PacketSanitizerOptions &&options) noexcept
{
    *this = std::move(options);
}

/// The max future time
void PacketSanitizerOptions::setMaximumFutureTime(
    const std::chrono::seconds &maxFutureTime)
{
    if (maxFutureTime.count() < 0)
    {
        throw std::invalid_argument("Maximum future time must be positive");
    }
    pImpl->mMaxFutureTime = maxFutureTime; 
}

std::chrono::seconds 
PacketSanitizerOptions::getMaximumFutureTime() const noexcept
{
    return pImpl->mMaxFutureTime;
}

void PacketSanitizerOptions::setCircularBufferDuration(
    const std::chrono::seconds &duration)
{
    if (duration.count() <= 0)
    {
        throw std::invalid_argument("Duration must be positive");
    }
    pImpl->mCircularBufferDuration = duration;
}

std::chrono::seconds
PacketSanitizerOptions::getCircularBufferDuration() const noexcept
{
    return pImpl->mCircularBufferDuration;
}

/// The max past time
void PacketSanitizerOptions::setMaximumLatency(
    const std::chrono::seconds &maxLatency)
{
    //if (maxLatency.count() <= 0)
    //{
    //    throw std::invalid_argument("Maximum latency time must be positive");
    //}
    if (maxLatency.count() > 0)
    {
        pImpl->mMaxLatency = maxLatency;
    }
    else
    {
        pImpl->mMaxLatency = std::chrono::seconds {-1};
    }
}

std::chrono::seconds 
PacketSanitizerOptions::getMaximumLatency() const noexcept
{
    return pImpl->mMaxLatency;
}


/// Logging interval
void PacketSanitizerOptions::setBadDataLoggingInterval(
    const std::chrono::seconds &interval) noexcept
{
    pImpl->mLogBadDataInterval = interval;
}

std::chrono::seconds 
PacketSanitizerOptions::getBadDataLoggingInterval() const noexcept
{
    return pImpl->mLogBadDataInterval;
}

bool PacketSanitizerOptions::logBadData() const noexcept
{
    return (pImpl->mLogBadDataInterval.count() > 0);
}

/// Copy assignment
PacketSanitizerOptions& 
PacketSanitizerOptions::operator=(
    const PacketSanitizerOptions &options)
{ 
    if (&options == this){return *this;}
    pImpl = std::make_unique<PacketSanitizerOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
PacketSanitizerOptions& 
PacketSanitizerOptions::operator=(
    PacketSanitizerOptions &&options) noexcept
{ 
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Reset class
void PacketSanitizerOptions::clear() noexcept
{
    pImpl = std::make_unique<PacketSanitizerOptionsImpl> ();
}

/// Destructor
PacketSanitizerOptions::~PacketSanitizerOptions() = default;
