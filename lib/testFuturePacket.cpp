#include <iostream>
#include <string>
#include <chrono>
#include <mutex>
#include <set>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include "uWaveServer/testFuturePacket.hpp"
#include "uWaveServer/packet.hpp"
#include "private/toName.hpp"

using namespace UWaveServer;

/*
namespace
{

[[nodiscard]] std::string toName(const UWaveServer::Packet &packet)
{
    auto network = packet.getNetwork();
    auto station = packet.getStation(); 
    auto channel = packet.getChannel();
    std::string locationCode;
    try
    {
        locationCode = packet.getLocationCode();
    }
    catch (...)
    {
    }
    auto name = network + "."  + station + "." + channel;
    if (!locationCode.empty())
    {
        name = name + "." + locationCode;
    }
    return name;

}

}
*/

class TestFuturePacket::TestFuturePacketImpl
{
public:
    TestFuturePacketImpl(const TestFuturePacketImpl &impl)
    {
        *this = impl;
    }
    TestFuturePacketImpl(const std::chrono::microseconds &maxFutureTime,
                         const std::chrono::seconds &logBadDataInterval,
                         std::shared_ptr<spdlog::logger> logger) :
        mMaxFutureTime(maxFutureTime),
        mLogBadDataInterval(logBadDataInterval),
        mLogger(logger)
    {
        // This might be okay if you really want to account for telemetry
        // lags.  But that's a dangerous game so I'll let the user know.
        if (mMaxFutureTime.count() < 0)
        {
            SPDLOG_LOGGER_WARN(mLogger, "Max future time is negative");
        }
        if (mLogBadDataInterval.count() >= 0)
        {
            mLogBadData = true;
            if (mLogger == nullptr)
            {
                mLogger
                    = spdlog::stdout_color_mt("future-packet-tester-console");
            }
        }
        else
        {
            mLogBadData = false;
        }
    }
    /// Logs the bad events
    void logBadData(const bool allow,
                    const UWaveServer::Packet &packet,
                    const std::chrono::microseconds &nowMuSec)
    {
        if (!mLogBadData){return;}
        std::string name;
        try
        {
            if (!allow){name = ::toName(packet);}
        }
        catch (...)
        {
            if (mLogger)
            {
                SPDLOG_LOGGER_WARN(mLogger, "Could not extract name of packet");
            }
        }
        auto nowSeconds
            = std::chrono::duration_cast<std::chrono::seconds> (nowMuSec);
        {
        std::lock_guard<std::mutex> lockGuard(mMutex); 
        try
        {
            if (!name.empty() && !mFutureChannels.contains(name))
            {
                mFutureChannels.insert(name);
            }
        }
        catch (...)
        {
            if (mLogger)
            {
                SPDLOG_LOGGER_WARN(mLogger, "Failed to add {} to set", name);
            }
        }
        if (nowSeconds >= mLastLogTime + mLogBadDataInterval)
        {
            if (!mFutureChannels.empty())
            {
                std::string message{"Future data detected for: "};
                for (const auto &channel : mFutureChannels)
                {
                    message = message + " " + channel;
                }
                if (mLogger){SPDLOG_LOGGER_INFO(mLogger, "{}", message);}
                mFutureChannels.clear();
                mLastLogTime = nowSeconds;
            }
        }
        }
    }
    TestFuturePacketImpl& operator=(const TestFuturePacketImpl &impl)
    {
        if (&impl == this){return *this;}
        {
        std::lock_guard<std::mutex> lockGuard(impl.mMutex);
        mFutureChannels = impl.mFutureChannels;
        mLastLogTime = impl.mLastLogTime; 
        }
        mMaxFutureTime = impl.mMaxFutureTime;
        mLogBadDataInterval = impl.mLogBadDataInterval;
        mLogBadData = impl.mLogBadData;
        return *this;
    }
//private:
    mutable std::mutex mMutex;
    std::set<std::string> mFutureChannels;
    std::chrono::microseconds mMaxFutureTime{0};
    std::chrono::seconds mLastLogTime{0};
    std::chrono::seconds mLogBadDataInterval{std::chrono::minutes {15}};
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    bool mLogBadData{true};
};

/// Constructor
TestFuturePacket::TestFuturePacket() :
    pImpl(std::make_unique<TestFuturePacketImpl> (std::chrono::microseconds {0},
                                                  std::chrono::seconds {3600},
                                                  nullptr))
{
}

/// Constructor with options
TestFuturePacket::TestFuturePacket(
    const std::chrono::microseconds &maxFutureTime,
    const std::chrono::seconds &logBadDataInterval,
    std::shared_ptr<spdlog::logger> logger) :
    pImpl(std::make_unique<TestFuturePacketImpl> (maxFutureTime,
                                                  logBadDataInterval,
                                                  logger))
{
}

/// Copy constructor
TestFuturePacket::TestFuturePacket(
    const TestFuturePacket &testFuturePacket)
{
    *this = testFuturePacket;
}

/// Move constructor
TestFuturePacket::TestFuturePacket(TestFuturePacket &&testFuturePacket) noexcept
{
    *this = std::move(testFuturePacket);
}

/// Copy assignment
TestFuturePacket& 
TestFuturePacket::operator=(const TestFuturePacket &testFuturePacket)
{
    if (&testFuturePacket == this){return *this;}
    pImpl = std::make_unique<TestFuturePacketImpl> (*testFuturePacket.pImpl);
    return *this;
}

/// Move assignment
TestFuturePacket&
TestFuturePacket::operator=(TestFuturePacket &&testFuturePacket) noexcept
{
    if (&testFuturePacket == this){return *this;}
    pImpl = std::move(testFuturePacket.pImpl);
    return *this;
}

/// Destructor
TestFuturePacket::~TestFuturePacket() = default;

/// Does the work
bool TestFuturePacket::allow(const Packet &packet) const
{
    auto packetEndTime = packet.getEndTime(); // Throws
    // Computing the current time after the scraping the ring is
    // conservative.  Basically, when the max future time is zero,
    // this allows for a zero-latency, 1 sample packet, to be
    // successfully passed through.
    auto now = std::chrono::high_resolution_clock::now();
    auto nowMuSeconds
        = std::chrono::time_point_cast<std::chrono::microseconds>
          (now).time_since_epoch();
    auto latestTime  = nowMuSeconds + pImpl->mMaxFutureTime;
    // Packet contains data after max allowable time?
    bool allow = (packetEndTime <= latestTime) ? true : false;
    // (Safely) handle logging
    try
    {
        pImpl->logBadData(allow, packet, nowMuSeconds);
    }
    catch (const std::exception &e)
    {
        if (pImpl->mLogger)
        {
            SPDLOG_LOGGER_WARN(pImpl->mLogger, "Error detect in logBadData: {}",
                               std::string {e.what()});
        }
    }
    return allow;
}
