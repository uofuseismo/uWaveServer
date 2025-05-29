#include <iostream>
#include <string>
#include <chrono>
#include <mutex>
#include <set>
#include <spdlog/spdlog.h>
#include "uWaveServer/testFuturePacket.hpp"
#include "uWaveServer/packet.hpp"

using namespace UWaveServer;

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

class TestFuturePacket::TestFuturePacketImpl
{
public:
    TestFuturePacketImpl(const TestFuturePacketImpl &impl)
    {
        *this = impl;
    }
    TestFuturePacketImpl(const std::chrono::microseconds &maxFutureTime,
                         const std::chrono::seconds &logBadDataInterval) :
        mMaxFutureTime(maxFutureTime),
        mLogBadDataInterval(logBadDataInterval)
    {
        // This might be okay if you really want to account for telemetry
        // lags.  But that's a dangerous game so I'll let the user know.
        if (mMaxFutureTime.count() < 0)
        {
            spdlog::warn("Max future time is negative");
        }
        if (mLogBadDataInterval.count() >= 0)
        {
            mLogBadData = true;
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
            spdlog::warn("Could not extract name of packet");
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
            spdlog::warn("Failed to add " + name + " to set");
        }
        if (nowSeconds > mLastLogTime + mLogBadDataInterval)
        {
            if (!mFutureChannels.empty())
            {
                std::string message{"Future data detected for: "};
                for (const auto &channel : mFutureChannels)
                {
                    message = message + " " + channel;
                }
                spdlog::info(message);
                mFutureChannels.clear();
            }
            mLastLogTime = nowSeconds;
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
    std::chrono::seconds mLogBadDataInterval{3600};
    bool mLogBadData{true};
};

/// Constructor
TestFuturePacket::TestFuturePacket() :
    pImpl(std::make_unique<TestFuturePacketImpl> (std::chrono::microseconds {0},
                                                  std::chrono::seconds {3600}))
{
}

/// Constructor with options
TestFuturePacket::TestFuturePacket(
    const std::chrono::microseconds &maxFutureTime,
    const std::chrono::seconds &logBadDataInterval) :
    pImpl(std::make_unique<TestFuturePacketImpl> (maxFutureTime,
                                                  logBadDataInterval))
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
        spdlog::warn("Error detect in logBadData: "
                   + std::string {e.what()});
    }
    return allow;
}
