#include <iostream>
#include <string>
#include <chrono>
#include <mutex>
#include <set>
#include <spdlog/spdlog.h>
#include "uWaveServer/testExpiredPacket.hpp"
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

class TestExpiredPacket::TestExpiredPacketImpl
{
public:
    TestExpiredPacketImpl(const TestExpiredPacketImpl &impl)
    {
        *this = impl;
    }
    TestExpiredPacketImpl(const std::chrono::microseconds &maxExpiredTime,
                         const std::chrono::seconds &logBadDataInterval) :
        mMaxExpiredTime(maxExpiredTime),
        mLogBadDataInterval(logBadDataInterval)
    {
        if (mMaxExpiredTime.count() <= 0)
        {
            throw std::invalid_argument("Max expired time must be positive");
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
            if (!name.empty() && !mExpiredChannels.contains(name))
            {
                mExpiredChannels.insert(name);
            }
        }
        catch (...)
        {
            spdlog::warn("Failed to add " + name + " to set");
        }
        if (nowSeconds >= mLastLogTime + mLogBadDataInterval)
        {
            if (!mExpiredChannels.empty())
            {
                std::string message{"Expired data detected for: "};
                for (const auto &channel : mExpiredChannels)
                {
                    message = message + " " + channel;
                }
                spdlog::info(message);
                mExpiredChannels.clear();
                mLastLogTime = nowSeconds;
            }
        }
        }
    }
    TestExpiredPacketImpl& operator=(const TestExpiredPacketImpl &impl)
    {
        if (&impl == this){return *this;}
        {
        std::lock_guard<std::mutex> lockGuard(impl.mMutex);
        mExpiredChannels = impl.mExpiredChannels;
        mLastLogTime = impl.mLastLogTime; 
        }
        mMaxExpiredTime = impl.mMaxExpiredTime;
        mLogBadDataInterval = impl.mLogBadDataInterval;
        mLogBadData = impl.mLogBadData;
        return *this;
    }
//private:
    mutable std::mutex mMutex;
    std::set<std::string> mExpiredChannels;
    std::chrono::microseconds mMaxExpiredTime{std::chrono::seconds{7776000}};
    std::chrono::seconds mLastLogTime{0};
    std::chrono::seconds mLogBadDataInterval{3600};
    bool mLogBadData{true};
};

/// Constructor
TestExpiredPacket::TestExpiredPacket() :
    pImpl(std::make_unique<TestExpiredPacketImpl> (
        std::chrono::microseconds {std::chrono::seconds{7776000}},
        std::chrono::seconds {3600}))
{
}

/// Constructor with options
TestExpiredPacket::TestExpiredPacket(
    const std::chrono::microseconds &maxExpiredTime,
    const std::chrono::seconds &logBadDataInterval) :
    pImpl(std::make_unique<TestExpiredPacketImpl> (maxExpiredTime,
                                                   logBadDataInterval))
{
}

/// Copy constructor
TestExpiredPacket::TestExpiredPacket(
    const TestExpiredPacket &testExpiredPacket)
{
    *this = testExpiredPacket;
}

/// Move constructor
TestExpiredPacket::TestExpiredPacket(
    TestExpiredPacket &&testExpiredPacket) noexcept
{
    *this = std::move(testExpiredPacket);
}

/// Copy assignment
TestExpiredPacket& 
TestExpiredPacket::operator=(const TestExpiredPacket &testExpiredPacket)
{
    if (&testExpiredPacket == this){return *this;}
    pImpl = std::make_unique<TestExpiredPacketImpl> (*testExpiredPacket.pImpl);
    return *this;
}

/// Move assignment
TestExpiredPacket&
TestExpiredPacket::operator=(TestExpiredPacket &&testExpiredPacket) noexcept
{
    if (&testExpiredPacket == this){return *this;}
    pImpl = std::move(testExpiredPacket.pImpl);
    return *this;
}

/// Destructor
TestExpiredPacket::~TestExpiredPacket() = default;

/// Does the work
bool TestExpiredPacket::allow(const Packet &packet) const
{
    auto packetStartTime = packet.getStartTime(); // Throws
    auto now = std::chrono::high_resolution_clock::now();
    auto nowMuSeconds
        = std::chrono::time_point_cast<std::chrono::microseconds>
          (now).time_since_epoch();
    auto earliestTime  = nowMuSeconds - pImpl->mMaxExpiredTime;
    // Packet contains data after max allowable time?
    bool allow = (packetStartTime >= earliestTime) ? true : false;
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
