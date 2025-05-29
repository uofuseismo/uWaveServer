#include <cmath>
#include <algorithm>
#include <chrono>
#include <map>
#include <set>
#include <string>
#ifndef NDEBUG
#include <cassert>
#endif
#include <boost/circular_buffer.hpp>
#include <spdlog/spdlog.h>
#include "uWaveServer/testDuplicatePacket.hpp"
#include "uWaveServer/packet.hpp"

using namespace UWaveServer;

namespace
{

struct DataPacketHeader
{
public:
    DataPacketHeader() = default;
    explicit DataPacketHeader(const UWaveServer::Packet &packet)
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
        // Trace name
        name = network + "."  + station + "." + channel;
        if (!locationCode.empty())
        {
            name = name + "." + locationCode;
        }
        // Start and end time
        startTime = packet.getStartTime();
        endTime = packet.getEndTime(); // Throws
        // Sampling rate (approximate)
        samplingRate
            = static_cast<int> (std::round(packet.getSamplingRate()));
        // Number of samples
        nSamples = packet.size();
        if (nSamples <= 0)
        {
            throw std::invalid_argument("No samples in packet");
        }
    } 
    bool operator<(const ::DataPacketHeader &rhs) const
    {
        return startTime < rhs.startTime;
    } 
    bool operator>(const ::DataPacketHeader &rhs) const
    {
        return startTime > rhs.startTime;
    }
    bool operator==(const ::DataPacketHeader &rhs) const
    {
        if (rhs.name != name){return false;}
        if (rhs.samplingRate - samplingRate != 0)
        {
            throw std::runtime_error("Inconsistent sampling rates for: "
                                   + name);
            //return false;
        }
        auto dStartTime = (rhs.startTime.count() - startTime.count());
        if (samplingRate < 105)
        {
            return (dStartTime < std::chrono::microseconds {15000}.count());
        }
        else if (samplingRate < 255)
        {
            return (dStartTime < std::chrono::microseconds {4500}.count());
        }
        else if (samplingRate < 505)
        {
            return (dStartTime < std::chrono::microseconds {2500}.count());
        }
        else if (samplingRate < 1005)
        {
            return (dStartTime < std::chrono::microseconds {1500}.count());
        }
        throw std::runtime_error(
            "Could not classify sampling rate: " + std::to_string(samplingRate)
          + " for " + name);
        //return false;
    } 
    std::string name; // Packet name NETWORK.STATION.CHANNEL.LOCATION
    std::chrono::microseconds startTime{0}; // UTC time of first sample
    std::chrono::microseconds endTime{0}; // UTC time of last sample
    // Typically `observed' sampling rates wobble around a nominal sampling rate
    int samplingRate{100};
    int nSamples{0}; // Number of samples in packet
};

[[nodiscard]] int estimateCapacity(const ::DataPacketHeader &header,
                                   const std::chrono::seconds &memory)
{
    auto duration
        = std::max(0.0,
                   std::round( (header.nSamples - 1.)
                               /std::max(1, header.samplingRate)));
    std::chrono::seconds packetDuration{static_cast<int> (duration)};
    return std::max(1000, static_cast<int> (memory.count()/duration)) + 1;
}

}

class TestDuplicatePacket::TestDuplicatePacketImpl
{
public:
    TestDuplicatePacketImpl() = default;
    void logBadData()
    {
        if (!mLogBadData){return;}
        auto now = std::chrono::high_resolution_clock::now();
        auto nowMuSeconds
           = std::chrono::time_point_cast<std::chrono::microseconds>
             (now).time_since_epoch();
        auto nowSeconds
            = std::chrono::duration_cast<std::chrono::seconds> (nowMuSeconds);
        if (nowSeconds > mLastLogTime + mLogBadDataInterval)
        {
            if (!mDuplicateChannels.empty())
            {
                std::string message{"Duplicate data detected for:"};
                for (const auto &channel : mDuplicateChannels)
                {
                    message = message + " " + channel;
                }
                spdlog::info(message);
                mDuplicateChannels.clear();
            }
            mLastLogTime = nowSeconds;
        }
    }
    [[nodiscard]] bool allow(const ::DataPacketHeader &header) const
    {
#ifndef NDEBUG
        assert(!header.name.empty());
        assert(header.nSamples > 0);
#endif
        // Does this channel exist?
        auto circularBufferIndex = mCircularBuffers.find(header.name);
        bool firstExample{false};
        if (circularBufferIndex == mCircularBuffers.end())
        {
             int capacity = mCircularBufferSize;
             if (mEstimateCapacity)
             {
                 capacity
                     = ::estimateCapacity(header,
                                          mCircularBufferDuration);
             }
             spdlog::info("Creating new circular buffer for: "
                        + header.name + " with capacity: "
                        + std::to_string(capacity));
             boost::circular_buffer<::DataPacketHeader>
                 newCircularBuffer(capacity);
             newCircularBuffer.push_back(header);
             mCircularBuffers.insert(std::pair{header.name,
                                               std::move(newCircularBuffer)});
             // Can't be a a duplicate because its the first one
             return true;
        }
        // Now we should definitely be able to find the appropriate circular
        // buffer for this stream 
        circularBufferIndex = mCircularBuffers.find(header.name);
        if (circularBufferIndex == mCircularBuffers.end())
        {
            spdlog::warn(
                "Algorithm error - circular buffer doesn't exist for: "
               + header.name);
            return false;
        }
        // See if this header exists (exactly)
        auto headerIndex
            = std::find(circularBufferIndex->second.begin(),
                        circularBufferIndex->second.end(),
                        header);
        if (headerIndex != circularBufferIndex->second.end())
        {
            if (mLogBadData)
            {
                spdlog::debug("Detected duplicate for: "
                            + header.name);
                if (!mDuplicateChannels.contains(header.name))
                {
                    mDuplicateChannels.insert(header.name);
                }
            }
            return false;
        }
        // Insert it (typically new stuff shows up)
        if (header > circularBufferIndex->second.back())
        {
            //spdlog::debug("Inserting " + header.name
            //            + " at end of circular buffer");
            circularBufferIndex->second.push_back(header);
            return true;
        }
        // If it is is really old and there's space then push to front
        if (header < circularBufferIndex->second.front())
        {
            if (!circularBufferIndex->second.full())
            {
                spdlog::debug("Inserting " + header.name 
                            + " at front of circular buffer");
                circularBufferIndex->second.push_front(header);
#ifndef NDEBUG
                assert(std::is_sorted(circularBufferIndex->second.begin(),
                                      circularBufferIndex->second.end(),
                       [](const ::DataPacketHeader &lhs, const ::DataPacketHeader &rhs)
                       {
                          return lhs.startTime < rhs.startTime;
                       }));
#endif
            }
            // Note, if the buffer is full then this packet is expired in the
            // eyes of the circular buffer.  For that, we let the database 
            // insert deal with it.
            return true;
        }
        // The packet is old.  We have to check for a GPS slip.
        for (const auto &streamHeader : circularBufferIndex->second)
        {
            if ((header.startTime >= streamHeader.startTime &&
                 header.startTime <= streamHeader.endTime) ||
                (header.endTime >= streamHeader.startTime &&
                 header.endTime <= streamHeader.endTime))
            {
                if (mLogBadData)
                {
                    spdlog::debug("Detected possible timing slip for: "
                                 + header.name);
                    if (!mBadTimingChannels.contains(header.name))
                    {   
                        mBadTimingChannels.insert(header.name);
                    }
                }
                return false;
            }
        }
        // This appears to be a valid (out-of-order) back-fill
        spdlog::debug("Inserting " + header.name
                    + " in circular buffer then sorting...");
        circularBufferIndex->second.push_back(header);
        std::sort(circularBufferIndex->second.begin(),
                  circularBufferIndex->second.end(),
                  [](const ::DataPacketHeader &lhs, const ::DataPacketHeader &rhs)
                  {
                      return lhs.startTime < rhs.startTime;
                  });
        return true;
    }
    mutable std::map<std::string, boost::circular_buffer<::DataPacketHeader>>
        mCircularBuffers;
    mutable std::set<std::string> mDuplicateChannels;
    mutable std::set<std::string> mBadTimingChannels;
    std::chrono::seconds mLogBadDataInterval{3600};
    std::chrono::seconds mLastLogTime{0};
    std::chrono::seconds mCircularBufferDuration{300};
    int mCircularBufferSize{100}; // ~3s packets 
    bool mLogBadData{true};
    bool mEstimateCapacity{false};
};

/*
/// Constructor
TestDuplicatePacket::TestDuplicatePacket() :
    pImpl(std::make_unique<TestDuplicatePacketImpl> ())
{
}
*/

/// Copy constructor
TestDuplicatePacket::TestDuplicatePacket(
    const TestDuplicatePacket &sanitizer)
{
    *this = sanitizer;
}

/// Move constructor
TestDuplicatePacket::TestDuplicatePacket(
    TestDuplicatePacket &&sanitizer) noexcept
{
    *this = std::move(sanitizer);
}

/// Copy assignment
TestDuplicatePacket&
TestDuplicatePacket::operator=(const TestDuplicatePacket &sanitizer)
{
    if (&sanitizer == this){return *this;} 
    pImpl = std::make_unique<TestDuplicatePacketImpl> (*sanitizer.pImpl);
    return *this;
}

/// Move assignment
TestDuplicatePacket&
TestDuplicatePacket::operator=(TestDuplicatePacket &&sanitizer) noexcept
{
    if (&sanitizer == this){return *this;}
    pImpl = std::move(sanitizer.pImpl);
    return *this;
}

/// Reset the class
/*
void TestDuplicatePacket::clear() noexcept
{
    pImpl = std::make_unique<TestDuplicatePacketImpl> ();
}
*/

/// Destructor
TestDuplicatePacket::~TestDuplicatePacket() = default;

/// Allow this packet?
bool TestDuplicatePacket::allow(const UWaveServer::Packet &packet) const
{
    // Construct the trace header for the circular buffer
    ::DataPacketHeader header;
    try
    {
        header = ::DataPacketHeader {packet}; // Copy elision
    }
    catch (const std::exception &e)
    {
        spdlog::warn(
            "Failed to unpack dataPacketHeader.  Failed because: "
          + std::string {e.what()} + "; Not allowing...");
        return false;
    }
    auto allow = pImpl->allow(header);
    pImpl->logBadData();
    return allow;
}
