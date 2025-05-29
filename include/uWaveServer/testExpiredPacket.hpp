#ifndef UWAVESERVER_TEST_EXPIRED_PACKET_HPP
#define UWAVESERVER_TEST_EXPIRED_PACKET_HPP
#include <chrono>
#include <string>
#include <memory>
namespace UWaveServer
{
class Packet;
}
namespace UWaveServer
{
/// @brief Tests whether or not a packet contains data that has expired.  This
///        indicates that a backfill is from too far back to be useful or 
///        there is a timing error.
/// @copyright Ben Baker (University of Utah) distributed under the MIT license.
class TestExpiredPacket
{
public:
    /// @brief Constructs an expired time checker with a default max past time
    ///        of 90 days (which is pretty conserative given that most 
    ///        stations at UUSS can only backfill for a few weeks).
    ///        Sensors sending packets that have samples at older timer
    ///        be flagged and logged every hour.
    TestExpiredPacket();
    /// @param[in] maxExpiredTime  If the time of the first sample in the packet
    ///                            is less than now - maxExpiredTime then the
    ///                            packet is rejected for having bad data.
    /// @param[in] logBadDataInterval  If this is positive then this will
    ///                                log flagged channels at approximately
    ///                                this interval.
    TestExpiredPacket(const std::chrono::microseconds &maxExpiredTime,
                      const std::chrono::seconds &logBadDataInterval);
    /// @brief Copy constructor.
    TestExpiredPacket(const TestExpiredPacket &testExpiredPacket);
    /// @brief Move constructor.
    TestExpiredPacket(TestExpiredPacket &&testExpiredPacket) noexcept;

    /// @param[in] packet  The packet to test.
    /// @result True indicates the data does not appear to have any expired
    ///         data.
    [[nodiscard]] bool allow(const Packet &packet) const;

    /// @brief Destructor.
    ~TestExpiredPacket();
    /// @brief Copy assignment.
    TestExpiredPacket& operator=(const TestExpiredPacket &testExpiredPacket);
    /// @brief Move constructor.
    TestExpiredPacket& operator=(TestExpiredPacket &&testExpiredPacket) noexcept;
private:
    class TestExpiredPacketImpl;
    std::unique_ptr<TestExpiredPacketImpl> pImpl;
};
}
#endif
