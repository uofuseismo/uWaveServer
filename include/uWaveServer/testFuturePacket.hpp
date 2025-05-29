#ifndef UWAVESERVER_TEST_FUTURE_PACKET_HPP
#define UWAVESERVER_TEST_FUTURE_PACKET_HPP
#include <chrono>
#include <string>
#include <memory>
namespace UWaveServer
{
class Packet;
}
namespace UWaveServer
{
/// @brief Tests whether or not a packet contains data from the future.  This
///        indicates that there is a timing error.
/// @copyright Ben Baker (University of Utah) distributed under the MIT license.
class TestFuturePacket
{
public:
    /// @brief Constructs a future time checker with a default max future time
    ///        of 0 (which after data transmission and scraping from some type
    ///        of import device is pretty conservative).  Sensors sending
    ///        packets from the future will be logged every hour.
    TestFuturePacket();
    /// @param[in] maxFutureTime  If the time of the last sample in the packet
    ///                           exceeds now + maxFutureTime then the packet
    ///                           is rejected for having bad data.
    /// @param[in] logBadDataInterval  If this is positive then this will
    ///                                log flagged channels at approximately
    ///                                this interval.
    TestFuturePacket(const std::chrono::microseconds &maxFutureTime,
                     const std::chrono::seconds &logBadDataInterval);
    /// @brief Copy constructor.
    TestFuturePacket(const TestFuturePacket &testFuturePacket);
    /// @brief Move constructor.
    TestFuturePacket(TestFuturePacket &&testFuturePacket) noexcept;

    /// @result True indicates the data does not appear to have any future data.
    [[nodiscard]] bool allow(const Packet &packet) const;

    /// @brief Destructor.
    ~TestFuturePacket();
    /// @brief Copy assignment.
    TestFuturePacket& operator=(const TestFuturePacket &testFuturePacket);
    /// @brief Move constructor.
    TestFuturePacket& operator=(TestFuturePacket &&testFuturePacket) noexcept;
private:
    class TestFuturePacketImpl;
    std::unique_ptr<TestFuturePacketImpl> pImpl;
};
}
#endif
