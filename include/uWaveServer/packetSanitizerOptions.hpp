#ifndef UWAVE_SERVER_PACKET_SANITIZER_OPTIONS_HPP
#define UWAVE_SERVER_PACKET_SANITIZER_OPTIONS_HPP
#include <chrono>
#include <memory>
namespace UWaveServer
{
/// @class PacketSanitizerOptions packetSanitizeOptions.hpp "uWaveServer/packetSanitizerOptions.hpp"
/// @brief The packet sanitizer is a utility that is run before
///        sending data packets to a subsequent module for processing.
///        This utility can:
///        (1) Prevent future packets from being broadcast
///        (2) Prevent very old packets from being broadcast
///        (3) Prevent duplicate packets from being broadcast
///        This defines the options.
/// @copyright Ben Baker (University of Utah) distributed under the MIT license.
class PacketSanitizerOptions
{
public:
    /// @name Constructors
    /// @{

    /// @brief Constructor.
    PacketSanitizerOptions();
    /// @brief Copy constructor.
    /// @param[in] options   The options from which to initialize this class.
    PacketSanitizerOptions(const PacketSanitizerOptions &options);
    /// @brief Move constructor.
    /// @param[in,out] options  The options from which to in initialize this
    ///                         class.  On exit, options's behavior is
    ///                         undefined.
    PacketSanitizerOptions(PacketSanitizerOptions &&options) noexcept;
    /// @}

    /// @brief Sets the approximate duration of the circular buffer.
    void setCircularBufferDuration(const std::chrono::seconds &duration);
    /// @result The approximate duration of the circular buffer.
    [[nodiscard]] std::chrono::seconds getCircularBufferDuration() const noexcept;

    /// @brief If the data packet's last sample is older than the current time 
    ///        minus the maximum latency then it will be rejected.
    /// @param[in] latency   The maximum latency.
    void setMaximumLatency(const std::chrono::seconds &latency);
    /// @result The maximum data latency.  By default this is 500 seconds (~8 minutes).
    [[nodiscard]] std::chrono::seconds getMaximumLatency() const noexcept;

    /// @brief If the data packet has a sample greater than the current time
    ///        plus this time then it will be rejected.
    void setMaximumFutureTime(const std::chrono::seconds &maxFutureTime);
    /// @result The maximum future time.  By default this is zero seconds.
    [[nodiscard]] std::chrono::seconds getMaximumFutureTime() const noexcept;

    /// @brief Sets the bad data logging interval.
    /// @param[in] interval   Bad data will be logged every this many seconds.
    /// @note Setting this to zero or less disables logging.
    void setBadDataLoggingInterval(const std::chrono::seconds &interval) noexcept; 
    /// @result The logging interval.  By default this is hourly.
    [[nodiscard]] std::chrono::seconds getBadDataLoggingInterval() const noexcept; 
    /// @result True indicates that bad data will be logged. 
    [[nodiscard]] bool logBadData() const noexcept;

    /// @name Destructors
    /// @{

    /// @brief Resets the class and releases memory.
    void clear() noexcept;
    /// @brief Destructor.
    ~PacketSanitizerOptions();
    /// @}

    /// @name Operators
    /// @{

    /// @brief Copy assignment operator.
    PacketSanitizerOptions &operator=(const PacketSanitizerOptions &options);
    /// @brief Move assignment operator.
    PacketSanitizerOptions &operator=(PacketSanitizerOptions &&options) noexcept;
    /// @}
private:
    class PacketSanitizerOptionsImpl;
    std::unique_ptr<PacketSanitizerOptionsImpl> pImpl;  
};
}
#endif
