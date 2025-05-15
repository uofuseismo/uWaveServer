#ifndef UWAVE_SERVER_PACKET_HPP
#define UWAVE_SERVER_PACKET_HPP
#include <optional>
#include <chrono>
#include <vector>
#include <memory>
#include <chrono>
namespace UWaveServer
{
/// @name Packet "packet.hpp" "uWaveServer/packet.hpp"
/// @brief Defines a packet of seismic data.
/// @copyright Ben Baker (UUSS) distributed under the MIT license.
class Packet
{
public:
    /// @brief Defines the underlying precision of the time series.
    enum class DataType
    {
        Integer32, /*!< 32-bit integer */
        Integer64, /*!< 64-bit integer */
        Float,     /*!< 32-bit float */
        Double,    /*!< 64-bit double precision */
        Unknown    /*!< The data type is unknown. */
    };
public:
    /// @breif Constructor.
    Packet();
    /// @brief Copy constructor.
    Packet(const Packet &packet);
    /// @brief Move constructor.
    Packet(Packet &&packet) noexcept;

    /// @brief Sets the network code.
    /// @param[in] network  The network code - e.g., UU.
    void setNetwork(const std::string &network);
    /// @result The network code.
    [[nodiscard]] std::string getNetwork() const;
    /// @result True indicates the network code was set.
    [[nodiscard]] bool haveNetwork() const noexcept;

    /// @brief Sets the station name.
    /// @param[in] station  The station name - e.g., TCU.
    void setStation(const std::string &station);
    /// @result The station name.
    [[nodiscard]] std::string getStation() const;
    /// @result True indicates the station was set. 
    [[nodiscard]] bool haveStation() const noexcept;

    /// @brief Sets the channel code.
    /// @param[in] channel  The channel code - e.g., HHZ.
    void setChannel(const std::string &channel);
    /// @result The channel code.
    [[nodiscard]] std::string getChannel() const;
    /// @result True indicates the channel code was set.
    [[nodiscard]] bool haveChannel() const noexcept;

    /// @brief Sets the location code.
    /// @param[in] location  The location code - e.g., 01.
    void setLocationCode(const std::string &locationCode);
    /// @result The location code.
    [[nodiscard]] std::string getLocationCode() const;
    /// @result True indicates the location code was set.
    [[nodiscard]] bool haveLocationCode() const noexcept;

    /// @brief Sets the start time of the packet.
    /// @param[in] startTime  The start time (UTC) of the packet in seconds
    ///                       since the epoch.
    void setStartTime(double startTime) noexcept;
    /// @brief Sets the start time of the packet.
    /// @param[in] startTime  The start time (UTC) of the packet in microseconds
    ///                       since the epoch.
    void setStartTime(const std::chrono::microseconds &startTime) noexcept;
    /// @result Gets the start time (UTC) of the packet in microseconds
    ///         since the epoch.
    [[nodiscard]] std::chrono::microseconds getStartTime() const noexcept;
    /// @result The end time (UTC) of the packet in microseconds since
    ///         the epoch.
    [[nodiscard]] std::chrono::microseconds getEndTime() const;

    /// @brief Sets the sampling rate.
    /// @param[in] samplingRate  The sampling rate in Hz.  
    /// @throws std::invalid_argument if this is not positive.
    void setSamplingRate(double samplingRate);
    /// @result The sampling rate in Hz.
    [[nodiscard]] double getSamplingRate() const;
    /// @result True indicates the sampling rate was set.
    [[nodiscard]] bool haveSamplingRate() const noexcept;

    /// @result The number of samples.
    [[nodiscard]] int size() const noexcept;
    /// @result True indicates there are no data samples.
    [[nodiscard]] bool empty() const noexcept;


    /// @brief Sets the data from a packet.
    template<typename U> void setData(std::vector<U> &&data);
    /// @brief Sets the data from a packet.
    template<typename U> void setData(const int nSamples, const U *data);
    /// @brief Sets the data from a packet.
    template<typename U> void setData(const std::vector<U> &data);
    /// @result A raw pointer to the underlying data.
    [[nodiscard]] const void *data() const noexcept;

    /// @brief Trims the time series so that the samples are between
    ///        start time and end time.
    void trim(const double startTime, const double endTime);
    void trim(const std::chrono::microseconds &startTime,
              const std::chrono::microseconds &endTime);

    /// @result The underlying data type.
    [[nodiscard]] DataType getDataType() const noexcept;

    /// @brief Reset the class.
    void clear() noexcept;
    /// @brief Destructor
    ~Packet(); 

    /// @brief Copy assignment.
    Packet& operator=(const Packet &packet);
    /// @brief Move assignent.
    Packet& operator=(Packet &&packet) noexcept;
    friend void swap(Packet &lhs, Packet &rhs);
private:
    class PacketImpl;
    std::unique_ptr<PacketImpl> pImpl;
};
/// @brief Swaps two time classes, lhs and rhs.
/// @param[in,out] lhs  On exit this will contain the information in rhs.
/// @param[in,out] rhs  On exit this will contain the information in lhs.
void swap(Packet &lhs, Packet &rhs);
}
#endif
