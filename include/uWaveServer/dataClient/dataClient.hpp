#ifndef UWAVE_SERVER_DATA_CLIENT_DATA_CLIENT_HPP
#define UWAVE_SERVER_DATA_CLIENT_DATA_CLIENT_HPP
#include <vector>
#include <functional>
#include <memory>
namespace UWaveServer
{
class Packet;
};
namespace UWaveServer::DataClient
{
/// @class IDataClient "dataClient.hpp" "uWaveServer/dataClient/dataClient.hpp"
/// @brief A data client connects to a data feed e.g., SEEDLink, and then 
///        gives the newly acquired data packets to a higher-level 
///        function via a callback.
/// @copyright Ben Baker (UUSS) distributed under the MIT license.
class IDataClient
{
public:
    enum Type
    {
        SEEDLink
    };
public:
    /// @brief Constructor.
    IDataClient();
    /// @brief Destructor.
    virtual ~IDataClient();
    /// @brief Connects the client to the data source.
    virtual void connect() = 0;
    /// @brief Starts the acquisition.
    virtual void start() = 0;
    /// @brief Terminates the acquisition.
    virtual void stop() = 0;
    /// @result The client type.
    virtual Type getType() const noexcept = 0;
    /// @result True indicates the client is ready to receive 
    ///         data packets.
    [[nodiscard]] virtual bool isInitialized() const noexcept = 0;
    /// @result True indicates the client is connected.
    [[nodiscard]] virtual bool isConnected() const noexcept = 0;
    /// @brief Allows a higher level function to add the newly
    ///        acquired packets from this data feed.
    void setCallback(const std::function<void (std::vector<Packet> &&packets)> &callback) noexcept;
    /// @brief Adds the packets.
    virtual void addPackets(std::vector<Packet> &&packets);
    virtual void addPacket(Packet &&packet);
private:
    class IDataClientImpl;
    std::unique_ptr<IDataClientImpl> pImpl;
};
}
#endif
