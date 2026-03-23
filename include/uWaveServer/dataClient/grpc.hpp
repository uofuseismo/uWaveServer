#ifndef UWAVE_SERVER_DATA_CLIENT_GRPC_HPP
#define UWAVE_SERVER_DATA_CLIENT_GRPC_HPP
#include <uWaveServer/dataClient/dataClient.hpp>
#include <spdlog/logger.h>
namespace UWaveServer::DataClient
{
 class GRPCOptions;
}
namespace UWaveServer::DataClient
{
/// @class GRPC grpc.hpp
/// @brief Receives seismic data packets via gRPC and the uDataPacketServiceAPI.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class GRPC : public IDataClient
{
public:
    GRPC(const std::function<void (std::vector<UWaveServer::Packet> &&packets)> &callback,
         const GRPCOptions &options,
         std::shared_ptr<spdlog::logger> logger);
    /// @brief Destructor.
    ~GRPC() override;
    /// @brief Connects to the data source.
    void connect() override;
    /// @brief Starts the acquisition.
    std::future<void> start() override;
    /// @brief Stops the acquisition.
    void stop() override;
    /// @result True indicates the client is initialized.
    [[nodiscard]] bool isInitialized() const noexcept final;
    /// @result True indicates the client is connected.
    [[nodiscard]] bool isConnected() const noexcept final;
    /// @result The client type.
    [[nodiscard]] std::string getType() const noexcept final;
private:
    class GRPCImpl;
    std::unique_ptr<GRPCImpl> pImpl;
};
}
#endif
