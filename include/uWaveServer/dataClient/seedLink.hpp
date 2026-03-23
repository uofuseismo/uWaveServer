#ifndef UWAVE_SERVER_DATA_CLIENT_SEED_LINK_HPP
#define UWAVE_SERVER_DATA_CLIENT_SEED_LINK_HPP
#include <uWaveServer/dataClient/dataClient.hpp>
#include <spdlog/spdlog.h>
namespace UWaveServer::DataClient
{
 class SEEDLinkOptions;
}
namespace UWaveServer::DataClient
{
/// @class SEEDLink seedLink.hpp
/// @brief Receives seismic data packets via SEEDLink.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class SEEDLink : public IDataClient
{
public:
    SEEDLink(const std::function<void (std::vector<UWaveServer::Packet> &&packets)> &callback,
             const SEEDLinkOptions &options,
             std::shared_ptr<spdlog::logger> logger);
    /// @brief Destructor.
    ~SEEDLink() override;
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
    class SEEDLinkImpl;
    std::unique_ptr<SEEDLinkImpl> pImpl;
};
}
#endif
