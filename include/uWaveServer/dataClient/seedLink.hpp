#ifndef UWAVE_SERVER_DATA_CLIENT_SEED_LINK_HPP
#define UWAVE_SERVER_DATA_CLIENT_SEED_LINK_HPP
#include <uWaveServer/dataClient/dataClient.hpp>
namespace UWaveServer::DataClient
{
 class SEEDLinkOptions;
}
namespace UWaveServer::DataClient
{
class SEEDLink : public IDataClient
{
public:
    SEEDLink(const std::function<void (std::vector<UWaveServer::Packet> &&packets)> &callback,
             const SEEDLinkOptions &options);
    ~SEEDLink() override;
    void connect() override;
    std::future<void> start() override;
    void stop() override;
    [[nodiscard]] bool isInitialized() const noexcept final;
    [[nodiscard]] bool isConnected() const noexcept final;
    [[nodiscard]] std::string getType() const noexcept final;
private:
    class SEEDLinkImpl;
    std::unique_ptr<SEEDLinkImpl> pImpl;
};
}
#endif
