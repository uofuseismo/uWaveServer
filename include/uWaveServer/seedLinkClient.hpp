#ifndef UWAVE_SERVER_SEED_LINK_CLIENT_HPP
#define UWAVE_SERVER_SEED_LINK_CLIENT_HPP
#include <uWaveServer/dataClient.hpp>
namespace UWaveServer
{
class SEEDLinkClient : public IDataClient
{
public:
    void stop() override;
private:
    class SEEDLinkClientImpl;
    std::unique_ptr<SEEDLinkClientImpl> pImpl;
};
}
#endif
