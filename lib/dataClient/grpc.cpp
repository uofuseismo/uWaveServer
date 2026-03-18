#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>
#include "uWaveServer/dataClient/grpc.hpp"
#include "uWaveServer/dataClient/grpcOptions.hpp"
#include "uWaveServer/packet.hpp"
#include "uDataPacketServiceAPI/v1/broadcast.grpc.pb.h"

using namespace UWaveServer::DataClient;

namespace
{

class CustomAuthenticator : public grpc::MetadataCredentialsPlugin
{
public:    
    CustomAuthenticator(const grpc::string &token) :
        mToken(token)
    {
    }
    grpc::Status GetMetadata(
        grpc::string_ref, // serviceURL, 
        grpc::string_ref, // methodName,
        const grpc::AuthContext &,//channelAuthContext,
        std::multimap<grpc::string, grpc::string> *metadata) override
    {   
        metadata->insert(std::make_pair("x-custom-auth-token", mToken));
        return grpc::Status::OK;
    }   
//private:
    grpc::string mToken;
};

std::shared_ptr<grpc::Channel>
    createChannel(const UWaveServer::DataClient::GRPCOptions &options,
                  spdlog::logger *logger)
{
    auto address = UWaveServer::DataClient::makeAddress(options);
    auto serverCertificate = options.getServerCertificate();
    if (serverCertificate)
    {
#ifndef NDEBUG
        assert(!serverCertificate->empty());
#endif
        if (options.getAccessToken())
        {
            auto apiKey = *options.getAccessToken();
#ifndef NDEBUG
            assert(!apiKey.empty());
#endif
            SPDLOG_LOGGER_INFO(logger,
                               "Creating secure channel with API key to {}",
                               address);
            auto callCredentials = grpc::MetadataCredentialsFromPlugin(
                std::unique_ptr<grpc::MetadataCredentialsPlugin> (
                    new ::CustomAuthenticator(apiKey)));
            grpc::SslCredentialsOptions sslOptions;
            sslOptions.pem_root_certs = *serverCertificate;
            auto channelCredentials
                = grpc::CompositeChannelCredentials(
                      grpc::SslCredentials(sslOptions),
                      callCredentials);
            return grpc::CreateChannel(address, channelCredentials);
        }
        SPDLOG_LOGGER_INFO(logger,
                           "Creating secure channel without API key to {}",
                           address);
        grpc::SslCredentialsOptions sslOptions;
        sslOptions.pem_root_certs = *serverCertificate;
        return grpc::CreateChannel(address,
                                   grpc::SslCredentials(sslOptions));
     }
     SPDLOG_LOGGER_INFO(logger,
                        "Creating non-secure channel to {}",
                         address);
     return grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
}

class AsyncPacketSubscriber :
    public grpc::ClientReadReactor<UDataPacketServiceAPI::V1::Packet>
{
public:
    AsyncPacketSubscriber
    (
        UDataPacketServiceAPI::V1::Broadcast::Stub *stub,
        const UDataPacketServiceAPI::V1::SubscriptionRequest &subscriptionRequest,
        std::function<void (UWaveServer::Packet &&)> &addPacketCallback,
        std::shared_ptr<spdlog::logger> logger,
        std::atomic<bool> *keepRunning
    ) :
        mSubscriptionRequest(subscriptionRequest),
        mAddPacketCallback(addPacketCallback),
        mLogger(logger),
        mKeepRunning(keepRunning)
    {
        mClientContext.set_wait_for_ready(false); // Fail immediately if server isn't there
        stub->async()->Subscribe(&mClientContext, &mSubscriptionRequest, this);
        StartRead(&mPacket);
        StartCall();
    }

    void OnReadDone(bool ok) override
    {
        if (ok)
        {
            mHadSuccessfulRead = true;
            try
            {
                auto copy = UWaveServer::fromGRPC(mPacket);
                mAddPacketCallback(std::move(copy));
            }
            catch (const std::exception &e)
            {
                SPDLOG_LOGGER_ERROR(
                    mLogger,
                    "Failed to add packet to callback because {}",
                    std::string {e.what()});
            }
            if (!mKeepRunning->load())
            {
                //Finish(grpc::Status::OK);
                mClientContext.TryCancel();
            }
            StartRead(&mPacket);
        }
        else // Not okay
        {
            // Quitting anyway?
            if (!mKeepRunning->load())
            {
                mClientContext.TryCancel();
            }
        }
    }

    void OnDone(const grpc::Status &status) override
    {
        std::unique_lock<std::mutex> lock(mMutex);
        mStatus = status;
        mDone = true;
        mConditionVariable.notify_one();
    }

    [[nodiscard]] std::pair<grpc::Status, bool> await()
    {
        std::unique_lock<std::mutex> lock(mMutex);
        mConditionVariable.wait(lock, [this] {return mDone;});
        return std::pair{std::move(mStatus), mHadSuccessfulRead};
    }

#ifndef NDEBUG
    ~AsyncPacketSubscriber()
    {
        SPDLOG_LOGGER_DEBUG(mLogger, "In destructor");
    }
#endif

    AsyncPacketSubscriber() = delete;

private:
    grpc::ClientContext mClientContext;
    UDataPacketServiceAPI::V1::SubscriptionRequest mSubscriptionRequest;
    std::function
    <
        void (UWaveServer::Packet &&packet)
    > mAddPacketCallback;
    std::shared_ptr<spdlog::logger> mLogger;
    std::mutex mMutex;
    std::condition_variable mConditionVariable;
    UDataPacketServiceAPI::V1::Packet mPacket;
    grpc::Status mStatus{grpc::Status::OK};
    bool mDone{false};
    std::atomic<bool> *mKeepRunning{nullptr};
    bool mHadSuccessfulRead{false};
};

}

class GRPC::GRPCImpl
{
public:
 
    ~GRPCImpl()
    {
        stop();
    }

    void acquirePackets()
    {
#ifndef NDEBUG
        assert(mLogger != nullptr);
#endif
        //auto reconnectSchedule = mOptions.getReconnectSchedule();
        std::vector<std::chrono::seconds> reconnectSchedule{ std::chrono::seconds{0}, std::chrono::seconds {1}, std::chrono::seconds {10} };
        auto nReconnect = static_cast<int> (reconnectSchedule.size());
        for (int kReconnect =-1; kReconnect < nReconnect; ++kReconnect)
        {
            if (!mKeepRunning.load()){break;} 
            if (kReconnect >= 0)
            {
                SPDLOG_LOGGER_INFO(mLogger,
                                   "Will attempt to reconnect in {} s",
                                   reconnectSchedule.at(kReconnect).count());
                std::unique_lock<std::mutex> lock(mShutdownMutex);
                mShutdownCondition.wait_for(lock,
                                            reconnectSchedule.at(kReconnect),
                                            [this]
                                            {
                                                return mShutdownRequested;
                                            });
                lock.unlock();
                if (!mKeepRunning.load()){break;}
            }
            // Create channel
            auto channel = ::createChannel(mGRPCOptions, mLogger.get());
            auto stub = UDataPacketServiceAPI::V1::Broadcast::NewStub(channel);
            UDataPacketServiceAPI::V1::SubscriptionRequest request;
            auto subscriberIdentifier = std::string {"uWaveServer"}; // TODO mOptions.getIdentifier();

        }
    }

    void stop()
    {
        mKeepRunning.store(false);
    }

    GRPCOptions mGRPCOptions; 
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    mutable std::mutex mShutdownMutex;
    std::condition_variable mShutdownCondition;
    std::atomic<bool> mKeepRunning{true};
    bool mShutdownRequested{false};
};

GRPC::~GRPC() = default;
