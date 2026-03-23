#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include "uWaveServer/dataClient/grpc.hpp"
#include "uWaveServer/dataClient/grpcOptions.hpp"
#include "uWaveServer/packet.hpp"
#include "uDataPacketServiceAPI/v1/broadcast.grpc.pb.h"

#define CLIENT_TYPE "gRPC"

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
    std::function<void (UWaveServer::Packet &&packet)> mAddPacketCallback;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
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
 
    GRPCImpl(const GRPCOptions &options,
             std::shared_ptr<spdlog::logger> logger) :
        mGRPCOptions(options),
        mLogger(logger)
    {
        if (mLogger == nullptr)
        {
            mLogger = spdlog::stdout_color_mt("GRPCSubscriber");
        }
    }

    ~GRPCImpl()
    {
        stop();
    }

    [[nodiscard]] std::future<void> start()
    {
        mKeepRunning.store(true);
        auto result = std::async(&GRPCImpl::acquirePackets, this);
        return result;
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

    std::function<void (UWaveServer::Packet &&packet)> mAddPacketFunction;
    GRPCOptions mGRPCOptions; 
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    mutable std::mutex mShutdownMutex;
    std::condition_variable mShutdownCondition;
    std::atomic<bool> mKeepRunning{true};
    bool mShutdownRequested{false};
};

GRPC::GRPC(const std::function<void (std::vector<UWaveServer::Packet> &&)> &callback,
           const GRPCOptions &options,
           std::shared_ptr<spdlog::logger> logger) :
    IDataClient(callback),
    pImpl(std::make_unique<GRPCImpl> (options, logger))
{
    pImpl->mAddPacketFunction
        = std::bind(&IDataClient::addPacket, this,
                    std::placeholders::_1);

}

/// Initialized
bool GRPC::isInitialized() const noexcept
{
    return true;
}

/// Connect
void GRPC::connect()
{
}

/// Is connected?
bool GRPC::isConnected() const noexcept
{
    return pImpl->mKeepRunning.load();
}

/// Start the acquisition
std::future<void> GRPC::start()
{
    return pImpl->start();
}

/// Stop the acquisition
void GRPC::stop()
{
    pImpl->stop();
}

/// Type
std::string GRPC::getType() const noexcept
{
    return CLIENT_TYPE;
}

GRPC::~GRPC() = default;

