#include <string>
#include <cstring>
#include <cmath>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <functional>
#include <libslink.h>
#include <slplatform.h>
#include <libmseed.h>
#include <spdlog/spdlog.h>
#include "uWaveServer/dataClient/seedLink.hpp"
#include "uWaveServer/dataClient/seedLinkOptions.hpp"
#include "uWaveServer/dataClient/streamSelector.hpp"
#include "uWaveServer/packet.hpp"

using namespace UWaveServer::DataClient;

namespace
{
/// @brief Unpacks a miniSEED record.
[[nodiscard]]
UWaveServer::Packet
    miniSEEDToDataPacket(char *msRecord,
                         const int seedLinkRecordSize = 512)
{
    UWaveServer::Packet dataPacket;
    constexpr int8_t verbose{0};
    constexpr uint32_t flags{MSF_UNPACKDATA};
    MS3Record *miniSEEDRecord{nullptr};
    auto returnValue = msr3_parse(msRecord, seedLinkRecordSize,
                                  &miniSEEDRecord, flags,
                                  verbose);
    if (returnValue == 0)
    {   
        // SNCL
        std::array<char, 64> networkWork;
        std::array<char, 64> stationWork;
        std::array<char, 64> channelWork;
        std::array<char, 64> locationWork;
        std::fill(networkWork.begin(),  networkWork.end(), '\0');
        std::fill(stationWork.begin(),  stationWork.end(), '\0');
        std::fill(channelWork.begin(),  channelWork.end(), '\0'); 
        std::fill(locationWork.begin(), locationWork.end(), '\0');
        returnValue = ms_sid2nslc(miniSEEDRecord->sid,
                                  networkWork.data(), stationWork.data(),
                                  locationWork.data(), channelWork.data());
        std::string network{networkWork.data()};
        std::string station{stationWork.data()};
        std::string channel{channelWork.data()};
        std::string location{locationWork.data()};
        if (locationWork[0] == '\0'){location = "--";}
        if (std::string {"  "} == location.substr(0, 2)){location = "--";}
        if (returnValue == 0)
        {
            dataPacket.setNetwork(network);
            dataPacket.setStation(station);
            dataPacket.setChannel(channel);
            dataPacket.setLocationCode(location);
        }
        else
        {
            msr3_free(&miniSEEDRecord);
            throw std::runtime_error("Failed to unpack SNCL");
        }
        // Sampling rate
        dataPacket.setSamplingRate(miniSEEDRecord->samprate);
        // Start time (convert from nanoseconds to microseconds)
        std::chrono::microseconds startTime
        {
            static_cast<int64_t> (std::round(miniSEEDRecord->starttime*1.e-3))
        };
        dataPacket.setStartTime(startTime);
        // Data
        auto nSamples = static_cast<int> (miniSEEDRecord->numsamples);
        if (nSamples > 0)
        {
            if (miniSEEDRecord->sampletype == 'i')
            {
                const auto data
                    = reinterpret_cast<const int *>
                      (miniSEEDRecord->datasamples);
                dataPacket.setData(nSamples, data);
            }
            else if (miniSEEDRecord->sampletype == 'f')
            {
                const auto data
                    = reinterpret_cast<const float *>
                      (miniSEEDRecord->datasamples);
                dataPacket.setData(nSamples, data);
            }
            else if (miniSEEDRecord->sampletype == 'd')
            {
                const auto data
                    = reinterpret_cast<const double *>
                      (miniSEEDRecord->datasamples);
                dataPacket.setData(nSamples, data);
            }
            else
            {
                msr3_free(&miniSEEDRecord);
                throw std::runtime_error("Unhandled sample type");
            }
        }
    }
    else
    {
        if (returnValue < 0)
        {
            msr3_free(&miniSEEDRecord);
            throw std::runtime_error("libmseed error detected");
        }
        msr3_free(&miniSEEDRecord);
        throw std::runtime_error(
             "Insufficient data.  Number of additional bytes estimated is "
            + std::to_string(returnValue));
    }
    // Cleanup and leave
    msr3_free(&miniSEEDRecord);
    return dataPacket;
}

}

class SEEDLink::SEEDLinkImpl
{
public:
    /// Destructor
    ~SEEDLinkImpl()
    {
        stop();
        disconnect();
    }
    /// Terminate the SEED link client connection
    void disconnect()
    {
        if (mSEEDLinkConnection != nullptr)
        {
            if (mSEEDLinkConnection->link != -1)
            {
                spdlog::debug("Disconnecting SEEDLink...");
                sl_disconnect(mSEEDLinkConnection);
            }
            if (mUseStateFile)
            {
                spdlog::debug("Saving state prior to disconnect...");
                sl_savestate(mSEEDLinkConnection, mStateFile.c_str());
            }
            spdlog::debug("Freeing SEEDLink structure...");
            sl_freeslcd(mSEEDLinkConnection);
            mSEEDLinkConnection = nullptr;
        }
    }
    /// Sends a terminate command to the SEEDLink connection
    void terminate()
    {
        if (mSEEDLinkConnection != nullptr)
        {
            spdlog::debug("Issuing terminate command to poller");
            sl_terminate(mSEEDLinkConnection);
        }
    }
    /// Toggles this as running or not running
    void setRunning(const bool running)
    {
        // Terminate the session
        if (!running && mKeepRunning)
        {
            spdlog::debug("Issuing terminate command");
            terminate();
        }
        // Tell the scraping thread to quit if it hasn't already given up
        // because it received a terminate request
        mKeepRunning = running;
    }
    /// Stops the service
    void stop()
    {
        setRunning(false); // Issues terminate command
        if (mSEEDLinkReaderThread.joinable()){mSEEDLinkReaderThread.join();}
    }
    /// Starts the service
    void start()
    {
        stop(); // Ensure module is stopped
        if (!mInitialized)
        {
            throw std::runtime_error("SEEDLink client not initialized");
        }
        setRunning(true);
        spdlog::debug("Starting the SEEDLink polling thread...");
        mSEEDLinkReaderThread = std::thread(&SEEDLinkImpl::packetToCallback,
                                            this);
    }
    /// Scrapes the packets and puts them to the callback
    void packetToCallback()
    {
        mConnected = true;
        // Recover state
        if (mUseStateFile)
        {
            if (!sl_recoverstate(mSEEDLinkConnection, mStateFile.c_str()))
            {
                 spdlog::warn("Failed to recover state");
            }
        }
        // Now start scraping
        SLpacket *seedLinkPacket{nullptr};
        int updateStateFile{1};
        while (mKeepRunning)
        {
            // Block until a packet is received.  In this case, an external
            // thread can terminate the broadcast in which case, we quit.
            auto returnValue = sl_collect(mSEEDLinkConnection, &seedLinkPacket);
            // Deal with packet
            if (returnValue == SLPACKET)
            {
                auto packetType = sl_packettype(seedLinkPacket);
                // I really only care about data packets
                if (packetType == SLDATA)
                {
                    //auto sequenceNumber = sl_sequence(seedLinkPacket);
                    auto miniSEEDRecord
                        = reinterpret_cast<char *> (seedLinkPacket->msrecord);
                    try
                    {
                        auto packet = ::miniSEEDToDataPacket(miniSEEDRecord,
                                                             mSEEDRecordSize);
                        mAddPacketFunction(std::move(packet));
                    }
                    catch (const std::exception &e)
                    {
                        spdlog::warn("Skipping packet.  Unpacking failed with: "
                                   + std::string(e.what()));
                    }
                    if (mUseStateFile)
                    {
                        if (updateStateFile > mStateFileUpdateInterval)
                        {
                            sl_savestate(mSEEDLinkConnection,
                                         mStateFile.c_str());
                            updateStateFile = 0;
                        }
                        updateStateFile = updateStateFile + 1;
                    }
                }
            }
            else if (returnValue == SLTERMINATE)
            {
                spdlog::info("SEEDLink terminate request received");
                mConnected = false;
                break;
            }
            else
            {
                spdlog::warn("Unhandled SEEDLink return value: "
                           + std::to_string(returnValue));
                continue;
            }
        }
        spdlog::debug("Thread leaving SEEDLink polling loop");
        mConnected = false;
    }
    /// Initialize
    void initialize(const SEEDLinkOptions &options)
    {
        mHaveOptions = false;
        disconnect();
        mInitialized = false;
        // Create a new instance
        mSEEDLinkConnection = sl_newslcd();
        // Set the connection string
        auto address = options.getAddress();
        auto port = options.getPort();
        auto seedLinkAddress = address +  ":" + std::to_string(port);
        spdlog::info("Connecting to SEEDLink server "
                   + seedLinkAddress + "...");
        mSEEDLinkConnection->sladdr = strdup(seedLinkAddress.c_str());
        // Set the record size and state file
        mSEEDRecordSize = options.getSEEDRecordSize();
        if (options.haveStateFile())
        {
            mStateFile = options.getStateFile();
            mStateFileUpdateInterval = options.getStateFileUpdateInterval();
            mUseStateFile = true;
        }   
        // If there are selectors then try to use them
        constexpr int sequenceNumber{-1}; // Start at next data
        const char *timeStamp{nullptr};
        auto streamSelectors = options.getStreamSelectors();
        for (const auto &selector : streamSelectors)
        {
            try
            {
                auto network = selector.getNetwork();
                auto station = selector.getStation();
                auto streamSelector = selector.getSelector();
                spdlog::info("Adding: "
                           + network + " "
                           + station + " "
                           + streamSelector);
                auto returnCode = sl_addstream(mSEEDLinkConnection,
                                               network.c_str(),
                                               station.c_str(),
                                               streamSelector.c_str(),
                                               sequenceNumber,
                                               timeStamp);
                if (returnCode != 0)
                {
                    throw std::runtime_error("Failed to add selector: "
                                           + network + " "
                                           + station + " "
                                           + streamSelector);
                }
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Could not add selector because "
                            + std::string {e.what()});
            }
        }
        // Configure uni-station mode if no streams were specified
        if (mSEEDLinkConnection->streams == nullptr)
        {
            const char *selectors{nullptr};
            auto returnCode = sl_setuniparams(mSEEDLinkConnection,
                                              selectors, sequenceNumber,
                                              timeStamp);
            if (returnCode != 0)
            {
                spdlog::error("Could not set SEEDLink uni-station mode");
                throw std::runtime_error(
                    "Failed to create a SEEDLink uni-station client");
            }
        }
        // Time out and reconnect delay
        auto networkTimeOut
            = static_cast<int> (options.getNetworkTimeOut().count());
        mSEEDLinkConnection->netto = networkTimeOut;
        auto reconnectDelay
            = static_cast<int> (options.getNetworkReconnectDelay().count());
        mSEEDLinkConnection->netdly = reconnectDelay;
        // Check this worked
        std::string slSite(256, '\0');
        std::string slServerID(256, '\0');
        auto returnCode = sl_ping(mSEEDLinkConnection,
                                  slServerID.data(),
                                  slSite.data());
        if (returnCode != 0)
        {
            if (returnCode ==-1)
            {
                spdlog::warn("Invalid ping response");
            }
            else
            {
                spdlog::error("Could not connect to server");
                throw std::runtime_error("Failed to connect");
            }
        }
        // All-good
        mOptions = options;
        mInitialized = true;
        mHaveOptions = true;
    }
    mutable std::mutex mMutex;
    std::function<void(Packet &&packet)> mAddPacketFunction;
    std::thread mSEEDLinkReaderThread;
    SLCD *mSEEDLinkConnection{nullptr};
    SEEDLinkOptions mOptions; 
    std::string mStateFile;
    std::atomic<bool> mKeepRunning{true};
    std::atomic<bool> mConnected{false};
    int mStateFileUpdateInterval{100};
    int mSEEDRecordSize{512};
    bool mHaveOptions{false};
    bool mUseStateFile{false};
    bool mInitialized{false};
};

/// Constructor
SEEDLink::SEEDLink(const SEEDLinkOptions &options) :
    pImpl(std::make_unique<SEEDLinkImpl> ()),
    IDataClient()
{
    pImpl->mAddPacketFunction
        = std::bind(&IDataClient::addPacket, this,
                    std::placeholders::_1);
}

/// Destructor
SEEDLink::~SEEDLink() = default;

/// Stop the client
void SEEDLink::stop()
{
    pImpl->stop();
}

/// Connect
void SEEDLink::connect()
{
    if (!pImpl->mHaveOptions)
    {
        throw std::runtime_error("SEEDLink client not initialized");
    }
    stop();
    auto optionsCopy = pImpl->mOptions;
    pImpl->initialize(optionsCopy);
}

/// Connected?
bool SEEDLink::isConnected() const noexcept
{
    return pImpl->mConnected;
}

/// Start the client
void SEEDLink::start()
{
    if (!isInitialized())
    {
        throw std::runtime_error("SEEDLink client not initialized");
    }
    pImpl->start();
}

/// Initialized
bool SEEDLink::isInitialized() const noexcept
{
    return pImpl->mInitialized;
}

/// Type
IDataClient::Type SEEDLink::getType() const noexcept
{
    return Type::SEEDLink;
}
