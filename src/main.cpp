#include <iostream>
#include <string>
#include <thread>
#include <atomic>
#include <csignal>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <opentelemetry/metrics/meter_provider.h>
#include <opentelemetry/metrics/provider.h>
#include "uWaveServer/packet.hpp"
#include "uWaveServer/packetSanitizer.hpp"
#include "uWaveServer/packetSanitizerOptions.hpp"
#include "uWaveServer/testDuplicatePacket.hpp"
#include "uWaveServer/testFuturePacket.hpp"
#include "uWaveServer/testExpiredPacket.hpp"
#include "uWaveServer/dataClient/grpc.hpp"
#include "uWaveServer/dataClient/grpcOptions.hpp"
#include "uWaveServer/dataClient/seedLink.hpp"
#include "uWaveServer/dataClient/seedLinkOptions.hpp"
#include "uWaveServer/dataClient/dataClient.hpp"
#include "uWaveServer/dataClient/streamSelector.hpp"
#include "uWaveServer/database/writeClient.hpp"
#include "uWaveServer/database/credentials.hpp"
#include "private/threadSafeBoundedQueue.hpp"
//#include "getEnvironmentVariable.hpp"
//#include "writerMetrics.hpp"

#define APPLICATION_NAME "uwsDataLoader"

import ProgramOptions;
import WriterMetrics;
import Logger;
import OTelOptions;
import GetEnvironmentVariable;

[[nodiscard]] std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[]);
//void setVerbosityForSPDLOG(const int verbosity);


namespace
{       
std::atomic_bool mInterrupted{false};

opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    totalPacketsReceivedCounter;
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    totalPacketsWrittenCounter;
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
    totalPacketsRejectedCounter;
opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
    databaseWritePerformanceHistogram{nullptr};

}

std::string toName(const UWaveServer::Packet &packet)
{
    auto name = packet.getNetwork() + "." 
              + packet.getStation() + "." 
              + packet.getChannel();
    if (!packet.getLocationCode().empty())
    {
        name = name + "." + packet.getLocationCode();
    }
    return name;
}

/*
struct ProgramOptions
{
    UWaveServer::PacketSanitizerOptions mPacketSanitizerOptions;
    std::vector<UWaveServer::DataClient::SEEDLinkOptions> seedLinkOptions;
    std::string applicationName{APPLICATION_NAME};
    std::string prometheusURL{"localhost:9020"};
    std::string databaseUser{UWaveServer::getEnvironmentVariable("UWAVE_SERVER_DATABASE_READ_WRITE_USER")};
    std::string databasePassword{UWaveServer::getEnvironmentVariable("UWAVE_SERVER_DATABASE_READ_WRITE_PASSWORD")};
    std::string databaseName{UWaveServer::getEnvironmentVariable("UWAVE_SERVER_DATABASE_NAME")};
    std::string databaseHost{UWaveServer::getEnvironmentVariable("UWAVE_SERVER_DATABASE_HOST", "localhost")};
    std::string databaseSchema{UWaveServer::getEnvironmentVariable("UWAVE_SERVER_DATABASE_SCHEMA", "")};
    int databasePort{UWaveServer::getIntegerEnvironmentVariable("UWAVE_SERVER_DATABASE_PORT", 5432)};
    int mQueueCapacity{8092}; // Want this big enough but not too big
    int nDatabaseWriterThreads{1}; 
    int verbosity{3};
};
*/

UWaveServer::ProgramOptions parseIniFile(const std::string &iniFile);

class Process
{
public:
    Process() = delete;
    /// Constructor
    Process(const UWaveServer::ProgramOptions &options,
            std::shared_ptr<spdlog::logger> logger) :
        mProgramOptions(options),
        mLogger(logger)
    //        std::unique_ptr<UWaveServer::Database::Client> &&databaseClient)
    {
        nDatabaseWriterThreads = options.nDatabaseWriterThreads;
        // Reserve size the queues
        mShallowPacketSanitizerQueue.setCapacity(options.mQueueCapacity);
        //mDeepPacketSanitizerQueue.setCapacity(options.mQueueCapacity);
        mWritePacketToDatabaseQueue.setCapacity(options.mQueueCapacity);

        // Create the database connection
        SPDLOG_LOGGER_DEBUG(mLogger,
            "Creating TimeSeriesDB PostgreSQL database connection...");
        for (int iThread = 0; iThread < nDatabaseWriterThreads; ++iThread)
        {
            UWaveServer::Database::Credentials databaseCredentials;
            databaseCredentials.setUser(mProgramOptions.databaseUser);
            databaseCredentials.setPassword(mProgramOptions.databasePassword);
            databaseCredentials.setHost(mProgramOptions.databaseHost);
            databaseCredentials.setPort(mProgramOptions.databasePort);
            databaseCredentials.setDatabaseName(mProgramOptions.databaseName);
            databaseCredentials.setApplication(
                 mProgramOptions.applicationName
               + "-" + std::to_string(iThread));
            if (!options.databaseSchema.empty())
            {
                SPDLOG_LOGGER_INFO(mLogger,
                                   "Will connect to schema {}",
                                   options.databaseSchema);
                databaseCredentials.setSchema(options.databaseSchema);
            }
            auto databaseClient 
                = std::make_unique<UWaveServer::Database::WriteClient>
                  (databaseCredentials, mLogger);
            mDatabaseClients.push_back(std::move(databaseClient)); 
        }

        // Create data clients
        if (!options.seedLinkOptions.empty())
        {
            SPDLOG_LOGGER_DEBUG(mLogger, "Creating SEEDLink clients...");
            for (const auto &seedLinkOptions : options.seedLinkOptions)
            {
                std::unique_ptr<UWaveServer::DataClient::IDataClient> client
                    = std::make_unique<UWaveServer::DataClient::SEEDLink>
                        (mAddPacketsFromAcquisitionCallback, seedLinkOptions, mLogger);
                mDataAcquisitionClients.push_back(std::move(client));
            }
        } 

        if (!options.grpcOptions.empty())
        {
            SPDLOG_LOGGER_DEBUG(mLogger, "Creating gRPC clients...");
            for (const auto &grpcOptions : options.grpcOptions)
            {
                std::unique_ptr<UWaveServer::DataClient::IDataClient> client
                    = std::make_unique<UWaveServer::DataClient::GRPC>
                      (mAddPacketsFromAcquisitionCallback, grpcOptions, mLogger);
                mDataAcquisitionClients.push_back(std::move(client));
            }
        }

        if (mDataAcquisitionClients.empty())
        {
            throw std::runtime_error("No acquistion clients created!");
        }

        // Initialize metrics
        if (mProgramOptions.exportMetrics)
        {
            // Need a provider from which to get a meter.  This is initialized
            // once and should last the duration of the application.
            auto provider 
                = opentelemetry::metrics::Provider::GetMeterProvider();
    
            // Meter will be bound to application (library, module, class, etc.)
            // so as to identify who is genreating these metrics.
            auto meter
                = provider->GetMeter(mProgramOptions.applicationName, "1.2.0");

            namespace UMetrics = UWaveServer::Metrics;
            // Packets received from import
            totalPacketsReceivedCounter
                = meter->CreateInt64ObservableCounter(
                    "seismic_data.waveform_storage.client.packets.received",
                    "Number of packets received from acquisition.",
                    "{packets}");
            totalPacketsReceivedCounter->AddCallback(
                UMetrics::observeNumberOfPacketsReceived,
                nullptr);

            totalPacketsReceivedCounter
                = meter->CreateInt64ObservableCounter(
                    "seismic_data.waveform_storage.client.packets.written",
                    "Number of packets written to database.",
                    "{packets}");
            totalPacketsReceivedCounter->AddCallback(
                UMetrics::observeNumberOfPacketsWritten,
                nullptr);

            auto histogramMeter
                = provider->GetMeter("database_write_duration", "1.2.0");
            databaseWritePerformanceHistogram
                = histogramMeter->CreateDoubleHistogram(
                  "seismic_data.waveform_storage.database.write.duration",
                  "Time required to write packet to the database",
                  "{s}");

        }

        mInitialized = true;
    }
    void addPacketsFromAcquisition(std::vector<UWaveServer::Packet> &&packetsIn)
    {
        for (auto &packet : packetsIn)
        {
            addPacketFromAcquisition(std::move(packet));
        } 
    }
    // Adds a packet obtained by the acquisition
    void addPacketFromAcquisition(UWaveServer::Packet &&packet)
    {
        if (!packet.hasNetwork())
        {
            SPDLOG_LOGGER_WARN(mLogger, "Network not set on packet - skipping");
            return;
        }
        if (!packet.hasStation())
        {
            SPDLOG_LOGGER_WARN(mLogger, "Station not set on packet - skipping");
            return;
        }
        if (!packet.hasChannel())
        {
            SPDLOG_LOGGER_WARN(mLogger, "Channel not set on packet - skipping");
            return;
        }
        if (!packet.hasLocationCode())
        {
            SPDLOG_LOGGER_WARN(mLogger,
                               "Location code not set on packet - skipping");
            return;
        }
        if (!packet.hasSamplingRate())
        {
            auto name = ::toName(packet);
            SPDLOG_LOGGER_WARN(mLogger,
                             "Sampling rate not set on {}'s packet - skipping",
                             name);
            return;
        }
        if (packet.empty())
        {
            auto name = ::toName(packet);
            SPDLOG_LOGGER_WARN(mLogger, "No data on {}'s packet - skipping",
                               name);
            return;
        }
        //mObservableReceivedPacketsCounter.fetch_add(
        //    1, std::memory_order_relaxed);
        mShallowPacketSanitizerQueue.push(std::move(packet));
    }
    // Data acquisitions likely will have similar latencies.  So the first
    // thing to do is just check if we're seeing the latest near real-time
    // packet.
    void shallowDeduplicator()
    {
#ifndef NDEBUG
        assert(mTestFuturePacket != nullptr);
        assert(mTestExpiredPacket != nullptr);
#endif
        SPDLOG_LOGGER_INFO(mLogger, "Thread entering shallow packet sanitizer");
        auto &metrics
            = UWaveServer::Metrics::MetricsSingleton::getInstance();
        const std::chrono::milliseconds mTimeOut{10};
        while (keepRunning())
        {
            UWaveServer::Packet packet;
            auto gotPacket 
                = mShallowPacketSanitizerQueue.wait_until_and_pop(
                     &packet, mTimeOut);
            if (gotPacket)
            {
                metrics.incrementReceivedPacketsCounter();
                bool allow{true};
                // Handle future data
                try
                {
                    if (allow)
                    {
                        allow = mTestFuturePacket->allow(packet);
                        if (!allow)
                        {
                            metrics.incrementRejectedPacketsCounter(UWaveServer::Metrics::MetricsSingleton::Reason::Future);
                            //mObservableRejectedPacketsCounter.add_or_assign(
                            //    "future", 1);
                        }
                    }
                }
                catch (const std::exception &e)
                {
                    SPDLOG_LOGGER_WARN(mLogger,
                           "Failed to check future packet data because {} - skipping", 
                           std::string {e.what()});
                    allow = false;
                } 
                // Handle expired data
                try
                {
                    if (allow)
                    {
                        allow = mTestExpiredPacket->allow(packet);
                        if (!allow)
                        {
                            metrics.incrementRejectedPacketsCounter(UWaveServer::Metrics::MetricsSingleton::Reason::Expired);
                            //mObservableRejectedPacketsCounter.add_or_assign(
                            //   "expired", 1);
                        }
                    }
                }
                catch (const std::exception &e)
                {
                    SPDLOG_LOGGER_WARN(mLogger,
                    "Failed to check expired packet data because {} - skipping",
                    std::string {e.what()});
                    allow = false;
                }
                // Handle duplicate data
                try
                {
                    if (allow &&
                        static_cast<int> (mDataAcquisitionClients.size()) > 1)
                    {
                        allow = mTestShallowDuplicatePacket.allow(packet);
                        if (!allow)
                        {
                            metrics.incrementRejectedPacketsCounter(UWaveServer::Metrics::MetricsSingleton::Reason::Duplicate);
                            //mObservableRejectedPacketsCounter.add_or_assign(
                            //    "duplicate", 1); 
                        }   
                    }
                    if (allow)
                    {
                        allow = mTestDeepDuplicatePacket.allow(packet);
                        if (!allow)
                        {
                            metrics.incrementRejectedPacketsCounter(UWaveServer::Metrics::MetricsSingleton::Reason::Duplicate);
                            //mObservableRejectedPacketsCounter.add_or_assign(
                            //    "duplicate", 1);
                        }
                    }
                }
                catch (const std::exception &e)
                {
                    SPDLOG_LOGGER_WARN(mLogger,
                    "Failed to test for duplicate packet because {} - skipping",
                        std::string {e.what()});
                    allow = false;
                }
                if (allow)
                {
                    mWritePacketToDatabaseQueue.push(std::move(packet));
                }
            }
        }
        SPDLOG_LOGGER_INFO(mLogger, "Thread leaving shallow packet sanitizer");
    }
/*
    /// Next, we perform a deeper deduplication process.  This helps to
    /// remove fairly latent data.
    void deepDeduplicator()
    {
        SPDLOG_LOGGER_INFO(mLogger, "Thread entering deep deduplicator");
        const std::chrono::milliseconds mTimeOut{10}; 
        while (keepRunning())
        {
            UWaveServer::Packet packet;
            auto gotPacket 
                = mDeepPacketSanitizerQueue.wait_until_and_pop(
                     &packet, mTimeOut);
            if (gotPacket)
            {
            }
        }
        SPDLOG_LOGGER_DEBUG(mLogger, "Thread leaving deep deduplicator");
    }
*/
    /// Finally, we write this to the database.  This is where the real compute
    /// work happens (on postgres's end).
    void writePacketToDatabase(int iThread)
    {
        SPDLOG_LOGGER_INFO(mLogger, "Thread {} entering database writer",
                           std::to_string(iThread));
        // We'll keep this pretty coarse - we could write stats for each
        // table but that is getting into the realm of telemetry metrics
        auto databaseKey = mProgramOptions.databaseName;
        if (!mProgramOptions.databaseSchema.empty())
        {
            databaseKey = databaseKey + "." + mProgramOptions.databaseSchema;
        }

        // Testers
        mTestFuturePacket
            = std::make_unique<UWaveServer::TestFuturePacket> (
                std::chrono::microseconds {0},
                std::chrono::hours {1},
                mLogger);
        mTestExpiredPacket
            = std::make_unique<UWaveServer::TestExpiredPacket> (
                std::chrono::days {60},
                std::chrono::hours {1},
                mLogger);

        //mObservablePacketsWritten.add_or_assign(databaseKey, 0);
        //mObservablePacketsNotWritten.add_or_assign(databaseKey, 0);
        auto &metrics
            = UWaveServer::Metrics::MetricsSingleton::getInstance();
        std::map<std::string, std::string> histogramKey{ {"database", databaseKey} };
        auto otelContext = opentelemetry::context::Context {};

        auto nowMuSeconds
           = std::chrono::time_point_cast<std::chrono::microseconds>
             (std::chrono::high_resolution_clock::now()).time_since_epoch();
        auto lastLogTime
            = std::chrono::duration_cast<std::chrono::seconds> (nowMuSeconds);

        const std::chrono::milliseconds mTimeOut{10};
//int printEvery{0};
        int consecutiveFailureCounter{0};
        int nRowsWritten{0};
        double averageTime{0};
        double cumulativeTime{0};
        while (keepRunning())
        {
            UWaveServer::Packet packet;
            auto gotPacket 
                = mWritePacketToDatabaseQueue.wait_until_and_pop(
                     &packet, mTimeOut);
            if (gotPacket)
            {
                try
                {
                    auto t1 = std::chrono::high_resolution_clock::now(); 
                    mDatabaseClients.at(iThread)->write(packet);
                    consecutiveFailureCounter = 0;
                    auto t2 = std::chrono::high_resolution_clock::now();
                    double duration
                        = std::chrono::duration_cast<std::chrono::microseconds>
                          (t2 - t1).count()*1.e-6;
                    //mObservablePacketsWritten.add_or_assign(databaseKey, 1);
                    metrics.incrementWrittenPacketsCounter();
                    if (databaseWritePerformanceHistogram)
                    {
                        databaseWritePerformanceHistogram->Record(duration,
                                                                  histogramKey,
                                                                  otelContext);
                    }
                    averageTime = averageTime + duration;
                    cumulativeTime = cumulativeTime + duration;
                    nRowsWritten = nRowsWritten + 1; //packet.size();
                    //printEvery = printEvery + 1;
 
                    nowMuSeconds
                       = std::chrono::time_point_cast<std::chrono::microseconds>
                         (t2).time_since_epoch();
                    auto nowSeconds
                        = std::chrono::duration_cast<std::chrono::seconds>
                          (nowMuSeconds);

                    //if (printEvery > 1000)
                    if (nowSeconds >= lastLogTime
                                    + mLogWritePerformanceInterval)
                    {
                        spdlog::info(std::to_string(nRowsWritten)
                                   + " packets written on thread " + std::to_string(iThread) 
                                   + ".  Average packet write time took "
                                   + std::to_string (averageTime/nRowsWritten)
                                   + " seconds.  ("
                                   + std::to_string(static_cast<int> (std::round(nRowsWritten/cumulativeTime)))
                                   + " rows/second)" );
                        //printEvery = 0;
                        averageTime = 0;
                        nRowsWritten = 0;
                        cumulativeTime = 0;
                        lastLogTime = nowSeconds;
                    }
                }
                catch (const std::exception &e)
                {
                    SPDLOG_LOGGER_WARN(mLogger,
                                   "Failed to add packet to database because {}",
                                   std::string {e.what()});
                    //mObservablePacketsNotWritten.add_or_assign(databaseKey, 1);
                    metrics.incrementNotWrittenPacketsCounter();
                    consecutiveFailureCounter = consecutiveFailureCounter + 1;
                    if (consecutiveFailureCounter == 100)
                    {
                        SPDLOG_LOGGER_CRITICAL(mLogger,
                           "Too many consecutive db write failures");
                        throw std::runtime_error(
                           "Too many consecutive failures writing packets");
                    }
                }
            }
        }
    }

    void addPacketsCallback(std::vector<UWaveServer::Packet> &&packets)
    {
        for (auto &packet : packets)
        {
            // If the packet doesn't have data then just skip it
            if (packet.empty()){continue;}
            // Is the packet valid?
            if (!packet.hasNetwork())
            {
                SPDLOG_LOGGER_WARN(mLogger,
                                   "Network code not set on packet - skipping");
                continue;
            }
            if (!packet.hasStation())
            {
                SPDLOG_LOGGER_WARN(mLogger,
                                   "Station name not set on packet - skipping");
                continue;
            }
            if (!packet.hasChannel())
            {
                SPDLOG_LOGGER_WARN(mLogger,
                                   "Channel code not set on packet - skipping");
                continue;
            }
            // Location code we can fake
            if (!packet.hasLocationCode()){packet.setLocationCode("--");}
            if (!packet.hasSamplingRate())
            {
                SPDLOG_LOGGER_WARN(mLogger,
                                  "Sampling rate not set on packet - skipping");
                continue;
            }
            // Add packet to shallow deduplicator
            try
            {
                mShallowPacketSanitizerQueue.push(std::move(packet));
            }
            catch (const std::exception &e)
            {
                SPDLOG_LOGGER_WARN(mLogger,
                    "Failed to add packet to initial packet queue because {}",
                    std::string {e.what()});
            }
        }
    }
    /// Handles sigterm and sigint
    static void signalHandler(const int )
    {   
        mInterrupted = true;
    }
    static void catchSignals()
    {
        struct sigaction action;
        action.sa_handler = signalHandler;
        action.sa_flags = 0;
        sigemptyset(&action.sa_mask);
        sigaction(SIGINT,  &action, NULL);
        // Kubernetes wants this.  Don't mess with SIGKILL b/c since that is
        // Kubernetes's hammmer.  You basically have 30 seconds to shut
        // down after SIGTERM or the hammer is coming down!
        sigaction(SIGTERM, &action, NULL);
    }
    /// Place for the main thread to sleep until someone wakes it up.
    void handleMainThread()
    {
        SPDLOG_LOGGER_DEBUG(mLogger, "Main thread entering waiting loop");
        catchSignals();
        {
            while (!mStopRequested)
            {
                if (mInterrupted)
                {
                    SPDLOG_LOGGER_INFO(mLogger,
                                       "SIGINT/SIGTERM signal received!");
                    mStopRequested = true;
                    break;
                }
                if (!checkFuturesOkay(std::chrono::milliseconds {5}))
                {
                    SPDLOG_LOGGER_CRITICAL(mLogger,
                       "Futures exception caught; terminating app");
                    mStopRequested = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds {50});
            }
        }
        if (mStopRequested)
        {
            SPDLOG_LOGGER_DEBUG(mLogger,
                                "Stop request received.  Terminating...");
            stop();
        }
    }

    /// @result True indicates the processes should continue to run.
    [[nodiscard]] bool keepRunning() const noexcept
    {
        return mRunning;
    }
    /// @brief Toggles the mode as running or not running.
    void setRunning(bool running)
    {
        mRunning = running; 
    }
    /// @brief Starts the processes
    void start()
    {
        if (!mInitialized)
        {
            throw std::runtime_error("Class not initialized");
        }
        stop();
        setRunning(true);
        mDataAcquisitionFutures.clear();
        for (auto &dataAcquisitionClient : mDataAcquisitionClients)
        {
            SPDLOG_LOGGER_INFO(mLogger, "Starting client");
            mDataAcquisitionFutures.push_back(
                dataAcquisitionClient->start());
        }
        mShallowPacketSanitizerThread
            = std::thread(&::Process::shallowDeduplicator, this);
        //mDeepPacketSanitizerThread
        //    = std::thread(&::Process::deepDeduplicator, this);
        mDatabaseWriterThreads.clear();
        for (int i = 0; i < nDatabaseWriterThreads; ++i)
        {
            auto databaseWriterThread
               = std::thread(&::Process::writePacketToDatabase, this, i);
            mDatabaseWriterThreads.push_back(std::move(databaseWriterThread));
        }
    }
    /// @brief Stops the processes.
    void stop()
    {
        setRunning(false);
        for (auto &dataAcquisitionClient : mDataAcquisitionClients)
        {
            dataAcquisitionClient->stop();
        }
        for (auto &dataAcquisitionFuture : mDataAcquisitionFutures)
        {
            if (dataAcquisitionFuture.valid())
            {
                dataAcquisitionFuture.get();
            }
        }
        mDataAcquisitionFutures.clear();

        if (mShallowPacketSanitizerThread.joinable())
        {
            mShallowPacketSanitizerThread.join();
        }
        //if (mDeepPacketSanitizerThread.joinable())
        //{
        //    mDeepPacketSanitizerThread.join();
        //}
        for (auto &databaseWriterThread : mDatabaseWriterThreads)
        {
            if (databaseWriterThread.joinable())
            {
                databaseWriterThread.join();
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds {50});
        mDatabaseWriterThreads.clear();
        //mWriterThroughPut.clear();
        emptyQueues();
    }
    /// @brief Starts the processes
    void emptyQueues()
    {   
        while (!mShallowPacketSanitizerQueue.empty())
        {
            mShallowPacketSanitizerQueue.pop();
        }
        //while (!mDeepPacketSanitizerQueue.empty())
        //{   
        //    mDeepPacketSanitizerQueue.pop();
        //}   
        while (!mWritePacketToDatabaseQueue.empty())
        {
            mWritePacketToDatabaseQueue.pop();
        }
    }
    /// @brief Checks the futures
    /// @result True indicates the all the processes are running a-okay.
    [[nodiscard]]
    bool checkFuturesOkay(const std::chrono::milliseconds &timeOut)
    {
        bool isOkay{true};
        for (auto &future : mDataAcquisitionFutures)
        {
            try
            {
                auto status = future.wait_for(timeOut);
                if (status == std::future_status::ready)
                {
                    future.get();
                }
            }
            catch (const std::exception &e) 
            {
                SPDLOG_LOGGER_CRITICAL(mLogger,
                                       "Fatal error in acquisition: {}",
                                       std::string {e.what()});
                isOkay = false;
            }
        }
        return isOkay;
    }
    ::ThreadSafeBoundedQueue<UWaveServer::Packet> mShallowPacketSanitizerQueue;
    //::ThreadSafeBoundedQueue<UWaveServer::Packet> mDeepPacketSanitizerQueue;
    ::ThreadSafeBoundedQueue<UWaveServer::Packet> mWritePacketToDatabaseQueue;
    std::vector<std::unique_ptr<UWaveServer::Database::WriteClient>>
        mDatabaseClients;
    std::vector<std::unique_ptr<UWaveServer::DataClient::IDataClient>>
        mDataAcquisitionClients;
    std::function<void(std::vector<UWaveServer::Packet> &&packet)>
        mAddPacketsFromAcquisitionCallback
    {
        std::bind(&::Process::addPacketsFromAcquisition, this,
                  std::placeholders::_1)
    };
    UWaveServer::TestDuplicatePacket mTestShallowDuplicatePacket {
        15, // Last 15 packets (good for multiple telemetry routes)
        std::chrono::seconds {-1}};
    UWaveServer::TestDuplicatePacket mTestDeepDuplicatePacket {
        std::chrono::seconds {120},
        std::chrono::hours {1}};
    std::unique_ptr<UWaveServer::TestFuturePacket> mTestFuturePacket{nullptr};
    std::unique_ptr<UWaveServer::TestExpiredPacket> mTestExpiredPacket{nullptr};
/*
    UWaveServer::TestFuturePacket mTestFuturePacket{
        std::chrono::microseconds {0},
        std::chrono::hours {1}};
    UWaveServer::TestExpiredPacket mTestExpiredPacket{
        // We'll likely hold onto data longer but telemetry won't
        // have data older than a few weeks
        std::chrono::days {60},
        std::chrono::hours {1}};
*/
    UWaveServer::ProgramOptions mProgramOptions;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    mutable std::mutex mStopContext;
    std::condition_variable mStopCondition;
    std::vector<std::future<void>> mDataAcquisitionFutures;
    std::vector<std::thread> mDatabaseWriterThreads;
    //std::vector<std::pair<int, double>> mWriterThroughPut;
    std::thread mShallowPacketSanitizerThread;
    std::chrono::seconds mLogWritePerformanceInterval{3600};
    std::chrono::seconds mMaximumLatency{-1};
    std::atomic<bool> mRunning{true};
    bool mStopRequested{false};
    int nDatabaseWriterThreads{4};
    bool mInitialized{false};
};

int main(int argc, char *argv[]) 
{
    // Get the ini file from the command line
    std::string iniFile;
    try
    {
        auto [iniFileName, isHelp] = ::parseCommandLineOptions(argc, argv);
        if (isHelp){return EXIT_SUCCESS;}
        iniFile = iniFileName;
    }
    catch (const std::exception &e)
    {    
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    UWaveServer::ProgramOptions programOptions;
    try
    {
        programOptions = ::parseIniFile(iniFile);
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    auto logger
        = UWaveServer::Logger::initialize(programOptions.verbosity,
                                          programOptions.exportLogs,
                                          programOptions.otelHTTPLogOptions);

    // Initialize metrics
    UWaveServer::Metrics::initializeMetricsSingleton();
    try
    {
        UWaveServer::Metrics::initialize(programOptions.exportMetrics,
                                         programOptions.otelHTTPMetricsOptions);
        //::initializeMetrics(programOptions.prometheusURL);
        //::initializeWriterMetrics(programOptions.applicationName);
    }    
    catch (const std::exception &e)  
    {    
        SPDLOG_LOGGER_CRITICAL(logger, "Failed to initalize metrics with {}",
                               std::string {e.what()});
        UWaveServer::Logger::cleanup();
        return EXIT_FAILURE;
    }    


    // Create the data source connections
    SPDLOG_LOGGER_INFO(logger, "Initializing processes...");
    std::unique_ptr<::Process> process{nullptr};
    try
    {
        process = std::make_unique<::Process> (programOptions, logger);
    }
    catch (const std::exception &e)
    {
        SPDLOG_LOGGER_CRITICAL(logger,
            "Failed to initialize worker class; failed with {}",
            std::string{e.what()});
        UWaveServer::Metrics::cleanup();
        UWaveServer::Logger::cleanup();
        return EXIT_FAILURE;
    }

    try
    {
        process->start();
        process->handleMainThread();
        //process->stop();
        UWaveServer::Metrics::cleanup();
        UWaveServer::Logger::cleanup();
    }
    catch (const std::exception &e)
    {
       SPDLOG_LOGGER_CRITICAL(logger, "An error occurred during processing {}",
                              std::string {e.what()});
       UWaveServer::Metrics::cleanup();
       UWaveServer::Logger::cleanup();
       return EXIT_FAILURE;
    }

    // Initialize the utility that will map from the acquisition to the database 

    return EXIT_SUCCESS;
}

///--------------------------------------------------------------------------///
///                            Utility Functions                             ///
///--------------------------------------------------------------------------///
/// Read the program options from the command line
std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[])
{
    std::string iniFile;
    boost::program_options::options_description desc(R"""(
The uwsDataLoader maps data from a telemetry to the waveserver TimescaleDB
Postgres database.

Example usage is

    uwsDataLoader --ini=loader.ini

Allowed options)""");
    desc.add_options()
        ("help", "Produces this help message")
        ("ini",  boost::program_options::value<std::string> (),
                 "The initialization file for this executable");
    boost::program_options::variables_map vm;
    boost::program_options::store(
        boost::program_options::parse_command_line(argc, argv, desc), vm); 
    boost::program_options::notify(vm);
    if (vm.count("help"))
    {    
        std::cout << desc << std::endl;
        return {iniFile, true};
    }
    if (vm.count("ini"))
    {    
        iniFile = vm["ini"].as<std::string>();
        if (!std::filesystem::exists(iniFile))
        {
            throw std::runtime_error("Initialization file: " + iniFile
                                   + " does not exist");
        }
    }
    return {iniFile, false};
}

/*
void setVerbosityForSPDLOG(const int verbosity)
{
    if (verbosity <= 1)
    {   
        spdlog::set_level(spdlog::level::critical);
    }   
    if (verbosity == 2){spdlog::set_level(spdlog::level::warn);}
    if (verbosity == 3){spdlog::set_level(spdlog::level::info);}
    if (verbosity >= 4){spdlog::set_level(spdlog::level::debug);}
}   
*/

[[nodiscard]]
std::string getOTelCollectorURL(boost::property_tree::ptree &propertyTree,
                                const std::string &section)
{
    std::string result;
    std::string otelCollectorHost
        = propertyTree.get<std::string> (section + ".host", "");
    uint16_t otelCollectorPort
        = propertyTree.get<uint16_t> (section + ".port", 4218);
    if (!otelCollectorHost.empty())
    {   
        result = otelCollectorHost + ":" 
               + std::to_string(otelCollectorPort);
    }   
    return result;
}

[[nodiscard]] UWaveServer::DataClient::SEEDLinkOptions
getSEEDLinkOptions(const boost::property_tree::ptree &propertyTree,
                   const std::string &clientName)
{
    UWaveServer::DataClient::SEEDLinkOptions clientOptions;
    auto address = propertyTree.get<std::string> (clientName + ".address");
    auto port = propertyTree.get<uint16_t> (clientName + ".port", 18000);
    clientOptions.setAddress(address);
    clientOptions.setPort(port);
    for (int iSelector = 1; iSelector <= 32768; ++iSelector)
    {
        std::string selectorName{clientName
                               + ".data_selector_"
                               + std::to_string(iSelector)};
        auto selectorString
            = propertyTree.get_optional<std::string> (selectorName);
        if (selectorString)
        {
            std::vector<std::string> splitSelectors;
            boost::split(splitSelectors, *selectorString,
                         boost::is_any_of(",|"));
            // A selector string can look like:
            // UU.FORK.HH?.01 | UU.CTU.EN?.01 | ....
            for (const auto &thisSplitSelector : splitSelectors)
            {
                /*
                std::vector<std::string> thisSelector; 
                auto splitSelector = thisSplitSelector;
                boost::algorithm::trim(splitSelector);

                // Need to preprocess selector so there's no double spaces
                for (int k = 1; k < static_cast<int> (splitSelector.size()); )
                {
                    if (splitSelector[k - 1] == splitSelector[k] &&
                        splitSelector[k] == ' ')
                    {
                        splitSelector.erase(k, 1);
                    }
                    else
                    {
                        ++k;
                    }
                }

                boost::split(thisSelector, splitSelector,
                             boost::is_any_of(" \t"));
                UWaveServer::DataClient::StreamSelector selector;
                if (splitSelector.empty())
                {
                    throw std::invalid_argument("Empty selector");
                }
                // Require a network
                auto network = thisSelector.at(0);
                boost::algorithm::trim(network);
                selector.setNetwork(network);
                // Add a station?
                if (splitSelector.size() > 1)
                {
                    auto station = thisSelector.at(1);
                    boost::algorithm::trim(station);
                    selector.setStation(station);
                }
                // Add channel + location code + data type
                std::string channel{"*"};
                std::string locationCode{"??"};
                if (splitSelector.size() > 2)
                {
                    channel = thisSelector[2];
                    boost::algorithm::trim(channel);
                }
                if (splitSelector.size() > 3)
                {
                    locationCode = thisSelector[3];
                    boost::algorithm::trim(locationCode);
                }
                // Data type
                auto dataType
                    = UWaveServer::DataClient::StreamSelector::Type::All;
                if (thisSelector.size() > 4)
                {
                    boost::algorithm::trim(thisSelector.at(4));
                    if (thisSelector.at(4) == "D")
                    {
                        dataType
                          = UWaveServer::DataClient::StreamSelector::Type::Data;
                    }
                    else if (thisSelector.at(4) == "A")
                    {
                        dataType 
                            = UWaveServer::DataClient::StreamSelector::Type::All;
                    }
                    // TODO other data types
                }
                selector.setSelector(channel, locationCode, dataType);
                */
                auto selector
                    = UWaveServer::DataClient::StreamSelector::fromString(
                        thisSplitSelector);
                clientOptions.addStreamSelector(selector);
            } // Loop on selectors
        } // End check on selector string
    } // Loop on selectors
    return clientOptions;
}

UWaveServer::ProgramOptions parseIniFile(const std::string &iniFile)
{   
    UWaveServer::ProgramOptions options;
    if (!std::filesystem::exists(iniFile)){return options;}
    // Parse the initialization file
    boost::property_tree::ptree propertyTree;
    boost::property_tree::ini_parser::read_ini(iniFile, propertyTree);

    // Application name
    options.applicationName
        = propertyTree.get<std::string> ("General.applicationName",
                                         options.applicationName);
    if (options.applicationName.empty())
    {   
        options.applicationName = APPLICATION_NAME;
    }   
    options.verbosity
        = propertyTree.get<int> ("General.verbosity", options.verbosity);

    options.nDatabaseWriterThreads
       = propertyTree.get<int> ("General.nDatabaseWriterThreads",
                                options.nDatabaseWriterThreads);
    if (options.nDatabaseWriterThreads < 1 ||
        options.nDatabaseWriterThreads > 2048)
    {
        throw std::invalid_argument(
            "Number of database threads must be between 1 and 2048");
    }

    /*
    // Prometheus
    uint16_t prometheusPort
        = propertyTree.get<uint16_t> ("Prometheus.port", 9200);
    std::string prometheusHost
        = propertyTree.get<std::string> ("Prometheus.host", "localhost");
    if (!prometheusHost.empty())
    {
        options.prometheusURL = prometheusHost + ":" 
                              + std::to_string(prometheusPort);
    }
    */

    // Logging
    UWaveServer::OTelHTTPLogOptions logOptions;
    logOptions.url
         = getOTelCollectorURL(propertyTree, "OTelHTTPLogOptions");
    logOptions.suffix
         = propertyTree.get<std::string>
           ("OTelHTTPLogOptions.suffix", "/v1/logs");
    if (!logOptions.url.empty())
    {   
        if (!logOptions.suffix.empty())
        {   
            if (!logOptions.url.ends_with("/") &&
                !logOptions.suffix.starts_with("/"))
            {   
                logOptions.suffix = "/" + logOptions.suffix;
            }
        }
    }
    if (!logOptions.url.empty())
    {   
        options.exportLogs = true;
        options.otelHTTPLogOptions = logOptions;
    }

    // Metrics
    UWaveServer::OTelHTTPMetricsOptions metricsOptions;
    metricsOptions.url
         = getOTelCollectorURL(propertyTree, "OTelHTTPMetricsOptions");
    metricsOptions.suffix
         = propertyTree.get<std::string> ("OTelHTTPMetricsOptions.suffix",
                                          "/v1/metrics");
    if (!metricsOptions.url.empty())
    {   
        if (!metricsOptions.suffix.empty())
        {
            if (!metricsOptions.url.ends_with("/") &&
                !metricsOptions.suffix.starts_with("/"))
            {
                metricsOptions.suffix = "/" + metricsOptions.suffix;
            }
        }
    }   

    // Database
    options.databaseUser
        = propertyTree.get<std::string> ("Database.user", 
                                         options.databaseUser);
    if (options.databaseUser.empty())
    {
        throw std::invalid_argument("Must specify database user as UWAVE_SERVER_DATABASE_READ_WRITE_USER or as Database.user in ini file");
    }
    options.databasePassword
        = propertyTree.get<std::string> ("Database.password",
                                         options.databasePassword);
    if (options.databasePassword.empty())
    {
        throw std::invalid_argument("Must specify database password as UWAVE_SERVER_DATABASE_READ_WRITE_PASSWORD or as Database.password in ini file");
    }
    options.databaseName
        = propertyTree.get<std::string> ("Database.name",
                                         options.databaseName);
    if (options.databaseName.empty())
    {
        throw std::invalid_argument("Must specify database name as UWAVE_SERVER_DATABASE_NAME or as Database.name in ini file");
    }
    options.databaseHost
        = propertyTree.get<std::string> ("Database.host",
                                         options.databaseHost);
    if (options.databaseHost.empty())
    {
        throw std::invalid_argument("Must specify database host as UWAVE_SERVER_DATABASE_HOST or as Database.host in ini file");
    }
    options.databasePort
        = propertyTree.get<uint16_t> ("Database.port", options.databasePort);
   
    options.databaseSchema
        = propertyTree.get<std::string> ("Database.schema", "");

    UWaveServer::PacketSanitizerOptions packetSanitizerOptions; 
    // Realistically, anything older than 2 -4 weeks isn't making it back
    // from the field.  2 months is pretty generous so we let the database
    // deal with it.
    packetSanitizerOptions.setMaximumLatency(std::chrono::seconds {60*86400});
    // We're really only interested in deduplicating from multiple data
    // feeds so this should be big enough to accomodate their latencies.
    packetSanitizerOptions.setCircularBufferDuration(std::chrono::seconds {60});
    packetSanitizerOptions.setMaximumFutureTime(std::chrono::seconds {0});
    packetSanitizerOptions.setBadDataLoggingInterval(std::chrono::seconds {60*10}); //-1});
    auto maximumLatency = static_cast<int> (packetSanitizerOptions.getMaximumLatency().count()); 
    auto maximumFutureTime = static_cast<int> (packetSanitizerOptions.getMaximumFutureTime().count());
    maximumLatency = propertyTree.get<int> ("PacketSanitizer.maximumLatency", maximumLatency);
    packetSanitizerOptions.setMaximumLatency(std::chrono::seconds {maximumLatency});
    maximumFutureTime = propertyTree.get<int> ("PacketSanitizer.maximumFutureTime", maximumFutureTime);
    packetSanitizerOptions.setMaximumFutureTime(std::chrono::seconds {maximumFutureTime});
    options.mPacketSanitizerOptions = packetSanitizerOptions;

    if (propertyTree.get_optional<std::string> ("SEEDLink.address"))
    {
        auto clientOptions = ::getSEEDLinkOptions(propertyTree, "SEEDLink");
        options.seedLinkOptions.push_back(std::move(clientOptions));
    }
    else
    {
        for (int iClient = 1; iClient <= 32768; ++iClient)
        {
            auto clientName = "SEEDLink_" + std::to_string(iClient);
            if (propertyTree.get_optional<std::string> (clientName + ".address"))
            {
                auto clientOptions
                    = ::getSEEDLinkOptions(propertyTree, clientName);
                options.seedLinkOptions.push_back(std::move(clientOptions));
            }
        }
    }
    return options;
}


