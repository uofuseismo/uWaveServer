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
#include "uWaveServer/packet.hpp"
#include "uWaveServer/packetSanitizer.hpp"
#include "uWaveServer/packetSanitizerOptions.hpp"
#include "uWaveServer/testDuplicatePacket.hpp"
#include "uWaveServer/testFuturePacket.hpp"
#include "uWaveServer/testExpiredPacket.hpp"
#include "uWaveServer/dataClient/seedLink.hpp"
#include "uWaveServer/dataClient/seedLinkOptions.hpp"
#include "uWaveServer/dataClient/dataClient.hpp"
#include "uWaveServer/dataClient/streamSelector.hpp"
#include "uWaveServer/database/writeClient.hpp"
#include "uWaveServer/database/credentials.hpp"
#include "private/threadSafeBoundedQueue.hpp"
#include "getEnvironmentVariable.hpp"
#include "metricsExporter.hpp"

[[nodiscard]] std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[]);

namespace
{       
std::atomic_bool mInterrupted{false};
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

struct ProgramOptions
{
    UWaveServer::PacketSanitizerOptions mPacketSanitizerOptions;
    std::vector<UWaveServer::DataClient::SEEDLinkOptions> seedLinkOptions;
    std::string applicationName{"uwsDataLoader"};
    std::string prometheusURL{"localhost:9020"};
    std::string databaseUser{::getEnvironmentVariable("UWAVE_SERVER_DATABASE_READ_WRITE_USER")};
    std::string databasePassword{::getEnvironmentVariable("UWAVE_SERVER_DATABASE_READ_WRITE_PASSWORD")};
    std::string databaseName{::getEnvironmentVariable("UWAVE_SERVER_DATABASE_NAME")};
    std::string databaseHost{::getEnvironmentVariable("UWAVE_SERVER_DATABASE_HOST", "localhost")};
    std::string databaseSchema{::getEnvironmentVariable("UWAVE_SERVER_DATABASE_SCHEMA", "")};
    int databasePort{::getIntegerEnvironmentVariable("UWAVE_SERVER_DATABASE_PORT", 5432)};
    int mQueueCapacity{8092}; // Want this big enough but not too big
    int mDatabaseWriterThreads{4};  
};

ProgramOptions parseIniFile(const std::string &iniFile);

class Process
{
public:
    Process() = delete;
    /// Constructor
    explicit Process(const ProgramOptions &options)
    //        std::unique_ptr<UWaveServer::Database::Client> &&databaseClient)
    {
        nDatabaseWriterThreads = options.mDatabaseWriterThreads;
        // Reserve size the queues
        mShallowPacketSanitizerQueue.setCapacity(options.mQueueCapacity);
        //mDeepPacketSanitizerQueue.setCapacity(options.mQueueCapacity);
        mWritePacketToDatabaseQueue.setCapacity(options.mQueueCapacity);

        // Create the database connection
        spdlog::debug("Creating TimeSeriesDB PostgreSQL database connection...");
        for (int iThread = 0; iThread < nDatabaseWriterThreads; ++iThread)
        {
            UWaveServer::Database::Credentials databaseCredentials;
            databaseCredentials.setUser(options.databaseUser);
            databaseCredentials.setPassword(options.databasePassword);
            databaseCredentials.setHost(options.databaseHost);
            databaseCredentials.setPort(options.databasePort);
            databaseCredentials.setDatabaseName(options.databaseName);
            databaseCredentials.setApplication(options.applicationName
                                             + "-" + std::to_string(iThread));
            if (!options.databaseSchema.empty())
            {
                spdlog::info("Will connect to schema "
                           + options.databaseSchema);
                databaseCredentials.setSchema(options.databaseSchema);
            }
            auto databaseClient 
                = std::make_unique<UWaveServer::Database::WriteClient>
                  (databaseCredentials);
            mDatabaseClients.push_back(std::move(databaseClient)); 
        }

        // Create data clients
        spdlog::debug("Creating SEEDLink clients...");
        for (const auto &seedLinkOptions : options.seedLinkOptions)
        {
            std::unique_ptr<UWaveServer::DataClient::IDataClient> client
                = std::make_unique<UWaveServer::DataClient::SEEDLink>
                    (mAddPacketsFromAcquisitionCallback, seedLinkOptions);
            mDataAcquisitionClients.push_back(std::move(client));
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
            spdlog::warn("Network not set on packet - skipping");
            return;
        }
        if (!packet.hasStation())
        {
            spdlog::warn("Station not set on packet - skipping");
            return;
        }
        if (!packet.hasChannel())
        {
            spdlog::warn("Channel not set on packet - skipping");
            return;
        }
        if (!packet.hasLocationCode())
        {
            spdlog::warn("Location code not set on packet - skipping");
            return;
        }
        if (!packet.hasSamplingRate())
        {
            auto name = ::toName(packet);
            spdlog::warn("Sampling rate not set on " + name 
                       + "'s packet - skipping");
            return;
        }
        if (packet.empty())
        {
            auto name = ::toName(packet);
            spdlog::warn("No data on " + name + "'s packet - skipping");
            return;
        }
        //spdlog::debug("Adding " + ::toName(packet));
        mShallowPacketSanitizerQueue.push(std::move(packet));
    }
    // Data acquisitions likely will have similar latencies.  So the first
    // thing to do is just check if we're seeing the latest near real-time
    // packet.
    void shallowDeduplicator()
    {
        spdlog::info("Thread entering shallow packet sanitizer");
        const std::chrono::milliseconds mTimeOut{10};
        while (keepRunning())
        {
            UWaveServer::Packet packet;
            auto gotPacket 
                = mShallowPacketSanitizerQueue.wait_until_and_pop(
                     &packet, mTimeOut);
            if (gotPacket)
            {
                bool allow{true};
                // Handle future data
                try
                {
                    if (allow){allow = mTestFuturePacket.allow(packet);}
                }
                catch (const std::exception &e)
                {
                    spdlog::warn("Failed to check future packet data because " 
                               + std::string {e.what()} + "; skipping");
                    allow = false;
                } 
                // Handle expired data
                try
                {
                    if (allow){allow = mTestExpiredPacket.allow(packet);}
                }
                catch (const std::exception &e)
                {
                    spdlog::warn("Failed to check expired packet data because "
                               + std::string {e.what()} + "; skipping");
                    allow = false;
                }
                // Handle duplicate data
                try
                {
                    if (allow &&
                        static_cast<int> (mDataAcquisitionClients.size()) > 1)
                    {
                        allow = mTestShallowDuplicatePacket.allow(packet);
                    }
                    if (allow){allow = mTestDeepDuplicatePacket.allow(packet);}
                }
                catch (const std::exception &e)
                {
                    spdlog::warn("Failed to test for duplicate packet because "
                               + std::string {e.what()} + "; skipping");
                    allow = false;
                }
                if (allow)
                {
                    mWritePacketToDatabaseQueue.push(std::move(packet));
                }
            }
        }
        spdlog::info("Thread leaving shallow packet sanitizer");
    }
/*
    /// Next, we perform a deeper deduplication process.  This helps to
    /// remove fairly latent data.
    void deepDeduplicator()
    {
        spdlog::info("Thread entering deep deduplicator");
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
        spdlog::debug("Thread leaving deep deduplicator");
    }
*/
    /// Finally, we write this to the database.  This is where the real compute
    /// work happens (on postgres's end).
    void writePacketToDatabase(int iThread)
    {
        spdlog::info("Thread " + std::to_string(iThread)
                   + " entering database writer");

        auto nowMuSeconds
           = std::chrono::time_point_cast<std::chrono::microseconds>
             (std::chrono::high_resolution_clock::now()).time_since_epoch();
        auto lastLogTime
            = std::chrono::duration_cast<std::chrono::seconds> (nowMuSeconds);

        const std::chrono::milliseconds mTimeOut{10};
//int printEvery{0};
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
                    auto t2 = std::chrono::high_resolution_clock::now();
                    double duration
                        = std::chrono::duration_cast<std::chrono::microseconds>
                          (t2 - t1).count()*1.e-6;
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
                    spdlog::warn("Failed to add packet to database because "
                               + std::string {e.what()});
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
                spdlog::warn("Network code not set on packet; skipping");
                continue;
            }
            if (!packet.hasStation())
            {
                spdlog::warn("Station name not set on packet; skipping");
                continue;
            }
            if (!packet.hasChannel())
            {
                spdlog::warn("Channel code not set on packet; skipping");
                continue;
            }
            // Location code we can fake
            if (!packet.hasLocationCode()){packet.setLocationCode("--");}
            if (!packet.hasSamplingRate())
            {
                spdlog::warn("Sampling rate not set on packet; skipping");
                continue;
            }
            // Add packet to shallow deduplicator
            try
            {
                mShallowPacketSanitizerQueue.push(std::move(packet));
            }
            catch (const std::exception &e)
            {
                spdlog::warn(
                    "Failed to add packet to initial packet queue because "
                  + std::string {e.what()});
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
        spdlog::debug("Main thread entering waiting loop");
        catchSignals();
        {
            while (!mStopRequested)
            {
                if (mInterrupted)
                {
                    spdlog::info("SIGINT/SIGTERM signal received!");
                    mStopRequested = true;
                    break;
                }
                if (!checkFuturesOkay(std::chrono::milliseconds {5}))
                {
                    spdlog::critical(
                       "Futures exception caught; terminating app");
                    mStopRequested = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds {50});
            }
        }
        if (mStopRequested)
        {
            spdlog::debug("Stop request received.  Terminating...");
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
            spdlog::info("Starting client");
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
        mWriterThroughPut.clear();
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
    /// True indicates the all the processes are running a-okay.
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
                spdlog::critical("Fatal error in SEEDLink import: "
                               + std::string {e.what()});
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
        std::chrono::seconds {3600}};
    UWaveServer::TestFuturePacket mTestFuturePacket{
        std::chrono::microseconds {0},
        std::chrono::seconds {3600}};
    UWaveServer::TestExpiredPacket mTestExpiredPacket{
        std::chrono::days {90},
        std::chrono::seconds {3600}};
    mutable std::mutex mStopContext;
    std::condition_variable mStopCondition;
    std::vector<std::future<void>> mDataAcquisitionFutures;
    std::vector<std::thread> mDatabaseWriterThreads;
    std::vector<std::pair<int, double>> mWriterThroughPut;
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
    ::ProgramOptions programOptions;
    try
    {
        programOptions = ::parseIniFile(iniFile);
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    // Create the data source connections
    spdlog::info("Initializing processes...");
    std::unique_ptr<::Process> process; 
    try
    {
        process = std::make_unique<::Process> (programOptions);
    }
    catch (const std::exception &e)
    {
        spdlog::error("Failed to initialize worker class; failed with "
                    + std::string{e.what()});
        return EXIT_FAILURE;
    }

    try
    {
        process->start();
        process->handleMainThread();
        //process->stop();
    }
    catch (const std::exception &e)
    {
       spdlog::critical("An error occurred during processing");
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
postgres database.

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
                std::vector<std::string> thisSelector; 
                auto splitSelector = thisSplitSelector;
                boost::algorithm::trim(splitSelector);

                boost::split(thisSelector, splitSelector,
                             boost::is_any_of(" \t"));
                UWaveServer::DataClient::StreamSelector selector;
                if (splitSelector.empty())
                {
                    throw std::invalid_argument("Empty selector");
                }
                // Require a network
                boost::algorithm::trim(thisSelector.at(0));
                selector.setNetwork(thisSelector.at(0));
                // Add a station?
                if (splitSelector.size() > 1)
                {
                    boost::algorithm::trim(thisSelector.at(1));
                    selector.setStation(thisSelector.at(1));
                }
                // Add channel + location code + data type
                std::string channel{"*"};
                std::string locationCode{"??"};
                if (splitSelector.size() > 2)
                {
                    boost::algorithm::trim(thisSelector.at(2));
                    channel = thisSelector.at(2);
                }
                if (splitSelector.size() > 3)
                {
                    boost::algorithm::trim(thisSelector.at(3));
                    locationCode = thisSelector.at(3);
                }
                // Data type
                auto dataType
                    = UWaveServer::DataClient::StreamSelector::Type::All;
                if (splitSelector.size() > 4)
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
                clientOptions.addStreamSelector(selector);
            } // Loop on selectors
        } // End check on selector string
    } // Loop on selectors
    return clientOptions;
}

ProgramOptions parseIniFile(const std::string &iniFile)
{   
    ProgramOptions options;
    if (!std::filesystem::exists(iniFile)){return options;}
    // Parse the initialization file
    boost::property_tree::ptree propertyTree;
    boost::property_tree::ini_parser::read_ini(iniFile, propertyTree);

    options.mDatabaseWriterThreads
       = propertyTree.get<int> ("uwsDataLoader.nDatabaseWriterThreads",
                                options.mDatabaseWriterThreads);
    if (options.mDatabaseWriterThreads < 1 ||
        options.mDatabaseWriterThreads > 2048)
    {
        throw std::invalid_argument(
            "Number of database threads must be between 1 and 2048");
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


