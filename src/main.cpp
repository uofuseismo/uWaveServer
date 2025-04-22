#include <iostream>
#include <string>
#include <thread>
#include <atomic>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <spdlog/spdlog.h>
#include "uWaveServer/packet.hpp"
#include "uWaveServer/dataClient/seedLink.hpp"
#include "uWaveServer/dataClient/seedLinkOptions.hpp"
#include "uWaveServer/dataClient/dataClient.hpp"
#include "uWaveServer/dataClient/streamSelector.hpp"
#include "uWaveServer/database/client.hpp"
#include "uWaveServer/database/connection/postgresql.hpp"
#include "private/threadSafeBoundedQueue.hpp"

[[nodiscard]] std::string parseCommandLineOptions(int argc, char *argv[]);

std::string getEnvironmentVariable(const std::string &variable,
                                   const std::string &defaultValue)
{
    std::string result{defaultValue};
    if (variable.empty()){return result;}
    try
    {
        auto resultPointer = std::getenv(variable.c_str());
        if (resultPointer)
        {
            if (std::strlen(resultPointer) > 0)
            {
                result = std::string {resultPointer};
            }
        }
    }
    catch (...)
    {
    }
    return result;
}

std::string getEnvironmentVariable(const std::string &variable)
{ 
    return ::getEnvironmentVariable(variable, "");
}

int getIntegerEnvironmentVariable(const std::string &variable, int defaultValue)
{
    int result{defaultValue};
    try
    {
        auto stringValue = ::getEnvironmentVariable(variable);
        if (!stringValue.empty())
        {
            result = std::stoi(stringValue.c_str());
        }
    }
    catch (...)
    {
    }
    return result;
}

struct ProgramOptions
{
    std::vector<UWaveServer::DataClient::SEEDLinkOptions> seedLinkOptions;
    std::string applicationName{"uwsDataLoader"};
    std::string databaseUser{::getEnvironmentVariable("UWAVE_SERVER_DATABASE_READ_WRITE_USER")};
    std::string databasePassword{::getEnvironmentVariable("UWAVE_SERVER_DATABASE_READ_WRITE_PASSWORD")};
    std::string databaseName{::getEnvironmentVariable("UWAVE_SERVER_DATABASE_NAME")};
    std::string databaseHost{::getEnvironmentVariable("UWAVE_SERVER_DATABASE_HOST")};
    std::string databaseSchema{"ynp"};//::getEnvironmentVariable("UWAVE_SERVER_DATABASE_SCHEMA")};
    int databasePort{::getIntegerEnvironmentVariable("UWAVE_SERVER_DATABASE_PORT", 5432)};
};

ProgramOptions parseIniFile(const std::string &iniFile);

class Process
{
public:
    /// Constructor
    Process(std::unique_ptr<UWaveServer::Database::Client> &&databaseClient)
    {
        if (databaseClient == nullptr)
        {
            throw std::runtime_error("Database client is null");
        }
        mDatabaseClient = std::move(databaseClient); 
    }
    // Data acquisitions likely will have similar latencies.  So the first
    // thing to do is just check if we're seeing the latest near real-time
    // packet.
    void shallowDeduplicator()
    {
        spdlog::info("Thread entering shallow deduplicator");
        const std::chrono::milliseconds mTimeOut{10};
        while (keepRunning())
        {
            UWaveServer::Packet packet;
            auto gotPacket 
                = mShallowDeduplicatorPacketQueue.wait_until_and_pop(
                     &packet, mTimeOut);
            if (gotPacket)
            {
                bool passToDatabaseWriter{true};
                if (mDataAcquisitionClients.size() > 1)
                {
                    // Is this new?
                    bool isNew{false};

                    passToDatabaseWriter = isNew;
                }
                // Do a deep dedpulication
                if (passToDatabaseWriter)
                {
                    mDeepDeduplicatorPacketQueue.push(std::move(packet));
                }
            }
        }
        spdlog::debug("Thread leaving shallow deduplicator");
    }
    /// Next, we perform a deeper deduplication process.  This helps to
    /// remove fairly latent data.
    void deepDeplicator()
    {
        spdlog::info("Thread entering deep deduplicator");
        const std::chrono::milliseconds mTimeOut{10}; 
        while (keepRunning())
        {
            UWaveServer::Packet packet;
          
        }
        spdlog::debug("Thread leaving deep deduplicator");
    }
    /// Finally, we write this to the database.  This is where the real compute
    /// work happens (on postgres's end).
    void writePacketToDatabase()
    {
        spdlog::info("Thread entering database writer");
        const std::chrono::milliseconds mTimeOut{10};
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
                    mDatabaseClient->write(packet);
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
            if (!packet.haveNetwork())
            {
                spdlog::warn("Network code not set on packet; skipping");
                continue;
            }
            if (!packet.haveStation())
            {
                spdlog::warn("Station name not set on packet; skipping");
                continue;
            }
            if (!packet.haveChannel())
            {
                spdlog::warn("Channel code not set on packet; skipping");
                continue;
            }
            // Location code we can fake
            if (!packet.haveLocationCode()){packet.setLocationCode("--");}
            if (!packet.haveSamplingRate())
            {
                spdlog::warn("Sampling rate not set on packet; skipping");
                continue;
            }
            // Add packet to shallow deduplicator
            try
            {
                mShallowDeduplicatorPacketQueue.push(std::move(packet));
            }
            catch (const std::exception &e)
            {
                spdlog::warn(
                    "Failed to add packet to initial packet queue because "
                  + std::string {e.what()});
            }
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
    /// @brief Stops the processes.
    void stop()
    {
        setRunning(false);
        for (auto &dataAcquisitionClient : mDataAcquisitionClients)
        {
            dataAcquisitionClient.stop();
        }
    }

    ::ThreadSafeBoundedQueue<UWaveServer::Packet> mShallowDeduplicatorPacketQueue;
    ::ThreadSafeBoundedQueue<UWaveServer::Packet> mDeepDeduplicatorPacketQueue;
    ::ThreadSafeBoundedQueue<UWaveServer::Packet> mWritePacketToDatabaseQueue;
    std::unique_ptr<UWaveServer::Database::Client> mDatabaseClient{nullptr};
    std::vector<UWaveServer::DataClient::IDataClient> mDataAcquisitionClients;
    std::atomic<bool> mRunning{true};
};

int main(int argc, char *argv[]) 
{
    // Get the ini file from the command line
    std::string iniFile;
    try
    {
        iniFile = parseCommandLineOptions(argc, argv);
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

    // Create the database connection
    UWaveServer::Database::Connection::PostgreSQL databaseConnection;
    try
    {
        spdlog::info("Creating TimeSeriesDB PostgreSQL database connection...");
        databaseConnection.setUser(programOptions.databaseUser);
        databaseConnection.setPassword(programOptions.databasePassword);
        databaseConnection.setAddress(programOptions.databaseHost);
        databaseConnection.setPort(programOptions.databasePort);
        databaseConnection.setDatabaseName(programOptions.databaseName);
	databaseConnection.setApplication(programOptions.applicationName);
        if (!programOptions.databaseSchema.empty())
        {
            databaseConnection.setSchema(programOptions.databaseSchema);
        }
        databaseConnection.connect(); 
        spdlog::info("Connected to " + programOptions.databaseName
                   + " postgresql database!");
    }
    catch (const std::exception &e)
    {
        spdlog::error(
            "Failed to create PostgreSQL database connection; failed with "
          + std::string {e.what()});
        return EXIT_FAILURE;
    }
    // Create the data source connections
    for (const auto &dataClientOptions : programOptions.seedLinkOptions)
    {
        UWaveServer::DataClient::SEEDLink client{dataClientOptions};
    } 
    // Initialize the utility that will map from the acquistion to the database 

    return EXIT_SUCCESS;
}

///--------------------------------------------------------------------------///
///                            Utility Functions                             ///
///--------------------------------------------------------------------------///
/// Read the program options from the command line
std::string parseCommandLineOptions(int argc, char *argv[])
{
    std::string iniFile;
    boost::program_options::options_description desc(R"""(
The uwsDataLoader maps data from a telemetry to the waveserver database.
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
        return iniFile;
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
    return iniFile;
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
/*
                UWaveServer::DataClient::SEEDLinkOptions clientOptions;
                auto address = propertyTree.get<std::string> (clientName + ".address");
                auto port = propertyTree.get<uint16_t> (clientName + ".port", 18000);
                clientOptions.setAddress(address);
                clientOptions.setPort(port);
std::cout << "got telemetry" << std::endl;
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
                        boost::split(splitSelectors, *selectorString, boost::is_any_of(",|"));
                        for (const auto &thisSplitSelector : splitSelectors)
                        {
                            std::vector<std::string> thisSelector; 
                            auto splitSelector = thisSplitSelector;
                            boost::algorithm::trim(splitSelector);
 
                            boost::split(thisSelector, splitSelector, boost::is_any_of(" \t"));
                            UWaveServer::DataClient::StreamSelector selector;
                            if (splitSelector.empty())
                            {
                                throw std::invalid_argument("Empty selector");
                            }
                            // Require a network
                            boost::algorithm::trim(thisSelector.at(0));
                            selector.setNetwork(thisSelector.at(0));
                            if (splitSelector.size() > 1)
                            {
                                 boost::algorithm::trim(thisSelector.at(1));
                                 selector.setStation(thisSelector.at(1));
                            }
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
                            UWaveServer::DataClient::StreamSelector::Type dataType{UWaveServer::DataClient::StreamSelector::Type::All};
                            if (splitSelector.size() > 4)
                            {
                                 boost::algorithm::trim(thisSelector.at(4));
                                 if (thisSelector.at(4) == "D")
                                 {
                                     dataType = UWaveServer::DataClient::StreamSelector::Type::Data;
                                 }
                                 else if (thisSelector.at(4) == "A")
                                 {
                                     dataType = UWaveServer::DataClient::StreamSelector::Type::All;
                                 }
                                 // TODO other data types
                            }
                            selector.setSelector(channel, locationCode, dataType);
                            clientOptions.addStreamSelector(selector);
                        } // Loop on selector split
                    } // End check on have selector
                    options.seedLinkOptions.push_back(clientOptions);
                } // Loop on selectors
*/
            }
        }
    }
    return options;
}


