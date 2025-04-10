#include <thread>
#include <atomic>
#include <spdlog/spdlog.h>

#include "uWaveServer/packet.hpp"
#include "uWaveServer/seedLinkClient.hpp"
#include "uWaveServer/dataClient.hpp"
#include "uWaveServer/database/client.hpp"
#include "uWaveServer/database/connection/postgresql.hpp"
#include "private/threadSafeBoundedQueue.hpp"

class Process
{
public:
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
    [[nodiscard]] bool keepRunning() const noexcept
    {
        return mRunning;
    }
    void setRunning(bool running)
    {
        mRunning = running; 
    }
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
    std::vector<UWaveServer::IDataClient> mDataAcquisitionClients;
  
    std::atomic<bool> mRunning{true};
};

int main(int argc, char *argv[]) 
{
    // Create connections

    // Create the database connection
    UWaveServer::Database::Connection::PostgreSQL databaseConnection;
    try
    {
        spdlog::info("Creating TimeSeriesDB PostgreSQL database connection...");
        auto user = std::string {std::getenv("UWAVE_SERVER_DATABASE_READ_WRITE_USER")};
        auto password = std::string {std::getenv("UWAVE_SERVER_DATABASE_READ_WRITE_PASSWORD")};
        auto databaseName = std::string {std::getenv("UWAVE_SERVER_DATABASE_NAME")};
        std::string host="localhost";
        try
        {
            host = std::string {std::getenv("UWAVE_SERVER_DATABASE_HOST")};
        }
        catch (...)
        {
        }
        int port = 5432;
        try 
        {
            port = std::stoi(std::getenv("UWAVE_SERVER_DATABASE_PORT"));
        }
        catch (...)
        {
        }
        databaseConnection.setUser(user);
        databaseConnection.setPassword(password);
        databaseConnection.setAddress(host);
        databaseConnection.setPort(port);
        databaseConnection.setDatabaseName(databaseName);
        databaseConnection.setApplication("uwsDataLoader");
        databaseConnection.setSchema("ynp");
        databaseConnection.connect(); 
        spdlog::info("Connected to " + databaseName + " postgresql database!");
    }
    catch (const std::exception &e)
    {
        spdlog::error(
            "Failed to create PostgreSQL database connection; failed with "
          + std::string {e.what()});
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
