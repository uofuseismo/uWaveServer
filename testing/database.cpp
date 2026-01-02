#include <iostream>
#include <numeric>
#include <cmath>
#include <cstdint>
#include <vector>
#include <random>
#include <pqxx/pqxx>
#include "uWaveServer/database/writeClient.hpp"
#include "uWaveServer/database/readOnlyClient.hpp"
#include "uWaveServer/database/credentials.hpp"
#include "uWaveServer/packet.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#define TESTING_READ_WRITE_USER "uws_read_write_user"
#define TESTING_READ_WRITE_PASSWORD "zyTcdi32S426VWT"
#define TESTING_DATABASE_NAME "uwsdevdb"
#define TESTING_HOST "localhost"
#define TESTING_PORT 5432
#define TESTING_APPLICATION "uWaveServerTesting"


TEST_CASE("uWaveServer::Database", "[connection]")
{
    UWaveServer::Database::Credentials credentials;
    credentials.setUser(TESTING_READ_WRITE_USER);
    credentials.setPassword(TESTING_READ_WRITE_PASSWORD);
    credentials.setHost(TESTING_HOST); 
    credentials.setDatabaseName(TESTING_DATABASE_NAME);
    credentials.setPort(TESTING_PORT);
    credentials.setApplication(TESTING_APPLICATION);
    REQUIRE(credentials.getUser() == std::string {TESTING_READ_WRITE_USER});
    REQUIRE(credentials.getPassword() == std::string {TESTING_READ_WRITE_PASSWORD} );
    REQUIRE(credentials.getDatabaseName() == std::string {TESTING_DATABASE_NAME} );
    REQUIRE(credentials.getHost() == std::string {TESTING_HOST} );
    REQUIRE(credentials.getPort() == TESTING_PORT);
    REQUIRE(credentials.getApplication() == std::string {TESTING_APPLICATION});
    //std::cout << credentials.getConnectionString() << std::endl;
}

TEST_CASE("uWaveServer::Database", "[connect]")
{
    UWaveServer::Database::Credentials credentials;
    REQUIRE_NOTHROW(credentials.setUser(TESTING_READ_WRITE_USER));
    REQUIRE_NOTHROW(credentials.setPassword(TESTING_READ_WRITE_PASSWORD));
    REQUIRE_NOTHROW(credentials.setHost(TESTING_HOST));
    REQUIRE_NOTHROW(credentials.setDatabaseName(TESTING_DATABASE_NAME));
    REQUIRE_NOTHROW(credentials.setPort(TESTING_PORT));
    REQUIRE_NOTHROW(credentials.setApplication(TESTING_APPLICATION));
    try
    {
        pqxx::connection connection(credentials.getConnectionString());
        if (!connection.dbname())
        {
            throw std::runtime_error("Failed to create connection");
        }
        connection.close();
        if (connection.dbname() != nullptr)
        {
            throw std::runtime_error("Failed to close connection");
        } 
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
        REQUIRE(false);
    }
}

TEST_CASE("uWaveServer::Database", "[client]")
{
    UWaveServer::Database::Credentials writeCredentials;
    writeCredentials.setUser(TESTING_READ_WRITE_USER);
    writeCredentials.setPassword(TESTING_READ_WRITE_PASSWORD);
    writeCredentials.setHost(TESTING_HOST); 
    writeCredentials.setDatabaseName(TESTING_DATABASE_NAME);
    writeCredentials.setPort(TESTING_PORT);
    writeCredentials.setSchema("ynp");
    writeCredentials.setApplication(TESTING_APPLICATION);
    writeCredentials.enableReadWrite();
    UWaveServer::Database::WriteClient writeClient{writeCredentials};

    UWaveServer::Database::Credentials readCredentials;
    readCredentials.setUser(TESTING_READ_WRITE_USER);
    readCredentials.setPassword(TESTING_READ_WRITE_PASSWORD);
    readCredentials.setHost(TESTING_HOST); 
    readCredentials.setDatabaseName(TESTING_DATABASE_NAME);
    readCredentials.setPort(TESTING_PORT);
    readCredentials.setSchema("ynp");
    readCredentials.setApplication(TESTING_APPLICATION);
    readCredentials.enableReadOnly();
    UWaveServer::Database::ReadOnlyClient readClient{readCredentials};

auto sensors = readClient.getSensors();
if (readClient.contains("WY", "YLT", "HHZ", "01"))
{
  std::cout << "have it" << std::endl;
}
if (!readClient.contains("WY", "YLT", "EHZ", "01"))
{
 std::cout << "dont have it" << std::endl;
}

 UWaveServer::Packet packet;
 const std::string network{"WY"};
 const std::string station{"YHN"};
 const std::string channel{"HHZ"};
 const std::string locationCode{"01"};
 packet.setNetwork(network);
 packet.setStation(station);
 packet.setChannel(channel);
 packet.setLocationCode(locationCode);
 packet.setSamplingRate(100);
 packet.setStartTime(std::chrono::seconds {1766102400});
 std::vector<int> samples(400);
 std::iota(samples.begin(), samples.end(), 0);
 packet.setData(samples);
 try
 {
  writeClient.write(packet); 
 }
 catch (const std::exception &e)
 {
  std::cerr << e.what() << std::endl;
 }
return;
 try
 {
  double t0 = 1766102400;
  double t1 = 1766102400 + 100;
  auto packets = readClient.query(network, station, channel, locationCode, t0, t1);
  for (const auto &thisPacket : packets)
  {
      REQUIRE(thisPacket.getStartTime() == packet.getStartTime());
      auto theseSamples = packet.getData<int> ();
      REQUIRE(theseSamples.size() == samples.size());
      for (int i = 0; i < static_cast<int> (samples.size()); ++i)
      {
          REQUIRE(theseSamples.at(i) == samples.at(i));
      }
  }
 }
 catch (const std::exception &e)
 {
  std::cerr << e.what() << std::endl;
 }
}
