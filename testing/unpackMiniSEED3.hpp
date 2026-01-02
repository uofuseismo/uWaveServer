#ifndef UNPACK_MINI_SEED_3_HPP
#define UNPACK_MINI_SEED_3_HPP
#include <cmath>
#include <vector>
#include <chrono>
#include <array>
#include <string>
#include <algorithm>
#include <libmseed.h>
#include <spdlog/spdlog.h>
#include <uWaveServer/packet.hpp>
namespace
{
[[nodiscard]] 
std::vector<UWaveServer::Packet>
    unpackMiniSEED(char *data,
                   const size_t dataLength,
                   const int8_t verbose = 0)
{
    std::vector<UWaveServer::Packet> result;
    auto bufferLength = static_cast<uint64_t> (dataLength);
    uint64_t offset{0};
    bool isFirst{true};
    while (bufferLength - offset > MINRECLEN)
    {
        UWaveServer::Packet packet;
        MS3Record *msr{nullptr};
        auto returnCode
            = msr3_parse(data + offset,
                         static_cast<uint64_t> (dataLength) - offset,
                         &msr, MSF_UNPACKDATA, verbose);
        if (returnCode == MS_NOERROR && msr)
        {
            // Waveform name
            std::array<char, 64> networkWork, stationWork, channelWork, locationCodeWork;
            std::fill(networkWork.begin(), networkWork.end(), '\0');
            std::fill(stationWork.begin(), stationWork.end(), '\0');
            std::fill(channelWork.begin(), channelWork.end(), '\0');
            std::fill(locationCodeWork.begin(), locationCodeWork.end(), '\0');
            returnCode
                = ms_sid2nslc_n(msr->sid,
                                networkWork.data(), networkWork.size(),
                                stationWork.data(), stationWork.size(),
                                locationCodeWork.data(), locationCodeWork.size(),
                                channelWork.data(), channelWork.size());
            if (returnCode != MS_NOERROR)
            {
                if (msr){msr3_free(&msr);}
                throw std::runtime_error("Could not unpack sid");
            }
            // Copy waveform identification information
            try
            {
                packet.setNetwork(std::string {networkWork.data()});
                packet.setStation(std::string {stationWork.data()});
                packet.setChannel(std::string {channelWork.data()});
                packet.setLocationCode(
                    std::string {locationCodeWork.data()});
            }
            catch (const std::exception &e)
            {
                if (msr)
                {
                    msr3_free(&msr);
                    msr = nullptr;
                }
                throw std::runtime_error(
                    "Couldn't set waveform identifier information; failed with: "
                  + std::string {e.what()});
            }
            auto startTime = static_cast<double> (msr->starttime)/NSTMODULUS;
            double samplingRate{msr->samprate};
            // Data
            auto nSamples = static_cast<int> (msr->numsamples);
            try
            {
                packet.setSamplingRate(samplingRate);
                packet.setStartTime(startTime);
                // Finally get the data
                if (msr->sampletype == 'i')
                {
                    packet.setData<int> (nSamples, reinterpret_cast<const int *> (msr->datasamples));
                }
                else if (msr->sampletype == 'f')
                {
                    packet.setData<float> (nSamples, reinterpret_cast<const float *> (msr->datasamples));
                }
                else if (msr->sampletype == 'd')
                {
                    packet.setData<double> (nSamples, reinterpret_cast<const double *> (msr->datasamples));
                }
                else if (msr->sampletype == 't')
                {
                    packet.setData<char> (nSamples, reinterpret_cast<const char *> (msr->datasamples));
                }
                else
                {
                    spdlog::warn("Unhandled data format: "
                               + std::string {msr->sampletype}
                               + "; skipping...");
                    if (msr)
                    {
                        msr3_free(&msr);
                        msr = nullptr;
                    }
                    offset = offset + msr->reclen;
                    continue;
                }
                result.push_back(std::move(packet));
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to create segment.  Failed with "
                           + std::string {e.what()});
            }
            offset = offset + msr->reclen;
        } // End check on have record and no error
        // Release memory
        if (msr)
        {
            msr3_free(&msr);
            msr = nullptr;
        }
        // We're done
        if (returnCode != MS_NOERROR){break;}
    }
    std::sort(result.begin(), result.end(),
              [](const auto &lhs, const auto &rhs)
              {
                 return lhs.getStartTime() < rhs.getStartTime();
              });
    return result;
}

std::vector<UWaveServer::Packet>
    unpackMiniSEED(const std::string &data,
                   const int8_t verbose = 0)
{
    auto copy = data;
    auto bufferLength = static_cast<uint64_t> (data.size());    
    return ::unpackMiniSEED(copy.data(), bufferLength);
}

}
#endif
