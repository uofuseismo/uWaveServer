#include <string>
#include <chrono>
#include <vector>
#include <filesystem>
#include "uWaveServer/dataClient/seedLinkOptions.hpp"
#include "uWaveServer/dataClient/streamSelector.hpp"

using namespace UWaveServer::DataClient;

class SEEDLinkOptions::SEEDLinkOptionsImpl
{
public:
    std::string mAddress{"rtserve.iris.washington.edu"};
    std::filesystem::path mStateFile;
    std::vector<StreamSelector> mSelectors;
    std::chrono::seconds mNetworkTimeOut{600};
    std::chrono::seconds mNetworkDelay{30};
    int mSEEDRecordSize{512};
    int mMaxQueueSize{8192};
    uint16_t mStateFileInterval{100};
    uint16_t mPort{18000};
};

/// Constructor
SEEDLinkOptions::SEEDLinkOptions() :
    pImpl(std::make_unique<SEEDLinkOptionsImpl> ())
{
}

/// Copy constructor
SEEDLinkOptions::SEEDLinkOptions(const SEEDLinkOptions &options)
{
    *this = options;
}

/// Move constructor
SEEDLinkOptions::SEEDLinkOptions(SEEDLinkOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
SEEDLinkOptions& SEEDLinkOptions::operator=(const SEEDLinkOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<SEEDLinkOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
SEEDLinkOptions& SEEDLinkOptions::operator=(SEEDLinkOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Address
void SEEDLinkOptions::setAddress(const std::string &address)
{
    if (address.empty()){throw std::invalid_argument("Address is empty");}
    pImpl->mAddress = address;
}

std::string SEEDLinkOptions::getAddress() const noexcept
{
    return pImpl->mAddress;
}

/// Port
void SEEDLinkOptions::setPort(const uint16_t port) noexcept
{
    pImpl->mPort = port;
}

uint16_t SEEDLinkOptions::getPort() const noexcept
{
    return pImpl->mPort;
}

/// Destructor
SEEDLinkOptions::~SEEDLinkOptions() = default;

/// Reset class
void SEEDLinkOptions::clear() noexcept
{
    pImpl = std::make_unique<SEEDLinkOptionsImpl> ();
}

/// Sets a SEEDLink state file
void SEEDLinkOptions::setStateFile(const std::string &stateFileName)
{
    if (stateFileName.empty())
    {
        pImpl->mStateFile.clear();
        return;
    }
    std::filesystem::path stateFile(stateFileName); 
    auto parentPath = stateFile.parent_path();
    if (!parentPath.empty())
    {
        if (!std::filesystem::exists(parentPath))
        {
            if (!std::filesystem::create_directories(parentPath))
            {
                throw std::runtime_error("Failed to create state file path");
            }
        }
    }
    pImpl->mStateFile = stateFile;
}

std::string SEEDLinkOptions::getStateFile() const
{
    if (!haveStateFile()){throw std::runtime_error("State file not set");}
    return pImpl->mStateFile;
}

bool SEEDLinkOptions::haveStateFile() const noexcept
{
    return !pImpl->mStateFile.empty();
}

/// State file interval
void SEEDLinkOptions::setStateFileUpdateInterval(const uint16_t interval) noexcept
{
    pImpl->mStateFileInterval = interval;
}

uint16_t SEEDLinkOptions::getStateFileUpdateInterval() const noexcept
{
    return pImpl->mStateFileInterval;
}

/// The SEEDLink record size
void SEEDLinkOptions::setSEEDRecordSize(const int recordSize)
{
    if (recordSize != 512 && recordSize != 256 && recordSize != 128)
    {
        throw std::invalid_argument("Record size " + std::to_string(recordSize)
                               + " is invalid.  Can only use 128, 256, or 512");
    }
    pImpl->mSEEDRecordSize = recordSize;
}

int SEEDLinkOptions::getSEEDRecordSize() const noexcept
{
    return pImpl->mSEEDRecordSize;
}

/// Maximum internal queue size
void SEEDLinkOptions::setMaximumInternalQueueSize(const int maxSize)
{
    if (maxSize < 1)
    {
        throw std::invalid_argument(
            "Maximum internal queue size must be postive");
    }
    pImpl->mMaxQueueSize = maxSize;
}

int SEEDLinkOptions::getMaximumInternalQueueSize() const noexcept
{
    return pImpl->mMaxQueueSize;
}

/// Network timeout
void SEEDLinkOptions::setNetworkTimeOut(const std::chrono::seconds &timeOut)
{
    if (timeOut < std::chrono::seconds {0})
    {
        throw std::invalid_argument("Network time-out cannot be negative");
    }
    pImpl->mNetworkTimeOut = timeOut;
}

std::chrono::seconds SEEDLinkOptions::getNetworkTimeOut() const noexcept
{
    return pImpl->mNetworkTimeOut;
}
    
void SEEDLinkOptions::setNetworkReconnectDelay(const std::chrono::seconds &delay)
{
    if (delay < std::chrono::seconds {0})
    {
        throw std::invalid_argument("Network delay cannot be negative");
    }
    pImpl->mNetworkDelay = delay;
}

std::chrono::seconds SEEDLinkOptions::getNetworkReconnectDelay() const noexcept
{
    return pImpl->mNetworkDelay;
}

/// Stream selectors
void SEEDLinkOptions::addStreamSelector(
    const StreamSelector &selector)
{
    if (!selector.haveNetwork())
    {
        throw std::invalid_argument("Network not set");
    }
    for (const auto &mySelector : pImpl->mSelectors)
    {
        if (mySelector.getNetwork() == selector.getNetwork() &&
            mySelector.getStation() == selector.getStation() &&
            mySelector.getSelector() == selector.getSelector())
        {
            throw std::invalid_argument("Duplicate selector");
        }
    }
    pImpl->mSelectors.push_back(selector);
}

std::vector<StreamSelector> SEEDLinkOptions::getStreamSelectors() const noexcept
{
    return pImpl->mSelectors;
}
