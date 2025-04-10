#include <string>
#include "uWaveServer/version.hpp"

using namespace UWaveServer;

int Version::getMajor() noexcept
{
    return UWAVE_SERVER_MAJOR;
}

int Version::getMinor() noexcept
{
    return UWAVE_SERVER_MINOR;
}

int Version::getPatch() noexcept
{
    return UWAVE_SERVER_PATCH;
}

bool Version::isAtLeast(const int major, const int minor,
                        const int patch) noexcept
{
    if (UWAVE_SERVER_MAJOR < major){return false;}
    if (UWAVE_SERVER_MAJOR > major){return true;}
    if (UWAVE_SERVER_MINOR < minor){return false;}
    if (UWAVE_SERVER_MINOR > minor){return true;}
    if (UWAVE_SERVER_PATCH < patch){return false;}
    return true;
}

std::string Version::getVersion() noexcept
{
    std::string version{UWAVE_SERVER_VERSION};
    return version;
}

