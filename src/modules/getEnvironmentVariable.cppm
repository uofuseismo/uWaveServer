module;

#include <cstdlib>
#include <cstring>
#include <string>
#include <cstdint>

export module GetEnvironmentVariable;

namespace UWaveServer
{

export
[[nodiscard]]
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

export
[[nodiscard]]
std::string getEnvironmentVariable(const std::string &variable)
{ 
    return getEnvironmentVariable(variable, "");
}

export
[[nodiscard]]
uint16_t getIntegerEnvironmentVariable(const std::string &variable,
                                       uint16_t defaultValue)
{
    uint16_t result{defaultValue};
    try 
    {
        auto stringValue = getEnvironmentVariable(variable);
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


}
