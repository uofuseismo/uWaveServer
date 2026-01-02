#ifndef GET_ENVIRONMENT_VARIABLE_HPP
#define GET_ENVIRONMENT_VARIABLE_HPP
#include <cstdlib>
#include <string>
namespace
{

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

uint16_t getIntegerEnvironmentVariable(const std::string &variable,
                                       uint16_t defaultValue)
{
    uint16_t result{defaultValue};
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


}
#endif
