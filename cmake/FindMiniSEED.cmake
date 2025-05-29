# Already in cache, be silent
if (MINISEED_INCLUDE_DIR AND MINISEED_LIBRARY)
    set(MINISEED_FIND_QUIETLY TRUE)
endif()

set(MINISEED mseed)

# Find the include directory
find_path(MINISEED_INCLUDE_DIR
          NAMES libmseed.h
          HINTS $ENV{MINISEED_ROOT}/include
                $ENV{MINISEED_ROOT}/
                /usr/local/include)
# Find the library components
if (BUILD_SHARED_LIBS)
   message("Looking for libmseed shared")
   find_library(MINISEED_LIBRARY
                NAME libmseed.so
                PATHS $ENV{MINISEED}/lib/
                      $ENV{MINISEED}/
                      /usr/local/lib64
                      /usr/local/lib
               )
else()
   message("Looking for libmseed static")
   find_library(MINISEED_LIBRARY
                NAME libmseed.a
                PATHS $ENV{MINISEED}/lib/
                      $ENV{MINISEED}/
                      /usr/local/lib64
                      /usr/local/lib
               )
endif()
file(STRINGS ${MINISEED_INCLUDE_DIR}/libmseed.h MSEED_HEADER_DATA)
set(MINISEED_VERSION "")
while (MSEED_HEADER_DATA)
  list(POP_FRONT MSEED_HEADER_DATA LINE)
  if (LINE MATCHES "#define LIBMSEED_VERSION")
     message("Found mseed version line: " ${LINE})
     string(REPLACE "#define LIBMSEED_VERSION " "" LINE ${LINE})
     string(REPLACE "\"" "" LINE ${LINE})
     string(REPLACE "//!< Library version" "" LINE ${LINE})
     string(STRIP LINE ${LINE})
     set(LINE_LIST ${LINE})
     list(GET LINE_LIST 0 MINISEED_VERSION)
     #string(COMPARE GREATER ${VERSION} "3.1.5" RESULT) 
     #if (RESULT)
     #   set(MINISEED_VERSION ${VERSION})
     #endif()
     message("Extracted mseed version ${MINISEED_VERSION}")
     break()
  endif()
endwhile()
#message("value " ${MSEED_HEADER_DATA})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MiniSEED
                                  DEFAULT_MSG MINISEED_INCLUDE_DIR MINISEED_LIBRARY MINISEED_VERSION)
mark_as_advanced(MINISEED_INCLUDE_DIR MINISEED_LIBRARY MINISEED_VERSION)
