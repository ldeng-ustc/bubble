cmake_minimum_required(VERSION 3.14...3.22)

# ---- Project ----
include(info.cmake)
include(cmake/CPM.cmake)
include(cmake/tools.cmake)
CPMAddPackage("gh:TheLartians/PackageProject.cmake@1.8.0")


project(
    ${MAIN_PROJECT_NAME}
    VERSION 1.0
    LANGUAGES CXX
)

# ---- Include guards ----
if(PROJECT_SOURCE_DIR STREQUAL PROJECT_BINARY_DIR)
    message( FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build directory) and run CMake from there." )
endif()

# use cmake -DCMAKE_BUILD_TYPE=[Debug|Release|RelWithDebInfo|MinSizeRel] to set build type
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()

# ---- Dependencies ----
CPMAddPackage(
    NAME fmt
    GIT_TAG 10.1.1
    GITHUB_REPOSITORY fmtlib/fmt
    OPTIONS "FMT_INSTALL YES" # create an installable target
)

CPMAddPackage("gh:cmuparlay/parlaylib#master")
CPMAddPackage("gh:cameron314/concurrentqueue@1.0.4")
CPMAddPackage("gh:cameron314/readerwriterqueue@1.0.6")
CPMAddPackage("gh:jarro2783/cxxopts@3.2.0")
CPMAddPackage("gh:p-ranav/indicators@2.3")


CPMAddPackage("gh:axboe/liburing#liburing-2.6")
if(liburing_ADDED)
    add_custom_command(
        OUTPUT ${liburing_SOURCE_DIR}/src/liburing.a
        COMMAND make
        WORKING_DIRECTORY ${liburing_SOURCE_DIR}
    )
    add_custom_target(liburing_build DEPENDS ${liburing_SOURCE_DIR}/src/liburing.a)
    add_library(liburing STATIC IMPORTED)
    set_target_properties(liburing PROPERTIES
        IMPORTED_LOCATION ${liburing_SOURCE_DIR}/src/liburing.a
        INTERFACE_INCLUDE_DIRECTORIES ${liburing_SOURCE_DIR}/src/include
    )
    add_dependencies(liburing liburing_build)
endif()

CPMAddPackage("gh:numactl/numactl@2.0.18")
if(numactl_ADDED)
    find_program(autoreconf_exec autoreconf REQUIRED)
    find_program(libtoolize_exec libtoolize REQUIRED)
    add_custom_command(
        OUTPUT ${numactl_SOURCE_DIR}/.libs/libnuma.a
        COMMAND mv configure configure.bak
        COMMAND ${autoreconf_exec} --install --symlink
        COMMAND ./configure
        COMMAND make clean
        COMMAND make -j
        COMMAND mv configure.bak configure
        WORKING_DIRECTORY ${numactl_SOURCE_DIR}
    )
    add_custom_target(numactl_build DEPENDS ${numactl_SOURCE_DIR}/.libs/libnuma.a)
    add_library(numactl STATIC IMPORTED)
    set_target_properties(numactl PROPERTIES
        IMPORTED_LOCATION ${numactl_SOURCE_DIR}/.libs/libnuma.a
        INTERFACE_INCLUDE_DIRECTORIES ${numactl_SOURCE_DIR}
    )
    add_dependencies(numactl numactl_build)
endif()

CPMAddPackage("gh:open-mpi/hwloc#hwloc-2.11.2")
if(hwloc_ADDED)
    find_program(autoreconf_exec autoreconf REQUIRED)
    find_program(libtoolize_exec libtoolize REQUIRED)
    add_custom_command(
        OUTPUT ${hwloc_SOURCE_DIR}/hwloc/.libs/libhwloc.a
        COMMAND ./autogen.sh
        COMMAND ./configure --enable-static --disable-shared --disable-readme --disable-cairo --disable-libxml2 --disable-io 
                --disable-pci --disable-opencl --disable-cuda --disable-nvml --disable-rsmi --disable-levelzero --disable-gl  --disable-libudev
                --disable-plugin-dlopen
        COMMAND make
        COMMAND rm -f ./configure.orig
        COMMAND rm -f ./contrib/completion/test-bash-completion.sh
        WORKING_DIRECTORY ${hwloc_SOURCE_DIR}
    )
    add_custom_target(hwloc_build DEPENDS ${hwloc_SOURCE_DIR}/hwloc/.libs/libhwloc.a)
    add_library(hwloc STATIC IMPORTED)
    set_target_properties(hwloc PROPERTIES
        IMPORTED_LOCATION ${hwloc_SOURCE_DIR}/hwloc/.libs/libhwloc.a
        INTERFACE_INCLUDE_DIRECTORIES ${hwloc_SOURCE_DIR}/include
    )
    add_dependencies(hwloc hwloc_build)
endif()

find_package(OpenMP REQUIRED)

# ---- Add Boost ----
# Use `cmake -S . -B build -DBOOST_ROOT=/path/to/boost` to specify the path to Boost

if(NOT BOOST_ROOT)
    message(STATUS "BOOST_ROOT not set, using default path ./boost")
    set(BOOST_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/boost)
endif()

find_package(Boost REQUIRED COMPONENTS stacktrace_backtrace)

# ---- Add source files ----
file(GLOB_RECURSE headers CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h")

# ---- Create library ----
add_library(${PROJECT_NAME} INTERFACE)
target_compile_features(${PROJECT_NAME} INTERFACE cxx_std_20)
target_include_directories(${PROJECT_NAME} INTERFACE ${Boost_INCLUDE_DIRS}) # Boost header-only libraries
target_link_libraries(${PROJECT_NAME}
    INTERFACE
        Boost::stacktrace_backtrace
        liburing
        numactl
        hwloc
        fmt::fmt
        OpenMP::OpenMP_CXX
        readerwriterqueue
        concurrentqueue
        indicators
        cxxopts
        parlay
)

target_link_options(${PROJECT_NAME} INTERFACE $<$<CONFIG:Release>:-O3 -funroll-loops -march=native -fconcepts-diagnostics-depth=4>)
target_link_options(${PROJECT_NAME} INTERFACE $<$<CONFIG:RelWithDebInfo>:-O2 -g -march=native -fconcepts-diagnostics-depth=4>)
target_link_options(${PROJECT_NAME} INTERFACE $<$<CONFIG:Debug>:-O0 -g -fconcepts-diagnostics-depth=4>)
target_link_options(${PROJECT_NAME} INTERFACE $<$<CONFIG:MinSizeRel>:-Os -fconcepts-diagnostics-depth=4>)
target_compile_definitions(${PROJECT_NAME} INTERFACE $<$<CONFIG:Debug>:DCSR_DEBUG>)

target_include_directories(${PROJECT_NAME}
    INTERFACE
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/third_party>
)

