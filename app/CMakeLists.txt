cmake_minimum_required(VERSION 3.14...3.22)

include(../info.cmake)
include(../cmake/CPM.cmake)
include(../cmake/tools.cmake)
CPMAddPackage("gh:TheLartians/PackageProject.cmake@1.11.2")

project(${MAIN_PROJECT_NAME}_apps LANGUAGES CXX)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()

CPMAddPackage(
    NAME ${MAIN_PROJECT_NAME}
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/.. 
    OPTIONS ""
)

# CPMAddPackage("gh:oneapi-src/oneTBB@2022.0.0")

# ---- Create binary ----
file(GLOB_RECURSE apps_sources ${PROJECT_SOURCE_DIR}/*.cpp)
file(GLOB_RECURSE apps_headers ${PROJECT_SOURCE_DIR}/*.h)

# For each .cpp file, create an executable target
foreach(app_src ${apps_sources})
    file(RELATIVE_PATH app_src_rel ${PROJECT_SOURCE_DIR} ${app_src})
    get_filename_component(app_subdir ${app_src_rel} DIRECTORY)
    get_filename_component(app_name ${app_src} NAME_WE)
    message(STATUS "Adding app \"${app_name}\" in \"${app_subdir}\"")

    add_executable(${app_name} ${sources} ${headers} ${app_src} ${apps_headers})
    set_target_properties(
        ${app_name} PROPERTIES
        OUTPUT_NAME ${app_name}
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${app_subdir}
    )
    set_target_properties(${app_name} PROPERTIES CXX_STANDARD ${MAIN_CXX_VERSION})
    target_include_directories(${app_name} PRIVATE ${PROJECT_SOURCE_DIR})
    target_link_libraries(${app_name} PRIVATE ${MAIN_PROJECT_NAME})
    # target_link_libraries(${app_name} PRIVATE TBB::tbb)
    target_compile_options(${app_name} PUBLIC -Wall -Wpedantic -Wextra -Werror -fconcepts-diagnostics-depth=10)
    target_compile_options(${app_name} PRIVATE $<$<CONFIG:Release>:>)
    target_compile_options(${app_name} PRIVATE $<$<CONFIG:RelWithDebInfo>:>)
endforeach()

# ---- Add Tests ----