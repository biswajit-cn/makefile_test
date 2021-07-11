cmake_minimum_required(VERSION 2.8)
project(mf_test)

include_directories(${PROJECT_SOURCE_DIR}/include)
add_library(recon STATIC IMPORTED)
set_property(TARGET recon PROPERTY IMPORTED_LOCATION ./libreconstructor.a)
add_executable(caller caller.c)
target_link_libraries(caller recon pthread)