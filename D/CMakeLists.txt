cmake_minimum_required(VERSION 3.10)
project(D)

set(CMAKE_CXX_STANDARD 11)

add_executable(D main.cpp BucketChain.hpp BucketPair.hpp Bucket.hpp)