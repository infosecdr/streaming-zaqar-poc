#!/bin/bash

# TODO: run this as part of a build

# change dir directory this script resides in so the relative paths below work
SRCDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SRCDIR"

# produce python and java classes for the protobuf messages described in our .proto
protoc --python_out=../../server/src ./MarconiStreamingAPI.proto
protoc --java_out=../../clients/src/main/java ./MarconiStreamingAPI.proto
