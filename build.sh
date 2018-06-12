#!/bin/bash

version="0.0.0"
if [ -n "$1" ]; then version="$1"
fi

dotnet pack src/eventstore/eventstore.csproj -o ../../dist -p:Version="$version" -p:PackageVersion="$version" -c Release