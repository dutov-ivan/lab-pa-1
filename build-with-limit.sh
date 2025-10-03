#!/bin/bash
# Set virtual memory limit to 512000 KB (~500 MB)
ulimit -v 512000

cd cmake-build-release-wsl
./main_exec
