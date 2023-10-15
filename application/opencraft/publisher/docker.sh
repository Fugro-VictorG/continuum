#!/bin/bash
docker buildx build --platform linux/amd64,linux/arm64 -t 2000arp/opencraft_benchmark:opencraft_bot --push .
