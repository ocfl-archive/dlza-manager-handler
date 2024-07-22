#!/bin/bash
docker build --rm -t dlza-manager-handler:latest .
docker tag dlza-manager-handler:latest registry.localhost:5001/dlza-manager-handler
docker push registry.localhost:5001/dlza-manager-handler
