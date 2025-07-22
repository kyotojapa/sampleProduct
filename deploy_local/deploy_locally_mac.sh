#!/bin/bash

OS=$(uname)

export VAULT_ADDR='https://localhost:8888'
export VAULT_SKIP_VERIFY=1
export RLF_DEBUG=""
export RLF_CONFIG='/Users/dtothazan/Documents/prod4Repos/prod1_logs_forwarder/deploy_local/config'

cicd scpme $1:/var/vault* ./var/
docker-compose down -v
docker-compose up -d --build
sleep 5

if [ "$OS" == "Darwin" ]; then
    osascript -e 'tell application "Terminal" to do script "/bin/bash -c \"/Users/dtothazan/Documents/prod4Repos/prod1_logs_forwarder/deploy_local/deploy_vnf_snapi_tunnel.sh '"$1-seep"'\""'
    osascript -e 'tell application "Terminal" to do script "/bin/bash -c \"/Users/dtothazan/Documents/prod4Repos/prod1_logs_forwarder/deploy_local/deploy_vnf_logs_tunnel.sh '"$1"' '"$2"' '"$3"'\""'
    osascript -e 'tell application "Terminal" to do script "/bin/bash -c \"/Users/dtothazan/Documents/prod4Repos/prod1_logs_forwarder/deploy_local/deploy_vault_tunnel.sh '"$1"'\""'
    osascript -e 'tell application "Terminal" to do script "/bin/bash -c \"/Users/dtothazan/Documents/prod4Repos/prod1_logs_forwarder/deploy_local/deploy_rabbitmq_tunnel.sh '"$1-seep"'\""'
fi

sleep 10
python ../src/main.py
