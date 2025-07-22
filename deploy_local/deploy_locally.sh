export VAULT_ADDR='https://localhost:8888'
export VAULT_SKIP_VERIFY=1
export RLF_DEBUG=""
export RLF_CONFIG='./config'

cicd scpme $1:/var/vault* ./var/
docker-compose down -v
docker-compose up -d --build
sleep 5
gnome-terminal -- bash -c "./deploy_vnf_snapi_tunnel.sh $1-seep;"
gnome-terminal -- bash -c "./deploy_vnf_logs_tunnel.sh $1 $2 $3;"
gnome-terminal -- bash -c "./deploy_vault_tunnel.sh $1;"
gnome-terminal -- bash -c "./deploy_rabbitmq_tunnel.sh $1-seep;"
gnome-terminal -- bash -c "./deploy_netcat_tunnel.sh $1;"
sleep 10
# python ../src/test_otel_metrics.py
python ../src/main.py
