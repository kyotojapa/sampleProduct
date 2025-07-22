export VAULT_ADDR='https://localhost:8888'
export VAULT_SKIP_VERIFY=1
export RLF_DEBUG=""
export RLF_CONFIG='./config'

docker-compose down -v
docker-compose up -d --build
sleep 5
cd ~/.ssh
./create_key_for_sdep.sh dev
./get_pem_for_toolsvm.sh dev
cd -
gnome-terminal -- bash -c "./deploy_vnf_snapi_tunnel_sdep.sh $1 $2"
#gnome-terminal -- bash -c "./deploy_vnf_logs_tunnel_sdep.sh 10.11.68.8 $1 $2;"

sleep 10
# python ../src/test_otel_metrics.py
python ../src/main.py
