# This script should SSH into all the nodes, and clear OS caches.
# It should also rollout a restart of all citus worker statefulsets and wait for it to be ready

#130.225.39.200 ais-pg-db-1
#130.225.39.121 ais-pg-db-2
#130.225.39.242 ais-pg-db-3
#130.225.39.166 ais-pg-db-4
#130.225.39.249 ais-pg-db-5

ssh -i /ssh/sshkey ubuntu@130.225.39.200 sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"
ssh -i ssh.key ubuntu@130.225.39.121 sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"
ssh -i ssh.key ubuntu@130.225.39.242 sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"
ssh -i ssh.key ubuntu@130.225.39.166 sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"
ssh -i ssh.key ubuntu@130.225.39.249 sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"

# Statefulsets to be restarted:
#ais-citus-master
#ais-citus-worker-1
#ais-citus-worker-2
#ais-citus-worker-3
#ais-citus-worker-4

ssh -i ssh.key ubuntu@130.225.39.200 microk8s kubectl rollout restart statefulset ais-citus-master ais-citus-worker-1 ais-citus-worker-2 ais-citus-worker-3 ais-citus-worker-4
# wait for all pods to be ready
ssh -i ssh.key ubuntu@130.225.39.200 microk8s kubectl wait --for=condition=ready pod kubectl wait --for=condition=ready pod ais-citus-master-0 ais-citus-worker-1-0 ais-citus-worker-2-0 ais-citus-worker-3-0 ais-citus-worker-4-0
# sleep 5 seconds to let it settle a bit.
sleep 5

# Cache should be cold now.

# create kubernetes secret named sshkey from ~/.ssh/aau
kubectl create secret generic sshkey --from-file=sshkey=~/.ssh/aau

