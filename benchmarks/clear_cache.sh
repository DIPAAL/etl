# This script should SSH into all the nodes, and clear OS caches.
# It should also rollout a restart of all citus worker statefulsets and wait for it to be ready

# set e to exit on error
set -e

# copy sshkey and set permissions
mkdir -p ~/.ssh
cp /ssh/sshkey ~/.ssh/id_rsa
chmod 600 ~/.ssh/id_rsa

#130.225.39.200 ais-pg-db-1
#130.225.39.121 ais-pg-db-2
#130.225.39.242 ais-pg-db-3
#130.225.39.166 ais-pg-db-4
#130.225.39.249 ais-pg-db-5
ssh -o "StrictHostKeyChecking=no" dipaal@s1.dipaal.dk 'sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh -o "StrictHostKeyChecking=no" dipaal@s2.dipaal.dk 'sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh -o "StrictHostKeyChecking=no" dipaal@s3.dipaal.dk 'sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh -o "StrictHostKeyChecking=no" dipaal@s4.dipaal.dk 'sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh -o "StrictHostKeyChecking=no" dipaal@s5.dipaal.dk 'sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh -o "StrictHostKeyChecking=no" dipaal@s6.dipaal.dk 'sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"'


# Statefulsets to be restarted:
#ais-citus-master
#ais-citus-worker-1
#ais-citus-worker-2
#ais-citus-worker-3
#ais-citus-worker-4

ssh -o "StrictHostKeyChecking=no" dipaal@s1.dipaal.dk 'microk8s kubectl rollout restart statefulset ais-citus-master ais-citus-worker-1 ais-citus-worker-2 ais-citus-worker-3 ais-citus-worker-4 ais-citus-worker-5 && microk8s kubectl rollout status statefulset ais-citus-master ais-citus-worker-1 ais-citus-worker-2 ais-citus-worker-3 ais-citus-worker-4 ais-citus-worker-5'
# sleep 5 seconds to let it settle a bit.
sleep 5

# Cache should be cold now.
