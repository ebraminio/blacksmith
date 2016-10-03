#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

IP=$1

ssh-keygen -R $IP &> /dev/null

# TODO: do this while master provisioning
ssh -o StrictHostKeyChecking=no core@$IP "sudo chown -R core /var/lib/blacksmith/"

scp -o StrictHostKeyChecking=no workspaces/current/files/workspace.tar.gz core@$IP:/var/lib/blacksmith/workspace.tar.gz
if ! ssh -o StrictHostKeyChecking=no core@$IP "gunzip /var/lib/blacksmith/workspace.tar.gz"; then
    echo "Error while extracting workspace on remote server $IP"
    exit 1
fi
ssh -o StrictHostKeyChecking=no core@$IP "rm -rf /var/lib/blacksmith/workspaces/ || true"
ssh -o StrictHostKeyChecking=no core@$IP "mkdir -p /var/lib/blacksmith/workspaces/current/"
ssh -o StrictHostKeyChecking=no core@$IP "tar -xvf /var/lib/blacksmith/workspace.tar -C /var/lib/blacksmith/workspaces/current/"
ssh -o StrictHostKeyChecking=no core@$IP "mv /var/lib/blacksmith/workspace.tar /var/lib/blacksmith/workspaces/current/workspace/files/"