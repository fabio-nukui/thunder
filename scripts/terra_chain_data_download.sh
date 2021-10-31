#!/usr/bin/env bash
set -eu

if [[ $(id -u) -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

TERRA_DIR=/mnt/nvme0/terra
S3_PATH=s3://crypto-thunder/chain_data/terra
RUNNER_USER=ubuntu

echo Stopping terrad
systemctl stop terrad
sleep 10

SNAPSHOT_NAME=$(aws s3 ls $S3_PATH | awk -F ' ' '{print $4}' | tail -n 1)

sudo -i -u "$RUNNER_USER" bash << EOF
set -eu
rm -rf "$TERRA_DIR"/data
aws s3 cp "$S3_PATH"/"$SNAPSHOT_NAME" - | pv | tar --use-compress-program=pigz -xC "$TERRA_DIR"
EOF

echo Restating terrad
systemctl start terrad
