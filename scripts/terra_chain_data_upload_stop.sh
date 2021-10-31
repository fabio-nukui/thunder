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

SNAPSHOT_NAME=terra-data-$(date -u +%Y-%m-%dT%H-%M).tar.gz

sudo -i -u "$RUNNER_USER" bash << EOF
set -eu
cd "$TERRA_DIR"
tar --use-compress-program='pigz --recursive | pv' -cf - data | aws s3 cp - "$S3_PATH"/"$SNAPSHOT_NAME"
EOF

echo Restating terrad
systemctl start terrad
