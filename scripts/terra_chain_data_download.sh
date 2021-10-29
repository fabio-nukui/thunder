#!/usr/bin/env bash
set -eu

if [[ $(id -u) -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

TERRA_DATA_DIR=/mnt/nvme0/terra/data
S3_PATH=s3://crypto-thunder/chain_data/terra/
RUNNER_USER=ubuntu
CUR_DIR=$PWD

snapshot_name=$(aws s3 ls $S3_PATH | awk -F ' ' '{print $4}' | tail -n 1)

sudo -i -u "$RUNNER_USER" bash << EOF
set -eu; cd $CUR_DIR
echo Downloading latest snapshot "$snapshot_name"
aws s3 cp $S3_PATH"$snapshot_name" .
EOF

echo Stopping terrad
systemctl stop terrad

sudo -i -u "$RUNNER_USER" bash << EOF
set -eu; cd $CUR_DIR
echo Decompressing "$snapshot_name"
rm -rf $TERRA_DATA_DIR
mkdir $TERRA_DATA_DIR
pv "$snapshot_name" | tar --use-compress-program=pigz -C $TERRA_DATA_DIR -xf -
rm "$snapshot_name"
EOF

echo Starting terrad
systemctl start terrad
