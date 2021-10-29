#!/usr/bin/env bash
set -eu

if [[ $(id -u) -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

TERRA_DATA_DIR=/mnt/nvme0/terra/data
CUR_DIR=$PWD

echo Stopping terrad
systemctl stop terrad

FILE_NAME=terra-data-$(date -u +%Y-%m-%dT%H-%M).tar.gz

sudo -i -u ubuntu bash << EOF
cd $CUR_DIR
echo Compressing data to $FILE_NAME
tar --use-compress-program='pigz --recursive | pv' -cf $FILE_NAME $TERRA_DATA_DIR
echo uploading to S3
aws s3 cp $FILE_NAME s3://crypto-thunder/chain_data/terra/
rm $FILE_NAME
EOF

echo Restating terrad
systemctl start terrad
