#!/usr/bin/env bash
set -eu

if [[ $(id -u) -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

TERRA_DATA_DIR=/mnt/nvme0/terra/data
S3_PATH=s3://crypto-thunder/chain_data/terra/
CUR_DIR=$PWD

sudo -i -u ubuntu bash << EOF
cd $CUR_DIR
echo Syncing data
rsync -a --info=progress2 --delete --del $TERRA_DATA_DIR data
EOF

echo Stopping terrad to sync latest files
systemctl stop terrad

sudo -i -u ubuntu bash << EOF
cd $CUR_DIR
echo Syncing latest files
rsync -a --info=progress2 --delete --del $TERRA_DATA_DIR data
EOF

echo Restating terrad
systemctl start terrad

FILE_NAME=terra-data-$(date -u +%Y-%m-%dT%H-%M).tar.gz

sudo -i -u ubuntu bash << EOF
cd $CUR_DIR
echo Compressing data to $FILE_NAME
tar --use-compress-program='pigz --recursive | pv' -cf $FILE_NAME $TERRA_DATA_DIR
echo uploading to S3
aws s3 cp $FILE_NAME $S3_PATH
rm $FILE_NAME
EOF
