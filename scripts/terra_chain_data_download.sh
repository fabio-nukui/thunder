#!/usr/bin/env bash
set -eux

if [[ $(id -u) -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

USER=ubuntu
TERRA_HOME=/home/$USER/.terra
S3_PATH=s3://crypto-thunder/chain_data/terra_genesis

CONFIG_FILE=$TERRA_HOME/config/config.toml
DATA_DIR=$TERRA_HOME/$(grep -m1 '^db_dir' $CONFIG_FILE | sed -r 's/^db_dir = "(.+)"/\1/')
DATA_DIR=$(cd -P $DATA_DIR && pwd)  # Follow symlinks

STATE_FILENAME_COMPRESSED=$(aws s3 ls $S3_PATH/ | awk -F ' ' '{print $4}' | tail -n 1)
STATE_FILENAME=$(echo $STATE_FILENAME_COMPRESSED | sed 's/\.gz//')

systemctl stop terrad
sleep 3

# Run as non-root to avoid file ownership issues
sudo -i -u "$USER" bash << EOF
set -eux -o pipefail
rm -rf $DATA_DIR
mkdir $DATA_DIR
aws s3 cp $S3_PATH/$STATE_FILENAME_COMPRESSED - | pigz -dc > $DATA_DIR/$STATE_FILENAME
sed -ri 's/^(genesis_file = "data\/).+"$/\1'$STATE_FILENAME'"/' $CONFIG_FILE
EOF

systemctl start terrad
