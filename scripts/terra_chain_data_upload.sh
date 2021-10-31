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

systemctl stop terrad
sleep 3

LAST_HEIGHT=$(journalctl -ru terrad.service | grep -m1 'indexed block height' | sed -r 's/.+block height=([0-9]+).+/\1/')
STATE_FILENAME=columbus-5-"$LAST_HEIGHT".json
terrad export | pigz | aws s3 cp - S3_PATH/$STATE_FILENAME.gz
sed -ri 's/^(genesis_file = "data\/).+"$/\1'$STATE_FILENAME'"/' $CONFIG_FILE

systemctl start terrad
