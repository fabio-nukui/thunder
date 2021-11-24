#!/usr/bin/env bash

STRATEGY_NAME=${STRATEGY_NAME:-m_terra}
REGION=$(grep region ~/.aws/config | sed -r 's/region = (.+)/\1/')
INSTANCE_NAME=$(cat ~/INSTANCE_NAME)
grep -E 'height=.+[0-9]+\.?[0-9]*ms' logs/"$STRATEGY_NAME".log* | sed -r 's/.+height=([0-9]+).+ ([0-9]+\.?[0-9*])ms.+/{"height":\1,"time_ms":\2}/' | aws s3 cp - s3://crypto-thunder/analysis/block_processing_time/time-$REGION-$INSTANCE_NAME.log
