#!/bin/bash
                               
set -eu

if [ ! -f $SNAP_COMMON/etc/exporter.config ]; then
	echo "configuration file $SNAP_COMMON/etc/exporter.config does not exist."
	exit 1
fi 

export HOME=$SNAP_COMMON
export CONFIG=$SNAP_COMMON/etc/exporter.config
export SARAMA_LOG_FILE=$SNAP_COMMON/log/sarama.log

$SNAP/bin/exporter
