#!/bin/bash
                               
set -eu

if [ ! -f $SNAP_COMMON/etc/exporter.config ]; then
	echo "configuration file $SNAP_COMMON/etc/exporter.config does not exist."
	exit 1
fi 

export HOME=$SNAP_COMMON
export CONFIG=$SNAP_COMMON/etc/exporter.config

$SNAP/bin/exporter