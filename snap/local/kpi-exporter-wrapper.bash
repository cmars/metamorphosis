#!/bin/bash
                               
set -eu

if [ ! -f $SNAP_COMMON/etc/kpi-exporter.config ]; then
	echo "configuration file $SNAP_COMMON/etc/kpi-exporter.config does not exist."
	exit 1
fi 

export HOME=$SNAP_COMMON
export CONFIG=$SNAP_COMMON/etc/kpi-exporter.config

$SNAP/bin/kpi-exporter