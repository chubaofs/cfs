#!/bin/sh
rm -rf /cfs/data/* /cfs/log/*
mkdir -p /cfs/bin /cfs/log /cfs/data/meta /cfs/data/raft
echo "start metanode"
/cfs/bin/cfs-server -f -c /cfs/conf/metanode.json

