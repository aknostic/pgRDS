#!/bin/bash

/usr/bin/pg_basebackup --pgdata=/var/lib/postgresql/basebackup
/usr/bin/s3cmd --no-progress --config=/var/lib/postgresql/.s3cfg put --recursive /var/lib/postgresql/basebackup/ s3://db-fashiolista-com/archive/basebackup/$(/usr/bin/curl --silent http://169.254.169.254/latest/meta-data/public-hostname/)/$(date +"%Y%m%d-%H%M%S")/
rm -rf /var/lib/postgresql/basebackup/*
