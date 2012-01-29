#!/bin/bash

dirname=`dirname $0`

source ${dirname}/config.sh
python ${dirname}/$1.py ${EC2_KEY_ID} ${EC2_SECRET_KEY}
