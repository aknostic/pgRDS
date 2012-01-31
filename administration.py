# Copyright (C) 2011, 2012 9apps B.V.
# 
# This file is part of pgRDS.
# 
# pgRDS is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# pgRDS is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with pgRDS. If not, see <http://www.gnu.org/licenses/>.

import os, sys
import json, urllib2
import hashlib

from time import gmtime,strftime

from boto.sdb.connection import SDBConnection
from boto.sdb.regioninfo import RegionInfo

try:
	url = "http://169.254.169.254/latest/"

	public_hostname = urllib2.urlopen(url + "meta-data/public-hostname").read()
	availability_zone = urllib2.urlopen(url + "meta-data/placement/availability-zone").read()
	region = availability_zone[:-1]
except:
	exit("We should be getting user-data here...")

region_info = RegionInfo(name=region,endpoint="sdb.{0}.amazonaws.com".format(region))

def add_snapshot(key, access, cluster, name, snapshot):
	sdb = SDBConnection(key, access, region=region_info)

	domain = sdb.lookup(cluster, True)
	if domain == None:
		domain = sdb.create_domain(cluster)

	now = strftime("%Y-%m-%d %H:%M:%S", gmtime())

	# add the snapshot for expiration
	backup = domain.new_item(snapshot[0])
	backup.add_value('name', name)
	backup.add_value('snapshot', snapshot[0])
	backup.add_value('created', now)
	backup.add_value('expires', snapshot[1])
	backup.save()

def get_latest_snapshot(key, access, cluster, name):
	sdb = SDBConnection(key, access, region=region_info)
	now = strftime("%Y-%m-%d %H:%M:%S", gmtime())

	domain = sdb.lookup(cluster, True)
	if domain == None:
		domain = sdb.create_domain(cluster)

	select = "select * from `{0}` where name = '{1}' and created < '{2}' order by created desc limit 1".format(cluster, name, now)
	snapshots = domain.select(select, consistent_read=True)
	return snapshots.next()

def delete_snapshot(key, access, cluster, snapshot_id):
	sdb = SDBConnection(key, access, region=region_info)

	domain = sdb.lookup(cluster, True)
	if domain == None:
		domain = sdb.create_domain(cluster)

	return domain.delete_item(domain.get_item(snapshot_id))

def get_expired_snapshots(key, access, cluster):
	sdb = SDBConnection(key, access, region=region_info)

	domain = sdb.lookup(cluster, True)
	if domain == None:
		domain = sdb.create_domain(cluster)

	now = strftime("%Y-%m-%d %H:%M:%S", gmtime())
	select = "select * from `{0}` where itemName() > 'snap-' and itemName() != 'snapshot' and expires < '{1}'".format(cluster, now)
	snapshots = domain.select(select, consistent_read=True)
	return snapshots

def get_all_snapshots(key, access, cluster):
	sdb = SDBConnection(key, access, region=region_info)

	domain = sdb.lookup(cluster, True)
	if domain == None:
		domain = sdb.create_domain(cluster)

	now = strftime("%Y-%m-%d %H:%M:%S", gmtime())
	select = "select * from `{0}` where itemName() > 'snap-' and itemName() != 'snapshot'".format(cluster)
	snapshots = domain.select(select, consistent_read=True)
	return snapshots

if __name__ == '__main__':
	print sys.argv
