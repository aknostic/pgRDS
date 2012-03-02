# Copyright (C) 2011, 2012 9apps B.V.
# 
# This file is part of Redis for AWS.
# 
# Redis for AWS is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Redis for AWS is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with Redis for AWS. If not, see <http://www.gnu.org/licenses/>.

import os, sys, re, subprocess
import json, urllib2

from boto.ec2.connection import EC2Connection
from boto.ec2.regioninfo import RegionInfo

from route53 import Route53Zone

import settings

try:
	url = "http://169.254.169.254/latest/"

	userdata = json.load(urllib2.urlopen(url + "user-data"))
	if not userdata.has_key('tablespaces'):
		userdata['tablespaces'] = [{ "device" : "/dev/sdf",
								"name" : "main", "size" : 2}]
	
	instance_id = urllib2.urlopen(url + "meta-data/instance-id").read()
	hostname = urllib2.urlopen(url + "meta-data/public-hostname/").read()

	zone = urllib2.urlopen(url + "meta-data/placement/availability-zone").read()
	region = zone[:-1]

	zone_name = os.environ['HOSTED_ZONE_NAME']
	zone_id = os.environ['HOSTED_ZONE_ID']
except Exception as e:
	exit( "We couldn't get user-data or other meta-data...")

pg_dir = '/var/lib/postgresql/9.1/'

import psycopg2

def pgbouncer():
	os.system("sudo -u postgres psql -t -c \"select \\\"'||rolname||'\\\"'||' \\\"'||rolpassword||'\\\"' from pg_authid ;\" | sed 's/^\\s*//' | sed '/^$/d' > /etc/pgbouncer/userlist.txt")
	os.system("/etc/init.d/pgbouncer restart")

def monitor():
	os.system("/usr/sbin/monit reload")
	os.system("/usr/sbin/monit monitor postgresql")

def create_tablespace(tablespace, location=None):
	conn = psycopg2.connect(host=settings.host,
							dbname=settings.database_name,
							user=settings.database_user,
							password=settings.database_password)
	conn.autocommit = True
	cur = conn.cursor()
	if location == None or location == "":
		location = "{0}{1}".format(pg_dir, tablespace)

	cur.execute('CREATE TABLESPACE {0} LOCATION \x27{1}\x27'.format(tablespace, location))

	cur.close()
	conn.close()

def alter_table_set_tablespace(table, tablespace):
	conn = psycopg2.connect(host=settings.host,
							dbname=settings.database_name,
							user=settings.database_user,
							password=settings.database_password)
	cur = conn.cursor()

	cur.execute('ALTER TABLE {0} SET TABLESPACE {1}'.format(table, tablespace))
	conn.commit()

	cur.close()
	conn.close()

def prepare_database():
	os.system('sudo -u postgres psql -c "create user root"')
	os.system('sudo -u postgres psql -c "create database root"')
	os.system('sudo -u postgres psql -c "grant all on database root to root"')
	os.system('sudo -u postgres psql -c "alter user {0} password \x27{1}\x27"'.format(settings.database_user, settings.database_password))

if __name__ == '__main__':
	region_info = RegionInfo(name=region,
							endpoint="ec2.{0}.amazonaws.com".format(region))
	ec2 = EC2Connection(sys.argv[1], sys.argv[2], region=region_info)
	r53_zone = Route53Zone(sys.argv[1], sys.argv[2], zone_id)

	name = "{0}.{1}".format(userdata['name'],
						os.environ['HOSTED_ZONE_NAME'].rstrip('.'))

	if sys.argv[3] == "start":
		r53_zone.create_record(name, hostname)
		ec2.create_tags([instance_id], { "Name": name })

		# we only prepare the database when we are NOT subservient
		try:
			slave = userdata['slave']
		except:
			prepare_database()

		pgbouncer()
		monitor()
	elif sys.argv[3] == "tablespaces":
		for tablespace in userdata['tablespaces']:
			name = tablespace['name']
			if name != "main":
				try:
					create_tablespace(name)
				except:
					print "tablespace {0} already exists?".format(name)

				try:
					alter_table_set_tablespace(name, name)
				except:
					print "table {0} does not exist yet?".format(name)
	elif sys.argv[3] == "stop":
		# we change r53 in stop.py, as soon as possible
		print "stop"
	elif sys.argv[3] == "restart":
		print "restart"
	elif sys.argv[3] == "reload":
		print "reload"
	else:
		print "else"
