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
import datetime, json, urllib2

from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.exception import S3CreateError

from boto.ec2.connection import EC2Connection
from boto.ec2.regioninfo import RegionInfo

import settings, administration
from route53 import Route53Zone

try:
	url = "http://169.254.169.254/latest/"

	userdata = json.load(urllib2.urlopen(url + "user-data"))
	if not userdata.has_key('tablespaces'):
		userdata['tablespaces'] = [{ "device" : "/dev/sdf",
								"name" : "main", "size" : 2}]
	
	instance_id = urllib2.urlopen(url + "meta-data/instance-id").read()
	instance_type = urllib2.urlopen(url + "meta-data/instance-type").read()
	hostname = urllib2.urlopen(url + "meta-data/public-hostname/").read()

	zone = urllib2.urlopen(url + "meta-data/placement/availability-zone").read()
	region = zone[:-1]

	zone_name = os.environ['HOSTED_ZONE_NAME']
	zone_id = os.environ['HOSTED_ZONE_ID']
except Exception as e:
	exit( "We couldn't get user-data or other meta-data...")

pg_dir = '/var/lib/postgresql/9.1/'
pg_conf = '/etc/postgresql/9.1/main/postgresql.conf'
pg_type_conf = '/etc/postgresql/9.1/main/instance_type.conf'
pgb_conf = '/etc/pgbouncer/pgbouncer.ini'

# we are going to work with local files, we need our path
path = os.path.dirname(os.path.abspath(__file__))

def create_device(device='/dev/sdf', snapshot=None, size=10):
	# if we have the device just don't do anything anymore
	mapping = ec2.get_instance_attribute(instance_id, 'blockDeviceMapping')
	try:
		volume_id = mapping['blockDeviceMapping'][device].volume_id
	except:
		volume = ec2.create_volume(size, zone, snapshot=snapshot)

		# nicely wait until the volume is available
		while volume.volume_state() != "available":
			volume.update()

		volume.attach(instance_id, device)
		volume_id = volume.id

		# we can't continue without a properly attached device
		os.system("while [ ! -b {0} ] ; do /bin/true ; done".format(device))

		# make sure the volume is deleted upon termination
		# should also protect from disaster like loosing an instance
		# (it doesn't work with boto, so we do it 'outside')
		os.system("/usr/bin/ec2-modify-instance-attribute --block-device-mapping \"{0}=:true\" {1} --region {2}".format(device, instance_id, region))

def create_mount(dev='/dev/sdf', name='main'):
	mount = pg_dir + name

	if os.path.ismount(mount) == False:
		# it is probably new, so try to make an XFS filesystem
		os.system("/sbin/mkfs.xfs {0}".format(dev))

		# mount, but first wait until the device is ready
		os.system("sudo -u postgres /bin/mkdir -p {0}".format(mount))
		os.system("/bin/mount -t xfs -o defaults {0} {1}".format(dev, mount))

	# and grow (if necessary)
	os.system("/usr/sbin/xfs_growfs {0}".format(mount))

	os.system("chown -R postgres.postgres {0}".format(mount))


def set_cron():
	cron = "{0}/cron.d/postgres.cron".format(path)
	os.system("/usr/bin/crontab {0}".format(cron))

def set_conf():
	bucket = userdata['cluster'].replace('.', '-')
	conf = "{0}/etc/postgresql/9.1/main/postgresql.conf".format(path)
	type_conf = "{0}/etc/postgresql/9.1/main/{1}.conf".format(path, instance_type)
	my_pgb_conf = "{0}/etc/pgbouncer/pgbouncer.ini".format(path)

	os.system("cp {0} {1}".format(type_conf, pg_type_conf))
	os.system("cp {0} {1}".format(conf, pg_conf))
	os.system("cp {0} {1}".format(my_pgb_conf, pgb_conf))
	os.system("/bin/chown postgres.postgres {0}".format(pg_conf))
	os.system("/bin/sed -i \x27s_s3://[^/]*/_s3://{0}/_\x27 {1}".format(bucket, pg_conf))
	try:
		slow = userdata['slow']
		os.system("/bin/sed -i \x27s/log_min_duration_statement.*/log_min_duration_statement = {0}/\x27 {1}".format(slow, pg_conf))
	except:
		pass

def set_recovery_conf():
	try:
		bucket = userdata['clone'].replace('.', '-')
	except:
		bucket = userdata['cluster'].replace('.', '-')
	f = open( "{0}/main/recovery.conf".format(pg_dir), "w")

	f.write("restore_command = '/usr/bin/s3cmd --config=/var/lib/postgresql/.s3cfg get s3://{0}/archive/wal/%f %p'\n".format(bucket))

	# lets by humble, lets try to be a slave first
	try:
		master = userdata['master']
	except:
		master = None

	try:
		clone = userdata['clone']
	except:
		clone = None

	try:
		timestamp = userdata['timestamp']
	except:
		timestamp = datetime.datetime.now()

	
	if master != None:
		f.write("primary_conninfo = 'host={0} port=5432 user={1} password={2} sslmode={3}'\n".format(userdata['master'], settings.database_user, settings.database_password, settings.sslmode))
		f.write("standby_mode = on\n")
	
	if clone != None:
		f.write("recovery_target_time = '{0}'\n".format(timestamp))

	# don't know if/how this works
	#f.write("recovery_target_timeline = latest\n")

	f.close()

	# and make sure we get rid of backup_label
	os.system("rm -f {0}main/backup_label".format(pg_dir))

def add_postgresql_monitor():
	f = open( "{0}/etc/monit/conf.d/postgresql".format(path), "w")
	f.write("  check process postgresql with pidfile /var/run/postgresql/9.1-main.pid")
	f.write("	start program = \"/etc/init.d/postgresql start\"")
	f.write("	stop  program = \"/etc/init.d/postgresql stop\"")
	f.write("	if failed unixsocket /var/run/postgresql/.s.PGSQL.5432 protocol pgsql then restart")
	f.write("	if failed unixsocket /var/run/postgresql/.s.PGSQL.5432 protocol pgsql then alert")
	f.write("	if failed host localhost port 5432 protocol pgsql then restart")
	f.write("	if failed host localhost port 5432 protocol pgsql then alert")
	f.write("	group database")
	f.close()

def add_monitor(device="/dev/sdf", name="main"):
	f = open( "{0}/etc/monit/conf.d/{1}".format(path, name), "w")
	f.write("  check filesystem {0} with path {1}".format(name, device))
	f.write("	if failed permission 660 then alert")
	f.write("	if failed uid root then alert")
	f.write("	if failed gid disk then alert")
	f.write("	if space usage > 80% for 5 times within 15 cycles then alert")
	f.close()

def meminfo():
	"""
	dict of data from meminfo (str:int).
	Values are in kilobytes.
	"""
	re_parser = re.compile(r'^(?P<key>\S*):\s*(?P<value>\d*)\s*kB')
	result = dict()
	for line in open('/proc/meminfo'):
		match = re_parser.match(line)
		if not match:
			continue # skip lines that don't parse
		key, value = match.groups(['key', 'value'])
		result[key] = int(value)
	return result

if __name__ == '__main__':
	region_info = RegionInfo(name=region,
							endpoint="ec2.{0}.amazonaws.com".format(region))
	ec2 = EC2Connection(sys.argv[1], sys.argv[2], region=region_info)
	s3 = S3Connection(sys.argv[1], sys.argv[2])
	r53_zone = Route53Zone(sys.argv[1], sys.argv[2], zone_id)

	name = "{0}.{1}".format(userdata['name'],
						os.environ['HOSTED_ZONE_NAME'].rstrip('.'))

	try:
		set_cron()

		# are we a new cluster, or a clone from another?
		try:
			cluster = userdata['clone']
		except:
			cluster = userdata['cluster']

		# postgres is not running yet, so we have all the freedom we need
		#if userdata.has_key('tablespaces'):
		for tablespace in userdata['tablespaces']:
			# keep the size of main for later (WAL)
			if tablespace['name'] == "main":
				size_of_main = tablespace['size']

			snapshot = administration.get_latest_snapshot(sys.argv[1],
					sys.argv[2], cluster, tablespace['name'])
			create_device(tablespace['device'], size=tablespace['size'],
					snapshot=snapshot)
			create_mount(tablespace['device'], tablespace['name'])

			add_monitor(tablespace['device'], tablespace['name'])

		# set the correct permissions, and some other necessities
		mount = pg_dir + "main"
		os.system("chmod 0700 {0}".format(mount))

		# prepare the new filesystem for postgres
		if not os.path.exists( "{0}/postgresql.conf".format(mount)):
			os.system("sudo -u postgres /usr/lib/postgresql/9.1/bin/pg_ctl -D {0} initdb".format(mount))
			os.symlink( "/etc/ssl/certs/ssl-cert-snakeoil.pem",
						"{0}/server.crt".format(mount))
			os.symlink( "/etc/ssl/private/ssl-cert-snakeoil.key",
						"{0}/server.key".format(mount))
		else:
			# we do have a postgresql.conf, we must restore (as long as user data lets us)
 			if 'recovery' in userdata and not userdata['recovery'] == 'no':
  			 	set_recovery_conf()

		# and now, create a separate WAL mount
		# (has to be only now, pg_ctl doesn't like a non-empty pg dir)
		os.system("cp -r {0}main/pg_xlog /mnt".format(pg_dir))
		device = "/dev/sdw"
		create_device(device, size=size_of_main)
		create_mount(device, "main/pg_xlog")
		if not os.path.exists( "{0}/pg_xlog/archive_status)".format(mount)):
			os.system("cp -r /mnt/pg_xlog/* {0}main/pg_xlog".format(pg_dir))
			os.system("chown -R postgres.postgres {0}main/pg_xlog".format(pg_dir))
		add_monitor(device, "pg_xlog")

		# we tuned postgres for instance types, we also need help the kernel along
		os.system('sysctl -w "kernel.shmall=4194304"')
		os.system('sysctl -w "kernel.shmmax={0}"'.format(meminfo()['MemTotal'] * 1024))
		# and lower the chances of being shot
		os.system('sysctl -w "vm.overcommit_memory=2"')

		# always overwrite the conf
		set_conf()
		add_postgresql_monitor()
	except Exception as e:
		print "{0} could not be prepared ({1})".format(name, e)
