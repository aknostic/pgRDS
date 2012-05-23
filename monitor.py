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


import os, sys
import json, hashlib

from urllib2 import urlopen
from datetime import datetime

import psycopg2
import psycopg2.extras

from boto.ec2.cloudwatch import CloudWatchConnection
from boto.ec2.regioninfo import RegionInfo

import settings

#
# pgRDS MONITOR
#
#
class Monitor:
	def __init__(self, key, access):
		try:
			url = "http://169.254.169.254/latest/"

			self.userdata = json.load(urlopen(url + "user-data/"))
			public_hostname = urlopen(url + "meta-data/public-hostname/").read()
			zone = urlopen(url + "meta-data/placement/availability-zone/").read()
			region = zone[:-1]
		except:
			sys.exit("We should be getting user-data here...")

		# the name (and identity) of the cluster (the master)
		self.cluster = self.userdata['cluster']
		self.name = "{0}.{1}".format(self.userdata['name'], self.cluster)

		endpoint = "monitoring.{0}.amazonaws.com".format(region)
		region_info = RegionInfo(name=region, endpoint=endpoint)

		self.cloudwatch = CloudWatchConnection(key, access, region=region_info)
		self.namespace = '9apps/postgres'

		self.connection = psycopg2.connect(host=settings.host,
								port=5432,
								dbname=settings.database_name,
								user=settings.database_user,
								password=settings.database_password)

		# now, the non-system database connections
		self.databases = []
		try:
			database_cursor = self.connection.cursor()

			database_cursor.execute("select datname from pg_stat_database where datname !~ '(template[0-9]+|root|postgres)'")
			for database in database_cursor:
				self.databases.append([database[0],
								psycopg2.connect(host=settings.host, port=5432,
								dbname=database[0], user=settings.database_user,
								password=settings.database_password)])
		finally:
			database_cursor.close()

		self.pgbouncer = psycopg2.connect(host=settings.host,
								port=6432,
								dbname='pgbouncer',
								user=settings.database_user,
								password=settings.database_password)
		# without this it doesn't work
		self.pgbouncer.set_isolation_level(0)

	def __del__(self):
		self.connection.close()

	def is_in_recovery(self):
		self.connection.autocommit = True

		try:
			cur = self.connection.cursor()

			cur.execute("select pg_is_in_recovery()")
			in_recovery = cur.fetchone()[0]
		finally:
			cur.close()

		return in_recovery == True

	def collect(self, monitoring = 'on'):
		if monitoring not in ['on', 'all']:
			return [[], [], [], {}]

		now = datetime.now()

		names = []
		values = []
		units = []
		dimensions = { 'name' : self.name,
					'cluster' : self.cluster }

		if 'master' in self.userdata:
			[offset, receive_offset, replay_offset] = self._get_standby_lag()

			names.append('receive_lag')
			values.append(int(offset - receive_offset))
			units.append('Bytes')

			names.append('replay_lag')
			values.append(int(offset - replay_offset))
			units.append('Bytes')

		for database in self.databases:
			for relation in ["heap", "idx"]:
				[read, hit, hitratio] = self._get_hitratio(database[1], relation)

				names.append("{0}_{1}_read".format(database[0], relation))
				values.append(int(read))
				units.append("Count")

				names.append("{0}_{1}_hit".format(database[0], relation))
				values.append(int(hit))
				units.append("Count")

				if hitratio != None:
					names.append("{0}_{1}_hitratio".format(database[0], relation))
					values.append(float(hitratio * 100))
					units.append("Percent")

			conflicts = self._get_conflicts(database[0])
			names.append("{0}_{1}".format(database[0], 'confl_tablespace'))
			values.append(int(conflicts[0]))
			units.append("Count")

			names.append("{0}_{1}".format(database[0], 'confl_lock'))
			values.append(int(conflicts[1]))
			units.append("Count")

			names.append("{0}_{1}".format(database[0], 'confl_snapshot'))
			values.append(int(conflicts[2]))
			units.append("Count")

			names.append("{0}_{1}".format(database[0], 'confl_bufferpin'))
			values.append(int(conflicts[3]))
			units.append("Count")

			names.append("{0}_{1}".format(database[0], 'confl_deadlock'))
			values.append(int(conflicts[4]))
			units.append("Count")

			indexes_size = self._get_indexes_size(database[1])
			names.append("{0}_indexes_size".format(database[0]))
			values.append(int(indexes_size))
			units.append("Bytes")

			tables_size = self._get_tables_size(database[1])
			names.append("{0}_tables_size".format(database[0]))
			values.append(int(tables_size))
			units.append("Bytes")

		# nr of wal files
		size = self._get_nr_wal_files()
		names.append("wal_files")
		values.append(int(size))
		units.append("Count")

		# pgbouncer stats
		stats = self._get_pgbouncer_stats()
		names.append("pgbouncer_avg_req")
		values.append(int(stats[0]))
		units.append("Count/Second")

		names.append("pgbouncer_avg_recv")
		values.append(int(stats[1]))
		units.append("Bytes/Second")

		names.append("pgbouncer_avg_sent")
		values.append(int(stats[2]))
		units.append("Bytes/Second")

		names.append("pgbouncer_avg_query")
		values.append(float(stats[3] / 1000000))
		units.append("Seconds")

		# pgbouncer pools
		pools = self._get_pgbouncer_pools()
		names.append("pgbouncer_cl_active")
		values.append(float(pools[0]))
		units.append("Count")

		names.append("pgbouncer_cl_waiting")
		values.append(float(pools[1]))
		units.append("Count")

		names.append("pgbouncer_sv_active")
		values.append(float(pools[2]))
		units.append("Count")

		names.append("pgbouncer_sv_idle")
		values.append(float(pools[3]))
		units.append("Count")

		names.append("pgbouncer_sv_used")
		values.append(float(pools[4]))
		units.append("Count")

		names.append("pgbouncer_sv_tested")
		values.append(float(pools[5]))
		units.append("Count")

		names.append("pgbouncer_sv_login")
		values.append(float(pools[6]))
		units.append("Count")

		names.append("pgbouncer_maxwait")
		values.append(float(pools[7]))
		units.append("Count")

		return [names, values, units, dimensions]

	def put(self):
		result = False
		try:
			# only monitor if we are told to (this will break, if not set)
			monitoring = self.userdata['monitoring']
		except:
			monitoring = 'on'

		if monitoring in ['on', 'all']:
			# first get all we need
			[names, values, units, dimensions] = self.collect(monitoring)
			while len(names) > 0:
				names20 = names[:20]
				values20 = values[:20]
				units20 = units[:20]

				# we can't send all at once, only 20 at a time
				# first aggregated over all
				result = self.cloudwatch.put_metric_data(self.namespace,
								names20, value=values20, unit=units20)
				for dimension in dimensions:
					dimension = { dimension : dimensions[dimension] }
					result &= self.cloudwatch.put_metric_data(
								self.namespace, names20, value=values20,
								unit=units20, dimensions=dimension)

				del names[:20]
				del values[:20]
				del units[:20]
		else:
			print "we are not monitoring"

		return result
	
	def metrics(self):
		return self.cloudwatch.list_metrics()

	def _get_nr_wal_files(self):
		try:
			cursor = self.connection.cursor()

			sql = "select count(name) from (select pg_ls_dir('pg_xlog') as name) as xlogs where name != 'archive_status'"
			cursor.execute(sql)
			
			[size] = cursor.fetchone()
		finally:
			cursor.close()

		return size

	def _get_tables_size(self, connection):
		try:
			cursor = connection.cursor()

			sql = "select sum(pg_relation_size(relid)) from pg_stat_user_tables"
			cursor.execute(sql)
			
			[size] = cursor.fetchone()
		finally:
			cursor.close()

		return size

	def _get_indexes_size(self, connection):
		try:
			cursor = connection.cursor()

			sql = "select sum(pg_relation_size(indexrelid)) from pg_stat_user_indexes"
			cursor.execute(sql)
			
			[size] = cursor.fetchone()
		finally:
			cursor.close()

		return size

	def _get_conflicts(self, database):
		try:
			cursor = self.connection.cursor()

			sql = "select * from pg_stat_database_conflicts where datname = '{0}'".format(database)
			cursor.execute(sql)

			conflicts = cursor.fetchone()
		finally:
			cursor.close()

		return [conflicts[2], conflicts[3], conflicts[4], 
				conflicts[5], conflicts[6]] 

	def _get_hitratio(self, connection, relation="heap"):
		if relation == "heap":
			table = "tables"
		else:
			table = "indexes"

		try:
			cursor = connection.cursor()

			sql = "select sum({0}_blks_read) as read, sum({0}_blks_hit) as hit, (sum({0}_blks_hit) - sum({0}_blks_read)) / nullif(sum({0}_blks_hit),0) as hitratio from pg_statio_user_{1}".format(relation, table)
			cursor.execute(sql)
			
			[read, hit, hitratio] = cursor.fetchone()
		finally:
			cursor.close()

		return [read, hit, hitratio]

	def _get_standby_lag(self):
		try:
			master = psycopg2.connect(host=self.userdata['master'],
							dbname=settings.database_name,
							user=settings.database_user,
							password=settings.database_password)

			master.autocommit = True
			try:
				cursor = master.cursor()
				cursor.execute( "SELECT pg_current_xlog_location() AS location")
				[x, y] = (cursor.fetchone()[0]).split('/')
				offset = (int('ff000000', 16) * int(x, 16)) + int(y, 16)
			finally:
				cursor.close()

			try:
				cursor = self.connection.cursor()

				cursor.execute( "SELECT pg_last_xlog_receive_location(), pg_last_xlog_replay_location()")
				one = cursor.fetchone()
				
				[x, y] = (one[0]).split('/')
				receive_offset = (int('ff000000', 16) * int(x, 16)) + int(y, 16)
				
				[x, y] = (one[0]).split('/')
				replay_offset = (int('ff000000', 16) * int(x, 16)) + int(y, 16)
			finally:
				cursor.close()
		finally:
			master.close()

		return [offset, receive_offset, replay_offset]

	def _get_pgbouncer_stats(self):
		try:
			cursor = self.pgbouncer.cursor()
			cursor.execute('show stats')

			# ('pgbouncer\x00', 119L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)
			[name, total_requests, total_received,
				total_sent, total_query_time, avg_req,
				avg_recv, avg_sent, avg_query] = cursor.fetchone()
		finally:
			cursor.close()

		return [avg_req, avg_recv, avg_sent, avg_query]

	def _get_pgbouncer_pools(self):
		cl_active = cl_waiting = sv_active = sv_idle = 0
		sv_used = sv_tested = sv_login = maxwait = 0
		try:
			cursor = self.pgbouncer.cursor()
			cursor.execute('show pools')

			# ('pgbouncer\x00', 'pgbouncer\x00', 1, 0, 0, 0, 0, 0, 0, 0)
			for pool in cursor:
				cl_active += pool[2]
				cl_waiting += pool[3]
				sv_active += pool[4]
				sv_idle += pool[5]
				sv_used += pool[6]
				sv_tested += pool[7]
				sv_login += pool[8]
				maxwait = max(maxwait, pool[9])
		finally:
			cursor.close()

		return [cl_active, cl_waiting, sv_active, sv_idle,
					sv_used, sv_tested, sv_login, maxwait]

if __name__ == '__main__':
	key = os.environ['EC2_KEY_ID']
	access = os.environ['EC2_SECRET_KEY']

	# easy testing, use like this (requires environment variables)
	#	python cluster.py get_master cluster 2c922342a.cluster
	monitor = Monitor(key, access)
	print getattr(monitor, sys.argv[1])(*sys.argv[3:])
