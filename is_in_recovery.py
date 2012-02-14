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

#

import os, sys, subprocess
import json, urllib2

import psycopg2

import settings, administration

def is_in_recovery():
	conn = psycopg2.connect(host=settings.host,
							dbname=settings.database_name,
							user=settings.database_user,
							password=settings.database_password)
	conn.autocommit = True
	cur = conn.cursor()

	cur.execute("select pg_is_in_recovery()")
	in_recovery = cur.fetchone()[0]
	cur.close()
	conn.close()

	return in_recovery == True

# for convenience we can call this file to make backups directly
if __name__ == '__main__':
	print is_in_recovery()
