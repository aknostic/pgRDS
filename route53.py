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
import platform
import json, urllib2

from boto.route53.connection import Route53Connection
from boto.route53.record import ResourceRecordSets

class Route53Zone:
	def __init__(self, key, access, zone_id):
		self.zone_id = zone_id
		self.route53 = Route53Connection(key, access)

	def create_record(self, name, value):
		changes = ResourceRecordSets(self.route53, self.zone_id)

		change = changes.add_change("CREATE", name + ".", "CNAME", 60)
		change.add_value(value)
		changes.commit()

	def update_record(self, name, value):
		changes = ResourceRecordSets(self.route53, self.zone_id)

		sets = self.route53.get_all_rrsets(self.zone_id, None)
		for rset in sets:
			if rset.name == name + ".":
				previous_value = rset.resource_records[0]

				change = changes.add_change("DELETE", name + ".", "CNAME", 60)
				change.add_value(previous_value)

		change = changes.add_change("CREATE", name + ".", "CNAME", 60)
		change.add_value(value)
		changes.commit()

	def delete_record(self, name):
		changes = ResourceRecordSets(self.route53, self.zone_id)

		value = None
		sets = self.route53.get_all_rrsets(self.zone_id, None)
		for rset in sets:
			if rset.name == name + ".":
				value = rset.resource_records[0]

		if value != None:
			change = changes.add_change("DELETE", name + ".", "CNAME", 60)
			change.add_value(value)
			changes.commit()

if __name__ == '__main__':
	# easy testing, use like this (requires environment variables)
	#	python route53.py create_record key access id name value
	r53_zone = Route53Zone(sys.argv[2], sys.argv[3], sys.argv[4])
	print getattr(r53_zone, sys.argv[1])(*sys.argv[5:])
