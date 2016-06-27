#!/usr/bin/python

import json

class ControllerDB:
	dbpath = None

	def __init__(self, dp):
		self.dbpath = dp

	def get_next_nodeid(self, nodenm):
		nids = []
		db = self.parse()
		ret = 255
		i=0
		if len(db) > 0:
			for ent in db:
				nids.append(ent['nodeid'])
			for nid in sorted(nids):
				if nid != i: # re-use a number if there's a gap
					ret = i
					break
				i += 1

			if i >= 255:
				return 255 # return invalid id, we are full
		else:
			i=1

		newent = {}
		newent['nodeid'] = i
		newent['nodenm'] = nodenm
		
		db.append(newent)
		self.save(db)
		return i

	def parse(self):
		ret = []
		gwent = {}
		gwent['nodeid'] = 0
		gwent['nodenm'] = 'Gateway'
		ret.append(gwent)
		dbf = open(self.dbpath, 'rw')
		s = dbf.read()
		if len(s) > 0:
			ret = json.loads(s)
		dbf.close()
		return ret

	def save(self, db):
		dbf = open(self.dbpath, 'w')
		dbf.write(json.dumps(db))
		dbf.close()

if __name__ == '__main__':
	cdb = ControllerDB('test.json')
	x = cdb.get_next_nodeid('Waxo')
	print x
