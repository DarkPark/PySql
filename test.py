"Unit tests"

__author__ = "DarkPark"

import unittest
import time
from sql import *

class DataBaseTest(unittest.TestCase):
	"Test connection to the specified database file or db in memory"
	
	def test_init_a(self):
		"Connect to db file and check connection"
		self.db = DataBase('test.sqlite')
		self.assertIsInstance(self.db.__conn__, sqlite3.Connection)
		del self.db
	
	def test_init_b(self):
		"Connect to not existing db file"
		self.db = DataBase('not_exist_db_file')
		self.assertIsInstance(self.db.__conn__, sqlite3.Connection)
		del self.db
		
	def test_init_c(self):
		"Connect to db in memory"
		self.db = DataBase()
		self.assertIsInstance(self.db.__conn__, sqlite3.Connection)
		del self.db
		
	def test_init_d(self):
		"Connect to db file and check dynamic tables and columns creation"
		self.db = DataBase('test.sqlite')
		self.assertEqual(self.db.sections.__name__, 'sections')
		self.assertEqual(self.db.sections.sum_inc.__name__, 'sum_inc')
		self.assertEqual(self.db.sections.sum_inc.__data__['table'], 'sections')
		self.assertEqual(self.db.sections.sum_inc.__data__['type'], 'FLOAT')
		self.assertEqual(self.db.sections.sum_inc.__data__['pk'], 0)
		del self.db

class SqlBuilderTest(unittest.TestCase):
	"Test sql queries generator"
	
	def setUp(self):
		self.db = DataBase('test.sqlite')
		self.query = SqlBuilder()
	
	def tearDown(self):
		del self.db
		del self.query

	def test_select(self):
		"Check select parameters"
		# default
		self.query.Select().From(self.db.items)
		self.assertEqual(self.query.data[self.query.mode]['select'], [])
		# check direct
		self.assertEqual(self.query.SetSelect(self.db.items.id, self.db.items.name, distinct=True).data[self.query.mode]['select'], ['items.id', 'items.name'])
		# object params
		self.query.Select(self.db.items.id, self.db.items.name, self.db.items.price)
		self.assertEqual(self.query.data[self.query.mode]['select'], ['items.id', 'items.name', 'items.price'])
		# whole tables
		self.query.Select(self.db.items, self.db.info)
		self.assertEqual(self.query.data[self.query.mode]['select'], ['items.*', 'info.*'])
		# simple strings with bad formatting
		self.query.Select(' id,name,, ', ' ,;,,users.*', ' items.price,;    ', "")
		self.assertEqual(self.query.data[self.query.mode]['select'], ['id', 'name', 'users.*', 'items.price'])
		# injections
		self.query.Select('id, name from users; delete * from users;--')
		self.assertEqual(self.query.data[self.query.mode]['select'], ['id'])
		# invalid params - none should be accepted
		self.query.Select(self.db, self, 123, None, -56, 0.01, [], {}, ())
		self.assertEqual(self.query.data[self.query.mode]['select'], [])
		# mixed
		self.query.Select(self.db, self, 123, None, -56, 0.01, [], {}, (), self.db.items, self.db.info.id, ' ;,info.price,, some not valid stuff, ', '', ',', '; delete * from users; ')
		self.assertEqual(self.query.data[self.query.mode]['select'], ['items.*', 'info.id', 'info.price'])
	
	def test_from(self):
		"Check from parameters"
		# simple case with one class-table
		self.query.Select(self.db.items.id).From(self.db.items)
		self.assertEqual(self.query.data[self.query.mode]['from'], ['items'])
		# check direct
		self.query.SetFrom(self.db.checks)
		self.assertEqual(self.query.data[self.query.mode]['from'], ['checks'])
		# multiple set
		self.query.Select(self.db.items.id).From(self.db.items, self.db.brands, 'sections, info')
		self.assertEqual(self.query.data[self.query.mode]['from'], ['items', 'brands', 'sections', 'info'])
		# bad formatting
		self.query.Select(self.db.items.id).From(' , sections,,, info, ; , ', '', ',', ' ; ')
		self.assertEqual(self.query.data[self.query.mode]['from'], ['sections', 'info'])
		# injections
		self.query.Select(self.db.items.id).From(self.db.brands, ' , sections, ; delete * from users; , ')
		self.assertEqual(self.query.data[self.query.mode]['from'], ['brands', 'sections'])
	
	def test_join(self):
		"Check joining"
		self.query.Select().From(self.db.items)
		self.query.Join(self.db.info, self.db.items.id == self.db.info.id_item)
		self.query.Join(self.db.checks, self.db.checks.id == self.db.info.id_item, 'left')
		self.assertEqual(self.query.BuildSelect(), 'select * from items join info on (items.id = info.id_item) left join checks on (checks.id = info.id_item)')
		self.query.Select().From(self.db.items)
		self.query.Join(self.db.checks, self.db.checks.id == self.db.info.id_item, 'right')
		self.query.Join(self.db.checks, self.db.checks.id == self.db.info.id_item, 'some wrong; params')
		self.assertEqual(self.query.BuildSelect(), 'select * from items right join checks on (checks.id = info.id_item) join checks on (checks.id = info.id_item)')
		self.query.Select().From(self.db.items)
		self.query.Join(self.db.checks, self.db.checks.id == self.db.info.id_item, 'inner')
		self.query.Join(self.db.info, self.db.info.id == self.db.info.id_item, 'cross')
		self.query.Join(self.db.brands, self.db.brands.id == self.db.items.id_brand, 'full outer')
		self.assertEqual(self.query.BuildSelect(), 'select * from items inner join checks on (checks.id = info.id_item) cross join info on (info.id = info.id_item) full outer join brands on (brands.id = items.id_brand)')
		# check direct
		self.query.SetJoin(self.db.info, self.db.items.id == self.db.info.id_item)
		self.assertEqual(self.query.BuildSelect(), 'select * from items join info on (items.id = info.id_item)')
		self.query.SetJoin(self.db.brands, self.db.brands.id == self.db.items.id_brand)
		self.assertEqual(self.query.BuildSelect(), 'select * from items join brands on (brands.id = items.id_brand)')
		self.query.AddJoin(self.db.info, self.db.items.id == self.db.info.id_item)
		self.assertEqual(self.query.BuildSelect(), 'select * from items join brands on (brands.id = items.id_brand) join info on (items.id = info.id_item)')

	def test_where(self):
		"Check conditions parameters"
		self.query.Select(self.db.items).From(self.db.items)
		# simple conditions
		self.assertEqual(self.query.Where(self.db.items.name == 'name').data[self.query.mode]['where'], "(items.name = 'name')")
		self.query.Select(self.db.items)
		self.assertEqual(self.query.From(self.db.items).Where(self.db.items.price < 5.0).data[self.query.mode]['where'], '(items.price < 5.0)')
		self.assertEqual(self.query.SetWhere('  (info.id = 5.0)  ').data[self.query.mode]['where'], '(info.id = 5.0)')
		self.assertEqual(self.query.SetWhere(self.db.info.id < -50).data[self.query.mode]['where'], '(info.id < -50)')
		self.assertEqual(self.query.SetWhere(self.db.info.id > 50).data[self.query.mode]['where'],  '(info.id > 50)')
		self.assertEqual(self.query.SetWhere(50 > self.db.info.id).data[self.query.mode]['where'],  '(info.id < 50)')
		self.assertEqual(self.query.SetWhere(-50 < self.db.info.id).data[self.query.mode]['where'], '(info.id > -50)')
		self.assertEqual(self.query.SetWhere(self.db.info.id == 50).data[self.query.mode]['where'], '(info.id = 50)')
		self.assertEqual(self.query.SetWhere(50 == self.db.info.id).data[self.query.mode]['where'], '(info.id = 50)')
		self.assertEqual(self.query.SetWhere(self.db.info.id != 50).data[self.query.mode]['where'], '(info.id != 50)')
		self.assertEqual(self.query.SetWhere(50 != self.db.info.id).data[self.query.mode]['where'], '(info.id != 50)')
		self.assertEqual(self.query.SetWhere(self.db.info.id < self.db.items.id).data[self.query.mode]['where'], '(info.id < items.id)')
		# complex boolean with different and same fields
		self.assertEqual(self.query.SetWhere((self.db.info.id < 50) & (self.db.info.id > 40)).data[self.query.mode]['where'], '((info.id < 50) and (info.id > 40))')
		self.assertEqual(self.query.SetWhere((self.db.info.id < 50) & (self.db.items.name != 'qwe')).data[self.query.mode]['where'], "((info.id < 50) and (items.name != 'qwe'))")
		self.assertEqual(self.query.SetWhere((self.db.info.id < 50) | (self.db.items.name == 'rty')).data[self.query.mode]['where'], "((info.id < 50) or (items.name = 'rty'))")
		self.assertEqual(self.query.SetWhere((self.db.info.id < 50) | (self.db.info.id > 40)).data[self.query.mode]['where'], '((info.id < 50) or (info.id > 40))')
		self.query.SetWhere((self.db.items.name != 'test') | (self.db.info.id < 50) & (self.db.info.id > 40) | (self.db.items.name == 'fdd'))
		self.assertEqual(self.query.data[self.query.mode]['where'], "(((items.name != 'test') or ((info.id < 50) and (info.id > 40))) or (items.name = 'fdd'))")
		# type check with type mismatch
		with self.assertRaises(TypeError): self.query.SetWhere(self.db.info.id == 'qwe')
		with self.assertRaises(TypeError): self.query.SetWhere(self.db.info.id != 'qwe')
		with self.assertRaises(TypeError): self.query.SetWhere(self.db.items.name == 50)
		with self.assertRaises(TypeError): self.query.SetWhere(self.db.items.name != 50)
		with self.assertRaises(TypeError): self.query.SetWhere(self.db.items.name > 50)
		with self.assertRaises(TypeError): self.query.SetWhere(self.db.items.name < 50)
		with self.assertRaises(TypeError): self.query.SetWhere(self.db.items.price == 'rty')
		# containment tests - in
		self.assertEqual(self.query.SetWhere(self.db.info.id.In(23,45,67)).data[self.query.mode]['where'], '(info.id in (23, 45, 67))')
		self.assertEqual(self.query.SetWhere(self.db.info.id.In('A', 'B', 'C')).data[self.query.mode]['where'], "(info.id in ('A', 'B', 'C'))")
		self.assertEqual(self.query.SetWhere(self.db.info.id.In((23,45,67))).data[self.query.mode]['where'], '(info.id in (23, 45, 67))')
		self.assertEqual(self.query.SetWhere(self.db.info.id.In([20,40,60])).data[self.query.mode]['where'], '(info.id in (20, 40, 60))')
		self.assertEqual(self.query.SetWhere(self.db.info.id.In(set([23,45,67]))).data[self.query.mode]['where'], '(info.id in (67, 45, 23))')
		self.assertEqual(self.query.SetWhere(self.db.info.id.In(['qw', 'as', 'zx'])).data[self.query.mode]['where'], "(info.id in ('qw', 'as', 'zx'))")
		self.assertEqual(self.query.SetWhere(self.db.info.id.In({'a':16, 'b':32, 'c':64})).data[self.query.mode]['where'], "(info.id in ('a', 'c', 'b'))")
		self.assertEqual(self.query.SetWhere(self.db.info.id.In(['qw', 'as', 'zx'])).data[self.query.mode]['where'], "(info.id in ('qw', 'as', 'zx'))")
		# containment tests - not in
		self.assertEqual(self.query.SetWhere(self.db.info.id.NotIn(23,45,67)).data[self.query.mode]['where'], '(info.id not in (23, 45, 67))')
		self.assertEqual(self.query.SetWhere(self.db.info.id.NotIn('A', 'B', 'C')).data[self.query.mode]['where'], "(info.id not in ('A', 'B', 'C'))")
		self.assertEqual(self.query.SetWhere(self.db.info.id.NotIn((23,45,67))).data[self.query.mode]['where'], '(info.id not in (23, 45, 67))')
		self.assertEqual(self.query.SetWhere(self.db.info.id.NotIn([20,40,60])).data[self.query.mode]['where'], '(info.id not in (20, 40, 60))')
		self.assertEqual(self.query.SetWhere(self.db.info.id.NotIn(set([23,45,67]))).data[self.query.mode]['where'], '(info.id not in (67, 45, 23))')
		self.assertEqual(self.query.SetWhere(self.db.info.id.NotIn(['qw', 'as', 'zx'])).data[self.query.mode]['where'], "(info.id not in ('qw', 'as', 'zx'))")
		self.assertEqual(self.query.SetWhere(self.db.info.id.NotIn({'a':16, 'b':32, 'c':64})).data[self.query.mode]['where'], "(info.id not in ('a', 'c', 'b'))")
		self.assertEqual(self.query.SetWhere(self.db.info.id.NotIn(['qw', 'as', 'zx'])).data[self.query.mode]['where'], "(info.id not in ('qw', 'as', 'zx'))")
	
	def test_andor(self):
		"Check additional conditions parameters"
		self.query.Select(self.db.items).From(self.db.items)
		# wrong order
		with self.assertRaises(Exception):
			self.query.And(self.db.items.id < 20).Where(self.db.items.id > 10)
		self.query.Select(self.db.items)
		self.assertEqual(self.query.From(self.db.items).Where(self.db.items.id > 10).And(self.db.items.id < 20).data[self.query.mode]['where'], '(items.id > 10) and (items.id < 20)')
		self.query.Select(self.db.items)
		self.assertEqual(self.query.From(self.db.items).Where(self.db.items.id != 0).Or(self.db.items.id.In(20,30,40)).data[self.query.mode]['where'], '(items.id != 0) or (items.id in (20, 30, 40))')
		
	def test_group(self):
		"Check group by parameters"
		self.query.Select(self.db.items).From(self.db.items)
		self.assertEqual(self.query.GroupBy(self.db.info.id).data[self.query.mode]['group'], ['info.id'])
		self.assertEqual(self.query.SetGroupBy(self.db.info.id, self.db.info.price).data[self.query.mode]['group'], ['info.id', 'info.price'])
		self.assertEqual(self.query.SetGroupBy(self.db.info.id, self.db.info.price, 'info.name').data[self.query.mode]['group'], ['info.id', 'info.price', 'info.name'])
		self.assertEqual(self.query.SetGroupBy(self.db.info.id, self.db.info.price, ' ;,info.name,info.data,,').data[self.query.mode]['group'], ['info.id', 'info.price', 'info.name', 'info.data'])
		
	def test_having(self):
		"Check having expression for group by"
		self.query.Select(self.db.items.id_section).From(self.db.items).Where(self.db.items.id > 20).GroupBy(self.db.items.id_section).Having('sum(items.price) > 100').OrderBy(self.db.items.id_section, 'desc')
		self.assertEqual(self.query.BuildSelect(), 'select items.id_section from items where (items.id > 20) group by items.id_section having sum(items.price) > 100 order by items.id_section desc')
		self.query.SetHaving('max(items.price) > 100')
		self.assertEqual(self.query.BuildSelect(), 'select items.id_section from items where (items.id > 20) group by items.id_section having max(items.price) > 100 order by items.id_section desc')
		
	def test_order(self):
		"Check order by parameters"
		self.query.Select(self.db.items.id_section).From(self.db.items)
		self.assertEqual(self.query.OrderBy(self.db.items.price).data[self.query.mode]['order'], [{'asc': 'items.price'}])
		self.assertEqual(self.query.SetOrderBy(self.db.items.price, 'asc').data[self.query.mode]['order'], [{'asc': 'items.price'}])
		self.assertEqual(self.query.SetOrderBy(self.db.items.price, 'qwe').data[self.query.mode]['order'], [{'asc': 'items.price'}])
		self.assertEqual(self.query.SetOrderBy(self.db.items.price, 'desc').data[self.query.mode]['order'], [{'desc': 'items.price'}])
		self.assertEqual(self.query.SetOrderBy('somecol desc, someother, thirdone desc').data[self.query.mode]['order'], [{'desc': 'somecol'}, {'asc': 'someother'}, {'desc': 'thirdone'}])
		self.assertEqual(self.query.SetOrderBy('some col desc,, someo;ther, thirdone deeesc').data[self.query.mode]['order'], [])
		self.assertEqual(self.query.SetOrderBy(self.db.items.price, 'desc').OrderBy('items.id asc').data[self.query.mode]['order'], [{'desc': 'items.price'}, {'asc': 'items.id'}])
		self.assertEqual(self.query.AddOrderBy(self.db.items.art, 'asc').data[self.query.mode]['order'], [{'desc': 'items.price'}, {'asc': 'items.id'}, {'asc': 'items.art'}])
		
	def test_limit(self):
		"Check limit parameters"
		self.query.Select(self.db.items.id_section).From(self.db.items)
		self.assertEqual(self.query.Limit(50).data[self.query.mode]['limit'], 'limit 50')
		self.assertEqual(self.query.SetLimit(50, 200).data[self.query.mode]['limit'], 'limit 50 offset 200')
		self.assertEqual(self.query.SetLimit(-50, -200).data[self.query.mode]['limit'], 'limit 50 offset 200')
		self.assertEqual(self.query.SetLimit(-50, '200').data[self.query.mode]['limit'], 'limit 50 offset 200')
		self.assertEqual(self.query.SetLimit('50', '200').data[self.query.mode]['limit'], 'limit 50 offset 200')
		self.assertEqual(self.query.SetLimit('50', 'qwe').data[self.query.mode]['limit'], 'limit 50')
		self.assertEqual(self.query.SetLimit('asd', 'qwe').data[self.query.mode]['limit'], '')


	def test_permissions(self):
		"Check wrong order and malformed query parts"
		# Exception: wrong query parts sequence: 'from' can't be after ''
		with self.assertRaises(Exception): self.query.From(self.db.items)
		# Exception: wrong query parts sequence: 'having' can't be after 'from'
		with self.assertRaises(Exception): self.query.Having('max(info.id) > 256')
		# Exception: wrong query parts sequence: 'from' can't be after 'having'
		with self.assertRaises(Exception): self.query.From(self.db.items).Select('')
		# Exception: wrong query parts sequence: 'where' can't be after 'select'
		with self.assertRaises(Exception): self.query.Select().Where(-50 < self.db.info.id)
		# Exception: wrong query parts sequence: 'where+' can't be after 'select'
		with self.assertRaises(Exception): self.query.Select().And('a > b')
		# Exception: wrong query parts sequence: 'join' can't be after 'select'
		with self.assertRaises(Exception): self.query.Select().Join(self.db.checks, self.db.checks.id == self.db.info.id_item)
		# Exception: wrong query parts sequence: 'join' can't be after 'order'
		with self.assertRaises(Exception): self.query.Select().From(self.db.items).OrderBy(self.db.info.id).Join(self.db.checks, self.db.checks.id == self.db.info.id_item)
		# Exception: wrong query parts sequence: 'join' can't be after 'group'
		with self.assertRaises(Exception): self.query.Select().From(self.db.items).GroupBy(self.db.info.id).Join(self.db.checks, self.db.checks.id == self.db.info.id_item)
		# Exception: wrong query parts sequence: 'join' can't be after 'limit'
		with self.assertRaises(Exception): self.query.Select().From(self.db.items).Limit(5).Join(self.db.checks, self.db.checks.id == self.db.info.id_item)
		# Exception: wrong query part
		with self.assertRaises(Exception): self.query.Select().Set(self.db.items)
		
	def test_build(self):
		"Check result builded sql"
		self.assertEqual(self.query.Select().From(self.db.items).Where(self.db.items.id > 20).BuildSelect(), 'select * from items where (items.id > 20)')
		self.assertEqual(self.query.Select(self.db.items.id, distinct=True).From(self.db.items).Where(self.db.items.id > 20).BuildSelect(), 'select distinct items.id from items where (items.id > 20)')
		self.assertEqual(self.query.Insert({'name': 'some', 'description': 'some'}).Into(self.db.brands).BuildInsert(), "insert into brands (name, description) values ('some', 'some')")
		self.assertEqual(self.query.Update(self.db.brands).Set({'name': 'new', 'description': 'new'}).Where(self.db.brands.name != 'old').BuildUpdate(), "update brands set name = 'new', description = 'new' where (brands.name != 'old')")
		self.assertEqual(self.query.Delete(self.db.brands).BuildDelete(), "delete from brands")
		self.assertEqual(self.query.Delete(self.db.brands).Where(self.db.brands.name == 'old').BuildDelete(), "delete from brands where (brands.name = 'old')")
		
	def test_fetch(self):
		"Check builded sql results fetching"
		# Exception: wrong db connection
		with self.assertRaises(Exception): self.query.Select().From(self.db.items).Where(self.db.items.id > 20).FetchFrom()
		with self.assertRaises(Exception): self.query.Select().From(self.db.items).Where(self.db.items.id > 20).FetchFrom(self)
		with self.assertRaises(Exception): self.query.Select().From(self.db.items).Where(self.db.items.id > 20).FetchFrom(12)
		# fetch and make dict
		rows = self.query.Select(self.db.items.id,self.db.items.price).From(self.db.items).Where(self.db.items.id < 20).FetchFrom(self.db)
		self.assertEqual(len(dict((row['id'], row['price']) for row in rows)), 19)
		# get all in an list of dict way
		rows = self.query.Select(self.db.items.id,self.db.items.price).From(self.db.items).Where(self.db.items.id < 200).FetchAllFrom(self.db)
		self.assertEqual(len(rows), 199)

	def test_crud(self):
		"Check sql insert queries"
		tm1 = str(time.time())
		self.query.Insert({'name': tm1, 'description': 'some description'}).Into(self.db.brands)
		self.query.SetInto(self.db.brands)
		self.assertEqual(self.query.BuildInsert(), "insert into brands (name, description) values ('"+tm1+"', 'some description')")
		self.query.InsertTo(self.db)
		self.assertEqual(len(self.query.Select().From(self.db.brands).Where(self.db.brands.name == tm1).FetchAllFrom(self.db)), 1)
		tm2 = str(time.time())
		self.query.Update(self.db.brands).Set({'name': tm2, 'description': 'new description'}).Where(self.db.brands.name == tm1).UpdateIn(self.db)
		self.assertEqual(len(self.query.Select().From(self.db.brands).Where(self.db.brands.name == tm2).FetchAllFrom(self.db)), 1)
		self.query.Delete(self.db.brands).Where(self.db.brands.name == tm2).DeleteFrom(self.db)
		self.assertEqual(len(self.query.Select().From(self.db.brands).Where(self.db.brands.name == tm2).FetchAllFrom(self.db)), 0)

if __name__ == '__main__':
	unittest.main()