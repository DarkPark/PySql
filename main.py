"""
Possible future improvements:
	- aliases support
	- strict check that "where" fields are from "from" tables
	- create/alter/update tables functions
	- triggers/indexes/view/transactions support
"""

from sql import *

db = DataBase('test.sqlite')
query = SqlBuilder()
query.Select(db.items.id_section, db.items.id_brand)\
	.From(db.items)\
	.Join(db.sections, db.sections.id == db.items.id_section, 'left')\
	.Join(db.info, db.info.id == db.info.id_item, 'inner')\
	.Where((db.items.name != 'test') | (db.info.id > 5) & (db.info.id < 4000) | (db.items.id_src.In(1,2)))\
	.And((db.items.id > 0) & (db.info.id > 0))\
	.Or(db.items.is_new == 0)\
	.GroupBy(db.items.id_section, db.items.id_brand)\
	.Having('sum(items.price) > 10')\
	.OrderBy(db.items.id_brand, 'desc')\
	.Limit(50, 5)
rows = query.FetchFrom(db)
print('Result:', dict((row['id_section'], row['id_brand']) for row in rows))
print('SQL:', query.sql)

id = query.Insert({'name': 'some', 'description': 'some description'})\
	.Into(db.brands)\
	.InsertTo(db)
print('SQL:', query.sql)

query.Update(db.brands)\
	.Set({'name': 'some', 'description': 'new description'})\
	.Where(db.brands.name == 'some')\
	.UpdateIn(db)
print('SQL:', query.sql)

query.Delete(db.brands)\
	.Where(db.brands.name == 'other')\
	.DeleteFrom(db)
print('SQL:', query.sql)