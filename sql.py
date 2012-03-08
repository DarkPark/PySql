# Copyright (C) 2011 DarkPark, darkpark@pisem.net

"""Sqlite database and query module"""

__author__  = "DarkPark"
__version__ = '0.1.a'

import os
import sqlite3

class dbTable:
	"""DataBase table class"""
	
	def __init__(self, name):
		"""Constructor"""
		self.__name__ = name

class dbTableColumn:
	"""DataBase table column class"""
	
	def __init__(self, name, data, base = False, repr = ''):
		"""Constructor
		*name* is table column name
		*data* is the dict with column info such as table name and type
		*base* is flag showing if the column object directly or its inheritor
		*repr* is the sql part representing the current column or operation result
		"""
		self.__name__ = name
		self.__data__ = data
		self.__base__ = base
		self.__repr__ = repr
		if base:
			self.__repr__ = '%s.%s' % (self.__data__['table'], self.__name__)
	
	def __get__(self, param):
		"""Returns the sql representation of the object or the given value otherwise
		*param* is mixed, can be object or simple int/str type
		"""
		if isinstance(param, dbTableColumn):
			if param.__repr__:
				return param.__repr__
		elif isinstance(param, str):
			return "'%s'" % param
		elif type(param).__name__ in ['tuple', 'list', 'set', 'dict']:
			list = []
			for item in param:
				if isinstance(item, str):
					list.append("'%s'" % item)
				elif isinstance(item, int):
					list.append(str(item))
			return '(%s)' % ', '.join(list)
		else:
			return param
	
	def __get_type__(self, param):
		"""Returns the param type adopting db types to python
		*param* is mixed, can be object or simple int/str type
		"""
		if isinstance(param, dbTableColumn):
			if param.__data__['type'] in ['INTEGER']:
				return 'int'
			elif param.__data__['type'] in ['VARCHAR', 'TEXT']:
				return 'str'
			elif param.__data__['type'] in ['REAL']:
				return 'float'
			else:
				return 'dbTableColumn'
		else:
			return type(param).__name__

	def __type_check__(self, first, second):
		"""Raise the exception in case operand type mismatch
		*first* is the first parameter of comparison
		*second* is the second parameter of comparison
		"""
		if self.__get_type__(first) != self.__get_type__(second):
			raise TypeError('%s != %s' % (self.__get_type__(first), self.__get_type__(second)))
	
	def __eq__(self, other):
		"""Operator ==
		*other* is the second parameter of operation
		Returns the object itself
		"""
		self.__type_check__(self, other)
		repr = '(%s = %s)' % (self.__get__(self), self.__get__(other))
		return dbTableColumn(self.__name__, self.__data__, False, repr)
	
	def __ne__(self, other):
		"""Operator !=
		*other* is the second parameter of operation
		Returns the object itself
		"""
		self.__type_check__(self, other)
		repr = '(%s != %s)' % (self.__get__(self), self.__get__(other))
		return dbTableColumn(self.__name__, self.__data__, False, repr)

	def __and__(self, other):
		"""Operator &
		*other* is the second parameter of operation
		Returns the object itself
		"""
		repr = '(%s and %s)' % (self.__get__(self), self.__get__(other))
		return dbTableColumn(self.__name__, self.__data__, False, repr)
	
	def __or__(self, other):
		"""Operator |
		*other* is the second parameter of operation
		Returns the object itself
		"""
		repr = '(%s or %s)' % (self.__get__(self), self.__get__(other))
		return dbTableColumn(self.__name__, self.__data__, False, repr)

	def __lt__(self, other):
		"""Operator <
		*other* is the second parameter of operation
		Returns the object itself
		"""
		self.__type_check__(self, other)
		repr = '(%s < %s)' % (self.__get__(self), self.__get__(other))
		return dbTableColumn(self.__name__, self.__data__, False, repr)

	def __gt__(self, other):
		"""Operator >
		*other* is the second parameter of operation
		Returns the object itself
		"""
		self.__type_check__(self, other)
		repr = '(%s > %s)' % (self.__get__(self), self.__get__(other))
		return dbTableColumn(self.__name__, self.__data__, False, repr)

	def In(self, *arguments):
		"""Operator in
		*arguments* is the second parameter of operation, can be list of values
		Returns the object itself
		"""
		if len(arguments) == 1:
			repr = '(%s in %s)' % (self.__get__(self), self.__get__(arguments[0]))
		elif len(arguments) > 1:
			list = []
			for arg in arguments:
				list.append(str(self.__get__(arg)))
			repr = '(%s in (%s))' % (self.__get__(self), ', '.join(list))
		return dbTableColumn(self.__name__, self.__data__, False, repr)
	
	def NotIn(self, *arguments):
		"""Operator not in
		*arguments* is the second parameter of operation, can be list of values
		Returns the object itself
		"""
		if len(arguments) == 1:
			repr = '(%s not in %s)' % (self.__get__(self), self.__get__(arguments[0]))
		elif len(arguments) > 1:
			list = []
			for arg in arguments:
				list.append(str(self.__get__(arg)))
			repr = '(%s not in (%s))' % (self.__get__(self), ', '.join(list))
		return dbTableColumn(self.__name__, self.__data__, False, repr)

class DataBase:
	"""DB wrapper"""
	
	"Database file name; in memory mode if empty"
	__dbfile__ = None
	
	"Connection handler"
	__conn__ = None
	
	def __init__(self, dbfile = None):
		"""Constructor
		*dbfile* is a file name of the sqlite db
		"""
		if dbfile and os.path.isfile(dbfile):
			# check input data and choose connection type
			self.__dbfile__ = dbfile
			self.__conn__ = sqlite3.connect(self.__dbfile__)
			self.__struct__()
		else:
			self.__conn__ = sqlite3.connect(':memory:')

	def __del__(self):
		"""Destructor to close connection if necessary"""
		if self.__conn__:
			self.__conn__.close()
	
	def __struct__(self):
		"""Investigate the db, collecting tables and columns data"""
		cur = self.__conn__.cursor()
		# get all tables
		cur.execute('select tbl_name from sqlite_master')
		struct = []
		for row in cur:
			struct.append(row[0])
		for tbl in struct:
			# iterate all tables and get columns with details for each
			cur.execute('pragma table_info(%s)' % tbl)
			# add table as class
			tbl_class = dbTable(tbl)
			setattr(self, tbl, tbl_class)
			for row in cur:
				# add columns for tables
				setattr(tbl_class, row[1], dbTableColumn(row[1], {'table':tbl, 'type':row[2], 'pk':row[5]}, True))
		cur.close()

class SqlBuilder:
	"""Query builder"""
	
	"Last executed query sql"
	sql = ''
	
	"Query mode - {select|insert|update|delete}"
	mode = ''
	
	"""Sql building parameters with flags and check rules
	Rules - shows after which parts the current one is allowed
	"""
	data = {
		'select' : {
			'select'  : [],
			'distinct': False,
			'from'    : [],
			'join'    : [],
			'where'   : '',
			'group'   : [],
			'having'  : '',
			'order'   : [],
			'limit'   : '',
			'rules'   : {
				'select' : [''],
				'from'   : ['select',],
				'join'   : ['from', 'join'],
				'where'  : ['from', 'join'],
				'where+' : ['where', 'where+'],
				'group'  : ['where', 'where+', 'from', 'join'],
				'having' : ['group'],
				'order'  : ['from', 'join', 'where', 'where+', 'group', 'having', 'order'],
				'limit'  : ['from', 'join', 'where', 'where+', 'group', 'having', 'order'],
			}
		},
		'insert' : {
			'id'    : None,
			'data'  : {},
			'into'  : '',
			'rules' : {
				'insert' : [''],
				'into'   : ['insert']
			}
		},
		'update' : {
			'table' : '',
			'data'  : {},
			'where' : '',
			'rules' : {
				'update' : [''],
				'set'    : ['update'],
				'where'  : ['set']
			}
		},
		'delete' : {
			'table' : '',
			'where' : '',
			'rules' : {
				'delete' : [''],
				'where'  : ['delete']
			}
		}
	}

	"Markers for parts orderding managing"
	marker_curr = ''
	marker_prev = ''
	
	def __init__(self):
		"""Constructor"""
		pass
	
	def __del__(self):
		"""Destructor to free resources"""
		pass
	
	def Reset(self):
		# set markers
		self.marker_curr = ''
		self.marker_prev = ''
		# set params for specific data parts
		if self.mode == 'select':
			self.data[self.mode].update({'select': [], 'distinct': False, 'from': [], 'join': [], 'where': '', 'group': [], 'having': '', 'order': [], 'limit': ''})
		elif self.mode == 'insert':
			self.data[self.mode].update({'id': None, 'data': {}, 'into': ''})
		elif self.mode == 'update':
			self.data[self.mode].update({'table': '', 'data': {}, 'where': ''})
		elif self.mode == 'delete':
			self.data[self.mode].update({'table': '', 'where': ''})

	def GetInt(self, param):
		"""Get the int value from param
		*param* is mixed, int expected, other types will be converted
		Returns int value or None on error
		"""
		if param:
			if isinstance(param, int):
				return param
			elif isinstance(param, str):
				try:
					return int(param)
				except ValueError:
					return None
	
	def SetCurrMarker(self, name, skip_check = False):
		"""Set current marker name and check permission on execute current part
		*name* is a marker name - {select|from|join|where|group|having|order|limit}
		*skip_check* is a flag showing if the parts sequence should be skipped
		"""
		self.marker_prev = self.marker_curr
		self.marker_curr = name
		
		if self.mode and self.mode in self.data:
			if not skip_check:
				if self.marker_curr and self.marker_prev in self.data[self.mode]['rules']:
					if self.marker_curr in self.data[self.mode]['rules']:
						if self.marker_prev not in self.data[self.mode]['rules'][self.marker_curr]:
							raise Exception("wrong query parts sequence: '%s' can't be after '%s'" % (self.marker_curr, self.marker_prev))
					else:
						raise Exception('wrong query part')
		else:
			raise Exception('inconsistent query')
		
	def SetCurrMode(self, name):
		"""Set query mode which means start of the query
		Set marker and reseting all data
		*name* is one of {select|insert|update|delete}
		"""
		name = name.lower().strip()
		if name in ['select', 'insert', 'update', 'delete']:
			self.mode = name
			self.Reset()
			self.SetCurrMarker(self.mode)
	
	def SetSelect(self, *arguments, **additional):
		"""Set select fields
		*arguments* is a list of select fields
		*additional* is a distinct flag
		Returns the object itself
		"""
		self.data[self.mode]['select'] = []
		if arguments:
			for arg in arguments:
				# table class -> "tblname.*"
				if isinstance(arg, dbTable):
					self.data[self.mode]['select'].append('%s.*' % arg.__name__)
				# table column -> "tblname.colname"
				if isinstance(arg, dbTableColumn):
					self.data[self.mode]['select'].append('%s.%s' % (arg.__data__['table'], arg.__name__))
				# simple non empty string
				if isinstance(arg, str) and arg:
					arg_list = arg.split(',') # split string and analyze
					for arg_part in arg_list:
						# check if string invalid
						arg_part = arg_part.strip()
						if arg_part:
							# string is not empty and not suspicious
							if (not ' ' in arg_part) and (not ';' in arg_part):
								self.data[self.mode]['select'].append(arg_part)
			# add distinct flag if necessary
			if self.data[self.mode]['select'] and additional and additional['distinct']:
				self.data[self.mode]['distinct'] = True
		return self
	
	def Select(self, *arguments, **additional):
		"""Set select fields wrapper with permission checks
		*arguments* is a list of select fields
		*additional* is a distinct flag
		Returns the object itself
		"""
		self.SetCurrMode('select')
		# exec cur part
		return self.SetSelect(*arguments, **additional)
	
	def SetFrom(self, *arguments):
		"""Set tables for query
		*arguments* is a list of from fields
		Returns the object itself
		"""
		self.data[self.mode]['from'] = []
		if arguments:
			for arg in arguments:
				# table class -> "tblname"
				if isinstance(arg, dbTable):
					self.data[self.mode]['from'].append(arg.__name__)
				# simple non empty string
				if isinstance(arg, str) and arg:
					arg_list = arg.split(',') # split string and analyze
					for arg_part in arg_list:
						# check if string invalid
						arg_part = arg_part.strip()
						if arg_part:
							# string is not empty and not suspicious
							if (not ' ' in arg_part) and (not ';' in arg_part):
								self.data[self.mode]['from'].append(arg_part)
		return self
	
	def From(self, *arguments):
		"""Set tables for query wrapper with permission checks
		*arguments* is a list of from fields
		Returns the object itself
		"""
		self.SetCurrMarker('from')
		# exec cur part
		return self.SetFrom(*arguments)
	
	def SetJoin(self, table, condition, mode = ''):
		"""Set join table with condition reseting previous joins
		*table* is a table to join
		*condition* is a join condition
		*mode* can be {INNER | {LEFT | RIGHT | FULL} OUTER | CROSS }
		Returns the object itself
		"""
		self.data[self.mode]['join'] = []
		return self.AddJoin(table, condition, mode)
	
	def AddJoin(self, table, condition, mode = ''):
		"""Add new join table with condition to existing or add the first one
		*table* is a table to join
		*condition* is a join condition
		*mode* can be {INNER | {LEFT | RIGHT | FULL} OUTER | CROSS }
		Returns the object itself
		"""
		if table:
			# table class -> "tblname"
			if isinstance(table, dbTable):
				table = table.__name__
			if condition:
				condition = condition.__repr__
			if table and condition:
				if mode != '':
					mode = mode.lower().strip()
					if not mode in ['inner', 'left outer', 'right outer', 'full outer', 'left', 'right', 'full', 'cross']:
						mode = ''
				self.data[self.mode]['join'].append({'table': table, 'cond': condition, 'mode': mode})
		return self
	
	def Join(self, table, condition, mode = ''):
		"""Set join table with condition wrapper with permission checks
		*table* is a table to join
		*condition* is a join condition
		*mode* can be {INNER | {LEFT | RIGHT | FULL} OUTER | CROSS }
		Returns the object itself
		"""
		self.SetCurrMarker('join')
		# exec cur part
		return self.AddJoin(table, condition, mode)

	def SetWhere(self, condition):
		"""Set conditions for select/update query
		*condition* is a list of where fields
		Returns the object itself
		"""
		where = ''
		if condition:
			if isinstance(condition, dbTableColumn):
				if condition.__repr__:
					where = condition.__repr__
					condition.__repr__ = ''
			if isinstance(condition, str):
				condition = condition.strip()
				if condition:
					where = condition
		self.data[self.mode]['where'] = where
		return self
	
	def Where(self, condition):
		"""Set conditions for select/update query wrapper with permission checks
		*condition* is a list of where fields
		Returns the object itself
		"""
		self.SetCurrMarker('where')
		# exec cur part
		return self.SetWhere(condition)
	
	def And(self, condition):
		"""Additional conditions to where
		*condition* is a list of where fields
		Returns the object itself
		"""
		self.SetCurrMarker('where+')
		where = self.data[self.mode]['where']
		self.SetWhere(condition)
		if self.data[self.mode]['where']:
			self.data[self.mode]['where'] = '%s and %s' % (where, self.data[self.mode]['where'])
		return self
	
	def Or(self, condition):
		"""Additional conditions to where
		*condition* is a list of where fields
		Returns the object itself
		"""
		self.SetCurrMarker('where+')
		where = self.data[self.mode]['where']
		self.SetWhere(condition)
		if self.data[self.mode]['where']:
			self.data[self.mode]['where'] = '%s or %s' % (where, self.data[self.mode]['where'])
		return self
	
	def SetGroupBy(self, *arguments):
		"""Set group by fields for query
		*arguments* is a list of group fields
		Returns the object itself
		"""
		self.data[self.mode]['group'] = []
		if arguments:
			for arg in arguments:
				# table column -> "tblname.colname"
				if isinstance(arg, dbTableColumn):
					self.data[self.mode]['group'].append('%s.%s' % (arg.__data__['table'], arg.__name__))
				# simple non empty string
				if isinstance(arg, str) and arg:
					arg_list = arg.split(',') # split string and analyze
					for arg_part in arg_list:
						# check if string invalid
						arg_part = arg_part.strip()
						if arg_part:
							# string is not empty and not suspicious
							if (not ' ' in arg_part) and (not ';' in arg_part):
								self.data[self.mode]['group'].append(arg_part)
		return self
	
	def GroupBy(self, *arguments):
		"""Set group by fields for query wrapper with permission checks
		*arguments* is a list of group fields
		Returns the object itself
		"""
		self.SetCurrMarker('group')
		# exec cur part
		return self.SetGroupBy(*arguments)

	def SetHaving(self, expression):
		"""Set having expression for group by query part
		*expression* is a free form expression string
		Returns the object itself
		"""
		self.data[self.mode]['having'] = ''
		if not self.data[self.mode]['group']:
			raise Exception('group by is not defined')
		if expression and isinstance(expression, str):
			expression = expression.strip()
			if expression and not ';' in expression:
				self.data[self.mode]['having'] = expression
		return self
	
	def Having(self, expression):
		"""Set having expression for group by query part wrapper with permission checks
		*expression* is a free form expression string
		Returns the object itself
		"""
		self.SetCurrMarker('having')
		# exec cur part
		return self.SetHaving(expression)

	def SetOrderBy(self, field, direction = 'asc'):
		"""Set order by fields for query, reset list
		*field* is a field for ordering
		*direction* is a ordering direction - asc|desc
		Returns the object itself
		"""
		self.data[self.mode]['order'] = []
		# exec cur part
		return self.AddOrderBy(field, direction)
	
	def AddOrderBy(self, field, direction = 'asc'):
		"""Set order by fields for query, add to the end
		*field* is a field for ordering
		*direction* is a ordering direction - asc|desc
		Returns the object itself
		"""
		# check and rework direction if necessary
		direction = direction.lower().strip()
		if direction not in ['asc', 'desc']:
			direction = 'asc'
		if field:
			# table column -> "tblname.colname"
			if isinstance(field, dbTableColumn):
				self.data[self.mode]['order'].append({direction: '%s.%s' % (field.__data__['table'], field.__name__)})
			# simple non empty string
			if isinstance(field, str):
				arg_list = field.split(',') # split string and analyze
				for arg_part in arg_list:
					# check if string invalid
					arg_part = arg_part.strip()
					if arg_part:
						# string is not empty and not suspicious
						if not ';' in arg_part:
							arg_part = arg_part.split(' ')
							if len(arg_part) == 1:  # just col name
								self.data[self.mode]['order'].append({'asc' : arg_part[0]})
							if len(arg_part) == 2:  # col name with order
								arg_part[1] = arg_part[1].lower().strip()
								if arg_part[1] and arg_part[1] in ['asc', 'desc']:
									self.data[self.mode]['order'].append({arg_part[1] : arg_part[0]})
		return self

	def OrderBy(self, field, direction = 'asc'):
		"""Set order by fields for query wrapper with permission checks
		*field* is a field for ordering
		*direction* is a ordering direction - asc|desc
		Returns the object itself
		"""
		self.SetCurrMarker('order')
		# exec cur part
		return self.AddOrderBy(field, direction)

	def SetLimit(self, restriction, offset = None):
		"""Set limit statement
		*restriction* is the limit integer
		*offset* is the offset integer if set
		"""
		self.data[self.mode]['limit'] = ''
		restriction = self.GetInt(restriction)
		offset = self.GetInt(offset)
		if restriction and isinstance(restriction, int):
			self.data[self.mode]['limit'] = 'limit %s' % abs(restriction)
			if offset and isinstance(offset, int):
				self.data[self.mode]['limit'] = '%s offset %s' % (self.data[self.mode]['limit'], abs(offset))
		return self
	
	def Limit(self, restriction, offset = None):
		"""Set limit statement wrapper with permission checks
		*restriction* is the limit integer
		*offset* is the offset integer if set
		"""
		self.SetCurrMarker('limit')
		# exec cur part
		return self.SetLimit(restriction, offset)
	
	def BuildSelect(self):
		"""Conbines all the parts of query
		Returns resutl sql string
		"""
		query  = ''
		select = '*'
		if self.data[self.mode]['select']:
			select = ', '.join(self.data[self.mode]['select'])
		if self.data[self.mode]['from']:
			if self.data[self.mode]['distinct']:
				query = 'select distinct %s from %s' % (select, ', '.join(self.data[self.mode]['from']))
			else:
				query = 'select %s from %s' % (select, ', '.join(self.data[self.mode]['from']))
			if self.data[self.mode]['join']:
				join = []
				for jpart in self.data[self.mode]['join']:
					join.append(('%s join %s on %s' % (jpart['mode'], jpart['table'], jpart['cond'])).strip())
				query = '%s %s' % (query, ' '.join(join))
			if self.data[self.mode]['where']:
				query = '%s where %s' % (query, self.data[self.mode]['where'])
			if self.data[self.mode]['group']:
				query = '%s group by %s' % (query, ', '.join(self.data[self.mode]['group']))
			if self.data[self.mode]['having']:
				query = '%s having %s' % (query, self.data[self.mode]['having'])
			if self.data[self.mode]['order']:
				for order in self.data[self.mode]['order']:
					if order:
						list = []
						for key in order.keys():
							list.append('%s %s' % (order[key], key))
							query = '%s order by %s' % (query, ', '.join(list))
			return query
		else:
			raise Exception('wrong query parameters')
	
	def FetchFrom(self, db):
		"""Get records from selected database
		*db* is a DataBase object with opened connection
		Returns pointer to query result
		"""
		if db and isinstance(db, DataBase) and isinstance(db.__conn__, sqlite3.Connection):
			self.sql = self.BuildSelect()
			if self.sql:
				db.__conn__.row_factory = sqlite3.Row
				cur = db.__conn__.cursor()
				cur.execute(self.sql)
				return cur
		else:
			raise Exception('wrong db connection')
		
	def FetchAllFrom(self, db):
		"""Get all records from selected database
		*db* is a DataBase object with opened connection
		Returns list of dict records result
		"""
		# get rows pointer
		cur = self.FetchFrom(db)
		if cur and isinstance(cur, sqlite3.Cursor):
			list = []
			# iterate and fill list with dicts
			for row in cur:
				dict = {}
				for key in row.keys():
					dict[key] = row[key]
				list.append(dict)
			return list
			
	def SetInsert(self, data):
		"""Set data for insert
		*data* list of fields in dict form
		"""
		if data and isinstance(data, dict):
			self.data[self.mode]['data'] = data
		return self
	
	def Insert(self, data):
		"""Set data for insert wrapper with permission checks
		*data* list of fields in dict form
		"""
		self.SetCurrMode('insert')
		# exec cur part
		return self.SetInsert(data)
	
	def SetInto(self, table):
		"""Set table for insert
		*table* is a table name
		"""
		self.data[self.mode]['into'] = ''
		if table:
			# table class -> "tblname"
			if isinstance(table, dbTable):
				self.data[self.mode]['into'] = table.__name__
			# simple non empty string
			if isinstance(table, str):
				table = table.strip()
				if table:
					# string is not empty and not suspicious
					if (not ' ' in table) and (not ';' in table):
						self.data[self.mode]['into'] = table
		return self
	
	def Into(self, table):
		"""Set table for insert wrapper with permission checks
		*table* is a table name
		"""
		self.SetCurrMarker('into')
		# exec cur part
		return self.SetInto(table)
	
	def BuildInsert(self):
		"""Conbines all the parts of query
		Returns resutl sql string
		"""
		fields = []
		values = []
		for key in self.data[self.mode]['data'].keys():
			fields.append(key)
			if isinstance(self.data[self.mode]['data'][key], str):
				values.append("'%s'" % self.data[self.mode]['data'][key])
			elif isinstance(self.data[self.mode]['data'][key], int):
				values.append(str(self.data[self.mode]['data'][key]))
		if fields and values and self.data[self.mode]['into']:
			return 'insert into %s (%s) values (%s)' % (self.data[self.mode]['into'], ', '.join(fields), ', '.join(values))
	
	def InsertTo(self, db):
		"""Insert prepared sql to the selected database
		*db* is a DataBase object with opened connection
		Returns pointer to query result
		"""
		if db and isinstance(db, DataBase) and isinstance(db.__conn__, sqlite3.Connection):
			self.sql = self.BuildInsert()
			if self.sql:
				cur = db.__conn__.cursor()
				cur.execute(self.sql)
				self.data[self.mode]['id'] = cur.lastrowid
				db.__conn__.commit()
				cur.close()
				return self.data[self.mode]['id']
		else:
			raise Exception('wrong db connection')
		
	def SetUpdate(self, table):
		"""Set table for update
		*table* is a table name
		"""
		self.data[self.mode]['table'] = ''
		if table:
			# table class -> "tblname"
			if isinstance(table, dbTable):
				self.data[self.mode]['table'] = table.__name__
			# simple non empty string
			if isinstance(table, str):
				table = table.strip()
				if table:
					# string is not empty and not suspicious
					if (not ' ' in table) and (not ';' in table):
						self.data[self.mode]['table'] = table
		return self

	def Update(self, table):
		"""Set table for update wrapper with permission checks
		*table* is a table name
		"""
		self.SetCurrMode('update')
		# exec cur part
		return self.SetUpdate(table)
	
	def SetSet(self, data):
		"""Set data for update
		*data* list of fields in dict form
		"""
		if data and isinstance(data, dict):
			self.data[self.mode]['data'] = data
		return self
	
	def Set(self, data):
		"""Set data for update wrapper with permission checks
		*data* list of fields in dict form
		"""
		self.SetCurrMarker('set')
		return self.SetSet(data)
	
	def BuildUpdate(self):
		"""Conbines all the parts of query
		Returns resutl sql string
		"""
		values = []
		for key in self.data[self.mode]['data'].keys():
			if isinstance(self.data[self.mode]['data'][key], str):
				values.append("%s = '%s'" % (key, self.data[self.mode]['data'][key]))
			elif isinstance(self.data[self.mode]['data'][key], int):
				values.append("%s = %s" % (key, self.data[self.mode]['data'][key]))
		if values and values and self.data[self.mode]['table']:
			query = 'update %s set %s' % (self.data[self.mode]['table'], ', '.join(values))
			if self.data[self.mode]['where']:
				query = '%s where %s' % (query, self.data[self.mode]['where'])
			return query
	
	def UpdateIn(self, db):
		"""Updates prepared sql to the selected database
		*db* is a DataBase object with opened connection
		Returns pointer to query result
		"""
		if db and isinstance(db, DataBase) and isinstance(db.__conn__, sqlite3.Connection):
			self.sql = self.BuildUpdate()
			if self.sql:
				cur = db.__conn__.cursor()
				cur.execute(self.sql)
				db.__conn__.commit()
				cur.close()
		else:
			raise Exception('wrong db connection')
		
	def SetDelete(self, table):
		"""Set table for delete
		*table* is a table name
		"""
		self.data[self.mode]['table'] = ''
		if table:
			# table class -> "tblname"
			if isinstance(table, dbTable):
				self.data[self.mode]['table'] = table.__name__
			# simple non empty string
			if isinstance(table, str):
				table = table.strip()
				if table:
					# string is not empty and not suspicious
					if (not ' ' in table) and (not ';' in table):
						self.data[self.mode]['table'] = table
		return self

	def Delete(self, table):
		"""Set table for delete wrapper with permission checks
		*table* is a table name
		"""
		self.SetCurrMode('delete')
		# exec cur part
		return self.SetDelete(table)

	def BuildDelete(self):
		"""Conbines all the parts of query
		Returns resutl sql string
		"""
		if self.data[self.mode]['table']:
			query = 'delete from %s' % self.data[self.mode]['table']
			if self.data[self.mode]['where']:
				query = '%s where %s' % (query, self.data[self.mode]['where'])
			return query
	
	def DeleteFrom(self, db):
		"""Delete using prepared sql from the selected database
		*db* is a DataBase object with opened connection
		Returns pointer to query result
		"""
		if db and isinstance(db, DataBase) and isinstance(db.__conn__, sqlite3.Connection):
			self.sql = self.BuildDelete()
			if self.sql:
				cur = db.__conn__.cursor()
				cur.execute(self.sql)
				db.__conn__.commit()
				cur.close()
		else:
			raise Exception('wrong db connection')