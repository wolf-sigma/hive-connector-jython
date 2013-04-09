# Copyright 2012 Alex Breshears.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from java.lang import *
from java.sql import *

import simplejson as json
import csv
import StringIO
import config

def runQueryDirectly(query):
	""" Runs a Query via JDBC in Hive and returns a Java result set	"""
	Class.forName(config.HIVE_JDBC_DRIVER_NAME)
	conn_string = config.HIVE_CONNECTION_STRING
	conn = DriverManager.getConnection(conn_string, "", "")
	stmt = conn.createStatement()
	result = stmt.executeQuery(query)
	return result

def getTableContentsSQL(table, columns=None, limit=None):
	""" Generates the SQL required to get full table contents from a Hive Table (most often for results)"""
	if not columns:
		sql = 'SELECT * FROM %s ' % table
	else:
		sql = 'SELECT '
		for c in columns:
			sql += ' %s, ' % c
		sql += 'FROM %s ' % table
	if limit:
		sql += 'LIMIT %s' % limit
	return sql

def getTables():
	""" Returns an array of strings of all tables in Hive """
	toReturn = []
	result = runQueryDirectly('SHOW tables')
	while result.next():
		toReturn.append(result.getString(1))
	return toReturn

def tableExists(table):
	""" Determines if a table exists in Hive """
	return table in getTables()

def getColumnInformation(table, columns=None):
	""" Returns a dictionary describing (selected) columns for a given Hive table """
	toReturn = {}
	if not columns:
		sql = 'SELECT * FROM %s LIMIT 0' %table
	else:
		sql = 'SELECT'
		for c in columns:
			sql += ' %s, ' % c
		sql += 'FROM %s LIMIT 1' %table
	result = runQueryDirectly(sql)
	result.next()
	toReturn['table'] = table
	columns = []
	num_of_columns = result.getMetaData().getColumnCount()
	i = 1
	while i <= num_of_columns:
		column_to_add = {'index': i, 'label': result.getMetaData().getColumnLabel(i),
						 'type': result.getMetaData().getColumnType(i)}
		columns.append(column_to_add)
		i += 1
	toReturn['columns'] = columns
	return toReturn

def getTableDescription(table):
	""" Returns a raw description of a Hive table"""
	sql = 'DESCRIBE %s' % table
	result = runQueryDirectly(sql)

	return result

def getTableContents(table, columns=None, limit=None):
	""" Generates a dictionary with full contents and descriptions for a given Hive table """
	toReturn = {}
	sql = getTableContentsSQL(table, columns, limit)
	result = runQueryDirectly(sql)
	toReturn['table'] = table
	columns = []
	num_of_columns = result.getMetaData().getColumnCount()
	i = 1
	while i <= num_of_columns:
		column_to_add = {'index': i, 'label': result.getMetaData().getColumnLabel(i),
						 'type': result.getMetaData().getColumnType(i)}
		columns.append(column_to_add)
		i += 1
	toReturn['columns'] = columns
	rows = []
	r = 0
	while result.next():
		r += 1
		row_to_add = {}
		for c in columns:
			row_to_add[c['label']] = result.getString(c['index'])
		rows.append(row_to_add)
	toReturn['rows'] = rows
	toReturn['row_count'] = r
	toReturn['column_count'] = num_of_columns
	return toReturn

def getTableContentsForDataTables(table, columns=None, limit=None):
	""" Generates a JSON-formatted string with contents in a format supported for Datatables for a given Hive table.
		See http://datatables.net/examples/data_sources/js_array.html for additional info
	"""
	toReturn = {}
	sql = getTableContentsSQL(table, columns, limit)
	result = runQueryDirectly(sql)
	columns = []
	num_of_columns = result.getMetaData().getColumnCount()
	i = 1
	while i <= num_of_columns:
		column_to_add = {'sTitle': result.getMetaData().getColumnLabel(i)}
		columns.append(column_to_add)
		i += 1
	toReturn['aoColumns'] = columns
	rows = []
	r = 0
	while result.next():
		r += 1
		row_to_add = []
		j = 1
		while j <= num_of_columns:
			row_to_add.append(result.getString(j))
			j += 1
		rows.append(row_to_add)
	toReturn['aaData'] = rows
	return json.dumps(toReturn)

def getTableContentsCSV(table, columns=None, limit=None):
	"""	Generates a CSV-formatted string with contents for a given Hive table. """
	sql = getTableContentsSQL(table, columns, limit)
	result = runQueryDirectly(sql)
	string_thing = StringIO.StringIO()
	csv_writer = csv.writer(string_thing)
	num_of_columns = result.getMetaData().getColumnCount()
	i = 1
	columns = []
	while i <= num_of_columns:
		columns.append(result.getMetaData().getColumnLabel(i))
		i += 1
	csv_writer.writerow(columns)
	while result.next():
		row_to_add = []
		j = 1
		while j <= num_of_columns:
			row_to_add.append(result.getString(j))
			j += 1
		csv_writer.writerow(row_to_add)

	return string_thing.getvalue().strip('\r\n')

def getFunctionList():
	""" Gets an array of functions from Hive. No details about the functions are returned.	"""
	sql = "SHOW functions"
	result = runQueryDirectly(sql)
	functions = []
	while result.next():
		functions.append(result.getString(1))
	return functions

	