# Copyright (C) 2021 Armando Neto <code@armandoneto.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import json
import sqlite3


class Database:
    """Simple dict-like (key/value) database interface powered by SQLite3 and json.

    Create a database
    >>> db = Database(path='test_database.sqlite3')

    Work with strings
    >>> db['a'] = 'letter A'
    >>> db['a']
    'letter A'

    Work with dicts
    >>> db['d'] = {'key': 'value'}
    >>> db['d']
    {'key': 'value'}
    >>> type(db['d'])
    <class 'dict'>

    Get all keys
    >>> db.keys()
    ['a', 'd']
    >>> len(db)
    2

    Items can be deleted
    >>> del db['d']
    >>> del db['a']
    >>> db['a']
    Traceback (most recent call last):
        ...
    KeyError: 'a'
    >>> db.keys()
    []
    >>> len(db)
    0
    """

    def __init__(self, path, key_lenght=1000):
        self.path = path
        self._create(key_lenght=key_lenght)

    def _connect(self):
        return sqlite3.connect(self.path)

    def _create(self, key_lenght):
        connection = self._connect()
        cursor = connection.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS kv_table"
            "(key varchar(%s) PRIMARY KEY, value JSON)"
            % key_lenght
        )

    def __getitem__(self, key):
        with self._connect() as connection:
            cursor = connection.execute(
                "SELECT value FROM kv_table WHERE key=? LIMIT 1", (key,))
            value = cursor.fetchone()
            if value is not None:
                value = value[0]
                return json.loads(value)
            raise KeyError(key)

    def __setitem__(self, key, value):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO kv_table VALUES(?, json(?))"
                "ON CONFLICT(key) DO UPDATE set value = json(?)",
                (key, json.dumps(value), json.dumps(value)),
            )

    def __delitem__(self, key):
        with self._connect() as connection:
            connection.execute("DELETE FROM kv_table WHERE key=?", (key))

    def __len__(self):
        with self._connect() as connection:
            cursor = connection.execute("SELECT count(*) FROM kv_table")
            value = cursor.fetchone()
            return value[0]

    def keys(self):
        with self._connect() as connection:
            cursor = connection.execute("SELECT key FROM kv_table")
            values = cursor.fetchall()
            return [v[0] for v in values]
