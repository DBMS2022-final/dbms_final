from mysql.connector.connection import MySQLConnection

from .cursor import Cursor


class Connection(MySQLConnection):
    def cursor(self, *args, **kwargs):
        return Cursor(self)
