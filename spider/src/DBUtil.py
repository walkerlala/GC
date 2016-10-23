#encoding:utf-8

""" Database handler.
    see here for a tutorial: http://zetcode.com/db/mysqlpython/ """

# pylint: disable=superfluous-parens, invalid-name, broad-except

import MySQLdb

class DBHandler:
    """ handle database issue """

    def __init__(self, DBname,  user_name, passwd, mysql_url="127.0.0.1", charset="utf8"):
        """ initialization """
        self.DBname = DBname
        self.user_name = user_name
        self.passwd = passwd
        self.mysql_url = mysql_url
        self.charset = charset
        self.dbcon = None
        self.cur = None

    def connect(self):
        """ connect database """
        try:
            self.dbcon = MySQLdb.connect(
                    host=self.mysql_url,
                    user=self.user_name,
                    passwd=self.passwd,
                    db=self.DBname,
                    charset=self.charset)
            self.cur = self.dbcon.cursor();
        except MySQLdb.Error as e:
            # database not exists, create it
            con = MySQLdb.connect(
                    host=self.mysql_url,
                    user=self.user_name,
                    passwd=self.passwd,
                    charset=self.charset)
            cur = con.cursor()
            if cur.execute("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET 'utf8'" % self.DBname):
                self.dbcon = MySQLdb.connect(
                        host=self.mysql_url,
                        user=self.user_name,
                        passwd=self.passwd,
                        db=self.DBname,
                        charset=self.charset)
                self.cur = self.dbcon.cursor();
            else:
                raise e

    def update(self, sql_string, *args):
        """ modify database """
        try:
            self.cur.execute(sql_string, *args) # parameterized statement
            self.dbcon.commit()
            return self.cur.fetchall()
        except MySQLdb.Error as e:
            if self.dbcon:
                self.dbcon.rollback()
            raise e

    def execute(self, sql_string, *args):
        """ modify database """
        return self.update(sql_string, *args)

    def query(self, sql_string, *args):
        """ query """
        return self.update(sql_string, *args)

    def close(self):
        """ close database connection """
        try:
            self.dbcon.close()
        except:
            raise
