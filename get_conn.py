#!/usr/bin/python
# -*- coding:utf-8 -*-
import sqlite3, MySQLdb
class DB():
    def sqlite_connection(self, db_path):
        print db_path
        con         = sqlite3.connect(db_path, timeout=30000)
        cur         = con.cursor()
        return con, cur
    def MySQLdb_connection(self, login_db):
        khost, kpasswd, kuser, kdb = login_db['host'], login_db['password'], login_db['user'], login_db['db']
        conn    = MySQLdb.connect(khost, kuser, kpasswd, kdb)
        cur     = conn.cursor()
        return conn, cur
    
