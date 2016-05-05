# coding: utf-8
# zhangxiaoyang.hit#gmail.com
# github.com/zhangxiaoyang

import re
import os

from GstoreConnector import GstoreConnector

class GstoreWrapper:

    def __init__(self, DB, *args, **kwargs):
        self._DB = DB
        self._gc = GstoreConnector(*args, **kwargs)

    def build(self, db_name, rdf_file_path):
        self._db = self._DB(db_name)
        self._db.connect()
        self._db.create(overwrite=True)

        with open(rdf_file_path, 'r') as f:
            for line in f:
                s, p, o = re.split(r'\s*', line.strip('\t\r\n .'))
                self._db.insert(s, p, o)

        self._db.disconnect()

        self._gc.build(db_name, rdf_file_path)

    def query(self, sparql):
        self._db = self._DB(self._db_name)
        self._db.connect()

        spo_text = re.search(r'(?s){(.*?)}', sparql).group(1)
        cleaned_spo_text = spo_text.strip().replace('.<', '. <')
        spo_list = []
        one_spo = []
        for seg in re.split(r'\s*', cleaned_spo_text):
            if seg == '.':
                continue
            one_spo.append(seg.strip('.'))
            if len(one_spo) == 3:
                spo_list.append(one_spo)
                one_spo = []

        array = []
        counter = []
        for s, p, o in spo_list:
            if p.startswith('?'):
                p_list = self._get_possible_p(
                    '*' if s.startswith('?') else s,
                    '*' if o.startswith('?') else o
                )
                array.append((p, p_list))
                counter.append(len(p_list))

        index = [0 for i in range(len(counter))]
        #index_list = []
        while True:
            certain_sparql = sparql
            for i in index:
                p = array[i][0]
                certain_p = array[i][1][index[i]]
                #TODO
                certain_sparql = re.sub(r'%s(?!\w)' % p.replace('?', '\?'), certain_p, certain_sparql)
            
            print certain_sparql
            print '===='
            #change index
            #if index > counter then break
            break

        self._db.disconnect()

        return self._gc.query(sparql)
 
    def load(self, db_name):
        self._db_name = db_name
        self._gc.load(db_name)

    def _get_possible_p(self, s, o):
        return self._db.select(s, o)

    def __getattr__(self, func_name, *args, **kwargs):
        print func_name
        return getattr(self._gc, func_name)


import sqlite3
class SqliteDB:

    def __init__(self, db_name):
        self._db_name = db_name

    def connect(self):
        self._conn = sqlite3.connect(self._db_name)

    def create(self, overwrite=False):
        if os.path.exists(self._db_name) and not overwrite:
            return

        cursor = self._conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS spo')
        cursor.execute('CREATE TABLE spo (s TEXT, p TEXT, o TEXT)')
        cursor.close()
        self._conn.commit()

    def disconnect(self):
        self._conn.close()

    def insert(self, s, p, o):
        cursor = self._conn.cursor()
        cursor.execute('INSERT INTO spo (s, p, o) VALUES ("%s", "%s", "%s")' % (s, p, o))
        cursor.close()
        self._conn.commit()

    def select(self, s, o):
        if s != '*' and o == '*':
            sql = 'SELECT p FROM spo WHERE s="%s"' % s
        elif s == '*' and o != '*':
            sql = 'SELECT p FROM spo WHERE o="%s"' % o
        elif s != '*' and o != '*':
            sql = 'SELECT p FROM spo WHERE s="%s" AND o="%s"' % (s, o)
        else:
            sql = 'SELECT p FROM spo'

        cursor = self._conn.cursor()
        cursor.execute(sql)
        values = cursor.fetchall()
        cursor.close()
        return map(lambda x:x[0], values)

gw = GstoreWrapper(SqliteDB)
#gw.build('test.db', os.path.abspath('test.n3'))
gw.load('test.db')
answer = gw.query('select ?s ?p ?o where {?s ?p ?o.}')
print answer
