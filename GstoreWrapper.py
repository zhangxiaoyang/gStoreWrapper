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
        self._db_name = db_name
        self._db = self._DB(self._db_name)
        self._db.connect()
        self._db.create(overwrite=True)

        with open(rdf_file_path, 'r') as f:
            for line in f:
                s, p, o = re.split(r'\t*', line.strip('\t\r\n .'))
                self._db.insert(*map(lambda x:re.sub('["\']', ' ', x), (s, p, o)))

        self._db.disconnect()

        return self._gc.build(self._db_name, rdf_file_path)

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
        limit = []
        for s, p, o in spo_list:
            if p.startswith('?'):
                p_list = self._get_possible_p(
                    '*' if s.startswith('?') else s,
                    '*' if o.startswith('?') else o
                )
                array.append((p, p_list))
                limit.append(len(p_list))

        result_list = []
        c = Counter(limit)
        for index in c:
            certain_sparql = sparql
            for i in index:
                p = array[i][0]
                certain_p = array[i][1][index[i]]
                tmp = re.sub(r'%s(?!\w)' % p.replace('?', '\?'), certain_p, certain_sparql)
                certain_sparql = re.sub(r' [^\?]\S*(?= .*?where\s?{)', '', tmp)
            
            result = self._gc.query(certain_sparql)
            line_list = []
            for line_index, line in enumerate(result.split('\n')[1:]):
                if not line:
                    continue

                new_line = line
                for i in index:
                    if line_index == 0:
                        p = array[i][0]
                        new_line = '\t'.join([new_line, p])
                    else:
                        certain_p = array[i][1][index[i]]
                        new_line = '\t'.join([new_line, certain_p])
                line_list.append(new_line)
            new_result = '\n'.join(line_list)
            result_list.append(new_result)

        self._db.disconnect()

        return self._merge_result(result_list)
 
    def load(self, db_name):
        self._db_name = db_name
        return self._gc.load(self._db_name)

    def _get_possible_p(self, s, o):
        return list(set(self._db.select(s, o)))

    def _merge_result(self, result_list):
        normal_header = None
        records = [] 
        for result in result_list:
            if not result:
                continue

            header = filter(lambda x: x, result.split('\n')[0].split('\t'))
            if not normal_header:
                normal_header = header

            items = [None for _ in normal_header]
            for line in result.split('\n')[1:]:
                for i, value in enumerate(line.split('\t')):
                    items[normal_header.index(header[i])] = value
            records.append(items)
        return {
            'keys': normal_header,
            'records': records
        }

    def __getattr__(self, func_name, *args, **kwargs):
        return getattr(self._gc, func_name)


class Counter:

    def __init__(self, limit):
        self._limit = limit
        self._value = [0 for i in self._limit]

    def __iter__(self):
        while True:
            yield self._value
            if not self._increment():
                raise StopIteration

    def _increment(self):
        value_length = len(self._value)
        carry = 1
        for i in range(value_length):
            v = self._value[value_length - i - 1]
            max_v = self._limit[value_length - i - 1]
            if v + carry >= max_v:
                self._value[value_length - i - 1] = 0
                carry = 1
            else:
                self._value[value_length - i - 1] += carry
                return True

        if carry:
            return False
        return True


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

if __name__ == '__main__':
    gw = GstoreWrapper(SqliteDB)
    gw.build('test.db', os.path.abspath('test.n3'))
    #sparql = 'select ?s1 ?p1 ?o1 ?s2 ?p2 ?o2 where {?s1 ?p1 ?o1. ?s2 ?p2 ?o2.}'
    sparql = 'select ?s ?o where {?s <name> ?o}'
    answer = gw.query(sparql)
    print answer
