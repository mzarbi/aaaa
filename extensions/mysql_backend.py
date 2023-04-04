import sqlite3

from task_queue.constants import EmptyData
from task_queue.storage import BaseSqlStorage, to_bytes
import mysql.connector

from task_queue.utils import to_timestamp


class MySqlStorage(BaseSqlStorage):
    table_kv = ('''
        CREATE TABLE IF NOT EXISTS kv (
            queue VARCHAR(255) NOT NULL,
            key_ VARCHAR(255) NOT NULL,
            value BLOB NOT NULL,
            PRIMARY KEY (queue, key_)
    );''')
    table_sched = ('''
        CREATE TABLE IF NOT EXISTS schedule (
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            queue VARCHAR(255) NOT NULL,
            data BLOB NOT NULL,
            timestamp DOUBLE NOT NULL
        );    
        ''')
    index_sched = ('''
        CREATE INDEX schedule_queue_timestamp ON schedule (queue, timestamp);
    ''')
    table_task = ('''
        CREATE TABLE IF NOT EXISTS task (
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            queue VARCHAR(255) NOT NULL,
            data BLOB NOT NULL,
            priority DOUBLE NOT NULL DEFAULT 0.0
        );    
    ''')
    index_task = ('''CREATE INDEX task_priority_id ON task (priority DESC, id ASC);''')
    ddl = [table_kv, table_sched, index_sched, table_task, index_task]

    def __init__(self, name='taskqueue', host="localhost", user="root",password="root",database="taskqueue",
                 **kwargs):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self._conn_kwargs = kwargs

        super(MySqlStorage, self).__init__(name)

    def _create_connection(self):
        conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        conn.autocommit = True
        return conn

    def enqueue(self, data, priority=None):
        self.sql('insert into task (queue, data, priority) values (%s, %s, %s)',
                 (self.name, to_bytes(data), priority or 0), commit=True)

    def dequeue(self):
        with self.db(commit=True) as curs:

            curs.execute('SELECT id, data FROM task WHERE queue = %s ORDER BY priority DESC, id ASC LIMIT 1;', (self.name,))
            result = curs.fetchone()
            if result is not None:
                tid, data = result
                curs.execute('delete from task where id = %s', (tid,))
                if curs.rowcount == 1:
                    return to_bytes(data)

    def queue_size(self):
        return self.sql('select count(id) from task where queue=%s',
                        (self.name,), results=True)[0][0]

    def enqueued_items(self, limit=None):
        sql = 'SELECT data FROM task WHERE queue = %s ORDER BY priority DESC, id ASC'
        params = (self.name,)
        if limit is not None:
            sql += ' LIMIT %s'
            params = (self.name, limit)

        return [to_bytes(i) for i, in self.sql(sql, params, results=True)]

    def flush_queue(self):
        self.sql('delete from task where queue=%s', (self.name,), commit=True)

    def add_to_schedule(self, data, ts, utc):
        params = (self.name, to_bytes(data), to_timestamp(ts))
        self.sql('insert into schedule (queue, data, timestamp) '
                 'values (%s, %s, %s)', params, commit=True)

    def read_schedule(self, ts):
        with self.db(commit=True) as curs:
            params = (self.name, to_timestamp(ts))
            curs.execute('select id, data from schedule where '
                         'queue = %s and timestamp <= %s', params)
            id_list, data = [], []
            for task_id, task_data in curs.fetchall():
                id_list.append(task_id)
                data.append(to_bytes(task_data))
            if id_list:
                plist = ','.join('%s' * len(id_list))
                curs.execute('delete from schedule where id IN (%s)' % plist,
                             id_list)
            return data

    def schedule_size(self):
        return self.sql('select count(id) from schedule where queue=%s',
                        (self.name,), results=True)[0][0]

    def scheduled_items(self, limit=None):
        sql = 'select data from schedule where queue=%s order by timestamp'
        params = (self.name,)
        if limit is not None:
            sql += ' limit %s'
            params = (self.name, limit)

        return [to_bytes(i) for i, in self.sql(sql, params, results=True)]

    def flush_schedule(self):
        self.sql('delete from schedule where queue = %s', (self.name,), True)

    def put_data(self, key, value, is_result=False):
        self.sql('INSERT INTO kv (queue, key_, value) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE value = VALUES(value);', (self.name, key, to_bytes(value)), True)

    def peek_data(self, key):
        res = self.sql('select value from kv where queue = %s and key_ = %s',
                       (self.name, key), results=True)
        return to_bytes(res[0][0]) if res else EmptyData

    def pop_data(self, key):
        with self.db(commit=True) as curs:
            curs.execute('select value from kv where queue = %s and key_ = %s',
                         (self.name, key))
            result = curs.fetchone()
            if result is not None:
                curs.execute('delete from kv where queue=%s and key_=%s',
                             (self.name, key))
                if curs.rowcount == 1:
                    return to_bytes(result[0])
            return EmptyData

    def has_data_for_key(self, key):
        return bool(self.sql('select 1 from kv where queue=%s and key_=%s',
                             (self.name, key), results=True))

    def put_if_empty(self, key, value):
        try:
            with self.db(commit=True) as curs:
                curs.execute('INSERT IGNORE INTO kv (queue, key_, value) VALUES (%s, %s, %s);',
                             (self.name, key, to_bytes(value)))
        except sqlite3.IntegrityError:
            return False
        else:
            return True

    def result_store_size(self):
        return self.sql('select count(*) from kv where queue=%s', (self.name,),
                        results=True)[0][0]

    def result_items(self):
        res = self.sql('select key_, value from kv where queue=%s', (self.name,),
                       results=True)
        return dict((k, to_bytes(v)) for k, v in res)

    def flush_results(self):
        self.sql('delete from kv where queue=%s', (self.name,), True)