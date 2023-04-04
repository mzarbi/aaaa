import contextlib
import hashlib
import heapq
import itertools
import os
import shutil
try:
    import sqlite3
except ImportError:
    sqlite3 = None
import struct
import threading
import time


from task_queue.constants import EmptyData
from task_queue.utils import FileLock
from task_queue.utils import text_type
from task_queue.utils import to_timestamp


class BaseStorage(object):
    """
    Base storage-layer interface. Subclasses should implement all methods.
    """
    blocking = False  # Does dequeue() block until ready, or should we poll?
    priority = True

    def __init__(self, name='taskqueue', **storage_kwargs):
        self.name = name

    def close(self):
        """
        Close or release any objects/handles used by storage layer.

        :returns: (optional) boolean indicating success
        """
        pass

    def enqueue(self, data, priority=None):
        """
        Given an opaque chunk of data, add it to the queue.

        :param bytes data: Task data.
        :param float priority: Priority, higher priorities processed first.
        :return: No return value.

        Some storage may not implement support for priority. In that case, the
        storage may raise a NotImplementedError for non-None priority values.
        """
        raise NotImplementedError

    def dequeue(self):
        """
        Atomically remove data from the queue. If no data is available, no data
        is returned.

        :return: Opaque binary task data or None if queue is empty.
        """
        raise NotImplementedError

    def queue_size(self):
        """
        Return the length of the queue.

        :return: Number of tasks.
        """
        raise NotImplementedError

    def enqueued_items(self, limit=None):
        """
        Non-destructively read the given number of tasks from the queue. If no
        limit is specified, all tasks will be read.

        :param int limit: Restrict the number of tasks returned.
        :return: A list containing opaque binary task data.
        """
        raise NotImplementedError

    def flush_queue(self):
        """
        Remove all data from the queue.

        :return: No return value.
        """
        raise NotImplementedError

    def add_to_schedule(self, data, ts, utc):
        """
        Add the given task data to the schedule, to be executed at the given
        timestamp.

        :param bytes data: Task data.
        :param datetime ts: Timestamp at which task should be executed.
        :param bool utc: Whether taskqueue is in UTC-mode or local mode.
        :return: No return value.
        """
        raise NotImplementedError

    def read_schedule(self, ts):
        """
        Read all tasks from the schedule that should be executed at or before
        the given timestamp. Once read, the tasks are removed from the
        schedule.

        :param datetime ts: Timestamp
        :return: List containing task data for tasks which should be executed
                 at or before the given timestamp.
        """
        raise NotImplementedError

    def schedule_size(self):
        """
        :return: The number of tasks currently in the schedule.
        """
        raise NotImplementedError

    def scheduled_items(self, limit=None):
        """
        Non-destructively read the given number of tasks from the schedule.

        :param int limit: Restrict the number of tasks returned.
        :return: List of tasks that are in schedule, in order from soonest to
                 latest.
        """
        raise NotImplementedError

    def flush_schedule(self):
        """
        Delete all scheduled tasks.

        :return: No return value.
        """
        raise NotImplementedError

    def put_data(self, key, value, is_result=False):
        """
        Store an arbitrary key/value pair.

        :param bytes key: lookup key
        :param bytes value: value
        :param bool is_result: indicate if we are storing a (volatile) task
            result versus metadata like a task revocation key or lock.
        :return: No return value.
        """
        raise NotImplementedError

    def peek_data(self, key):
        """
        Non-destructively read the value at the given key, if it exists.

        :param bytes key: Key to read.
        :return: Associated value, if key exists, or ``EmptyData``.
        """
        raise NotImplementedError

    def pop_data(self, key):
        """
        Destructively read the value at the given key, if it exists.

        :param bytes key: Key to read.
        :return: Associated value, if key exists, or ``EmptyData``.
        """
        raise NotImplementedError

    def delete_data(self, key):
        """
        Delete the value at the given key, if it exists.

        :param bytes key: Key to delete.
        :return: boolean success or failure.
        """
        return self.pop_data(key) is not EmptyData

    def has_data_for_key(self, key):
        """
        Return whether there is data for the given key.

        :return: Boolean value.
        """
        raise NotImplementedError

    def put_if_empty(self, key, value):
        """
        Atomically write data only if the key is not already set.

        :param bytes key: Key to check/set.
        :param bytes value: Arbitrary data.
        :return: Boolean whether key/value was set.
        """
        if self.has_data_for_key(key):
            return False
        self.put_data(key, value)
        return True

    def result_store_size(self):
        """
        :return: Number of key/value pairs in the result store.
        """
        raise NotImplementedError

    def result_items(self):
        """
        Non-destructively read all the key/value pairs from the data-store.

        :return: Dictionary mapping all key/value pairs in the data-store.
        """
        raise NotImplementedError

    def flush_results(self):
        """
        Delete all key/value pairs from the data-store.

        :return: No return value.
        """
        raise NotImplementedError

    def flush_all(self):
        """
        Remove all persistent or semi-persistent data.

        :return: No return value.
        """
        self.flush_queue()
        self.flush_schedule()
        self.flush_results()


class BlackHoleStorage(BaseStorage):
    def enqueue(self, data, priority=None): pass
    def dequeue(self): pass
    def queue_size(self): return 0
    def enqueued_items(self, limit=None): return []
    def flush_queue(self): pass
    def add_to_schedule(self, data, ts, utc): pass
    def read_schedule(self, ts): return []
    def schedule_size(self): return 0
    def scheduled_items(self, limit=None): return []
    def flush_schedule(self): pass
    def put_data(self, key, value, is_result=False): pass
    def peek_data(self, key): return EmptyData
    def pop_data(self, key): return EmptyData
    def has_data_for_key(self, key): return False
    def result_store_size(self): return 0
    def result_items(self): return {}
    def flush_results(self): pass


class MemoryStorage(BaseStorage):
    def __init__(self, *args, **kwargs):
        super(MemoryStorage, self).__init__(*args, **kwargs)
        self._c = 0  # Counter to ensure FIFO behavior for queue.
        self._queue = []
        self._results = {}
        self._schedule = []
        self._lock = threading.RLock()

    def enqueue(self, data, priority=None):
        with self._lock:
            self._c += 1
            priority = 0 if priority is None else -priority
            heapq.heappush(self._queue, (priority, self._c, data))

    def dequeue(self):
        try:
            _, _, data = heapq.heappop(self._queue)
        except IndexError:
            pass
        else:
            return data

    def queue_size(self):
        return len(self._queue)

    def enqueued_items(self, limit=None):
        items = [data for _, _, data in sorted(self._queue)]
        if limit:
            items = items[:limit]
        return items

    def flush_queue(self):
        self._queue = []

    def add_to_schedule(self, data, ts, utc):
        heapq.heappush(self._schedule, (ts, data))

    def read_schedule(self, ts):
        with self._lock:
            accum = []
            while self._schedule:
                sts, data = heapq.heappop(self._schedule)
                if sts <= ts:
                    accum.append(data)
                else:
                    heapq.heappush(self._schedule, (sts, data))
                    break

        return accum

    def schedule_size(self):
        return len(self._schedule)

    def scheduled_items(self, limit=None):
        items = sorted(data for _, data in self._schedule)
        if limit:
            items = items[:limit]
        return items

    def flush_schedule(self):
        self._schedule = []

    def put_data(self, key, value, is_result=False):
        self._results[key] = value

    def peek_data(self, key):
        return self._results.get(key, EmptyData)

    def pop_data(self, key):
        return self._results.pop(key, EmptyData)

    def has_data_for_key(self, key):
        return key in self._results

    def result_store_size(self):
        return len(self._results)

    def result_items(self):
        return dict(self._results)

    def flush_results(self):
        self._results = {}

class _ConnectionState(object):
    def __init__(self, **kwargs):
        super(_ConnectionState, self).__init__(**kwargs)
        self.reset()
    def reset(self):
        self.conn = None
        self.closed = True
    def set_connection(self, conn):
        self.conn = conn
        self.closed = False
class _ConnectionLocal(_ConnectionState, threading.local): pass

to_bytes = lambda b: bytes(b) if not isinstance(b, bytes) else b
to_blob = lambda b: sqlite3.Binary(b)




class BaseSqlStorage(BaseStorage):
    begin_sql = 'begin'
    ddl = []

    def __init__(self, *args, **kwargs):
        super(BaseSqlStorage, self).__init__(*args, **kwargs)
        self._state = _ConnectionLocal()
        self.initialize_schema()

    def close(self):
        if self._state.closed: return False
        self._state.conn.close()
        self._state.reset()
        return True

    @property
    def conn(self):
        if self._state.closed:
            self._state.set_connection(self._create_connection())
        return self._state.conn

    def _create_connection(self):
        raise NotImplementedError

    @contextlib.contextmanager
    def db(self, commit=False, close=False):
        conn = self.conn
        cursor = conn.cursor()
        try:
            if commit: cursor.execute(self.begin_sql)
            yield cursor
        except Exception:
            if commit: conn.rollback()
            raise
        else:
            if commit: conn.commit()
        finally:
            cursor.close()
            if close:
                conn.close()
                self._state.reset()

    def initialize_schema(self):
        with self.db(commit=True, close=True) as curs:
            for sql in self.ddl:
                try:
                    curs.execute(sql)
                except:
                    pass

    def sql(self, query, params=None, commit=False, results=False):
        with self.db(commit=commit) as curs:
            curs.execute(query, params or ())
            if results:
                return curs.fetchall()


class SqliteStorage(BaseSqlStorage):
    begin_sql = 'begin exclusive'
    table_kv = ('create table if not exists kv ('
                'queue text not null, key text not null, value blob not null, '
                'primary key(queue, key))')
    table_sched = ('create table if not exists schedule ('
                   'id integer not null primary key, queue text not null, '
                   'data blob not null, timestamp real not null)')
    index_sched = ('create index if not exists schedule_queue_timestamp '
                   'on schedule (queue, timestamp)')
    table_task = ('create table if not exists task ('
                  'id integer not null primary key, queue text not null, '
                  'data blob not null, priority real not null default 0.0)')
    index_task = ('create index if not exists task_priority_id on task '
                  '(priority desc, id asc)')
    ddl = [table_kv, table_sched, index_sched, table_task, index_task]

    def __init__(self, name='taskqueue', filename='taskqueue.db', cache_mb=8,
                 fsync=False, journal_mode='wal', timeout=5, strict_fifo=False,
                 **kwargs):
        self.filename = filename
        self._cache_mb = cache_mb
        self._fsync = fsync
        self._journal_mode = journal_mode
        self._timeout = timeout  # Busy timeout in seconds, default is 5.
        self._conn_kwargs = kwargs

        if strict_fifo:
            self.ddl[3] = self.table_task.replace(
                'primary key',
                'primary key autoincrement')

        super(SqliteStorage, self).__init__(name)

    def _create_connection(self):
        conn = sqlite3.connect(self.filename, timeout=self._timeout,
                               **self._conn_kwargs)
        conn.isolation_level = None  # Autocommit mode.
        conn.execute('pragma journal_mode="%s"' % self._journal_mode)
        if self._cache_mb:
            conn.execute('pragma cache_size=%s' % (-1000 * self._cache_mb))
        conn.execute('pragma synchronous=%s' % (2 if self._fsync else 0))
        return conn

    def enqueue(self, data, priority=None):
        self.sql('insert into task (queue, data, priority) values (?, ?, ?)',
                 (self.name, to_blob(data), priority or 0), commit=True)

    def dequeue(self):
        with self.db(commit=True) as curs:
            curs.execute('select id, data from task where queue = ? '
                         'order by priority desc, id limit 1', (self.name,))
            result = curs.fetchone()
            if result is not None:
                tid, data = result
                curs.execute('delete from task where id = ?', (tid,))
                if curs.rowcount == 1:
                    return to_bytes(data)

    def queue_size(self):
        return self.sql('select count(id) from task where queue=?',
                        (self.name,), results=True)[0][0]

    def enqueued_items(self, limit=None):
        sql = 'select data from task where queue=? order by priority desc, id'
        params = (self.name,)
        if limit is not None:
            sql += ' limit ?'
            params = (self.name, limit)

        return [to_bytes(i) for i, in self.sql(sql, params, results=True)]

    def flush_queue(self):
        self.sql('delete from task where queue=?', (self.name,), commit=True)

    def add_to_schedule(self, data, ts, utc):
        params = (self.name, to_blob(data), to_timestamp(ts))
        self.sql('insert into schedule (queue, data, timestamp) '
                 'values (?, ?, ?)', params, commit=True)

    def read_schedule(self, ts):
        with self.db(commit=True) as curs:
            params = (self.name, to_timestamp(ts))
            curs.execute('select id, data from schedule where '
                         'queue = ? and timestamp <= ?', params)
            id_list, data = [], []
            for task_id, task_data in curs.fetchall():
                id_list.append(task_id)
                data.append(to_bytes(task_data))
            if id_list:
                plist = ','.join('?' * len(id_list))
                curs.execute('delete from schedule where id IN (%s)' % plist,
                             id_list)
            return data

    def schedule_size(self):
        return self.sql('select count(id) from schedule where queue=?',
                        (self.name,), results=True)[0][0]

    def scheduled_items(self, limit=None):
        sql = 'select data from schedule where queue=? order by timestamp'
        params = (self.name,)
        if limit is not None:
            sql += ' limit ?'
            params = (self.name, limit)

        return [to_bytes(i) for i, in self.sql(sql, params, results=True)]

    def flush_schedule(self):
        self.sql('delete from schedule where queue = ?', (self.name,), True)

    def put_data(self, key, value, is_result=False):
        self.sql('insert or replace into kv (queue, key, value) '
                 'values (?, ?, ?)', (self.name, key, to_blob(value)), True)

    def peek_data(self, key):
        res = self.sql('select value from kv where queue = ? and key = ?',
                       (self.name, key), results=True)
        return to_bytes(res[0][0]) if res else EmptyData

    def pop_data(self, key):
        with self.db(commit=True) as curs:
            curs.execute('select value from kv where queue = ? and key = ?',
                         (self.name, key))
            result = curs.fetchone()
            if result is not None:
                curs.execute('delete from kv where queue=? and key=?',
                             (self.name, key))
                if curs.rowcount == 1:
                    return to_bytes(result[0])
            return EmptyData

    def has_data_for_key(self, key):
        return bool(self.sql('select 1 from kv where queue=? and key=?',
                             (self.name, key), results=True))

    def put_if_empty(self, key, value):
        try:
            with self.db(commit=True) as curs:
                curs.execute('insert or abort into kv '
                             '(queue, key, value) values (?, ?, ?)',
                             (self.name, key, to_blob(value)))
        except sqlite3.IntegrityError:
            return False
        else:
            return True

    def result_store_size(self):
        return self.sql('select count(*) from kv where queue=?', (self.name,),
                        results=True)[0][0]

    def result_items(self):
        res = self.sql('select key, value from kv where queue=?', (self.name,),
                       results=True)
        return dict((k, to_bytes(v)) for k, v in res)

    def flush_results(self):
        self.sql('delete from kv where queue=?', (self.name,), True)


class FileStorage(BaseStorage):
    """
    Simple file-system storage implementation.

    This storage implementation should NOT be used in production as it utilizes
    exclusive locks around all file-system operations. This is done to prevent
    race-conditions when reading from the file-system.
    """
    MAX_PRIORITY = 0xffff

    def __init__(self, name, path, levels=2, use_thread_lock=False,
                 **storage_kwargs):
        super(FileStorage, self).__init__(name, **storage_kwargs)

        self.path = path
        if os.path.exists(self.path) and not os.path.isdir(self.path):
            raise ValueError('path "%s" is not a directory' % path)
        if levels < 0 or levels > 4:
            raise ValueError('%s levels must be between 0 and 4' % self)

        self.queue_path = os.path.join(self.path, 'queue')
        self.schedule_path = os.path.join(self.path, 'schedule')
        self.result_path = os.path.join(self.path, 'results')
        self.levels = levels

        if use_thread_lock:
            self.lock = threading.Lock()
        else:
            self.lock_file = os.path.join(self.path, '.lock')
            self.lock = FileLock(self.lock_file)

    def _flush_dir(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
            os.makedirs(path)

    def enqueue(self, data, priority=None):
        priority = priority or 0
        if priority < 0: raise ValueError('priority must be a positive number')
        if priority > self.MAX_PRIORITY:
            raise ValueError('priority must be <= %s' % self.MAX_PRIORITY)

        with self.lock:
            if not os.path.exists(self.queue_path):
                os.makedirs(self.queue_path)

            # Encode the filename so that tasks are sorted by priority (desc) and
            # timestamp (asc).
            prefix = '%04x-%012x' % (
                self.MAX_PRIORITY - priority,
                int(time.time() * 1000))

            base = filename = os.path.join(self.queue_path, prefix)
            conflict = 0
            while os.path.exists(filename):
                conflict += 1
                filename = '%s.%03d' % (base, conflict)

            with open(filename, 'wb') as fh:
                fh.write(data)

    def _get_sorted_filenames(self, path):
        if not os.path.exists(path):
            return ()
        return [f for f in sorted(os.listdir(path)) if not f.endswith('.tmp')]

    def dequeue(self):
        with self.lock:
            filenames = self._get_sorted_filenames(self.queue_path)
            if not filenames:
                return

            filename = os.path.join(self.queue_path, filenames[0])
            tmp_dest = filename + '.tmp'
            os.rename(filename, tmp_dest)

            with open(tmp_dest, 'rb') as fh:
                data = fh.read()
            os.unlink(tmp_dest)
        return data

    def queue_size(self):
        return len(self._get_sorted_filenames(self.queue_path))

    def enqueued_items(self, limit=None):
        filenames = self._get_sorted_filenames(self.queue_path)[:limit]
        accum = []
        for filename in filenames:
            with open(os.path.join(self.queue_path, filename), 'rb') as fh:
                accum.append(fh.read())
        return accum

    def flush_queue(self):
        self._flush_dir(self.queue_path)

    def _timestamp_to_prefix(self, ts):
        ts = time.mktime(ts.timetuple()) + (ts.microsecond * 1e-6)
        return '%012x' % int(ts * 1000)

    def add_to_schedule(self, data, ts, utc):
        with self.lock:
            if not os.path.exists(self.schedule_path):
                os.makedirs(self.schedule_path)

            ts_prefix = self._timestamp_to_prefix(ts)
            base = filename = os.path.join(self.schedule_path, ts_prefix)
            conflict = 0
            while os.path.exists(filename):
                conflict += 1
                filename = '%s.%03d' % (base, conflict)

            with open(filename, 'wb') as fh:
                fh.write(data)

    def read_schedule(self, ts):
        with self.lock:
            prefix = self._timestamp_to_prefix(ts)
            accum = []
            for basename in self._get_sorted_filenames(self.schedule_path):
                if basename[:12] > prefix:
                    break
                filename = os.path.join(self.schedule_path, basename)
                new_filename = filename + '.tmp'
                os.rename(filename, new_filename)
                accum.append(new_filename)

            tasks = []
            for filename in accum:
                with open(filename, 'rb') as fh:
                    tasks.append(fh.read())
                    os.unlink(filename)

        return tasks

    def schedule_size(self):
        return len(self._get_sorted_filenames(self.schedule_path))

    def scheduled_items(self, limit=None):
        filenames = self._get_sorted_filenames(self.schedule_path)[:limit]
        accum = []
        for filename in filenames:
            with open(os.path.join(self.schedule_path, filename), 'rb') as fh:
                accum.append(fh.read())
        return accum

    def flush_schedule(self):
        self._flush_dir(self.schedule_path)

    def path_for_key(self, key):
        if isinstance(key, text_type):
            key = key.encode('utf8')
        checksum = hashlib.md5(key).hexdigest()
        prefix = checksum[:self.levels]
        prefix_filename = itertools.chain(prefix, (checksum,))
        return os.path.join(self.result_path, *prefix_filename)

    def put_data(self, key, value, is_result=False):
        if isinstance(key, text_type):
            key = key.encode('utf8')

        filename = self.path_for_key(key)
        dirname = os.path.dirname(filename)

        with self.lock:
            if not os.path.exists(dirname):
                os.makedirs(dirname)

            with open(self.path_for_key(key), 'wb') as fh:
                key_len = len(key)
                fh.write(struct.pack('>I', key_len))
                fh.write(key)
                fh.write(value)

    def _unpack_result(self, data):
        key_len, = struct.unpack('>I', data[:4])
        key = data[4:4 + key_len]
        if len(key) != key_len:
            return None, None
        return key, data[4 + key_len:]

    def peek_data(self, key):
        filename = self.path_for_key(key)
        if not os.path.exists(filename):
            return EmptyData

        with open(filename, 'rb') as fh:
            _, value = self._unpack_result(fh.read())

        # If file is corrupt or has been tampered with, return EmptyData.
        return value if value is not None else EmptyData

    def pop_data(self, key):
        filename = self.path_for_key(key)

        with self.lock:
            if not os.path.exists(filename):
                return EmptyData

            with open(filename, 'rb') as fh:
                _, value = self._unpack_result(fh.read())

            os.unlink(filename)

        # If file is corrupt or has been tampered with, return EmptyData.
        return value if value is not None else EmptyData

    def has_data_for_key(self, key):
        return os.path.exists(self.path_for_key(key))

    def result_store_size(self):
        return sum(len(filenames) for _, _, filenames
                   in os.walk(self.result_path))

    def result_items(self):
        accum = {}
        for root, _, filenames in os.walk(self.result_path):
            for filename in filenames:
                path = os.path.join(root, filename)
                with open(path, 'rb') as fh:
                    key, value = self._unpack_result(fh.read())
                accum[key] = value
        return accum

    def flush_results(self):
        self._flush_dir(self.result_path)