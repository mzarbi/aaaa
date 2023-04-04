from task_queue import SqliteTaskQueue, FileTaskQueue
from task_queue.api import MySqlTaskQueue, RedisTaskQueue


def create_sqlite_queue():
    _task_queue = SqliteTaskQueue(
        name="AMLCM",
        filename='taskqueue.db',
    )


def create_file_queue():
    _task_queue = FileTaskQueue(
        name="AMLCM",
        path=r"<folder_path>",
        use_thread_lock=True
    )


def create_mysql_queue():
    _task_queue = MySqlTaskQueue(
        name="AMLCM",
        host="localhost",
        user="taskqueue",
        password="taskqueue",
        database="taskqueue"
    )


def create_redis_queue():
    _task_queue = RedisTaskQueue()



def create_task_queue():
    # _task_queue = MySqlTaskQueue(
    #    name="AMLCM",
    #    host="localhost",
    #    user="taskqueue",
    #    password="taskqueue",
    #    database="taskqueue"
    # )

    # _task_queue = FileTaskQueue(
    #    name="AMLCM",
    #    path=r"C:\Users\medzi\Desktop\bnp\tq",
    #    use_thread_lock=True
    # )

    _task_queue = SqliteTaskQueue(
        name="AMLCM",
        filename='taskqueue.db'
    )
    return _task_queue
