#!/usr/bin/env python
import datetime
import logging
import os
import socket
import sys
from filelock import FileLock

from task_queue.consumer_options import ConsumerConfig
from task_queue.consumer_options import OptionParserHandler
from task_queue.utils import load_class


def err(s):
    sys.stderr.write('\033[91m%s\033[0m\n' % s)


def load_taskqueue(path):
    try:
        return load_class(path)
    except:
        cur_dir = os.getcwd()
        if cur_dir not in sys.path:
            sys.path.insert(0, cur_dir)
            return load_taskqueue(path)
        err('Error importing %s' % path)
        raise


def consumer_main():
    parser_handler = OptionParserHandler()
    parser = parser_handler.get_option_parser()
    options, args = parser.parse_args()

    if len(args) == 0:
        err('Error:   missing import path to `taskqueue` instance')
        err('Example: consumer.py app.queue.instance')
        sys.exit(1)

    options = {k: v for k, v in options.__dict__.items()
               if v is not None}

    config = ConsumerConfig(**options)
    config.validate()

    instance = load_taskqueue(args[0])

    # Set up logging for the "taskqueue" namespace.
    logger = logging.getLogger('taskqueue')
    config.setup_logger(logger)

    consumer = instance.create_consumer(**config.values)
    consumer.run()


def consumer_main_in_code():
    options = {'logfile': 'aaa.logs', 'workers': 5, 'worker_type': 'thread'}
    config = ConsumerConfig(**options)
    config.validate()

    instance = load_taskqueue('server.tasks.tq')

    # Set up logging for the "taskqueue" namespace.
    logger = logging.getLogger('taskqueue')
    config.setup_logger(logger)

    consumer = instance.create_consumer(**config.values)
    consumer.run()


if __name__ == '__main__':
    with FileLock(os.path.join(r"C:\Users\medzi\Desktop\bnp\tq", 'consumer.lock')):
        if sys.version_info >= (3, 8) and sys.platform == 'darwin':
            import multiprocessing
            try:
                multiprocessing.set_start_method('fork')
            except RuntimeError:
                pass
        consumer_main()
