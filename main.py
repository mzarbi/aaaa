import time

from queue_extension import create_task_queue

tq = create_task_queue()


@tq.task()
def send_email():
    time.sleep(10)
    print("I simulate sending emails")


