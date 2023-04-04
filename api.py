from main import tq

pending = tq.pending()

# Print information about each running task
for task in pending:
    if task.is_running():
        print(f"Task ID: {task.task_id}, Task Name: {task.name}, Task Args: {task.args}, Task ETA: {task.eta}")