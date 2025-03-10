from celery import Celery
import subprocess
import os


# Configure Celery to connect to the Redis broker
celery_app = Celery(
    "orion_worker",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
)


# Define the task handler for orion.data_ingestion
# Make sure the name aligns with the task name used by celery in the upstream app
# so that the data ingestion task can be picked up from the task queue
@celery_app.task(name="orion.data_ingestion")
def run_build_manager(task_data):
    try:
        # Ensure we're in the correct directory (ORION root)
        pwd_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Common')
        os.chdir(pwd_path)
        print(f'pwd_path: {pwd_path}', flush=True)
        print(f'task_data: {task_data}', flush=True)
        # Run build_manager.py as a subprocess with the provided config
        os.environ["ORION_GRAPH_SPEC"] = task_data["graph_spec_filename"]
        result = subprocess.run(
            ["python", "build_manager.py", task_data["graph_id"]],
            capture_output=True,
            text=True,
            check=True
        )
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        return {"status": "failure", "error": e.stderr}
