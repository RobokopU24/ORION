from celery import Celery
import subprocess
import os


# Configure Celery to connect to the Redis broker
celery_app = Celery(
    "orion_worker",
    broker="redis://redis:6379/0",
    backend="redis://redis:6379/0"
)


# Define the task handler for orion.data_ingestion
@celery_app.task(name="orion.data_ingestion")
def run_build_manager(task_data):
    try:
        # Ensure we're in the correct directory (ORION root)
        pwd_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Common')
        os.chdir(pwd_path)
        print(f'pwd_path: {pwd_path}', flush=True)
        print(f'task_data: {task_data}', flush=True)
        # Run build_manager.py as a subprocess with the provided config
        result = subprocess.run(
            ["python", "build_manager.py", "Example_Graph"],
            capture_output=True,
            text=True,
            check=True
        )
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        return {"status": "failure", "error": e.stderr}
