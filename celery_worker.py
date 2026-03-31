from celery import Celery
import subprocess
import os


# Configure Celery to connect to the Redis broker
celery_app = Celery(
    "orion_worker",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_track_started=True,
    broker_connection_retry_on_startup=True
)

# Define the task handler for orion.data_ingestion
# Make sure the name aligns with the task name used by celery in the upstream app
# so that the data ingestion task can be picked up from the task queue
@celery_app.task(name="orion.data_ingestion", queue="orion")
def run_build_manager(task_data):
    print(f'task_data: {task_data}', flush=True)
    # Run orion-build as a subprocess with the provided config
    os.environ["ORION_GRAPH_SPEC"] = task_data["graph_spec_filename"]
    # no need to catch CalledProcessError exception, but rather let it propogate to Celery task handling
    result = subprocess.run(
        ["orion-build", task_data["graph_id"], "--graph_specs_dir",
         os.getenv('SHARED_SOURCE_DATA_PATH', None)],
        capture_output=True,
        text=True,
        check=True
    )
    return {"status": "success", "output": f'graph {task_data["graph_id"]} is created successfully'}
