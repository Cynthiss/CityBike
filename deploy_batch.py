from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from pipeline_orchestration import citybike_etl

deployment = Deployment.build_from_flow(
    flow=citybike_etl,
    name="weekly-batch",
    schedule=CronSchedule(cron="0 2 * * 1"),  # Todos los lunes a las 2am
    work_queue_name="default"
)

deployment.apply()


# CORRER UNA VEZ DESDE TERMINAL
# COMANDO - python deploy_batch.py

# Revisar en https://app.prefect.cloud/
# Si existe deployment: citybike_etl_pipeline > weekly-batch
# Verificas que diga: Next Run: Monday at 2:00 AM

# Correr en consola:
# prefect agent start -q default


