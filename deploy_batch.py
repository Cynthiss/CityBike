from pipeline_orchestration import citybike_etl_pipeline

if __name__ == "__main__":
    citybike_etl_pipeline.deploy(
        name = "weekly-batch", 
        work_queue_name = "default", 
        schedule = "0.2 * * 1",
        tags = ["batch", "citybikes"]
    )

# CORRER UNA VEZ DESDE TERMINAL
# COMANDO - python deploy_batch.py

# Revisar en https://app.prefect.cloud/
# Si existe deployment: citybike_etl_pipeline > weekly-batch
# Verificas que diga: Next Run: Monday at 2:00 AM

# Correr en consola:
# prefect agent start -q default


