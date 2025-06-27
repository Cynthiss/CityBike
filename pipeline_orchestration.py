from prefect import flow, task
from datetime import timedelta
import subprocess
from prefect.blocks.notifications import SlackWebhook

# Failure Alert
slack_alert = SlackWebhook.load("slack-alert")

# Tasks
@task
def download_data():
    subprocess.run(["python", "Scripts/download_data.py"])

@task
def extract_data():
    subprocess.run(["python", "Scripts/extract_data.py"])

@task
def bronze_conversion():
    subprocess.run(["python", "Scripts/bronze_trans.py"])

@task
def silver_transformation():
    subprocess.run(["python", "Scripts/silver_trans.py"])

@task
def gold_aggregations():
    subprocess.run(["python", "Scripts/gold_trans.py"])

@task
def upload_to_blob():
    subprocess.run(["python", "Connection/up_data.py"])

'''
@task
def validate_data():
    subprocess.run(["python", "Validation/data_quality.py"])
'''

# Flow
@flow(name="citybike_etl_pipeline")
def citybike_etl():
    try: 
        download_data()
        extract_data()
        bronze_conversion()
        silver_transformation()
        gold_aggregations()
        # validate_data()
        upload_to_blob()
    except Exception as e:
        slack_alert.notify(f"ETL Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    citybike_etl()

# COMANDO TERMINAL:
# python pipeline_orchestration.py