import os
from datetime import datetime, timedelta

from docker.types import Mount
from google.cloud import bigquery
from utils.gcp import query_bq_to_df

from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator

RAW_BUCKET = os.environ.get("RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET")
DATASET_PREFIX = os.environ.get("DATASET_PREFIX")
BLOB_NAME = "gmaps"
CRAWER_GOOGLE_CREDENTIALS_LOCAL_PATH = os.environ.get(
    "CRAWER_GOOGLE_CREDENTIALS_LOCAL_PATH"
)

BQ_CLIENT = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["gmaps"],
)
def d_gmaps_crawler_to_src():
    @task
    def get_cafe_list(top_n: int = 1500) -> list[list[dict]]:
        query = f"""
            SELECT DISTINCT
                `cafe_id`,
                `cafe_name`,
                `rating`,
            FROM
                `{DATASET_PREFIX}ods.ods-cafe-list`
            ORDER BY `rating` DESC
        """
        df = query_bq_to_df(BQ_CLIENT, query)[:top_n][["cafe_id", "cafe_name"]]
        print(f"total cafe: {len(df)}")
        # random df to avoid task 1 always crawler the attraction with more reviews
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)
        attractions = df.to_dict(orient="records")
        # batch
        batch_size = 400  # default max_map_length is 1024
        return [
            attractions[i : i + batch_size]
            for i in range(0, len(attractions), batch_size)
        ]

    @task
    def e_gmaps_crawler(batch: list[dict]):
        for attraction in batch:
            print(f"crawling attraction: {attraction}")
            cafe_name = attraction["cafe_name"]
            cafe_id = attraction["cafe_id"]
            crawler_task = DockerOperator(
                task_id=f"crawl_{cafe_id}",
                # Image source from https://github.com/yeha98555/google-maps-reviews-scraper
                image="gmaps-scraper",
                api_version="auto",
                auto_remove=True,
                environment={
                    "ATTRACTION_ID": cafe_id,
                    "ATTRACTION_NAME": cafe_name,
                    "GCS_BUCKET_NAME": RAW_BUCKET,
                    "GCS_BLOB_NAME": BLOB_NAME,
                },
                command="make run",
                mounts=[
                    Mount(
                        source=CRAWER_GOOGLE_CREDENTIALS_LOCAL_PATH,
                        target="/app/crawler_gcp_keyfile.json",
                        type="bind",
                        read_only=True,
                    ),
                ],
                mount_tmp_dir=False,
                mem_limit="12g",
                shm_size="2g",
                docker_url="tcp://docker-proxy:2375",
                network_mode="bridge",
            )
            crawler_task.execute({})

    trigger_d_gmaps_places_src_to_ods = TriggerDagRunOperator(
        task_id="trigger_d_gmaps_places_src_to_ods",
        trigger_dag_id="d_gmaps_places_src_to_ods",
    )

    trigger_d_gmaps_reviews_src_to_ods = TriggerDagRunOperator(
        task_id="trigger_d_gmaps_reviews_src_to_ods",
        trigger_dag_id="d_gmaps_reviews_src_to_ods",
    )

    batches = get_cafe_list(top_n=-1)
    crawl_tasks = e_gmaps_crawler.expand(batch=batches)
    crawl_tasks >> trigger_d_gmaps_places_src_to_ods
    crawl_tasks >> trigger_d_gmaps_reviews_src_to_ods


d_gmaps_crawler_to_src()
