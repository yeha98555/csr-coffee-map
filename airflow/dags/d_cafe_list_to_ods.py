import os
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery, storage
from utils.common import rename_place_id
from utils.gcp import download_df_from_gcs, upload_df_to_gcs, build_bq_from_gcs

from airflow.decorators import dag, task

RAW_BUCKET = os.environ.get("RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET")
DATASET_PREFIX = os.environ.get("DATASET_PREFIX")
SRC_BLOB_NAME = "src_taiwan_cafe_list.csv"
PROCESSED_BLOB_NAME = "processed_taiwan_cafe_list.parquet"
GCS_CLIENT = storage.Client()
BQ_CLIENT = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["cafe-list"],
)
def d_cafe_list_to_dim():
    @task
    def e_download_cafe_list_from_gcs(
        bucket_name: str, blob_name: str
    ) -> pd.DataFrame:
        return download_df_from_gcs(
            client=GCS_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            filetype="csv",
        )

    @task
    def t_rename_cafe_list_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.rename(
            columns={
                "店名": "cafe_name",
                "官網": "website_url",
                "評價": "rating",
                "電話": "phone",
                "營業時間": "open_time",
                "地址": "address",
            },
            inplace=True,
        )
        return df

    @task
    def t_remove_cafe_list_null_rows(df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(how="all")

    @task
    def t_remove_cafe_list_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=["cafe_name", "rating"], keep="first")

    @task
    def l_upload_cafe_list_to_gcs(df: pd.DataFrame, bucket_name: str, blob_name: str):
        upload_df_to_gcs(
            client=GCS_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            df=df,
        )

    @task
    def t_rename_place_id(df: pd.DataFrame) -> pd.DataFrame:
        df["cafe_id"] = df["cafe_name"].apply(rename_place_id)
        return df

    @task
    def l_create_cafe_bq_external_table(
        dataset_name: str, table_name: str
    ):
        build_bq_from_gcs(
            client=BQ_CLIENT,
            dataset_name=dataset_name,
            table_name=table_name,
            bucket_name=PROCESSED_BUCKET,
            blob_name=PROCESSED_BLOB_NAME,
            schema=[
                bigquery.SchemaField("cafe_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("cafe_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("website_url", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("rating", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("phone", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("open_time", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("address", "STRING", mode="REQUIRED"),
            ],
        )

    t1 = e_download_cafe_list_from_gcs(
        bucket_name=RAW_BUCKET,
        blob_name=SRC_BLOB_NAME,
    )
    t2 = t_rename_cafe_list_columns(t1)
    t3 = t_remove_cafe_list_null_rows(t2)
    t4 = t_remove_cafe_list_duplicate_rows(t3)
    t5 = t_rename_place_id(t4)
    l_upload_cafe_list_to_gcs(
        t5,
        PROCESSED_BUCKET,
        PROCESSED_BLOB_NAME,
    ) >> l_create_cafe_bq_external_table(
        dataset_name=f"{DATASET_PREFIX}ods",
        table_name="ods-cafe-list",
    )


d_cafe_list_to_dim()