# Airflow


## Usage

### Setup Service Account Key

1. Generate airflow service account credentials in GCP(Google Cloud Console) and save it to `airflow/gcp_keyfile.json` and `airflow/crawler_gcp_keyfile.json`.

2. Copy `.env.example` to `.env`.

3. Update the values of project id, bucket name, dataset name and credentials path in `.env`.

### Setup Airflow

1. Generate `AIRFLOW_SECRET_KEY` in `.env`

Use below command to generate a random secret key.
```sh
openssl rand -hex 24
```
Paste the result to `AIRFLOW_SECRET_KEY` in `.env`.

2. Run
```sh
docker-compose build
docker-compose up
```

3. Navigate to `localhost:8085`
