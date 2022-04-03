import json
import gdown
from datetime import datetime
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import logging

from google.cloud import bigquery


logging.basicConfig(level=logging.INFO)

options = PipelineOptions(
    # direct_num_workers=4
)

GDRIVE_FILE_ID = '1dEGQepQcHQM30yA1fPOheX9t-8X7A-Nj'
TEMP_FILE_NAME = 'input.json'
DEFAULT_RAW_TABLE_NAME = 'raw_user_activity'
DEFAULT_CLEANED_TABLE_NAME = 'cleaned_user_activity'
DEFAULT_DATASET_NAME = 'user_activity_data'

GCS_TEMP_LOCATION = 'gs://pipeline-bucket-zjqx/user-activity/temp/'
GCS_INPUT = 'gs://pipeline-bucket-zjqx/user-activity/input.json'
PROJECT_ID = 'hyper-cell-home-work'

execution_datetime = datetime.now().strftime("%m%d%Y%H%M%S")

USER_ACTIVITY_SCHEMA = {
    "fields": [
        {
            'name': 'event_date',
            'type': 'STRING',
            'mode': 'NULLABLE'
        },
        {
            'name': 'event_timestamp',
            'type': 'NUMERIC',
            'mode': 'NULLABLE'
        },
        {
            'name': 'platform',
            'type': 'STRING',
            'mode': 'NULLABLE'
        },
        {
            'name': 'event_name',
            'type': 'STRING',
            'mode': 'NULLABLE'
        },
        {
            'name': 'user_pseudo_id',
            'type': 'STRING',
            'mode': 'NULLABLE'
        },
        {
            'name': 'current_config_version',
            'type': 'STRING',
            'mode': 'NULLABLE'
        },
        {
            'name': 'session_id',
            'type': 'NUMERIC',
            'mode': 'NULLABLE'
        },
        {
            'name': 'level_num',
            'type': 'NUMERIC',
            'mode': 'NULLABLE'
        },
        {
            'name': 'ad_placement',
            'type': 'STRING',
            'mode': 'NULLABLE'
        }
    ]
}

DATA_CLEAN_QUERY = """
CREATE TABLE hyper-cell-home-work.user_activity_data.cleaned_user_activity AS
WITH filtered_values AS (
    SELECT 
        * 
    FROM `hyper-cell-home-work.user_activity_data.raw_user_activity`
    WHERE user_pseudo_id IS NOT NULL
        AND current_config_version IS NOT NULL
        AND session_id IS NOT NULL
        AND NOT (event_name LIKE 'level_%' AND level_num IS NULL)
        AND NOT (event_name LIKE 'rew_%' AND ad_placement IS NULL)
        AND NOT (event_name LIKE 'int_%' AND ad_placement IS NULL)
), event_count_per_user as (
    SELECT 
        user_pseudo_id, 
        COUNT(*) AS event_count 
    FROM filtered_values
    GROUP BY user_pseudo_id
), percentile as (
    SELECT 
        user_pseudo_id, 
        event_count,
        PERCENT_RANK() OVER (ORDER BY event_count ASC) as percentile
    FROM event_count_per_user 
    WHERE event_count > 1
)
SELECT filt.* FROM percentile perc
LEFT JOIN filtered_values filt on perc.user_pseudo_id = filt.user_pseudo_id
WHERE percentile < 0.95
"""

LEVEL_COMPLETION_QUERY = """
CREATE TABLE hyper-cell-home-work.user_activity_data.level_completion_summary_{} AS
WITH events_per_session AS (
    SELECT 
        current_config_version, 
        session_id,
        user_pseudo_id,
        count(*) as event_count 
    FROM `hyper-cell-home-work.user_activity_data.cleaned_user_activity`
    WHERE event_name = 'level_complete'
    GROUP BY current_config_version, user_pseudo_id, session_id
), median_per_current_config AS (
    SELECT
        DISTINCT(current_config_version),
        median_count
    FROM(
        SELECT
            current_config_version,
            PERCENTILE_CONT(event_count, 0.5) OVER (PARTITION BY current_config_version) as median_count
        from events_per_session
    )
), avg_and_user_count AS (
    SELECT 
        current_config_version, 
        COUNT(DISTINCT user_pseudo_id) AS user_count, 
        AVG(event_count) AS avg_levels_per_session 
    FROM events_per_session
    GROUP BY current_config_version
)

SELECT 
    av.*,
    mpcs.median_count AS median_levels_per_session 
FROM avg_and_user_count av
LEFT JOIN median_per_current_config mpcs ON av.current_config_version = mpcs.current_config_version
""".format(execution_datetime)

ADS_SUMMARY_QUERY = """
CREATE TABLE hyper-cell-home-work.user_activity_data.ads_summary_{} AS
WITH dau AS(
    SELECT 
        current_config_version, 
        count(distinct user_pseudo_id) user_count
    FROM `hyper-cell-home-work.user_activity_data.cleaned_user_activity`
    GROUP BY current_config_version
), ads AS(
    SELECT 
        current_config_version,  
        ad_placement,
        event_name,
        count(*) as ads_count

    FROM hyper-cell-home-work.user_activity_data.cleaned_user_activity
    WHERE event_name = 'rew_start' OR event_name = 'int_start'
    GROUP BY current_config_version, ad_placement, event_name
)
SELECT 
    ads.current_config_version, 
    dau.user_count,
    ads.event_name,
    ads.ad_placement,
    (ads.ads_count / dau.user_count) as count_per_dau
FROM ads
LEFT JOIN dau ON ads.current_config_version = dau.current_config_version
""".format(execution_datetime)

# This function downloads file from GDrive
def download_file_from_gdrive(file_id, output_path):
    gdown.download(id=file_id, output=output_path, quiet=False)

# This pipeline is used to Download dataset from GDrive and save it on GCS
def run_landing():

    download_file_from_gdrive(GDRIVE_FILE_ID, TEMP_FILE_NAME)

    with beam.Pipeline(options=options) as pipeline:

        read_temp_data = (
            pipeline
            | 'Read data from file' >> beam.io.ReadFromText(TEMP_FILE_NAME)
        )

        store = (
            read_temp_data
            | 'Save to File' >> beam.io.WriteToText(GCS_INPUT)
        )


# This stage reads raw input data from GDrive and loads it to the BigQuery
# During upload - data types are verified automatically. If one of the records
# has incompatible value - whole load will be rejected. 
def run_data_upload():

    with beam.Pipeline(options=options) as pipeline:
        input = (
            pipeline
            | 'Read input data from Google Cloud Storage' >> beam.io.ReadFromText(GCS_INPUT + '*')
            | 'Convert JSON to DICT' >> beam.Map(json.loads)
        )
        
        load_raw_input = (
            input
            | 'Write to BigQuery' >> beam.io.Write(
                beam.io.WriteToBigQuery(
                    custom_gcs_temp_location=GCS_TEMP_LOCATION,
                    schema=USER_ACTIVITY_SCHEMA,
                    table=DEFAULT_RAW_TABLE_NAME,
                    dataset=DEFAULT_DATASET_NAME,
                    project=PROJECT_ID,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY
                )
            )
        )


def run_query_job(query):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    client.query(
        query=query,
        job_config=job_config
    )

# This job creates cleaned data table
def run_data_cleaning():
    run_query_job(DATA_CLEAN_QUERY)

# This job generate level completion summary
def run_level_summary():
    run_query_job(LEVEL_COMPLETION_QUERY)

# This job genered showed ads summary
def run_ads_summary():
    run_query_job(ADS_SUMMARY_QUERY)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--land', action=argparse.BooleanOptionalAction, help="Load data from GDrive")
    parser.add_argument('--stage', action=argparse.BooleanOptionalAction, help="Upload Data to The BigQuery")
    parser.add_argument('--clean', action=argparse.BooleanOptionalAction, help="Execute data clean Job")
    parser.add_argument('--level-summary', action=argparse.BooleanOptionalAction, help="Prepare level completion summary")
    parser.add_argument('--ads-summary', action=argparse.BooleanOptionalAction, help="Prepare Ads summary")


    args = parser.parse_args()

    if (args.land):
        run_landing()
    if (args.stage):
        run_data_upload()
    if (args.clean):
        run_data_cleaning()
    if (args.level_summary):
        run_level_summary()
    if (args.ads_summary):
        run_ads_summary()    
