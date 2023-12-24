import os

import redshift_connector
from sqlalchemy import create_engine

TEST=False

OSRM_BATCH_SIZE = 100
# REDSHIFT_ETL_LIVE = create_engine(os.environ.get('REDSHIFT_ETL_URI'))
REDSHIFT_ETL_LIVE = redshift_connector.connect(
        iam=True,
        auto_create=True,
        cluster_identifier="etl",
        database="etl",
        db_user="ebru.catak",
        db_groups=["adhoc_users"],
        profile="data-prod"
    )

GETIR_MAPS_API_KEY = 'ANoi4iPb4NIiOQ9T'
IUGO_API_KEY = os.environ.get('IUGO_API_KEY')

REDSHIFT_ETL_IAM_ROLE = None
S3_BUCKET_NAME = None
S3_REGION = None

REDSHIFT_FRANCHISE_CLUSTER = redshift_connector.connect(
        iam=True,
        auto_create=True,
        cluster_identifier="franchise-sharing-cluster",
        database="etl",
        db_user="ebru.catak",
        db_groups=["adhoc_users"],
        profile="data-prod"
    )