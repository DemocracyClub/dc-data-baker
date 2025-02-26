import csv
import datetime
import io

import boto3
import psycopg

ssm_client = boto3.client("ssm")

host_response = ssm_client.get_parameter(
    Name="/EveryElectionProd/DATABASE_HOST",
)
db_host = host_response["Parameter"]["Value"]

password_response = ssm_client.get_parameter(
    Name="/EveryElectionProd/DatabasePassword",
)
db_password = password_response["Parameter"]["Value"]


def export_sql(date: str):
    return f"""
    SELECT
        ee.election_id,
        COALESCE(odd.id, ogd.id) AS geography_id,
        COALESCE(st_astext(odd.geography), st_astext(ogd.geography)) AS geography_text,
        CASE
           WHEN ogd.id IS NOT NULL THEN 'Organisation'
           WHEN odd.id IS NOT NULL THEN 'Division'
           ELSE 'None'
        END AS source_table
    FROM
        elections_election ee
        LEFT JOIN organisations_divisiongeographysubdivided odd
               ON ee.division_geography_id = odd.division_geography_id
        LEFT JOIN organisations_organisationgeographysubdivided ogd
               ON ee.organisation_geography_id = ogd.organisation_geography_id
    WHERE
        current_status = 'Approved'
        AND ("poll_open_date" >= '{date}' OR "current")
        AND NOT (NOT "current" AND "current" IS NOT NULL)
        AND group_type IS NULL
    """


def handler(event, context):
    db_name = "every_election"
    db_user = "every_election"
    db_port = "5432"

    s3_bucket = "ee.data-cache.production"
    s3_key = "ballots-with-wkt/current_elections.csv"

    delta = datetime.datetime.now() - datetime.timedelta(days=30)

    query = export_sql(delta.date().strftime("%Y-%m-%d"))

    conn = psycopg.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password,
        port=db_port,
    )
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    # Get column headers from the cursor description
    colnames = [desc[0] for desc in cur.description]

    # Write CSV data to an in-memory string buffer
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(colnames)  # Write header row
    csv_writer.writerows(rows)  # Write data rows

    # Upload the CSV data to S3
    s3 = boto3.client("s3")
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

    # Clean up
    cur.close()
    conn.close()

    return {"statusCode": 200, "body": "CSV successfully exported to S3."}
