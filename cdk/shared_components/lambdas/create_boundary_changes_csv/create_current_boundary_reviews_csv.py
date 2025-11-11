import csv
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


def export_sql():
    return """
    SELECT
        obr.id AS review_id,
        obr.slug,
        obr.status,
        obr.latest_event,
        obr.consultation_url,
        obr.legislation_title,
        obr.effective_date,
        obr.created AS review_created,
        obr.modified AS review_modified,
        o.common_name AS organisation_name,
        o.official_name AS organisation_official_name,
        og_main.gss AS organisation_gss,
        st_astext(ogs.geography) AS organisation_boundary_wkt
    FROM organisations_organisationboundaryreview obr
    LEFT JOIN organisations_organisation o ON o.id = obr.organisation_id
    LEFT JOIN organisations_organisationgeography og_main ON og_main.organisation_id = o.id
    LEFT JOIN organisations_organisationgeographysubdivided ogs ON ogs.organisation_geography_id = og_main.id
    WHERE obr.id IN (963, 964)
    ORDER BY obr.created DESC, o.common_name;

    """


def handler(event, context):
    db_name = "every_election"
    db_user = "every_election"
    db_port = "5432"

    s3_bucket = "dc-data-baker-results-bucket"
    s3_key = "current_boundary_reviews-with-wkt/current_boundary_reviews.csv"

    query = export_sql()

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

    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerows(rows)  # Write data rows

    s3 = boto3.client("s3")
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

    cur.close()
    conn.close()

    return {"statusCode": 200, "body": "CSV successfully exported to S3."}
