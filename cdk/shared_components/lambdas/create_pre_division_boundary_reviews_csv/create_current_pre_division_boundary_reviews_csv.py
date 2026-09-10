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

ee_public_data_bucket = "ee.public.data"


def export_sql():
    return """
    WITH
        reviews AS (
            SELECT
                obr.*,
                COALESCE(obr.effective_date, CURRENT_DATE) AS comparison_date
            FROM
                organisations_organisationboundaryreview obr
        ),
        org_geographies AS (
            SELECT
                og.*,
                COALESCE(og.start_date, o.start_date) AS active_from,
                COALESCE(og.end_date, o.end_date) AS active_until
            FROM
                organisations_organisationgeography og
                JOIN organisations_organisation o ON o.id = og.organisation_id
        ),
        review as (
            SELECT
                obr.id AS boundary_review_id,
                obr.slug,
                obr.status AS scraped_status,
                obr.public_visibility AS dc_stage,
                obr.latest_event,
                obr.consultation_url,
                obr.legislation_title,
                obr.effective_date,
                obr.created AS review_created,
                obr.modified AS review_modified,
                o.common_name AS organisation_name,
                obr.organisation_id AS organisation_id,
                o.slug as organisation_slug,
                o.official_name AS organisation_official_name,
                og.gss AS organisation_gss
            FROM
                reviews obr
                JOIN organisations_organisation o ON o.id = obr.organisation_id
                JOIN org_geographies og ON og.organisation_id = o.id
                AND og.active_from < obr.comparison_date
                AND (
                    og.active_until IS NULL
                    OR og.active_until > obr.comparison_date
                )
            WHERE
                obr.public_visibility = 'CONSULTATION'
                AND NOT EXISTS (
                    SELECT
                        e.election_id
                    FROM
                        elections_election e
                    WHERE
                        e.current_status = 'Approved'
                        AND NOT e.cancelled
                        AND e.current IS NOT TRUE
                        AND e.organisation_id = o.id
                        AND e.poll_open_date >= obr.effective_date
                        AND e.poll_open_date <= CURRENT_DATE - INTERVAL '20 days'
                    LIMIT
                        1
                )
        )
    SELECT
        r.slug,
        r.scraped_status,
        r.dc_stage,
        r.latest_event,
        r.consultation_url,
        r.legislation_title,
        r.effective_date,
        r.review_created,
        r.review_modified,
        r.organisation_name,
        r.organisation_official_name,
        r.organisation_gss,
        st_astext (ogs.geography) AS organisation_boundary_wkt,
        r.boundary_review_id
    FROM
        review r
        JOIN organisations_organisationgeography og ON og.organisation_id = r.organisation_id
        JOIN organisations_organisationgeographysubdivided ogs ON ogs.organisation_geography_id = og.id
    ORDER BY
        r.review_created DESC,
        r.organisation_name
"""


def handler(event, context):
    db_name = "every_election"
    db_user = "every_election"
    db_port = "5432"

    s3_bucket = event["s3_bucket"]
    s3_prefix = event["s3_prefix"]

    query = export_sql()

    conn = psycopg.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password,
        port=db_port,
    )
    cur = conn.cursor(row_factory=psycopg.rows.dict_row)
    cur.execute(query)
    rows = cur.fetchall()

    # Partition buffers: {(boundary_review_id): StringIO}
    partition_buffers = {}

    for row in rows:
        boundary_review_id = row["boundary_review_id"]

        key = boundary_review_id
        if key not in partition_buffers:
            buf = io.StringIO()
            writer = csv.writer(buf)
            partition_buffers[key] = (buf, writer)
        else:
            buf, writer = partition_buffers[key]

        writer.writerow(row.values())

    s3 = boto3.client("s3")
    for (boundary_review_id), (buf, writer) in partition_buffers.items():
        buf.seek(0)
        s3_key = (
            f"{s3_prefix}/boundary_review_id={boundary_review_id}/part-0000.csv"
        )
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=buf.getvalue())

    cur.close()
    conn.close()

    return {
        "statusCode": 200,
        "body": "Partitioned CSVs successfully exported to S3.",
    }
