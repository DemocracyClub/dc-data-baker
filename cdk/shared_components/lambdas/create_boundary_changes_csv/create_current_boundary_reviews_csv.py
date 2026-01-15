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
    return f"""
    WITH
        review AS (
            SELECT
                obr.id AS boundary_review_id,
                obr.slug,
                obr.status,
                obr.latest_event,
                obr.consultation_url,
                obr.legislation_title,
                obr.effective_date,
                obr.created AS review_created,
                obr.modified AS review_modified,
                o.common_name AS organisation_name,
                o.slug as organisation_slug,
                o.official_name AS organisation_official_name,
                og.gss AS organisation_gss,
                obr.divisionset_id AS new_divisionset_id,
                (
                    SELECT
                        ds.id
                    FROM
                        organisations_organisationdivisionset ds
                    WHERE
                        ds.organisation_id = o.id
                    ORDER BY
                        ds.end_date DESC NULLS LAST
                    LIMIT
                        1
                ) AS old_divisionset_id
            FROM
                organisations_organisationboundaryreview obr
                JOIN organisations_organisation o ON o.id = obr.organisation_id
                JOIN organisations_organisationgeography og ON og.organisation_id = o.id
            WHERE
                obr.id IN (963, 964)
        )
    SELECT
        r.slug,
        r.status,
        r.latest_event,
        r.consultation_url,
        r.legislation_title,
        r.effective_date,
        r.review_created,
        r.review_modified,
        r.organisation_name,
        r.organisation_official_name,
        r.organisation_gss,
        ds.id AS divisionset_id,
        concat('https://s3.eu-west-2.amazonaws.com/', '{ee_public_data_bucket}', '/pmtiles-store/', r.organisation_slug, '_', ds.id, '_', ds.pmtiles_md5_hash, '.pmtiles') AS divisionset_pmtiles_url,
        d.slug AS division_slug,
        d.name AS division_name,
        d.official_identifier AS division_official_identifier,
        st_astext (dgs.geography) AS division_boundary_wkt,
        r.boundary_review_id,
        CASE
            WHEN ds.id = r.old_divisionset_id THEN 'old'
            WHEN ds.id = r.new_divisionset_id THEN 'new'
            ELSE NULL
        END AS divisionset_generation,
        d.division_type AS division_type
    FROM
        review r
        JOIN organisations_organisationdivisionset ds ON ds.id IN (r.old_divisionset_id, r.new_divisionset_id)
        JOIN organisations_organisationdivision d ON d.divisionset_id = ds.id
        JOIN organisations_divisiongeography dg ON dg.division_id = d.id
        JOIN organisations_divisiongeographysubdivided dgs ON dgs.division_geography_id = dg.id
    ORDER BY
        r.review_created DESC,
        r.organisation_name;

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

    # Partition buffers: {(boundary_review_id, divisionset_generation, division_type): StringIO}
    partition_buffers = {}

    for row in rows:
        boundary_review_id = row["boundary_review_id"]
        divisionset_generation = row["divisionset_generation"]
        division_type = row["division_type"]

        key = (boundary_review_id, divisionset_generation, division_type)
        if key not in partition_buffers:
            buf = io.StringIO()
            writer = csv.writer(buf)
            partition_buffers[key] = (buf, writer)
        else:
            buf, writer = partition_buffers[key]

        writer.writerow(row.values())

    s3 = boto3.client("s3")
    for (boundary_review_id, divisionset_generation, division_type), (
        buf,
        writer,
    ) in partition_buffers.items():
        buf.seek(0)
        s3_key = (
            f"{s3_prefix}/boundary_review_id={boundary_review_id}/"
            f"divisionset_generation={divisionset_generation}/division_type={division_type}/part-0000.csv"
        )
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=buf.getvalue())

    cur.close()
    conn.close()

    return {
        "statusCode": 200,
        "body": "Partitioned CSVs successfully exported to S3.",
    }
