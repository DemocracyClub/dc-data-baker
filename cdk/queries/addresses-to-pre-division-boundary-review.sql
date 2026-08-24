UNLOAD (
    WITH
        review AS (
            SELECT
                slug,
                status,
                latest_event,
                consultation_url,
                legislation_title,
                effective_date,
                organisation_name,
                organisation_official_name,
                organisation_gss,
                boundary_review_id,
                organisation_boundary_wkt
            FROM current_pre_division_boundary_reviews
            WHERE
                boundary_review_id = {boundary_review_id}
        ),
        results AS (
            SELECT
                a.uprn,
                a.address,
                a.postcode,
                a.addressbase_source,
                r.boundary_review_id,
                r.consultation_url,
                r.legislation_title,
                r.effective_date,
                r.organisation_name,
                r.organisation_official_name,
                r.organisation_gss
            FROM
                addressbase_partitioned a JOIN review r ON ST_WITHIN (
                    ST_POINT(a.longitude, a.latitude),
				ST_POLYGON(r.organisation_boundary_wkt)
			)
        )
    SELECT
        uprn,
        address,
        postcode,
        addressbase_source,
        boundary_review_id,
        MAP(
            ARRAY['consultation_url', 'legislation_title', 'effective_date', 'organisation_name', 'organisation_official_name', 'organisation_gss'],
            ARRAY[
                consultation_url,
                legislation_title,
                effective_date,
                organisation_name,
                organisation_official_name,
                organisation_gss
            ]
        ) AS boundary_review_details
    FROM results
) TO '$table_full_s3_path' WITH (
    format = 'PARQUET',
    compression = 'SNAPPY'
)
