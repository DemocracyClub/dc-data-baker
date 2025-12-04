UNLOAD (
    WITH
        divisionsets AS (
            SELECT
                division_slug,
                division_composite_id,
                boundary_review_id,
                division_type,
                division_boundary_wkt,
                divisionset_generation
            FROM current_boundary_changes
            WHERE
                boundary_review_id = {boundary_review_id}
                AND division_type = '{division_type}'
        ),
        old_divisionset AS (
            SELECT * FROM divisionsets WHERE divisionset_generation = 'old'
        ),
        new_divisionset AS (
            SELECT * FROM divisionsets WHERE divisionset_generation = 'new'
        ),
        addresses AS (
            SELECT
                a.uprn,
                a.address,
                a.postcode,
                a.addressbase_source,
                od.boundary_review_id,
                od.division_type,
                od.division_slug AS old_division_slug,
                od.division_composite_id AS old_division_composite_id,
                nd.division_slug AS new_division_slug,
                nd.division_composite_id AS new_division_composite_id
            FROM
                addressbase_partitioned a JOIN old_divisionset od ON ST_WITHIN (
                    ST_POINT(a.longitude, a.latitude),
				ST_POLYGON(od.division_boundary_wkt)
			) JOIN new_divisionset nd ON ST_WITHIN(
			    ST_POINT(a.longitude, a.latitude),
				ST_POLYGON(nd.division_boundary_wkt)
			)
        ),
        old_divisions AS (
            SELECT
                a.old_division_composite_id,
                a.old_division_slug,
                COUNT(a.uprn),
                ARRAY_AGG(a.uprn ORDER BY a.uprn) as addresses_agg
            FROM
                addresses a
            GROUP BY a.old_division_composite_id, a.old_division_slug
        ),
        new_divisions AS (
            SELECT
                a.new_division_composite_id,
                a.new_division_slug,
                COUNT(a.uprn),
                ARRAY_AGG(a.uprn ORDER BY a.uprn) as addresses_agg
            FROM
                addresses a
            GROUP BY a.new_division_composite_id, a.new_division_slug
        ),
        boundaries_the_same AS (
            SELECT
                od.old_division_composite_id, nd.new_division_composite_id
            FROM
                old_divisions od JOIN new_divisions nd ON od.addresses_agg = nd.addresses_agg
        ),
        names_the_same AS (
            SELECT
                od.old_division_composite_id, nd.new_division_composite_id
            FROM
                old_divisions od JOIN new_divisions nd ON od.old_division_slug = nd.new_division_slug
        ),
        results AS (
        	SELECT
        		a.uprn,
        		a.address,
        		a.postcode,
                a.addressbase_source,
                a.division_type,
        		a.boundary_review_id,
        		a.old_division_composite_id AS old_division_composite_id,
        		a.new_division_composite_id AS new_division_composite_id,
            bts.old_division_composite_id IS NOT NULL AS boundary_same,
            nts.old_division_composite_id IS NOT NULL AS name_same
        FROM addresses a
        LEFT JOIN boundaries_the_same bts
            ON a.old_division_composite_id = bts.old_division_composite_id
            AND a.new_division_composite_id = bts.new_division_composite_id
        LEFT JOIN names_the_same nts
            ON a.old_division_composite_id = nts.old_division_composite_id
            AND a.new_division_composite_id = nts.new_division_composite_id
    )
    SELECT
        uprn,
        address,
        postcode,
        addressbase_source,
        division_type,
        boundary_review_id,
        MAP(
            ARRAY['division_type', 'old_division_composite_id', 'new_division_composite_id', 'change_scenario'],
            ARRAY[
                division_type,
                old_division_composite_id,
                new_division_composite_id,
                CASE
                    WHEN boundary_same AND name_same THEN 'NO_CHANGE'
                    WHEN boundary_same AND NOT name_same THEN 'NAME_CHANGED'
                    WHEN NOT boundary_same AND name_same THEN 'BOUNDARY_CHANGED'
                    ELSE 'BOTH_CHANGED'
                END
            ]
        ) AS boundary_change_details
    FROM results
) TO '$table_full_s3_path' WITH (
    format = 'PARQUET',
    compression = 'SNAPPY'
)
