UNLOAD (
    WITH
        divisionsets AS (
            SELECT
                division_slug,
                division_official_identifier,
                boundary_review_id,
                division_type,
                division_boundary_wkt,
                divisionset_pmtiles_url,
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
                od.divisionset_pmtiles_url AS old_divisionset_pmtiles_url,
                nd.divisionset_pmtiles_url AS new_divisionset_pmtiles_url,
                od.division_type,
                od.division_slug AS old_division_slug,
                od.division_official_identifier AS old_division_official_identifier,
                nd.division_slug AS new_division_slug,
                nd.division_official_identifier AS new_division_official_identifier
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
                a.old_division_slug,
                COUNT(a.uprn),
                ARRAY_AGG(a.uprn ORDER BY a.uprn) as addresses_agg
            FROM
                addresses a
            GROUP BY a.old_division_slug
        ),
        new_divisions AS (
            SELECT
                a.new_division_slug,
                COUNT(a.uprn),
                ARRAY_AGG(a.uprn ORDER BY a.uprn) as addresses_agg
            FROM
                addresses a
            GROUP BY a.new_division_slug
        ),
        boundaries_the_same AS (
            SELECT
                od.old_division_slug, nd.new_division_slug
            FROM
                old_divisions od JOIN new_divisions nd ON od.addresses_agg = nd.addresses_agg
        ),
        names_the_same AS (
            SELECT
                od.old_division_slug, nd.new_division_slug
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
                a.old_divisionset_pmtiles_url AS old_divisionset_pmtiles_url,
                a.new_divisionset_pmtiles_url AS new_divisionset_pmtiles_url,
        		a.old_division_slug AS old_division_slug,
        		a.old_division_official_identifier AS old_division_official_identifier,
        		a.new_division_slug AS new_division_slug,
        		a.new_division_official_identifier AS new_division_official_identifier,
            bts.old_division_slug IS NOT NULL AS boundary_same,
            nts.old_division_slug IS NOT NULL AS name_same
        FROM addresses a
        LEFT JOIN boundaries_the_same bts
            ON a.old_division_slug = bts.old_division_slug
            AND a.new_division_slug = bts.new_division_slug
        LEFT JOIN names_the_same nts
            ON a.old_division_slug = nts.old_division_slug
            AND a.new_division_slug = nts.new_division_slug
    )
    SELECT
        uprn,
        address,
        postcode,
        addressbase_source,
        division_type,
        boundary_review_id,
        MAP(
            ARRAY['division_type', 'old_division_slug', 'old_division_official_identifier', 'old_divisionset_pmtiles_url', 'new_division_slug', 'new_division_official_identifier', 'new_divisionset_pmtiles_url', 'change_scenario'],
            ARRAY[
                division_type,
                old_division_slug,
                old_division_official_identifier,
                old_divisionset_pmtiles_url,
                new_division_slug,
                new_division_official_identifier,
                new_divisionset_pmtiles_url,
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
