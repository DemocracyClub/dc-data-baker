UNLOAD (
	WITH all_addresses AS (
		SELECT
			uprn,
			address,
			postcode,
			first_letter,
			addressbase_source
		FROM addressbase_partitioned
	),
	grouped_by_review AS (
		SELECT
			uprn,
			boundary_review_id,
			map_agg(division_type, boundary_change_details) AS division_map
		FROM addresses_to_boundary_change
		GROUP BY uprn, boundary_review_id
	),
	aggregated_reviews AS (
		SELECT
			uprn,
			map_agg(boundary_review_id, division_map) AS boundary_review_ids
		FROM grouped_by_review
		GROUP BY uprn
	)
	SELECT
		aa.uprn,
		aa.address,
		aa.postcode,
		aa.addressbase_source,
		COALESCE(ar.boundary_review_ids, MAP()) AS boundary_review_ids,
		aa.first_letter
	FROM all_addresses aa
	LEFT JOIN aggregated_reviews ar ON aa.uprn = ar.uprn
) TO '$table_full_s3_path' WITH (
	format = 'PARQUET',
	compression = 'SNAPPY',
	partitioned_by = ARRAY [ 'first_letter' ]
)
