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
       -- This is one row per (uprn, review_id) with a list of changes for that review.
       -- number of changes corresponds to number of division types affected by the review.
       -- boundary_review_details is the same for all rows with the same boundary_review_id,
       -- so we use arbitrary() to pick one value.
		SELECT
			uprn,
			boundary_review_id,
            arbitrary(boundary_review_details) AS boundary_review_details,
            array_agg(boundary_change_details) AS boundary_changes
		FROM addresses_to_boundary_change
		GROUP BY uprn, boundary_review_id
	),
	aggregated_reviews AS (
       -- This is one row per uprn with a list of reviews for that uprn.
		SELECT
			uprn,
            array_agg(
                   '{{"boundary_review_id":' || json_format(CAST(boundary_review_id AS JSON)) ||
                   ',"boundary_review_details":' || json_format(CAST(boundary_review_details AS JSON)) ||
                   ',"changes":' || json_format(CAST(boundary_changes AS JSON)) || '}}'
            ) AS boundary_reviews
		FROM grouped_by_review
		GROUP BY uprn
	)
	SELECT
		aa.uprn,
		aa.address,
		aa.postcode,
		aa.addressbase_source,
        COALESCE(ar.boundary_reviews, ARRAY[]) AS boundary_reviews,
		aa.first_letter
	FROM all_addresses aa
	LEFT JOIN aggregated_reviews ar ON aa.uprn = ar.uprn
) TO '$table_full_s3_path' WITH (
	format = 'PARQUET',
	compression = 'SNAPPY',
	partitioned_by = ARRAY [ 'first_letter' ]
)
