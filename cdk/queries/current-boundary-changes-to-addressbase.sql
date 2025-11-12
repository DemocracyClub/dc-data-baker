UNLOAD (
		SELECT results.uprn,
			results.address,
			results.postcode,
			results.addressbase_source,
			array_sort(filter(array_agg(DISTINCT results.review_id), x -> x IS NOT NULL)) AS boundary_review_ids,
			'PLACEHOLDER' AS change_scenario, -- TODO: populate with actual change scenario
			results.first_letter AS first_letter
		FROM (
				SELECT ab.uprn,
					ab.address,
					ab.postcode,
					ab.first_letter,
					ab.addressbase_source,
					cbc.review_id
				FROM addressbase_partitioned ab
					LEFT JOIN current_boundary_changes cbc ON ST_CONTAINS(
						ST_Polygon(cbc.organisation_boundary_wkt),
						ST_POINT(ab.longitude, ab.latitude)
					)

				WHERE ab.first_letter = '{first_letter}'
			) AS results
		GROUP BY results.uprn,
			results.address,
			results.postcode,
			results.addressbase_source,
			results.first_letter
	) TO '$table_full_s3_path' WITH (
		format = 'PARQUET',
		compression = 'SNAPPY',
		partitioned_by = ARRAY [ 'first_letter' ]
	)
