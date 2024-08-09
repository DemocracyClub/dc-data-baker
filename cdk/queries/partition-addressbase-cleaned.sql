UNLOAD (
		SELECT split_part(postcode, ' ', 1) AS outcode,
			uprn,
			address,
			postcode,
			ST_X(
				ST_GeometryFromText(
					split_part(
						location,
						';',
						2
					)
				)
			) AS longitude,
			ST_Y(
				ST_GeometryFromText(
					split_part(
						location,
						';',
						2
					)
				)
			) AS latitude,
			substr(postcode, 1, 1) AS first_letter
		FROM addressbase_cleaned_raw
	) TO 's3://pollingstations.private.data/addressbase/development/addressbase_partitioned/' WITH (
		format = 'PARQUET',
		compression = 'SNAPPY',
		partitioned_by = ARRAY [ 'first_letter' ]
	)
