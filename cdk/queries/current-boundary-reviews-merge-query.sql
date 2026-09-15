UNLOAD (
    SELECT
    mbr.uprn,
    mbr.address,
    mbr.postcode,
    mbr.addressbase_source,
    mbr.boundary_reviews || pdbr.boundary_reviews as boundary_reviews,
    mbr.first_letter
FROM
    current_mappable_boundary_reviews_joined_to_addressbase mbr
    JOIN current_pre_division_boundary_reviews_joined_to_addressbase pdbr ON mbr.uprn = pdbr.uprn
) TO '$table_full_s3_path' WITH (
	format = 'PARQUET',
	compression = 'SNAPPY',
	partitioned_by = ARRAY [ 'first_letter' ]
)
