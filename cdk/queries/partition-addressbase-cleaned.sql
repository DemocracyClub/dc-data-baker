SELECT
	split_part(postcode, ' ', 1) as outcode,
	uprn,
	address,
	postcode,
	ST_X(ST_GeometryFromText(split_part(location, ';', 2))) as longitude,
	ST_Y(ST_GeometryFromText(split_part(location, ';', 2))) as latitude,
	substr(postcode, 1,1) as first_letter
FROM $$from_table$$
