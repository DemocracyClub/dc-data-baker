import sqlglot

# SQL example taken from the Athena docs
sql = """
UNLOAD
(
    SELECT split_part(postcode, ' ', 1) AS outcode,
       uprn,
       address,
       postcode,
       ST_X(ST_GeometryFromText(split_part(location, ';', 2))) AS longitude,
       ST_Y(ST_GeometryFromText(split_part(location, ';', 2))) AS latitude,
       substr(postcode, 1, 1) AS first_letter
    FROM 'from_table'
)
TO 's3://foo/table_full_s3_path/'
WITH (
    format = 'PARQUET', compression = 'SNAPPY', partitioned_by = ARRAY['first_letter']
    )

"""

sqlglot.parse_one(sql, dialect="athena")
