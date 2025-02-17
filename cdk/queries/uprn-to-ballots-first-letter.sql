UNLOAD
(
SELECT combined_results.uprn,
       combined_results.address,
       combined_results.postcode,
       array_sort(array_agg(DISTINCT combined_results.election_id)) AS ballot_ids,
       combined_results.first_letter AS first_letter
    FROM (SELECT ab.uprn,
                 ab.address,
                 ab.postcode,
                 ab.first_letter,
                 cb.election_id
              FROM $from_table cb
                       CROSS JOIN addressbase_partitioned ab
              WHERE ST_CONTAINS(
                      ST_Polygon(cb.geometry),
                      ST_POINT(ab.longitude, ab.latitude)
                    )
                AND cb.source_table = 'Organisation'
                AND ab.first_letter = '{first_letter}'
          UNION
          SELECT ab.uprn,
                 ab.address,
                 ab.postcode,
                 ab.first_letter,
                 cb.election_id
              FROM $from_table cb
                       CROSS JOIN addressbase_partitioned ab
              WHERE ST_CONTAINS(
                      ST_Polygon(cb.geometry),
                      ST_POINT(ab.longitude, ab.latitude)
                    )
                AND cb.source_table = 'Division'
                AND ab.first_letter = '{first_letter}') AS combined_results
    GROUP BY combined_results.uprn, combined_results.address, combined_results.postcode, combined_results.first_letter )
TO '$table_full_s3_path'
WITH (
    format = 'PARQUET', compression = 'SNAPPY', partitioned_by = ARRAY['first_letter']
    )
