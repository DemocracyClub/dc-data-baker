SELECT
    	combined_results.uprn,
    	combined_results.address,
    	combined_results.postcode,
    	array_sort(array_agg(distinct combined_results.election_id)) AS ballot_ids,
    	combined_results.first_letter as first_letter
    FROM (
    	SELECT
        	ab.uprn,
        	ab.address,
        	ab.postcode,
        	ab.first_letter,
        	cb.election_id
    	FROM
        	$$from_table$$ cb
    	CROSS JOIN addressbase_partitioned ab
    	WHERE
        	ST_CONTAINS(
            	ST_Polygon(cb.geometry),
            	ST_POINT(ab.longitude, ab.latitude)
        	)
        	AND cb.source_table = 'Organisation'
        UNION
    	SELECT
        	ab.uprn,
        	ab.address,
        	ab.postcode,
            ab.first_letter,
        	cb.election_id
    	FROM
        	$$from_table$$ cb
    	CROSS JOIN addressbase_partitioned ab
    	WHERE
        	ST_CONTAINS(
            	ST_Polygon(cb.geometry),
            	ST_POINT(ab.longitude, ab.latitude)
        	)
        	AND cb.source_table = 'Division'
    ) AS combined_results
    GROUP BY
    	combined_results.uprn, combined_results.address, combined_results.postcode, combined_results.first_letter
