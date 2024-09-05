from layers.models import S3Bucket

pollingstations_private_data = S3Bucket(bucket_name="pollingstations.private.data")

data_baker_results_bucket = S3Bucket(bucket_name="dc-data-baker-results-bucket")

BUCKETS = [
    pollingstations_private_data,
    data_baker_results_bucket
]
