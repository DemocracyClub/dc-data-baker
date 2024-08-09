from layers.models import S3Bucket

pollingstations_private_data = S3Bucket(bucket_name="pollingstations.private.data")

BUCKETS = [
    pollingstations_private_data
]
