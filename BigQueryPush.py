from google.cloud import storage

bucket_uri = 'gs://your-bucket/'
bucket_name = 'your-bucket'
bucket_target = 'datasets/data_upload.csv'
local_dataset = 'data/test.csv'


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Upload a CSV to Google Cloud Storage.

    1. Retrieve the target bucket.
    2. Set destination of data to be uploaded.
    3. Upload local CSV.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    # Commence Upload
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


upload_blob(bucket_name, local_dataset, bucket_target)