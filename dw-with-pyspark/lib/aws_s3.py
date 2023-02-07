import boto3

bucket_name = "easewithdata"

# Creates S3 Resourse Session
def create_s3_session():
    session = boto3.Session()
    s3 = session.resource('s3')
    return s3

# Creates S3 Resourse Client
def create_s3_client():
    client = boto3.client('s3')
    return client

# List all S3 Resourse in bucket
def list_bucket_objects(bucket_name: str) -> list:
    _bucket = create_s3_session().Bucket(bucket_name)
    return _bucket.objects.all()

# Archives landing files for a dataset and filename
def archive_landing_object(filename: str, dataset: str) -> bool:
    try:
        client = create_s3_client()
        copy_source = {'Bucket': bucket_name, 'Key': f"dw-with-pyspark/landing/{dataset}/{filename}"}
        response = client.copy_object(Bucket = bucket_name, CopySource = copy_source, Key = f"dw-with-pyspark/archive/{dataset}/{filename}")
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            client.delete_object(Bucket = bucket_name, Key = f"dw-with-pyspark/landing/{dataset}/{filename}")
            return True
        else:
            return False
    except Exception as e:
        print(e)
        return False
    
def archive_wildcard_landing_objects(filename: str, dataset: str) -> bool:
    try:
        _file = filename.split("*")[0]
        _bucket = create_s3_session().Bucket(bucket_name)
        _file_list = [i.key for i in _bucket.objects.all() if 'landing' in i.key and _file in i.key]
        #print(_file_list)
        for key in _file_list: 
            client = create_s3_client()
            copy_source = {'Bucket': bucket_name, 'Key': f"{key}"}
            response = client.copy_object(Bucket = bucket_name, CopySource = copy_source, Key = f"{str(key).replace('landing', 'archive')}")
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                client.delete_object(Bucket = bucket_name, Key = f"{key}")
        return True
    except Exception as e:
        print(e)
        return False