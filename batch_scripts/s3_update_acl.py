import boto3, botocore

s3: botocore.client.BaseClient = boto3.client('s3')
bucket_nm: str = "your-bucket"
prefix: str = "your-path"

# get the latest partition dt
def get_latest_partition(s3: botocore.client.BaseClient, bucket: str, prefix: str):
    result: object = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    partition_dts: list = [o.get("Prefix") for o in result.get("CommonPrefixes")]
    return max(partition_dts)


def scan_s3_files(s3: botocore.client.BaseClient, bucket: str, prefix: str):
    print(f"Start scanning! - S3 Path: {bucket}/{prefix}")
    paginator: botocore.paginate.Paginator = s3.get_paginator("list_objects_v2")
    pages: botocore.paginate.PageIterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    path_key_list = list()    
    for idx, page in enumerate(pages):
        print(f"Page: {idx}")
        sum_val = 0
        if page.get("Contents"):
            for obj in page["Contents"]:
                sum_val += 1
                path_key_list.append(obj["Key"])
                print(f"Full path {bucket}/{obj['Key']}")
            
    print(f"Files are successfully scanned!")
    return path_key_list


# Remember to have / in the prefix, then the commonprefixes can show the directories under it. 
for key in scan_s3_files(s3, bucket_nm, get_latest_partition(s3, bucket_nm, prefix)):
    client = boto3.client('s3')
    response = client.put_object_acl(
    	ACL = 'bucket-owner-full-control',  
    	Bucket=bucket_nm,
    	Key=key
    )
    print(response)
