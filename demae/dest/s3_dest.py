import pandas as pd
import gzip
import boto3
import re


def default_key_map(key):
    return re.sub('_input', '_output',  key)


class S3Dest():
    def __init__(self, key_map=default_key_map):
        self.key_map = key_map

    def skip_keys(self, bucket, source_prefix):
        s3 = boto3.resource('s3')
        objs = s3.Bucket(bucket).objects.filter(Prefix=self.key_map(source_prefix))
        return [obj.key for obj in objs]

    def put(self, data, obj):
        body = self.generate_output_file(data)
        dest_key = self.key_map(obj.key)

        s3 = boto3.resource('s3')
        s3.Object(obj.bucket_name, dest_key).put(Body=body)

    def generate_output_file(self, data):
        df = pd.DataFrame(data)
        tsv = df.to_csv(sep='\t', header=False, index=False)
        return gzip.compress(tsv.encode())
