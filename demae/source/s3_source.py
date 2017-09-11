import pandas as pd
import numpy as np
import boto3
from pandas.io.common import EmptyDataError
import os
from ..util import split_size


class S3Source:
    def __init__(
        self,
        bucket,
        prefix,
        columns=[],
        parallel_env=None,
        target_key_env=None,
    ):
        self.bucket = bucket
        self.prefix = prefix
        self.columns = columns
        self.parallel_env = parallel_env
        self.target_key_env = target_key_env

    def get(self, dest=None, **args):
        formatted_prefix = self.formatted_prefix(**args)
        objects = list(self.get_objects(formatted_prefix))
        objects = self.__select_target_objects(objects)

        if dest:
            skip_keys = dest.skip_keys(
                self.bucket,
                formatted_prefix
            )
            objects = [obj for obj in objects if dest.key_map(obj.key) not in skip_keys]

        for obj in objects:
            buf = obj.get()['Body']

            try:
                data = pd.read_csv(buf, sep='\t', compression='gzip', header=None)
            except EmptyDataError:
                continue

            if len(self.columns) > 0:
                data.columns = self.columns

            if len(data.shape) == 1:
                data = np.expand_dims(data, axis=0)

            yield (obj, data)

    def get_objects(self, prefix):
        s3 = boto3.resource('s3')
        return s3.Bucket(self.bucket).objects.filter(Prefix=prefix)

    def formatted_prefix(self, **args):
        return self.prefix.format(**args)

    def __select_target_objects(self, objects):
        objects = self.__filter_objects_if_parallel(objects)
        objects = self.__filter_objects_if_keys_specified(objects)

        return objects

    def __filter_objects_if_parallel(self, objects):
        if not self.parallel_env:
            return objects

        if os.getenv(self.parallel_env['size']):
            parallel_count = int(os.getenv(self.parallel_env['size']))
            if os.getenv(self.parallel_env['index']) is None:
                raise RuntimeError('environment variable `%s` must be specified' % self.parallel_env['index'])

            parallel_index = int(os.getenv(self.parallel_env['index']))
            objects = split_size(objects, parallel_count)[parallel_index]

        return objects

    def __filter_objects_if_keys_specified(self, objects):
        if not self.target_key_env:
            return objects

        keys_env = os.getenv(self.target_key_env)
        if keys_env:
            keys = [key.strip() for key in keys_env.split(',')]
            objects = [obj for obj in objects if obj.key in keys]

        return objects
