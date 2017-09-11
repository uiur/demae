import pytest
import boto3
import re
import pandas as pd
import gzip
import os
from moto import mock_s3
from botocore.exceptions import ClientError
from unittest.mock import patch, Mock

from demae import Base
from demae.source import S3Source
from demae.dest import S3Dest

BUCKET = 's3-bucket'


def upload_as_gzipped_tsv(key, data):
    df = pd.DataFrame(data)
    tsv = df.to_csv(sep="\t", header=False, index=False)
    f = gzip.compress(tsv.encode())

    s3 = boto3.resource('s3')
    s3.Object(
        BUCKET,
        key,
    ).put(Body=f)


@pytest.fixture
def mock_s3_fixture():
    mock_s3().start()

    yield boto3.resource("s3")

    mock_s3().stop()


@pytest.fixture
def create_buckets():
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=BUCKET)


class Batch(Base):
    source = S3Source(
        bucket=BUCKET,
        prefix='development/foo/foo.tsv',
        columns=['id', 'text'],
        parallel_env={'index': 'PARALLEL_INDEX', 'size': 'PARALLEL_SIZE'},
        target_key_env='SOURCE_KEY_ONLY',
    )

    dest = S3Dest(
        key_map=lambda key: re.sub(r'(.+)/foo/foo.tsv', r'\1/bar/bar.tsv', key)
    )

    def transform(self, data):
        return data


def test_success(mock_s3_fixture, create_buckets):
    s3 = mock_s3_fixture

    source_data = [
        [1, 'foo'],
        [2, 'bar'],
    ]
    upload_as_gzipped_tsv(
        'development/foo/foo.tsv.0000_part_00.gz', source_data
    )

    def transform(data):
        assert data.columns.tolist == ['id', 'text']
        return data

    batch = Batch()
    batch.transform = lambda data: data
    batch.run()

    buf = s3.Object(
        BUCKET,
        'development/bar/bar.tsv.0000_part_00.gz',
    ).get()['Body']
    df = pd.read_csv(buf, sep='\t', header=None, compression='gzip')

    assert df.as_matrix().tolist() == source_data


def test_empty_columns(mock_s3_fixture, create_buckets):
    class Batch(Base):
        source = S3Source(
            bucket=BUCKET,
            prefix='development/foo_input/foo_input.tsv',
        )

        dest = S3Dest()

        def transform(self, data):
            return data

    batch = Batch()

    source_data = [
        [1, 'foo'],
        [2, 'bar'],
    ]
    upload_as_gzipped_tsv(
        'development/foo_input/foo_input.tsv.0000_part_00.gz', source_data
    )

    batch.run()  # success without an error


def test_dry_run(mock_s3_fixture, create_buckets):
    s3 = mock_s3_fixture

    upload_as_gzipped_tsv(
        'development/foo/foo.tsv.0000_part_00.gz',
        [
            [1, 'foo'],
            [2, 'bar'],
        ]
    )

    batch = Batch()
    batch.run(dry=True)

    with pytest.raises(ClientError):
        s3.Object(
            BUCKET,
            'development/bar/bar.tsv.0000_part_00.gz',
        ).get()['Body']


@patch.dict(os.environ, {
    'PARALLEL_SIZE': '4',
    'PARALLEL_INDEX': '2',
})
def test_parallel(mock_s3_fixture, create_buckets):
    s3 = mock_s3_fixture

    for i in range(100):
        upload_as_gzipped_tsv(
            'development/foo/foo.tsv.%04d_part_00.gz' % i,
            [
                [1, 'foo'],
                [2, 'bar'],
            ],
        )

    batch = Batch()
    batch.transform = lambda data: data
    batch.run()

    objects = s3.Bucket(BUCKET).objects.filter(Prefix='development/bar/bar')
    result_keys = [obj.key for obj in objects]

    assert result_keys == [
        'development/bar/bar.tsv.%04d_part_00.gz' % i for i in range(50, 75)
    ]


@patch.dict(os.environ, {
    'SOURCE_KEY_ONLY': (
        'development/foo/foo.tsv.0010_part_00.gz'
        ',development/foo/foo.tsv.0011_part_00.gz'
    )
})
def test_filter_source_key(mock_s3_fixture, create_buckets):
    s3 = mock_s3_fixture

    for i in range(20):
        upload_as_gzipped_tsv(
            'development/foo/foo.tsv.%04d_part_00.gz' % i,
            [
                [1, 'foo'],
                [2, 'bar'],
            ],
        )

    batch = Batch()
    batch.transform = lambda data: data
    batch.run()

    objects = s3.Bucket(BUCKET).objects.filter(Prefix='development/bar/bar')
    result_keys = [obj.key for obj in objects]

    assert result_keys == [
        'development/bar/bar.tsv.0010_part_00.gz',
        'development/bar/bar.tsv.0011_part_00.gz',
    ]


def test_skip_result(mock_s3_fixture, create_buckets):
    s3 = mock_s3_fixture

    for i in range(4):
        upload_as_gzipped_tsv(
            'development/foo/foo.tsv.%04d_part_00.gz' % i,
            [
                [1, 'foo'],
                [2, 'bar'],
            ],
        )

    # upload partial result
    for i in range(2):
        upload_as_gzipped_tsv(
            'development/bar/bar.tsv.%04d_part_00.gz' % i,
            [
                [1, 'foo'],
                [2, 'bar'],
            ],
        )

    mock = Mock(return_value=[])
    batch = Batch()
    batch.transform = mock
    batch.run()

    objects = s3.Bucket(BUCKET).objects.filter(Prefix='development/bar/bar')
    result_keys = [obj.key for obj in objects]

    assert result_keys == [
        'development/bar/bar.tsv.%04d_part_00.gz' % i for i in range(4)
    ]

    assert mock.call_count == 2


def test_batch_without_dest(mock_s3_fixture, create_buckets):
    s3 = mock_s3_fixture

    class Batch(Base):
        source = S3Source(
            bucket=BUCKET,
            prefix='development/foo_input/foo_input.tsv',
        )

        def transform(self, data):
            return data

    source_data = [
        [1, 'foo'],
        [2, 'bar'],
    ]
    upload_as_gzipped_tsv(
        'development/foo_input/foo_input.tsv.0000_part_00.gz', source_data
    )

    batch = Batch()
    batch.run()

    objects = s3.Bucket(BUCKET).objects.filter(
        Prefix='development/foo_output/foo_output'
    )
    result_keys = [obj.key for obj in objects]

    assert result_keys == [
        'development/foo_output/foo_output.tsv.0000_part_00.gz'
    ]


def test_stats(mock_s3_fixture, create_buckets, capsys):
    source_data = [
        [1, 'foo'],
        [2, 'bar'],
    ]
    upload_as_gzipped_tsv(
        'development/foo/foo.tsv.0000_part_00.gz', source_data
    )

    batch = Batch()
    batch.transform = lambda data: data
    batch.run()

    out, err = capsys.readouterr()
    assert re.search('2 rows processed', out)
    assert err == '', 'there is no output to stderr when success'
