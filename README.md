# demae
demae is a framework to build a batch program using Machine Learning.
Makes it easier to deploy your ML model into production.

Main features:

- handle data source and destination easily
- support parallel execution
- print stats of execution time

This example is to fetch input from S3, transform it and push output to S3.

`S3 -> transform -> S3`

```python
from demae import Base
from demae.source import S3Source
from demae.dest import S3Dest

"""
requires `source`, `dest` and `transform` to be implemented
"""
class Batch(Base):
    """
    Set data source

    This reads input from files with the prefix in `redshift-copy-buffer` bucket.
    Input files must be in tsv format.
    """
    source = S3Source(
        bucket='bucket',
        prefix='{env}/example_input/{date}/example_input.tsv',
        columns=['id', 'text'],
    )

    """
    Specify output destination in s3.

    key_map : a function (input key -> output key)

    This example maps input:
      from: development/example_input/2017-12-24/example_input.0000_part_00.gz
      to:   development/example_output/2017-12-24/example_output.0000_part_00.gz
    """
    dest = S3Dest(
        key_map=lambda key: re.sub('_input', '_output', key)
    )

    """
    Write your inference code here
    data : pandas DataFrame
        columns is automatically set from source.columns.
    must returns array-like objects (DataFrame, numpy array or list)
    """
    def transform(self, data):
        output = predict(data[:, 'text'])
        return output

```

To run:

```python
batch = Batch(
  env='development',
  date='2017-02-13'
)
batch.run()
```

## Parallel execution
Parallel execution is supported by providing environment variables that are specified in `parallel_env`.

A batch handles only a corresponding part of input.


```python
source = S3Source(
    bucket='bucket',
    prefix='development/foo/foo.tsv',
    columns=['id', 'text'],
    parallel_env={'index': 'PARALLEL_INDEX', 'size': 'PARALLEL_SIZE'},
)
```

For example,
input files: `input.tsv.part0` `input.tsv.part1` `input.tsv.part2`

When `PARALLEL_INDEX=1` and `PARALLEL_SIZE=3` are provided, it handles only `input.tsv.part1`.


## License

MIT

This software is developed while working for [Cookpad Inc.](https://github.com/cookpad)
