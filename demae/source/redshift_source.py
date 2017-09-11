import psycopg2 as pg
import re
from inspect import cleandoc
from . import S3Source as Source
from .redshift_config import RedshiftConfig


class RedshiftSource:
    """
    RedshiftSource(
        sql="select upload_id, photo_id from photo_uploads;",
        prefix='development/input/2016-11-01/input.tsv.',
        columns=['upload_id', 'photo_id'],
        bucket='bucket',
        config=RedshiftConfig(
            user='kazatosugimoto',  # default: $RSUSER
            iam_role='arn:aws:iam::~~'
        ),
    )
    """

    def __init__(self, sql=None, prefix=None, bucket=None, columns=[], config=RedshiftConfig()):
        self.sql = sql
        self.prefix = prefix
        self.bucket = bucket

        self.config = config

        self.s3_source = Source(
            bucket=self.bucket,
            prefix=self.prefix,
            columns=columns
        )

    def get(self, **args):
        self.execute_unload()

        return self.s3_source.fetch(**args)

    def execute_unload(self):
        conn = pg.connect(
            dbname=self.config.database,
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
        )

        cur = conn.cursor()

        dest_file = 's3://{bucket}/{prefix}'.format(
            bucket=self.bucket,
            prefix=self.prefix
        )

        statement = self.__unload_statement(sql=self.sql, dest_file=dest_file)

        print(statement)
        cur.execute(statement)

        cur.close()
        conn.close()

    def __unload_statement(self, sql=None, dest_file=None):
        return cleandoc("""
            unload ('
            {sql}
            ')
            to '{dest_file}'
            credentials '{credential_string}'
            delimiter '\\t'
            gzip
            allowoverwrite
            ;
        """).format(
            sql=cleandoc(self.__escape_query(sql)),
            dest_file=dest_file,
            credential_string=self.config.credential_string
        )

    @staticmethod
    def __escape_query(query):
        return re.sub("'", "\\'", query)
