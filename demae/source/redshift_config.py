import os


class RedshiftConfig:
    def __init__(self, user=None, database=None, host=None, port=None,
                 iam_role=None, access_key_id=None, secret_access_key=None):
        self.user = user or os.environ.get('RSUSER')
        self.database = database
        self.host = host
        self.port = port

        self.iam_role = iam_role
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    @property
    def credential_string(self):
        if self.iam_role:
            return 'aws_iam_role={iam_role}'.format(iam_role=self.iam_role)

        if self.access_key_id and self.secret_access_key:
            return 'aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}'.format(
                access_key_id=self.access_key_id,
                secret_access_key=self.secret_access_key
            )

        raise RuntimeError('s3 credential is required')
