import pytest
from demae import RedshiftConfig


def test_credential_string():
    config = RedshiftConfig(
        iam_role="arn:aws:iam::0123456789:role/Developers",
    )

    assert config.credential_string == 'aws_iam_role=arn:aws:iam::0123456789:role/Developers'

    config = RedshiftConfig(
        access_key_id='foo',
        secret_access_key='bar'
    )

    assert config.credential_string == 'aws_access_key_id=foo;aws_secret_access_key=bar'

    config = RedshiftConfig()
    with pytest.raises(RuntimeError):
        config.credential_string
