# flake8: noqa
from .base import Base
from .source import S3Source as Source
from .source import RedshiftSource
from .source import RedshiftConfig

__version__ = '0.8.1'
