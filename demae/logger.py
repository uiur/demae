import logging
import sys
from copy import deepcopy


# logger sends log to stdout
def setup_logger(name, level):
    format_string = "%(asctime)s %(name)s [%(levelname)s] %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def setup_boto_logger():
    logger = setup_logger('boto3.resources.action', logging.INFO)

    class ObjectBodyFilter(logging.Filter):
        def filter(self, record):
            record = deepcopy(record)

            if len(record.args) == 3:
                _, action, params = record.args
                if action == 'put_object' and 'Body' in params:
                    del params['Body']
                    logger.handle(record)

                    return False

            return True

    logger.addFilter(ObjectBodyFilter())

    return logger
