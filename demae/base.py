import logging
import time
from .logger import setup_logger, setup_boto_logger
from .dest import S3Dest


class Base:
    def __init__(self, **args):
        self.args = args
        setup_boto_logger()
        self.logger = setup_logger(__name__, logging.INFO)

        if not hasattr(self, 'dest'):
            self.dest = S3Dest()

    def run(self, dry=False):
        source_exists = False
        stats = []

        for obj, data in self.source.get(dest=self.dest, **self.args):
            source_exists = True

            self.logger.info("Start transform: %s" % obj.key)
            start_time = time.time()

            transformed = self.transform(data)

            self.logger.info("Finish transform: %s" % obj.key)

            stats.append({
                'time': time.time() - start_time,
                'row': len(data),
            })

            if not dry:
                self.dest.put(transformed, obj)

        if not source_exists:
            raise RuntimeError(
                "source does not exist: `%s`" % self.source.prefix
            )

        self.show_stats(stats)

    def show_stats(self, stats):
        if not stats:
            return

        self.logger.info('')
        self.logger.info('%d rows processed (%d files)' % (
            sum([stat['row'] for stat in stats]),
            len(stats)
        ))

        self.logger.info('transform / file: %.3fs' % (
            sum(stat['time'] for stat in stats) / len(stats)
        ))

        self.logger.info('transform / row: %.3fs' % (
            sum(stat['time'] / stat['row'] for stat in stats) / len(stats)
        ))
