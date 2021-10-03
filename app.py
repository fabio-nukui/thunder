import importlib
import logging

import configs
from startup import setup

log = logging.getLogger(__name__)

GIT_COMMIT = open('git_commit').read().strip()


def main():
    strategy = importlib.import_module(f'strategies.{configs.STRATEGY}')
    log.info(f'Running on git commit {GIT_COMMIT}')
    while True:
        try:
            log.info(f'Starting strategy {configs.STRATEGY}')
            strategy.run()  # type: ignore
        except Exception as e:
            log.error('Error during strategy execution')
            log.exception(e)
            log.info('Restarting strategy')


if __name__ == '__main__':
    setup()
    main()
