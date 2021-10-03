import logging.config
import os
import warnings

import yaml

import configs


def setup_warnings():
    warnings.simplefilter(action='ignore', category=FutureWarning)
    warnings.simplefilter(action='ignore', category=UserWarning, append=True)
    warnings.simplefilter(action='ignore', category=DeprecationWarning, append=True)


def setup_logger():
    dict_config = yaml.safe_load(open('logging_config.yaml'))
    dict_config['handlers']['logfile']['filename'] = f'logs/{configs.STRATEGY}.log'

    if configs.LOG_AWS:
        dict_config['handlers']['watchtower']['stream_name'] = \
            f'{configs.STRATEGY}-{{strftime:%y-%m-%d}}'
    else:
        del dict_config['handlers']['watchtower']
        dict_config['root']['handlers'].remove('watchtower')

    dict_config['loggers']['utils.cache'] = {'level': configs.CACHE_LOG_LEVEL}

    os.makedirs('logs', exist_ok=True)
    logging.config.dictConfig(dict_config)


def setup():
    setup_warnings()
    setup_logger()
