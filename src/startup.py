import logging
import logging.config
import os
import warnings

import yaml

import configs


def setup_warnings():
    warnings.simplefilter(action="ignore", category=FutureWarning)
    warnings.simplefilter(action="ignore", category=UserWarning, append=True)
    warnings.simplefilter(action="ignore", category=DeprecationWarning, append=True)


def setup_logger():
    dict_config = yaml.safe_load(open("logging_config.yaml"))
    dict_config["handlers"]["logfile"]["filename"] = f"logs/{configs.STRATEGY}.log"

    if configs.LOG_AWS:
        stream_name = f"{configs.LOG_AWS_PREFIX}{configs.STRATEGY}-{{strftime:%y-%m-%d}}"
        dict_config["handlers"]["watchtower"]["stream_name"] = stream_name
    else:
        del dict_config["handlers"]["watchtower"]
        dict_config["root"]["handlers"].remove("watchtower")
    if not configs.LOG_STDOUT:
        del dict_config["handlers"]["console"]
        dict_config["root"]["handlers"].remove("console")

    dict_config["loggers"]["utils.cache"] = {"level": configs.CACHE_LOG_LEVEL}
    for handler in dict_config["handlers"].values():
        handler["level"] = max(
            logging.getLevelName(configs.MIN_LOG_LEVEL), logging.getLevelName(handler["level"])
        )

    os.makedirs("logs", exist_ok=True)
    logging.config.dictConfig(dict_config)


def setup_ipython():
    try:
        get_ipython()
    except NameError:
        pass
    else:
        import nest_asyncio

        nest_asyncio.apply()


def setup():
    setup_warnings()
    setup_logger()
    setup_ipython()
