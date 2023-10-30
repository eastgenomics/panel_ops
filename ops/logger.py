import logging.config

from ops.config import path_to_logs


def setup_logging(type_logger):
    """ Return appropriate logger given name of logger

    Args:
        type_logger (str): Name of logger to be retrived

    Returns:
        logger: Logger object
    """

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "normal": {
                "format": "%(asctime)s %(name)s %(levelname)s - %(message)s"
            },
            "brief": {
                "format": "%(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "brief",
                "stream": "ext://sys.stdout"
            },
            "generate_file": {
                "level": "DEBUG",
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "normal",
                "filename": f"{path_to_logs}/panel_generation.log",
                "mode": "a",
                "maxBytes": 10000000,
                "backupCount": 5,
            },
            "check_file": {
                "level": "DEBUG",
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "normal",
                "filename": f"{path_to_logs}/panel_checking.log",
                "mode": "a",
                "maxBytes": 10000000,
                "backupCount": 5,
            },
            "mod_db_file": {
                "level": "DEBUG",
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "normal",
                "filename": f"{path_to_logs}/panel_mod_db.log",
                "mode": "a",
                "maxBytes": 10000000,
                "backupCount": 5,
            },
            "utils_file": {
                "level": "DEBUG",
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "normal",
                "filename": f"{path_to_logs}/panel_utils.log",
                "mode": "a",
                "maxBytes": 10000000,
                "backupCount": 5,
            },
        },
        "loggers": {
            "normal_ops": {
                "level": "INFO",
                "handlers": ["console"]
            },
            "generation": {
                "level": "DEBUG",
                "handlers": ["generate_file"]
            },
            "check": {
                "level": "DEBUG",
                "handlers": ["check_file"]
            },
            "mod_db": {
                "level": "DEBUG",
                "handlers": ["mod_db_file"]
            },
            "utils": {
                "level": "DEBUG",
                "handlers": ["utils_file"]
            }
        }
    })

    return logging.getLogger("normal_ops"), logging.getLogger(type_logger)


def output_to_loggers(msg: str, level: str, *loggers):
    """ Add msgs to the all the loggers given
    Args:
        msg (str): Message to add for all the loggers given
    """

    for logger in loggers:
        if level == "info":
            logger.info(msg)
        elif level == "warning":
            logger.warning(msg)
