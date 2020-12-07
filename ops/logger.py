import logging.config

from .config import path_to_logs


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
            "generation": {
                "level": "DEBUG",
                "handlers": ["console", "generate_file"]
            },
            "check": {
                "level": "DEBUG",
                "handlers": ["console", "check_file"]
            },
            "mod_db": {
                "level": "DEBUG",
                "handlers": ["console", "mod_db_file"]
            },
            "utils": {
                "level": "DEBUG",
                "handlers": ["console", "utils_file"]
            }
        }
    })

    return logging.getLogger(type_logger)