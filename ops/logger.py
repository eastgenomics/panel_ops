import logging.config


def setup_logging(type_logger):
    path_to_logs = "/home/egg-user/panels/panel_logs"

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "normal": {
                "format": "%(asctime)s %(name)-8s %(levelname)s - %(message)s"
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
            }
        }
    })

    return logging.getLogger(type_logger)
