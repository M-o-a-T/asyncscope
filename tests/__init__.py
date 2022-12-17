_cfg = {  # a magic incantation
    "version": 1,
    # "loggers": {"asyncserf": {"level": "INFO"}, "xknx.raw_socket": {"level": "INFO"}},
    "root": {"handlers": ["stderr"], "level": "INFO"},
    "handlers": {
        #           "logfile": {
        #               "class": "logging.FileHandler",
        #               "filename": "test.log",
        #               "level": "DEBUG",
        #               "formatter": "std",
        #           },
        "stderr": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "std",
            "stream": "ext://sys.stderr",
        }
    },
    "formatters": {
        "std": {
            "class": "moat.util.TimeOnlyFormatter",
            "format": "%(asctime)s %(levelname)s:%(name)s:%(message)s",
        }
    },
    "disable_existing_loggers": False,
}
from logging.config import dictConfig as _config

_config(_cfg)


class Stepper:
    def __init__(self):
        self.value = 0

    def __call__(self, value):
        if self.value + 1 != value:
            raise RuntimeError(f"Current {self.value}, next {value}")
        self.value = value
