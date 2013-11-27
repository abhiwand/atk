#TODO need more functionality like logging to file, setting the log levels, etc.
import logging

levels = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}
try:
    from intel_analytics.config import global_config as conf
    logger_level = levels[conf['py_logger_level'].upper()]
except:
    logger_level = logging.ERROR

logging.basicConfig(level=logger_level)
stdout_logger = logging.getLogger(__name__)

