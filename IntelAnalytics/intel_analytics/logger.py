#TODO need more functionality like logging to file, setting the log levels, etc.
import logging

logging.basicConfig(level=logging.ERROR)
stdout_logger = logging.getLogger(__name__)
