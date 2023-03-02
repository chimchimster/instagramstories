import logging


class LoggerWarn:
    logger = logging.getLogger(__name__)
    stream_formater = logging.Formatter('%(name)s - %(asctime)s - %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.WARNING)
    stream_handler.setFormatter(stream_formater)
    logger.addHandler(stream_handler)