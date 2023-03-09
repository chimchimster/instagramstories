import os
import logging

from datetime import date


class LoggerHandle:
    # Set up logger
    insta_story_logger = logging.getLogger('INSTA_STORY_LOGGER')

    @classmethod
    def logger_config(cls):
        cls.insta_story_logger.setLevel(logging.WARNING)

        # Set up filehandler for insta_story_logger
        insta_story_logger_file_handler = logging.FileHandler(os.getcwd() + f'/logs/history/story_logger_{date.today()}.log', mode='a')

        # Set up formater for insta_story_logger
        insta_story_logger_formatter = logging.Formatter('%(name)s - %(asctime)s - %(message)s')

        # Adding formater to handler
        insta_story_logger_file_handler.setFormatter(insta_story_logger_formatter)

        # Adding handler to logger
        cls.insta_story_logger.addHandler(insta_story_logger_file_handler)

    @property
    def logger(self):
        return self.insta_story_logger