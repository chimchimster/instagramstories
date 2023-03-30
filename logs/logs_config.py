import os
import logging

from logging.handlers import TimedRotatingFileHandler


class LoggerHandle:
    # Set up logger
    insta_story_logger = logging.getLogger('INSTA_STORY_LOGGER')

    @classmethod
    def logger_config(cls):
        cls.insta_story_logger.setLevel(logging.WARNING)

        # os.chdir('./logs')
        #
        # # Set up filehandler for insta_story_logger
        # insta_story_logger_file_handler = logging.FileHandler(os.getcwd() + f'/history/story_logger_{date.today()}.log', mode='a')

        # Set up filehandler for insta_story_logger
        insta_story_logger_file_handler = TimedRotatingFileHandler(os.getcwd() + f'/instagramstories/logs/history/story_logger.log', when='D', interval=1, backupCount=90, encoding='utf-8', delay=False)

        # Set up formater for insta_story_logger
        insta_story_logger_formatter = logging.Formatter('%(name)s - %(asctime)s - %(message)s')

        # Adding formater to handler
        insta_story_logger_file_handler.setFormatter(insta_story_logger_formatter)

        # Adding handler to logger
        cls.insta_story_logger.addHandler(insta_story_logger_file_handler)

    @property
    def logger(self):
        return self.insta_story_logger


log = LoggerHandle()
log.logger_config()