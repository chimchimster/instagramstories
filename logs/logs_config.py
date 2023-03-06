import os
import logging


class LoggerHandle:
    # Set up logger
    insta_story_logger = logging.getLogger('INSTA_STORY_LOGGER')

    @classmethod
    def logger_config(cls):
        cls.insta_story_logger.setLevel(logging.WARNING)

        # Change directory to determine where log file must be located
        os.chdir('..' + '/logs')

        # Set up filehandler for insta_story_logger
        insta_story_logger_file_handler = logging.FileHandler(os.getcwd() + '/story_logger.log', mode='w')

        # Set up formater for insta_story_logger
        insta_story_logger_formatter = logging.Formatter('%(name)s - %(asctime)s - %(message)s')

        # Adding formater to handler
        insta_story_logger_file_handler.setFormatter(insta_story_logger_formatter)

        # Adding handler to logger
        cls.insta_story_logger.addHandler(insta_story_logger_file_handler)

    @property
    def logger(self):
        return self.insta_story_logger