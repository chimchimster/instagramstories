import time
import random
import instaloader

from instagramstories.logs.logger_init import LoggerWarn

class ControlRateLimit(instaloader.RateController):
    def sleep(self, secs: float = 21):
        raise logger_warn.logger.exception('There is a 429 error occurred')

    def query_waittime(self, query_type, current_time, untracked_queries=False):
        return 5 + super().query_waittime(query_type, current_time, untracked_queries)


logger_warn = LoggerWarn()
loader = instaloader.Instaloader(rate_controller=lambda ctx: ControlRateLimit(ctx),
                                 storyitem_metadata_txt_pattern='',
                                 download_geotags=False,
                                 )


class SignIn:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password

    def sign_in(self):
        try:
            # Login into account
            loader.login(self.username, self.password)
        except logger_warn.logger.exception(instaloader.InvalidArgumentException, instaloader.BadCredentialsException, instaloader.ConnectionException):
            ...

class LoadStoriesOfUser:
    def __init__(self, target: str) -> None:
        self.target = target

    def download_stories_of_target(self):
        try:
            # Apply to specific User
            profile = instaloader.Profile.from_username(loader.context, self.target)

            # Determine pattern where StoryItems have to be saved
            loader.dirname_pattern = f'/home/newuser/work_artem/instagramstories/media/{profile.username}/stories'

            loader.download_stories(userids=[profile.userid])
        except logger_warn.logger.exception(instaloader.ConnectionException):
            pass








