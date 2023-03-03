import time
import instaloader

loader = instaloader.Instaloader(storyitem_metadata_txt_pattern='', download_geotags=False)


class SignIn:
    def __init__(self, username: str, password: str, path_to_session: str = None) -> None:
        self.username = username
        self.password = password
        self.path_to_session = path_to_session

    def sign_in(self):
        try:
            # Loading session from /sessions/session.txt
            # loader.load_session_from_file(self.username, self.path_to_session)

            # Login into account
            loader.login(self.username, self.password)
        except Exception:
            print(f'Problem with signing to {self.username} account')


class LoadStoriesOfUser:
    def __init__(self, target: str) -> None:
        self.target = target

    def download_stories_of_target(self):
        try:
            # Apply to specific User
            profile = instaloader.Profile.from_username(loader.context, self.target)

            # Simulates human behaviour
            time.sleep(15)

            # Determine pattern where StoryItems have to be saved
            loader.dirname_pattern = f'/home/newuser/work_artem/instagramstories/media/{profile.username}/stories'

            time.sleep(15)

            loader.download_stories(userids=[profile.userid])
        except Exception:
            print(f'Problem downloading {self.target} stories')





# from instagramstories.logs.logger_init import LoggerWarn
#
# class ControlRateLimit(instaloader.RateController):
#     def sleep(self, secs: float = 21):
#         raise logger_warn.logger.exception('There is a 429 error occurred')
#
#     def query_waittime(self, query_type, current_time, untracked_queries=False):
#         return 5 + super().query_waittime(query_type, current_time, untracked_queries)


# logger_warn = LoggerWarn()




