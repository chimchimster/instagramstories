import os
import time
import instaloader

from instagramstories.logs.logs_config import LoggerHandle


loader = instaloader.Instaloader(storyitem_metadata_txt_pattern='', download_geotags=False)
log = LoggerHandle()
log.logger_config()


class SignIn:
    def __init__(self, username: str, password: str, path_to_session: str = None) -> None:
        self.username = username
        self.password = password
        self.path_to_session = path_to_session

    def sign_in(self):
        try:
            # If its needed load session from /sessions/session.txt
            # loader.load_session_from_file(self.username, self.path_to_session)

            # Login into account
            loader.login(self.username, self.password)
        except Exception:
            log.logger.warning(f'Problem with signing to {self.username} account')
            raise Exception(f'Problem with signing to {self.username} account')


class LoadStoriesOfUser:
    def __init__(self, target: str) -> None:
        self.target = target

    def download_stories_of_target(self):
        try:
            # Apply to specific User
            profile = instaloader.Profile.from_username(loader.context, self.target)

            # Simulates human behaviour
            time.sleep(15)

            # Get up on level ../media/ - equivalent of bash 'cd'
            os.chdir('..' + '/media/')

            # Determine pattern where StoryItems have to be saved
            loader.dirname_pattern = os.getcwd() + f'/{profile.username}/stories'

            time.sleep(15)

            loader.download_stories(userids=[profile.userid])

            time.sleep(15)
        except Exception:
            log.logger.warning(f'Problem downloading {self.target} stories')
            raise print(f'Problem downloading {self.target} stories')





