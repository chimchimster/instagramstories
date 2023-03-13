import os
import time
import instaloader

from instagramstories.logs.logs_config import LoggerHandle


log = LoggerHandle()
log.logger_config()

loader = instaloader.Instaloader(storyitem_metadata_txt_pattern='', download_geotags=False)


class SignIn:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password

    def sign_in(self):
        try:
            # Login into account
            loader.login(self.username, self.password)

            print(f'Successfully logged in with account: {self.username}')

        except Exception:
            log.logger.warning(f'Problem with signing to {self.username} account')
            return


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
        except:
            log.logger.warning(f'Problem downloading {self.target} stories')
            return





