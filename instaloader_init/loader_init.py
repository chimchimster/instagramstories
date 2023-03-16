import os
import time

import executor as executor
import requests
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

    def download_stories_of_target(self, username, password, accounts_counter):
        try:
            # Apply to specific User
            profile = instaloader.Profile.from_username(loader.context, self.target)

            # Simulates human behaviour
            time.sleep(7)

            # Get up on level ../media/ - equivalent of bash 'cd'
            os.chdir('..' + '/media/')

            # Determine pattern where StoryItems have to be saved
            loader.dirname_pattern = os.getcwd() + f'/{profile.username}/stories'

            # This condition handles 401 unauthorized HTTP error
            if accounts_counter % 10 == 0:
                loader.context._session = loader.context.get_anonymous_session()

                new_authorization = SignIn(username, password)
                new_authorization.sign_in()

            time.sleep(8)

            loader.download_stories(userids=[profile.userid])

            time.sleep(15)
        except:
            log.logger.warning(f'Problem downloading {self.target} stories')
            return