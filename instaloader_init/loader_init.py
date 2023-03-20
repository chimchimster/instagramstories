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
        """ Signing to instagram account """
        try:
            # Login into account
            loader.login(self.username, self.password)

            log.logger.warning(f'Successfully logged in with account: {self.username}')
        except Exception as e:
            log.logger.warning(e)
            log.logger.warning(f'Problem with signing to {self.username} account')
            return


class LoadStoriesOfUser:
    def __init__(self, target: str) -> None:
        self.target = target

    def download_stories_of_target(self, username: str, password: str, accounts_counter: int):
        """ Downloading stories of particular account.
            NOTE!
            accounts_counter is used for re-login each 10 parsed accounts
            with old credentials but new anonymous session
            otherwise you will meet 401 unauthorized HTTP error"""

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
            try:
                loader.download_stories(userids=[profile.userid])
                log.logger.warning(f'Stories of {self.target} successfully downloaded')
            except Exception as e:
                log.logger.warning(e)
                log.logger.warning(f'Problem downloading {self.target} stories')

            time.sleep(15)
        except Exception as e:
            log.logger.warning(e)

            return