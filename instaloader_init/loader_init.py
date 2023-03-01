import os
import time
import random
import instaloader

loader = instaloader.Instaloader(storyitem_metadata_txt_pattern='', download_geotags=False)


class SignIn:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password

    def sign_in(self):
        # Login into account
        loader.login(self.username, self.password)


class LoadStoriesOfUser:
    def __init__(self, target: str) -> None:
        self.target = target

    def download_stories_of_target(self):
        # Simulate human behaviour - wait from 3 to 10 seconds
        time.sleep(random.randint(1,10))

        profile = instaloader.Profile.from_username(loader.context, self.target)

        time.sleep(random.randint(1, 10))

        loader.dirname_pattern = f'/home/newuser/work_artem/instagramstories/media/{profile.username}/stories'
        loader.download_stories(userids=[profile.userid])
