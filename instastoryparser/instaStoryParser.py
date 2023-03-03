import os

from instagramstories.db_init.database import DataBase
from instagramstories.accounts import get_accs, get_cred
from instagramstories.imagehandling import imagehandle
from instagramstories.instaloader_init import loader_init
from instagramstories.logs.logger_init import Logging

PATH_TO_ACCOUNTS = '/home/newuser/work_artem/instagramstories/accounts/account_for_parse.txt'
PATH_TO_CREDENTIALS = '/home/newuser/work_artem/instagramstories/accounts/credentials.txt'


def parse_instagram_stories():
    db = DataBase('stories')

    # Creates database
    db.create_db()

    # Creating table accounts
    db.create_table('accounts',
                    'account_id int PRIMARY KEY AUTO_INCREMENT',
                    'account VARCHAR(255) NOT NULL',
                    )

    # Creating table attachments
    db.create_table('attachments',
                    'record_id int PRIMARY KEY AUTO_INCREMENT',
                    'account_id int NOT NULL',
                    'type int NOT NULL',
                    'path TEXT NOT NULL',
                    )

    # Creating table credentials
    db.create_table('credentials',
                    'credentials_id int PRIMARY KEY AUTO_INCREMENT',
                    'login VARCHAR(255)',
                    'password VARCHAR(255)',
                    'session VARCHAR(255)',
                    'status VARCHAR(255)',
                    )

    # Fill table credentials with initial data
    try:
        db.send_to_table('credentials', ('login', 'password', 'session', 'status'), get_cred.get_credentials(PATH_TO_CREDENTIALS))
    except Exception:
        print('There is no accounts to send!')

    # Fill table accounts with initial data
    try:
        db.send_to_table('accounts', ('account',), get_accs.get_accounts(PATH_TO_ACCOUNTS))
    except Exception:
        print('There is no accounts to send!')

    def login_handle():
        # Get length of table Credentials which elements' status is "stream_"
        table_length = db.get_length_of_table('credentials', 'credentials_id')

        # Get random credential from database
        credential = db.get_account_credentials('credentials')

        while len(used_credentials) < table_length[0]:
            if credential not in used_credentials:
                print('NOW I USE THIS CREDENTIAL', credential)
                # Mark used credential
                used_credentials.add(credential)
                username, password, session = credential

                def update_session_file(session):
                    os.chdir('..' + '/sessions/')
                    path_to_session = os.getcwd() + '/session.txt'
                    with open(path_to_session, 'w') as path:
                        path.write(session)
                    return path_to_session

                path_to_session = update_session_file(session)

                try:
                    # Login into account
                    login(username, password, path_to_session)

                    # Collect StoryItems while being logged-in
                    collect_data()
                except:
                    continue

            else:
                credential = db.get_account_credentials('credentials')

    def login(username, password, path_to_session):
        try:
            # Trying to sign in into user's account
            signin = loader_init.SignIn(username, password, path_to_session)
            signin.sign_in()
        except Exception:
            raise print(f'Account {username} might be restricted')


    def collect_data():
        # Data which must be sent to database
        data_to_db = {}

        # Get accounts for parse from database
        instagram_accounts = [account[0] for account in db.get_data_for_parse('account', 'accounts')]

        if not instagram_accounts:
            raise Exception('There is no account to parse!')

        accounts_counter = 0
        for account in instagram_accounts:
            if account not in used_accounts:
                print(account, accounts_counter)

                # Add account to set of used_accounts
                used_accounts.add(account)

                directory_of_account = f'/{account}/stories'
                try:
                    # Collect stories from account
                    user = loader_init.LoadStoriesOfUser(account)
                    user.download_stories_of_target()
                except Exception:
                    print(f'There is an error while loading data from {account}')

                try:
                    # Changing directory to media
                    os.chdir('..' + '/media')

                    # Trying to drag text from photos
                    text_files = imagehandle.ImageHandling(os.getcwd() + directory_of_account)
                    text_files.create_txt_files()
                except Exception:
                    print(f'{account} has no text on photo to drag it')

                try:
                    if account not in data_to_db:
                        data_to_db[account] = {'path_video': [], 'path_photo': [], 'path_text': []}
                        for file in os.listdir(os.getcwd() + directory_of_account):
                            if file.endswith('.txt'):
                                data_to_db[account]['path_text'].append(os.getcwd() + directory_of_account + file)
                            elif file.endswith('.mp4'):
                                data_to_db[account]['path_video'].append(os.getcwd() + directory_of_account + file)
                            elif file.endswith('.jpg'):
                                data_to_db[account]['path_photo'].append(os.getcwd() + directory_of_account + file)
                except Exception:
                    print('Account responded with status code 404 or does not have StoryItems to load')

                for account, data in data_to_db.items():
                    account_id = db.get_account_id(account)
                    for path, collection in data.items():
                        for element in collection:
                            if path == 'path_video':
                                collection_to_send.append([account_id, 1, element])
                            elif path == 'path_photo':
                                collection_to_send.append([account_id, 2, element])
                            elif path == 'path_text':
                                collection_to_send.append([account_id, 3, element])

                accounts_counter += 1
            else:
                continue

    # Controls repeating of credentials
    used_credentials = set()

    # Controls repeating of accounts
    used_accounts = set()

    # Initiate collection which will be sent to database
    collection_to_send = []

    # Maintain parser log-in and collecting data logic
    login_handle()

    # Migration to database
    db.send_to_table('attachments', ('account_id', 'type', 'path',), collection_to_send)


def main():
    parse_instagram_stories()


if __name__ == '__main__':
    main()

