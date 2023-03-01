import os

import instagramstories.instaloader_init.loader_init
import instagramstories.imagehandling.imagehandle
import instagramstories.accounts.get_accs
import instagramstories.db_init.database

PATH_TO_ACCOUNTS = '/home/newuser/work_artem/instagramstories/accounts/account_for_parse.txt'

def main():
    db = instagramstories.db_init.database.DataBase('stories')

    try:
        # Creating database
        db.create_db()
    except FileExistsError:
        print('Database already exists!')

    try:
        # Creating table accounts
        db.create_table('accounts',
                        'account_id int PRIMARY KEY AUTO_INCREMENT',
                        'account VARCHAR(255) NOT NULL',
                        )
    except FileExistsError:
        print('Table already exists!')

    try:
        # Creating table attachments
        db.create_table('attachments',
                        'record_id int PRIMARY KEY AUTO_INCREMENT',
                        'account_id int NOT NULL',
                        'type int NOT NULL',
                        'path TEXT NOT NULL',
                        )
    except FileExistsError:
        print('Table already exists!')

    # Fill table accounts with initial data
    try:
        db.send_to_table('accounts', ('account',), instagramstories.accounts.get_accs.get_accounts(PATH_TO_ACCOUNTS))
    except Exception:
        print('There is no accounts to send!')

    # Get accounts for parse from database
    instagram_accounts = [account[0] for account in db.get_data_for_parse('account', 'accounts')]

    if not instagram_accounts:
        raise Exception('There is no account to parse!')

    def login():
        try:
            # Trying to sign in into user's account
            signin = instagramstories.instaloader_init.loader_init.SignIn('fin.lab', 'QweQwe777')
            signin.sign_in()
        except Exception:
            print('Account might be restricted')

    collection_to_send = []
    def collect_data():
        data_to_db = {}

        for account in instagram_accounts:
            directory_of_account = f'/{account}/stories'
            try:
                # Collect stories from account
                user = instagramstories.instaloader_init.loader_init.LoadStoriesOfUser(account)
                user.download_stories_of_target()
            except:
                print(f'There is an error while loading data from {account}')

            try:
                # Trying to drag text from photos
                os.chdir('/home/newuser/work_artem/instagramstories/media')
                text_files = instagramstories.imagehandling.imagehandle.ImageHandling(os.getcwd() + directory_of_account)
                text_files.create_txt_files()
            except:
                print(f'{account} has no text on photo to drag it')

            if account not in data_to_db:
                data_to_db[account] = {'path_video': [], 'path_photo': [], 'path_text': []}
                for file in os.listdir(os.getcwd() + directory_of_account):
                    if file.endswith('.txt'):
                        data_to_db[account]['path_text'].append(os.getcwd() + directory_of_account + file)
                    elif file.endswith('.mp4'):
                        data_to_db[account]['path_video'].append(os.getcwd() + directory_of_account + file)
                    elif file.endswith('.jpg'):
                        data_to_db[account]['path_photo'].append(os.getcwd() + directory_of_account + file)

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

    login()
    collect_data()

    # Migration
    db.send_to_table('attachments', ('account_id', 'type', 'path',), collection_to_send)

    # print(os.getcwd())
    # os.chdir('/home/newuser/work_artem/instagramstories/media')
    # print(os.getcwd())


if __name__ == '__main__':
    main()


