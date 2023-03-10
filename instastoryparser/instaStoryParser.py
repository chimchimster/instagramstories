import os
import time
import clickhouse_connect

from mysql.connector import connect
from multiprocessing import Process
from instagramstories import settings
from instagramstories.db_init.database import MariaDataBase, ClickHouseDatabase
from instagramstories.logs.logs_config import LoggerHandle

from instagramstories.imagehandling import imagehandle
from instagramstories.instaloader_init import loader_init

log = LoggerHandle()
log.logger_config()


def parse_instagram_stories(flow_number, instagram_accounts, credential, proxies):

    # Initiate collection which will be sent to database
    collection_to_send = []

    def login_handle():
        print(f'THIS IS {flow_number} FLOW NOW I USE THIS CREDENTIAL - {credential}')

        username, password = credential

        # Once you want to add session to login
        # you will be needed to throw session arguments
        # inside a SignIn class, then you have to
        # update obj.context._session dictionary
        def update_session_file(session):
            os.chdir('..' + '/sessions/')
            path_to_session = os.getcwd() + '/session.txt'
            with open(path_to_session, 'w') as path:
                path.write(session)
            return path_to_session

        try:
            # Simulates human behaviour
            time.sleep(15)

            # Login into account
            if login(username, password):
                # Collect StoryItems while being logged-in
                collect_data()
        except:
            print(f'Probably {credential} is blocked')
            log.logger.warning(f'Probably {credential} is blocked')

    def login(username, password):
        try:
            # Trying to sign in into user's account
            signin = loader_init.SignIn(username, password)

            # Set up proxies
            _proxy, _port, _login, _password = proxies
            set_proxies = {
                'http': f'http://{_login}:{_password}@{_proxy}:{_port}',
                'https': f'https://{_login}:{_password}@{_proxy}:{_port}',
            }

            loader_init.loader.context._session.proxies = set_proxies
            signin.sign_in()
            return True
        except:
            log.logger.warning(f'Account {username} might be restricted')
            return

    def collect_data():
        # Data which must be sent to database
        data_to_db = {}

        def migration_to_attachments():
            db_attachments.send_to_table('attachments', collection_to_send)

        if not instagram_accounts:
            log.logger.warning('There is no account to parse!')
            return

        accounts_counter = 1
        for account in instagram_accounts:
            print(account, accounts_counter)

            directory_of_account = f'/{account}/stories'
            try:
                # Simulates human behaviour
                time.sleep(15)

                # Collect stories from account
                user = loader_init.LoadStoriesOfUser(account)
                user.download_stories_of_target()
            except Exception:
                log.logger.warning(f'There is an error while loading data from {account}')
                print()

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
            except:
                print(f'Account {account} responded with status code 404 or does not have StoryItems to load')

            for account, data in data_to_db.items():
                account_id = db_imas.get_account_id(account)
                for path, collection in data.items():
                    for element in collection:
                        if path == 'path_video':
                            collection_to_send.append([account_id[0][0], 1, element])
                        elif path == 'path_photo':
                            collection_to_send.append([account_id[0][0], 2, element])
                        elif path == 'path_text':
                            collection_to_send.append([account_id[0][0], 3, element])

            accounts_counter += 1

            if accounts_counter % 3 == 0 and len(collection_to_send) > 0:
                try:
                    # Migrate
                    migration_to_attachments()

                    log.logger.warning(f'Data successfully added to database {collection_to_send}')

                    # Empty space inside of structure which stores elements of collection
                    collection_to_send.clear()
                    data_to_db.clear()
                except:
                    log.logger.warning(f'There is a problem with adding collection {collection_to_send}')

    # Maintain parser log-in and collecting data logic
    login_handle()


def get_data_from_db():
    global db_attachments, db_imas
    db_imas = ClickHouseDatabase('imas', settings.imas_db['host'], settings.imas_db['port'], settings.imas_db['user'], settings.imas_db['password'])
    db_social_services = MariaDataBase('social_services')
    db_attachments = ClickHouseDatabase('attachments', settings.attachments_db['host'], settings.attachments_db['port'], settings.attachments_db['user'], settings.attachments_db['password'])

    accounts = [account[0] for account in db_imas.get_data_for_parse('resource_social')]
    credential = db_social_services.get_account_credentials('soc_accounts')
    time.sleep(2)
    proxy = db_social_services.get_proxies('proxies')
    print(proxy)

    return accounts, credential, proxy


def main(instagram_accounts, credentials, proxies):
    global db_attachments, db_imas

    # Here should be 5 streams
    # that's why let's divide
    # instagram_accounts/credentials/proxies into 5 parts

    flows = {
        1: {'accounts': '', 'credentials': '', 'proxy': ''},
        # 2: {'accounts': '', 'credentials': '', 'proxy': ''},
        # 3: {'accounts': '', 'credentials': '', 'proxy': ''},
        # 4: {'accounts': '', 'credentials': '', 'proxy': ''},
        # 5: {'accounts': '', 'credentials': '', 'proxy': ''},
    }

    # Number of streams
    streams = len(flows)

    # Determine delimiter
    delimiter = len(instagram_accounts) // streams

    # Determines accounts_set
    accounts_set = []

    # Store processes
    procs = []

    # Algorithm which split instagram_accounts into equivalent chunks
    for i in range(0, len(instagram_accounts), delimiter):
        accounts_set.append(instagram_accounts[i:i + delimiter])

    def fill_flows(object: [tuple, list], obj_name: str) -> None:
        for num, lst in enumerate(object):
            num += 1
            if num in flows:
                flows[num][obj_name] = lst

    # Fill accounts
    fill_flows(accounts_set, 'accounts')

    # Fill credentials
    fill_flows(credentials, 'credentials')

    # Fill proxies
    fill_flows(proxies, 'proxy')

    # Applying multiprocessing
    for stream in flows:
        process = Process(target=parse_instagram_stories,
                          args=(stream,
                                flows[stream]['accounts'],
                                flows[stream]['credentials'],
                                flows[stream]['proxy'],
                                ))
        procs.append(process)
        process.start()
        time.sleep(5)

    # Join processes
    for proc in procs:
        proc.join()


if __name__ == '__main__':
    instagram_accounts, credentials, proxies = get_data_from_db()
    main(instagram_accounts, credentials, proxies)




