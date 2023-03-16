import os
import time
import yadisk
import shutil
import datetime

from multiprocessing import Process
from instagramstories import settings
from instagramstories.imagehandling import imagehandle
from instagramstories.instaloader_init import loader_init
from instagramstories.logs.logs_config import LoggerHandle
from instagramstories.db_init.database import MariaDataBase, ClickHouseDatabase
from instagramstories.yadisk_hanlde.yadisk_conf import yandex_disk_configuration
from instagramstories.yadisk_hanlde.yadisk_module import create_folder, upload_file, get_uploaded_file_url


log = LoggerHandle()
log.logger_config()

disk = yadisk.YaDisk(token=yandex_disk_configuration['TOKEN'])


def parse_instagram_stories(flow_number, instagram_accounts, credential, proxies):
    def login_handle():
        global username, password

        print(f'THIS IS {flow_number} FLOW NOW I USE THIS CREDENTIAL - {credential}')

        username, password = credential

        try:
            # Login into account
            login(username, password)
            # Collect StoryItems while being logged-in
            collect_data()
        except:
            print(f'Probably {credential} is blocked')
            log.logger.warning(f'Probably {credential} is blocked')

    def mark_account_in_db(func):
        def wrapper(username, password):
            result = func(username, password)
            db_social_services.mark_account_as_used('soc_accounts', username)
            return result
        return wrapper

    @mark_account_in_db
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
        # Initiate collection which will be sent to database
        collection_to_send = []

        # Data which must be sent to database
        data_to_db = {}

        # Object which handles refilling free space inside yandex disk (in megabytes)
        yandex_disk_capacity = 5000

        def migration_to_attachments():
            nonlocal collection_to_send

            # Delete all duplicates inside collection to send
            collection_to_send = list(map(list, set(map(tuple, collection_to_send))))

            # Migrate to atc_resource
            db_attachments.send_to_table('atc_resource', collection_to_send)

        def check_refilling_of_yandex_disk(_file, path_to_account):
            nonlocal yandex_disk_capacity
            # Determine file size
            size = os.stat(f'{path_to_account}/{_file}').st_size / (1024 ** 2)
            print(size)

            # Check if file could be loaded within free yandex disk space
            if size <= yandex_disk_capacity:
                yandex_disk_capacity -= size
                print(yandex_disk_capacity)
                return True
            return False

        if not instagram_accounts:
            log.logger.warning('There is no account to parse!')
            return

        os.chdir('..' + '/media')

        # Handles migration and reloading session at the same time
        accounts_counter = 1

        for account in instagram_accounts:
            data_to_db[account] = {'path_video': [], 'path_photo': [], 'path_text': []}

            # Track on working accounts to determine where errors could be reached
            print(account, accounts_counter)
            log.logger.warning(f'Working account {account}. Its counter = {accounts_counter}')

            directory_of_account = f'/{account}/stories'
            try:
                # Simulates human behaviour
                time.sleep(7)
                user = loader_init.LoadStoriesOfUser(account)

                # Collect stories from account
                user.download_stories_of_target(username, password, accounts_counter)
            except:
                log.logger.warning(f'There is an error while loading data from {account}')

            try:
                # Creates empty folder if there haven't been downloaded any stories
                # This logic handles creating folders inside yandex disk
                os.chmod(os.O_WRONLY, mode=0o777)
                os.mkdir(os.getcwd() + f'/{account}')
                os.chdir(os.getcwd() + f'/{account}')
                os.mkdir(os.getcwd() + '/stories')
                os.chdir('..')
            except:
                log.logger.warning(f'Empty folder for {account} have not been created')

            if os.listdir(os.getcwd() + directory_of_account):
                # Check if uploaded files has mp4 or jpg formats
                if any(map(lambda _file: _file.endswith('.jpg') or _file.endswith('.mp4'), os.listdir(os.getcwd() + directory_of_account))):
                    try:
                        # Creates folder if not exists inside yandex disk storage
                        create_folder(f'{account}')

                        log.logger.warning(f'Folder {account} has been successfully created')
                    except:
                        log.logger.warning(f'Folder {account} has not been created')

            if os.listdir(os.getcwd() + directory_of_account):
                for file in os.listdir(os.getcwd() + directory_of_account):
                    # Handling image files
                    if file.endswith('.jpg'):
                        try:
                            if check_refilling_of_yandex_disk(file, os.getcwd() + directory_of_account):
                                # Upload image files into yandex disk
                                upload_file(os.getcwd() + f'{directory_of_account}/{file}', f'{account}/{file}')

                                # Mark uploaded file as published
                                disk.publish(f'{account}/{file}')

                                public_url = get_uploaded_file_url(f'{account}/{file}')
                                data_to_db[account]['path_photo'].append(public_url)
                            else:
                                log.logger.warning('Yandex disk is refilled')
                                # HERE MUST BE A DATABASE FUNCTIONALITY WHICH TAKES NEW YANDEX DISK TOKEN
                                break
                        except:
                            log.logger.warning(f'Account {account} has no photo to append it to database')

                        try:
                            # Extract text from photo
                            text_file = imagehandle.ImageHandling(os.getcwd() + f'{directory_of_account}/{file}')
                            data_to_db[account]['path_text'].append(text_file.extract_text_from_image())
                        except:
                            log.logger.warning(f'Account {account} has no text on photo to drag it')

                    # Handling video files
                    elif file.endswith('.mp4'):
                        try:
                            if check_refilling_of_yandex_disk(file, os.getcwd() + directory_of_account):
                                # Upload video files into yandex disk
                                upload_file(os.getcwd() + f'{directory_of_account}/{file}', f'{account}/{file}')

                                # Mark uploaded files as published
                                disk.publish(f'{account}/{file}')

                                public_url = get_uploaded_file_url(f'{account}/{file}')
                                data_to_db[account]['path_video'].append(get_uploaded_file_url(public_url))
                            else:
                                log.logger.warning('Yandex disk is refilled')
                                # HERE MUST BE A DATABASE FUNCTIONALITY WHICH TAKES NEW YANDEX DISK TOKEN
                                break
                        except:
                            log.logger.warning(f'Account {account} has no video to drag it')
                    else:
                        continue

            # Preparing data for the migration
            for _account, data in data_to_db.items():
                account_id = db_imas.get_account_id(_account)
                for path, collection in data.items():
                    for element in collection:
                        if path == 'path_video':
                            collection_to_send.append([account_id[0][0], 1, element, '', str(datetime.datetime.now()).split('.')[0]])
                        elif path == 'path_photo':
                            collection_to_send.append([account_id[0][0], 2, element, '', str(datetime.datetime.now()).split('.')[0]])
                        elif path == 'path_text':
                            collection_to_send.append([account_id[0][0], 3, '', element, str(datetime.datetime.now()).split('.')[0]])

            # For debugging
            log.logger.warning(f'Data to db {data_to_db}')

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

            if os.path.exists(os.getcwd() + f'/{account}'):
                # Recursively deletes directory and all files of account from media directory
                shutil.rmtree(os.getcwd() + f'/{account}')
                log.logger.warning(f'Directory {directory_of_account} successfully deleted')

            accounts_counter += 1

    # Maintain parser log-in and collecting data logic
    login_handle()


def get_data_from_db():
    global db_attachments, db_imas, db_social_services
    db_imas = ClickHouseDatabase('imas', settings.imas_db['host'], settings.imas_db['port'], settings.imas_db['user'], settings.imas_db['password'])
    db_social_services = MariaDataBase('social_services')
    db_attachments = ClickHouseDatabase('attachments', settings.attachments_db['host'], settings.attachments_db['port'], settings.attachments_db['user'], settings.attachments_db['password'])

    accounts = [account[0] for account in db_imas.get_data_for_parse('resource_social')]
    credential = db_social_services.get_account_credentials('soc_accounts')
    proxy = db_social_services.get_proxies('proxies')

    return accounts, credential, proxy


def main(instagram_accounts, credentials, proxies):
    global db_imas, db_attachments, db_social_services
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
    chunk = len(instagram_accounts) // streams

    # Determines accounts_set
    accounts_set = []

    # Store processes
    procs = []

    # Algorithm which split instagram_accounts into equivalent chunks
    for start in range(0, len(instagram_accounts), chunk):
        accounts_set.append(instagram_accounts[start:start + chunk])

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
    global db_imas, db_attachments, db_social_services
    instagram_accounts, credentials, proxies = get_data_from_db()
    main(instagram_accounts, credentials, proxies)

    # upload_file('/home/newuser/work_artem/instagramstories/media/saraalpanova/stories/2023-03-13_10-57-42_UTC.jpg', 'saraalpanova/2023-03-13_10-57-42_UTC.jpg')

    # account = 'check'
    # shutil.rmtree(f'/home/newuser/work_artem/instagramstories/media/{account}')
    #
    # # Changing directory to media
    # os.chdir('..' + '/media')
    #
    # account = 'erlanman'
    # directory_of_account = f'/{account}/stories'
    # if any(map(lambda _file: _file.endswith('.jpg') or _file.endswith('.mp4'), os.listdir(os.getcwd() + directory_of_account))):
    #     print('yes')
    # else:
    #     print('no')

    # os.chdir('..' + '/media')
    # os.chmod(os.O_WRONLY, mode=0o777)
    # os.mkdir(os.getcwd() + '/newacc')
    # os.chdir(os.getcwd() + '/newacc')
    # os.mkdir(os.getcwd() + '/stories')
    # print(os.getcwd())
    # os.chdir('..')
    # print(os.getcwd())
