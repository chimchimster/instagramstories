import os
import time
import yadisk
import shutil

from multiprocessing import Process
from instagramstories import settings
from instagramstories.yadisk_hanlde import yadisk_conf
from instagramstories.imagehandling import imagehandle
from instagramstories.instaloader_init import loader_init
from instagramstories.logs.logs_config import LoggerHandle
from instagramstories.db_init.database import MariaDataBase, ClickHouseDatabase
from instagramstories.yadisk_hanlde.yadisk_conf import yandex_disk_configuration
from instagramstories.yadisk_hanlde.yadisk_module import create_folder, upload_file, get_uploaded_file_url


log = LoggerHandle()
log.logger_config()

def parse_instagram_stories(flow_number, instagram_accounts, credential, proxies):
    def login_handle():
        """ Maintaining presence in system.
            While we logged in we can parse data,
            otherwise stream simply closes. """

        global username, password
        log.logger.warning(f'THIS IS {flow_number} FLOW NOW I USE THIS CREDENTIAL - {credential}')

        username, password = credential

        try:
            # Login into account
            login(username, password)
            # Collect StoryItems while being logged-in
            collect_data()
        except:
            log.logger.warning(f'Probably {credential} is blocked')

    def mark_account_in_db(func):
        """ Marks account in database that it's already taken """

        def wrapper(username, password):
            result = func(username, password)
            db_social_services.mark_account_as_used('soc_accounts', username)
            return result
        return wrapper

    @mark_account_in_db
    def login(username, password):
        """ Simple login handle with given credentials """

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
        except:
            log.logger.warning(f'Account {username} might be restricted')
            return

    def collect_data():
        """ Staying logged in collects all accessible data
            This function does next things:
                1) Downloading Stories from instagram targets;
                2) Uploading them into Yandex Disk;
                3) Remembering all paths to collected data;
                4) Migrates collected data into DataBase.
            Make changes here carefully, cause this is main parsing logic! """

        # Data which must be sent to database
        data_to_db = {}

        # Collection which will be sent to database
        collection_to_send = []

        # Initial settings for yandex disk token
        yadisk_conf.TOKEN = db_social_services.get_new_yadisk_token('yandex_tokens')

        def migration_to_attachments(collection_to_send):
            """ Final migration of collected data to database. """

            # Delete all duplicates inside collection to send
            collection_to_send = list(map(list, set(map(tuple, collection_to_send))))

            try:
                # Migrate to atc_resource
                db_attachments.send_to_table('atc_resource', collection_to_send)

                log.logger.warning(f'Migration of collection {collection_to_send} is successful')
            except:
                log.logger.warning(f'Problem with migrating collection {collection_to_send}')

        def check_refilling_of_yandex_disk(_file, path_to_account):
            """ Answers the question: how much free space in yandex disk is left? """

            # Object which handles refilling free space inside yandex disk (in megabytes)
            yandex_disk_capacity = db_social_services.get_yandex_disk_capacity('yandex_tokens', yadisk_conf.TOKEN)

            # Determine file size
            size = os.stat(f'{path_to_account}/{_file}').st_size / (1024 ** 2)

            # Check if file could be loaded within free yandex disk space
            if size <= yandex_disk_capacity:
                space_used = yandex_disk_capacity - size
                db_social_services.update_yandex_disk_capacity('yandex_tokens', space_used, yadisk_conf.TOKEN)
                return True

            return False

        if not instagram_accounts:
            log.logger.warning('There is no account to parse!')
            return

        # Jump to media directory
        os.chdir('..' + '/media')

        # Handles migration and reloading session at the same time
        accounts_counter = 1

        for account in instagram_accounts:
            data_to_db[account] = {'path_video': [], 'path_photo': [], 'path_text': []}

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

                                # If disk is refilled then new token will be generated and loading will continue
                                db_social_services.update_status_of_yadisk_token('yandex_tokens', yadisk_conf.TOKEN)
                                yadisk_conf.TOKEN = db_social_services.get_new_yadisk_token('yandex_tokens')

                                # Upload image files into yandex disk
                                upload_file(os.getcwd() + f'{directory_of_account}/{file}', f'{account}/{file}')

                                # Mark uploaded file as published
                                disk.publish(f'{account}/{file}')

                                public_url = get_uploaded_file_url(f'{account}/{file}')
                                data_to_db[account]['path_photo'].append(public_url)
                        except:
                            log.logger.warning(f'Account {account} has no photo to append it to database')

                        try:
                            # Extract text from image
                            image = imagehandle.ImageHandling(os.getcwd() + f'{directory_of_account}/{file}')
                            text = image.extract_text_from_image()
                            data_to_db[account]['path_text'].append(text)
                        except:
                            data_to_db[account]['path_text'].append('empty')
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
                                data_to_db[account]['path_video'].append(public_url)
                            else:
                                log.logger.warning('Yandex disk is refilled')
                                yadisk_conf.TOKEN = db_social_services.get_new_yadisk_token('yandex_tokens')
                                # Upload video files into yandex disk
                                upload_file(os.getcwd() + f'{directory_of_account}/{file}', f'{account}/{file}')

                                # Mark uploaded files as published
                                disk.publish(f'{account}/{file}')

                                public_url = get_uploaded_file_url(f'{account}/{file}')
                                data_to_db[account]['path_video'].append(public_url)
                        except:
                            log.logger.warning(f'Account {account} has no video to drag it')
                    else:
                        continue

            # Preparing data for the migration
            for _account, data in data_to_db.items():
                account_id = db_imas.get_account_id(_account)
                for path, collection in data.items():
                    if collection:
                        for element in collection:
                            if path == 'path_video':
                                collection_to_send.append([account_id, element, 'empty', 'empty'])
                            elif path == 'path_photo':
                                collection_to_send.append([account_id, 'empty', element, data['path_text'][data['path_photo'].index(element)]])
                            else:
                                continue
                    else:
                        continue

            # For debugging sending collections
            log.logger.warning(f'Data to db {data_to_db}')
            log.logger.warning(f'Collection to send {collection_to_send}')

            if accounts_counter % 3 == 0 and len(collection_to_send) > 0:
                try:
                    # Migrate
                    migration_to_attachments(collection_to_send)

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
    """ Retrieving whole pool of data from database. """

    global db_attachments, db_imas, db_social_services

    db_imas = ClickHouseDatabase('imas', settings.imas_db['host'], settings.imas_db['port'], settings.imas_db['user'], settings.imas_db['password'])
    db_social_services = MariaDataBase('social_services')
    db_attachments = ClickHouseDatabase('recognition', settings.attachments_db['host'], settings.attachments_db['port'], settings.attachments_db['user'], settings.attachments_db['password'])

    accounts = [account[0] for account in db_imas.get_data_for_parse('resource_social')]
    credential = db_social_services.get_account_credentials('soc_accounts')
    proxy = db_social_services.get_proxies('proxies')

    return accounts, credential, proxy


def chunks_processing(instagram_accounts, credentials, proxies):
    """ Divides instagram accounts into chunks and launch it on 5 flows. """

    global db_imas, db_attachments, db_social_services

    # Here should be 5 streams
    # that's why let's divide
    # instagram_accounts/credentials/proxies into 5 parts

    flows = {
        1: {'accounts': '', 'credentials': '', 'proxy': ''},
        2: {'accounts': '', 'credentials': '', 'proxy': ''},
        3: {'accounts': '', 'credentials': '', 'proxy': ''},
        4: {'accounts': '', 'credentials': '', 'proxy': ''},
        5: {'accounts': '', 'credentials': '', 'proxy': ''},
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


def start():
    global db_imas, db_attachments, db_social_services, disk
    instagram_accounts, credentials, proxies = get_data_from_db()
    chunks_processing(instagram_accounts, credentials, proxies)

    if not yadisk_conf.TOKEN:
        yadisk_conf.TOKEN = db_social_services.get_new_yadisk_token('yandex_tokens')

    # Initial settings for YaDisk
    disk = yadisk.YaDisk(token=yandex_disk_configuration['TOKEN'])


if __name__ == '__main__':
    start()