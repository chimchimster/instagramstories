import requests
from . import yadisk_conf

from instagramstories.logs.logs_config import log


def create_folder(path):
    """ Creates empty folder.
        This is necessary behaviour if
        we decided upload files into disk."""

    requests.put(f'{yadisk_conf.yandex_disk_configuration["URL"]}?path={path}', headers=yadisk_conf.yandex_disk_configuration["headers"])


def upload_file(path_to_file, path_to_folder, replace=False):
    """ Uploading files via Yandex API using PUT method """

    res = requests.get(f'{yadisk_conf.yandex_disk_configuration["URL"]}/upload?path={path_to_folder}&overwrite={replace}', headers=yadisk_conf.yandex_disk_configuration["headers"]).json()
    log.logger.warning(f'UPLOADING INTO YANDEX DISK... {path_to_folder}')
    if 'href' not in res.keys():
        log.logger.warning(f'There is a problem uploading file {path_to_folder}')
        print(f'There is a problem uploading file {path_to_folder}')
    else:
        with open(path_to_file, 'rb') as file:
            requests.put(res['href'], files={'file': file})


def get_uploaded_file_url(path_to_folder):
    """ Retrieves public URL from uploaded file """

    return requests.get(f'{yadisk_conf.yandex_disk_configuration["URL"]}?path={path_to_folder}&fields=public_url', headers=yadisk_conf.yandex_disk_configuration["headers"]).json()['public_url']

