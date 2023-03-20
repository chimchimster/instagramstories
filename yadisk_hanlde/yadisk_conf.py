from instagramstories.db_init.database import MariaDataBase

# Connect to MariaDb Database
db_social_services = MariaDataBase('social_services')

# Retrieves new token
TOKEN = db_social_services.get_new_yadisk_token('yandex_tokens')

# Configurate yandex API
yandex_disk_configuration = {
    'URL': 'https://cloud-api.yandex.net/v1/disk/resources',
    'headers': {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'OAuth {TOKEN}'},
    'TOKEN': TOKEN,
}