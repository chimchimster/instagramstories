def get_credentials(path):
    with open(path, 'r') as cred:
        return [tuple(credentials.strip().split(',')) for credentials in cred.readlines()]