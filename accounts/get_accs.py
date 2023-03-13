def get_accounts(path):
    with open(path, 'r') as accs:
        return [(account.strip(), 4, 1, 4) for account in accs.readlines()]