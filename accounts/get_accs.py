def get_accounts(path):
    with open(path, 'r') as accs:
        return [(account.strip(),) for account in accs.readlines()]