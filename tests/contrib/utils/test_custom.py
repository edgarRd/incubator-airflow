from datetime import datetime


def today():
    return datetime.utcnow().strftime('%Y-%m-%d')
