import psycopg2


class Storage:
    def __init__(self):
        self._db_name = "prs"

    def connect(self):
        return psycopg2.connect(f"dbname={self._db_name}")


storage = Storage()
