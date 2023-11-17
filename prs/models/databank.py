import tempfile
import shutil
import os
from datetime import date as Date
from typing import List

from prs.storage import storage


DATABANKS_TABLE = "databanks"
ENTRIES_SEQUENCES_TABLE = "entries_sequences"
UNIQUE_STRINGS_ENTRIES_TABLE = "unique_strings_entries"
STRINGS_ENTRIES_TABLE = "strings_entries"
NUMBERS_ENTRIES_TABLE = "numbers_entries"
DATES_ENTRIES_TABLE = "dates_entries"
WORDS_ENTRIES_TABLE = "words_entries"
LINKS_TABLE = "links"


class Databank:
    def __init__(self, name: str):
        self._name = name

    def __enter__(self):
        self._directory_path = tempfile.mkdtemp(prefix=f"{self._name}_")
        os.system(f"chmod a+rx {self._directory_path}")

        self._entries_path = os.path.join(self._directory_path, 'entry-ids.tsv')
        self._sequences_path = os.path.join(self._directory_path, 'entry-sequences.tsv')
        self._words_path = os.path.join(self._directory_path, 'words-table.tsv')
        self._strings_path = os.path.join(self._directory_path, 'string-table.tsv')
        self._unique_strings_path = os.path.join(self._directory_path, 'unique-string-table.tsv')
        self._numbers_path = os.path.join(self._directory_path, 'number-table.tsv')
        self._dates_path = os.path.join(self._directory_path, 'date-table.tsv')
        self._links_path = os.path.join(self._directory_path, 'links-table.tsv')

        self._id = self._create_databank(self._name)

        return self

    def __exit__(self, exc_type, exc, tb):

        if exc is None:
            self._store_sequences()
            self._store_words()
            self._store_unique_strings()
            self._store_strings()
            self._store_numbers()
            self._store_dates()
            self._store_links()

        shutil.rmtree(self._directory_path)

        if exc is None:
            self._remove_old_links()
            self._remove_old_dates()
            self._remove_old_numbers()
            self._remove_old_strings()
            self._remove_old_unique_strings()
            self._remove_old_words()
            self._remove_old_sequences()
            self._remove_old_databanks()

    def _create_databank(self, name: str) -> int:
        connection = storage.connect()
        cursor = connection.cursor()

        cursor.execute(f"insert into {DATABANKS_TABLE} (name) values ('{self._name}')")
        id_ = None
        cursor.execute(f"select id from {DATABANKS_TABLE} where name='{self._name}' order by date")
        id_ = cursor.fetchall()[-1][0]

        connection.commit()

        return id_

    def _remove_old_databanks(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"delete from {DATABANKS_TABLE} where id!={self._id} and name='{self._name}'")
        connection.commit()

    def set_sequence(self, entry_id: str, sequence: str):

        with open(self._sequences_path, 'at') as f:
            f.write(f"{self._id}\t{entry_id}\t{sequence}\n")

    def _store_sequences(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"copy {ENTRIES_SEQUENCES_TABLE}(databank_id, entry_id, sequence) from '{self._sequences_path}' delimiter '\t' csv")
        connection.commit()

    def _remove_old_sequences(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"delete from {ENTRIES_SEQUENCES_TABLE} using {DATABANKS_TABLE} where databank_id!={self._id}"
                       f" and {DATABANKS_TABLE}.id=databank_id and {DATABANKS_TABLE}.name='{self._name}'")
        connection.commit()

    def index_link(self, entry_id: str, other_database_name: str, other_entry_id: str):

        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"select id from {DATABANKS_TABLE} where name='{other_database_name}' order by date")
        other_database_id = cursor.fetchall()[-1][0]
        connection.close()

        with open(self._links_path, 'at') as f:
            f.write(f"{self._id}\t{entry_id}\t{other_database_id}\t{other_entry_id}\n")

    def _store_links(self, entry_id: str, other_databank_name: str, other_entry_id: str):
        connection = storage.connect()
        cursor = connection.cursor()

        cursor.execute(f"copy {LINKS_TABLE}(databank1_id, entry1_id, databank2_id, entry2_id) from '{self._links_path}' delimiter '\t' csv")

        connection.commit()

    def _remove_old_links(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"delete from {LINKS_TABLE} using where databank1_name!={self._name}"
                       f" and {DATABANKS_TABLE}.id=databank_id and {DATABANKS_TABLE}.name='{self._name}'")
        cursor.execute(f"delete from {LINKS_TABLE} using {DATABANKS_TABLE} where databank2_id!={self._id}"
                       f" and {DATABANKS_TABLE}.id=databank_id and {DATABANKS_TABLE}.name='{self._name}'")
        connection.commit()

    def index_text(self, entry_id: str, key: str, text: str):

        words = Databank._get_words(text)

        with open(self._words_path, 'at') as f:
            for word in words:
                f.write(f"{self._id}\t{entry_id}\t{key}\t{word}\n")

    @staticmethod
    def _get_words(text: str) -> List[str]:
        words = set([])

        i = 0
        word = ""
        while i < len(text):
            if text[i].isalpha() or text[i].isdigit():
                word += text[i]

            elif len(word) >= 3 and len(word) < 20:
                words.add(word.lower())
                word = ""

            i += 1

        return list(words)

    def _store_words(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"copy {WORDS_ENTRIES_TABLE}(databank_id, entry_id, key, word) from '{self._words_path}' delimiter '\t' csv")
        connection.commit()

    def _remove_old_words(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"delete from {WORDS_ENTRIES_TABLE} using {DATABANKS_TABLE} where databank_id!={self._id}"
                       f" and {DATABANKS_TABLE}.id=databank_id and {DATABANKS_TABLE}.name='{self._name}'")
        connection.commit()

    def index_string(self, entry_id: str, key: str, s: str):

        s = s.replace('\n', ' ')

        with open(self._strings_path, 'at') as f:
            f.write(f"{self._id}\t{entry_id}\t{key}\t{s}\n")

    def _store_strings(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"copy {STRINGS_ENTRIES_TABLE}(databank_id, entry_id, key, string) from '{self._strings_path}' delimiter '\t' csv")
        connection.commit()

    def _remove_old_strings(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"delete from {STRINGS_ENTRIES_TABLE} using {DATABANKS_TABLE} where databank_id!={self._id}"
                       f" and {DATABANKS_TABLE}.id=databank_id and {DATABANKS_TABLE}.name='{self._name}'")
        connection.commit()

    def index_unique_string(self, entry_id: str, key: str, s: str):

        s = s.replace('\n', ' ')

        with open(self._unique_strings_path, 'at') as f:
            f.write(f"{self._id}\t{entry_id}\t{key}\t{s}\n")

    def _store_unique_strings(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"copy {UNIQUE_STRINGS_ENTRIES_TABLE}(databank_id, entry_id, key, string) from '{self._unique_strings_path}' delimiter '\t' csv")
        connection.commit()

    def _remove_old_unique_strings(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"delete from {UNIQUE_STRINGS_ENTRIES_TABLE} using {DATABANKS_TABLE} where databank_id!={self._id}"
                       f" and {DATABANKS_TABLE}.id=databank_id and {DATABANKS_TABLE}.name='{self._name}'")
        connection.commit()

    def index_number(self, entry_id: str, key: str, number: str):

        with open(self._numbers_path, 'at') as f:
            f.write(f"{self._id}\t{entry_id}\t{key}\t{number}\n")

    def _store_numbers(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"copy {NUMBERS_ENTRIES_TABLE}(databank_id, entry_id, key, number) from '{self._numbers_path}' delimiter '\t' csv")
        connection.commit()

    def _remove_old_numbers(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"delete from {NUMBERS_ENTRIES_TABLE} using {DATABANKS_TABLE} where databank_id!={self._id}"
                       f" and {DATABANKS_TABLE}.id=databank_id and {DATABANKS_TABLE}.name='{self._name}'")
        connection.commit()

    def index_date(self, entry_id: str, key: str, date: Date):

        with open(self._dates_path, 'at') as f:
            f.write(f"{self._id}\t{entry_id}\t{key}\t{date.strftime('%Y-%m-%d')}\n")

    def _store_dates(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"copy {DATES_ENTRIES_TABLE}(databank_id, entry_id, key, date) from '{self._dates_path}' delimiter '\t' csv")
        connection.commit()

    def _remove_old_dates(self):
        connection = storage.connect()
        cursor = connection.cursor()
        cursor.execute(f"delete from {DATES_ENTRIES_TABLE} using {DATABANKS_TABLE} where databank_id!={self._id}"
                       f" and {DATABANKS_TABLE}.id=databank_id and {DATABANKS_TABLE}.name='{self._name}'")
        connection.commit()
