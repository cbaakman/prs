import logging
import os
import sys

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_path)

from prs.storage import storage


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    connection = storage.connect()
    cursor = connection.cursor()

    cursor.execute("create table databanks(id serial primary key, name char(50) not null, date timestamp not null default current_timestamp)")

    cursor.execute("create table entries_sequences(databank_id serial references databanks(id), entry_id char(50) not null, sequence text not null)")
    cursor.execute("create unique index on entries_sequences using btree(databank_id, entry_id)")

    cursor.execute("create table words_entries(databank_id serial references databanks(id), entry_id char(50) not null, key char(50) not null, word char(50) not null)")
    cursor.execute("create index on words_entries using btree(key, word)")
    cursor.execute("create index on words_entries using btree(word)")

    cursor.execute("create table strings_entries(databank_id serial references databanks(id), entry_id char(50) not null, key char(50) not null, string text not null)")
    cursor.execute("create index on strings_entries using btree(key, string)")

    cursor.execute("create table unique_strings_entries(databank_id serial references databanks(id), entry_id char(50) not null, key char(50) not null, string text not null)")
    cursor.execute("create index on unique_strings_entries using btree(key, string)")
    cursor.execute("create unique index on unique_strings_entries using btree(databank_id, key, string)")

    cursor.execute("create table numbers_entries(databank_id serial references databanks(id), entry_id char(50) not null, key char(50) not null, number float not null)")
    cursor.execute("create index on numbers_entries using btree(key, number)")

    cursor.execute("create table dates_entries(databank_id serial references databanks(id), entry_id char(50) not null, key char(50) not null, date date not null)")
    cursor.execute("create index on dates_entries using btree(key, date)")

    cursor.execute("create table links(databank1_id serial references databanks(id), entry1_id char(50) not null, databank2_id serial references databanks(id), entry2_id char(50) not null)")
    cursor.execute("create index on links using btree(databank1_id, entry1_id)")
    cursor.execute("create index on links using btree(databank2_id, entry2_id)")

    connection.commit()
