import sqlite3
import re

total_insert = 0


def create_table_url_list():
    conn.execute('DROP TABLE IF EXISTS url_list')
    conn.execute('CREATE TABLE url_list(url text UNIQUE)')
    conn.execute('CREATE INDEX url_idx on url_list(url)')


def create_table_headline_list():
    conn.execute('DROP TABLE IF EXISTS headline_list')
    conn.execute('CREATE TABLE headline_list(headline text, url_id integer)')
    conn.execute('CREATE INDEX url_id_idx on headline_list(url_id)')


def create_table_word_list():
    conn.execute('DROP TABLE IF EXISTS word_list')
    conn.execute('CREATE TABLE word_list(word text UNIQUE)')
    conn.execute('CREATE INDEX word_idx on word_list(word)')


def create_table_word_location():
    conn.execute('DROP TABLE IF EXISTS word_location')
    conn.execute('CREATE TABLE word_location(url_id integer,word_id integer,location integer)')
    conn.execute('CREATE INDEX word_id_idx on word_location(word_id)')


def create_tables():
    create_table_url_list()
    create_table_headline_list()
    create_table_word_list()
    create_table_word_location()
    conn.commit()


def insert_to_url_list(url):
    global total_insert
    total_insert += 1
    conn.execute('INSERT INTO url_list VALUES(?)', [url])


def insert_to_headline_list(headline, url_id):
    global total_insert
    total_insert += 1
    conn.execute('INSERT INTO headline_list VALUES(?,?)', [headline, url_id])


def insert_to_word_list(word_list):
    for word in word_list:
        global total_insert
        total_insert += 1
        if word in ignore_words:
            continue
        conn.execute("INSERT OR IGNORE INTO word_list VALUES(?)", [word])
    return


def insert_to_word_location(word_list, url_id):
    location = 0
    for word in word_list:
        global total_insert
        total_insert += 1
        if word in ignore_words:
            continue
        word_id = get_word_id(word)
        conn.execute('INSERT INTO word_location VALUES(?,?,?)', [url_id, word_id, location])
        location += 1


def get_word_id(word):
    return conn.execute('SELECT rowid from word_list where word IN(?)', [word]).fetchone()[0]


# Returns maximum 150 words from the body
def get_word_list(body):
    word_list = separate_words(body)
    word_count = min(len(word_list), 150)
    return word_list[:word_count]


def separate_words(text):
    splitter = re.compile('\\W+')
    return [s.lower() for s in splitter.split(text) if s != '']


# For every news article in news_table insert values to necessary tables
def populate_tables():
    c = conn.cursor()
    c.execute('SELECT rowid, webUrl, headline, body from news_table order by rowid')
    row_id = 1
    global total_insert
    for row in c:
        insert_to_url_list(row[1])
        insert_to_headline_list(row[2], row[0])
        word_list = get_word_list(row[3])
        insert_to_word_list(word_list)
        insert_to_word_location(word_list, row[0])
        if row_id % 1000 == 0:  # Commit only after 1000 rows for fast insertion
            conn.commit()
            print(row_id, total_insert)
        row_id += 1
    conn.commit()


conn = sqlite3.connect('guardian_news.db')
create_tables()
# Create a list of words to ignore
ignore_words = ['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it', 'on', 'by', 'with', 'for']
populate_tables()
print(total_insert)
