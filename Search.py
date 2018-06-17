import sqlite3


def get_match_rows(q):

    # Strings to build the query
    field_list = 'w0.url_id'
    table_list = ''
    clause_list = ''
    word_ids = []
    # Split the words by spaces
    words = q.split(' ')
    table_number = 0
    for word in words:
        word_row = conn.execute("select rowid from word_list where word ='%s'" % word).fetchone()
        if word_row is not None:
            word_id = word_row[0]
            word_ids.append(word_id)
            if table_number > 0:
                table_list += ','
                clause_list += ' and '
                clause_list += 'w%d.url_id=w%d.url_id and ' % (table_number-1, table_number)
            field_list += ',w%d.location' % table_number
            table_list += 'word_location w%d' % table_number
            clause_list += 'w%d.word_id=%d' % (table_number, word_id)
            table_number += 1
    # Create the query from the separate parts
    full_query = 'select %s from %s where %s' % (field_list, table_list, clause_list)
    cur = conn.execute(full_query)
    rows = [row for row in cur]
    return rows, word_ids


def get_scored_list(rows, wordids):
    total_scores = dict([(row[0], 0) for row in rows])
    # This is where you'll later put the scoring functions
    weights = [(1.0, frequency_score(rows)), (1.5, location_score(rows))]
    for (weight, scores) in weights:
        for url in total_scores:
            total_scores[url] += weight*scores[url]
    return total_scores


def get_url_name(id):
    return conn.execute("select url from url_list where rowid=%d" % id).fetchone()[0]


def query(q):
    rows, word_ids = get_match_rows(q)
    scores = get_scored_list(rows, word_ids)
    ranked_scores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
    for (score, url_id) in ranked_scores[0:10]:
        print('%f\t%s' % (score, get_url_name(url_id)))


def normalize_scores(scores, small_is_better=0):
    v_small = 0.00001  # Avoid division by zero errors
    if small_is_better:
        min_score = min(scores.values())
        return dict([(u, float(min_score)/max(v_small, l)) for (u, l) in scores.items()])
    else:
        max_score = max(scores.values())
        if max_score == 0:
            max_score = v_small
        return dict([(u, float(c)/max_score) for (u, c) in scores.items()])


def frequency_score(rows):
    counts = dict([(row[0], 0) for row in rows])
    for row in rows:
        counts[row[0]] += 1
    return normalize_scores(counts)


def location_score(rows):
    locations = dict([(row[0], 1000000) for row in rows])
    for row in rows:
        loc = sum(row[1:])
        if loc < locations[row[0]]:
            locations[row[0]] = loc
    return normalize_scores(locations, small_is_better=1)


def filter_query(q):
    remove_words = ['how', 'when', 'where', 'who', 'what', 'is', 'was', 'were']
    for word in remove_words:
        q = q.replace(word, '')
    return q


conn = sqlite3.connect('guardian_news.db')
query_text = 'president of soviet union'
filtered_query = filter_query(query_text)
query(filtered_query)
