import sqlite3
import requests
from datetime import date, timedelta

# Up to 12 calls per second (Restricted by Guardian developer API policy)
Max_Call_Per_Sec = 12
# Up to 5,000 calls per day (Restricted by Guardian developer API policy)
Max_Call_Per_Day = 5000


# Replaces one single quote (if present) by two single quotes to avoid sql error
def handle_single_quote(list_values):
    list_updated_values = []
    for value in list_values:
        list_updated_values.append(str(value).replace("'", "''"))
    return list_updated_values


# Replaces null by space
def replace_nulls_by_space(list_values):
    list_updated_values = []
    for value in list_values:
        list_updated_values.append(str(value).replace("\x00", ' '))
    return list_updated_values


# Creates comma separated string by inserting comma between list values
def insert_comma_between_two_elements(list_values):
    return ','.join(list_values)


# Returns a new list where every value is surrounded by a single quote
def surround_by_quote(list_values):
    list_updated_values = []
    for value in list_values:
        list_updated_values.append("'" + value + "'")
    return list_updated_values


# Prepares values for sql query by using other helper methods
def process_values_for_sql_parameter(list_values):
    list_updated_values = handle_single_quote(list_values)
    list_updated_values = replace_nulls_by_space(list_updated_values)
    quote_surrounded_values = surround_by_quote(list_updated_values)
    comma_separated_values = insert_comma_between_two_elements(quote_surrounded_values)
    return '(' + comma_separated_values + ')'


# Prepares sql query string
def get_query_string(list_values):
    values_for_sql = process_values_for_sql_parameter(list_values)
    sql_query = "INSERT OR IGNORE INTO news_table VALUES" + values_for_sql
    return sql_query


# Returns true if all fields are available
def all_fields_available(api_response):
    return 'webUrl' in api_response and 'webPublicationDate' in api_response and 'headline' in api_response['fields'] \
           and 'bodyText' in api_response['fields']


def create_table():
    conn.execute('''CREATE TABLE IF NOT EXISTS news_table 
                (webUrl text UNIQUE,
                 webPublicationDate text,
                 headline text,
                 body text);''')
    conn.commit()


conn = sqlite3.connect('guardian_news.db')
create_table()

# Sample URL
#
# http://content.guardianapis.com/search?from-date=2000-01-01&
# to-date=2018-06-01&show-fields=headline,body&page-size=200
# &api-key=your-api-key-goes-here

MY_API_KEY = open("credential_guardian.txt").read().strip()
API_ENDPOINT = 'http://content.guardianapis.com/search'
my_params = {
    'from-date': "",
    'to-date': "",
    'show-fields': 'headline,bodyText',
    'page-size': 200,
    'api-key': MY_API_KEY
}

# day iteration from here:
start_date = date(2016, 2, 17)
end_date = date(2018, 6, 15)
day_range = range((end_date - start_date).days + 1)

total_api_call = 0
call_before_sleep = 0

for day_count in day_range:
    news_date = start_date + timedelta(days=day_count)
    date_str = news_date.strftime('%Y-%m-%d')

    my_params['from-date'] = date_str
    my_params['to-date'] = date_str

    # scrolls one day at a time
    print("Downloading", date_str)

    current_page = 1
    total_pages = 1

    while current_page <= total_pages:
        my_params['page'] = current_page
        resp = requests.get(API_ENDPOINT, my_params)
        data = resp.json()

        responses = data['response']['results']
        for response in responses:
            # Response fields we are interested
            if not all_fields_available(response):
                continue
            webUrl = response['webUrl']
            webPublicationDate = response['webPublicationDate']
            headline = response['fields']['headline']
            body = response['fields']['bodyText']

            query = get_query_string([webUrl, webPublicationDate, headline, body])

            conn.execute(query)

        conn.commit()
        # if there is more than one page
        current_page += 1
        total_pages = data['response']['pages']

        total_api_call += 1
        call_before_sleep += 1
        if call_before_sleep == Max_Call_Per_Sec:
            # Due to network latency it was not possible to call more than 12 calls under 1 sec so no need to call sleep
            # time.sleep(1)
            call_before_sleep = 0
        if total_api_call == Max_Call_Per_Day:
            print("Done for one day! Last Response date was", date_str)
            break
