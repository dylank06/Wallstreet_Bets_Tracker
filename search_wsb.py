# Import from readthedocs.io
from psaw import PushshiftAPI
import config
import datetime
import psycopg2
import psycopg2.extras


connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASSWORD)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

cursor.execute("""
    SELECT * FROM stock
""")
rows = cursor.fetchall()

stocks = {}
for row in rows:
    stocks['$'+row['symbol']] =row['id']

print(stocks)
# Instantiate api client
api = PushshiftAPI()


# Integer that represents moment in time 
start_time=int(datetime.datetime(2021, 1, 30).timestamp())


# Result is casted to list
submissions = api.search_submissions(after=start_time,
                            subreddit='wallstreetbets',
                            filter=['url','author', 'title', 'subreddit'],)

# To print subminssion as .txt file - python ./myscript.py > output.txt

for submission in submissions:
    # Returen words with "$" tag
    words= submission.title.split()
    cashtags = list(set(filter(lambda word: word.lower().startswith('$'),words)))

    if len(cashtags) > 0:
        print(cashtags)
        print(submission.title)

        for cashtag in cashtags:
            submitted_time = datetime.datetime.fromtimestamp(submission.created_utc).isoformat()

            try:
                cursor.execute("""
                    INSERT INTO mention (dt, stock_id, message, source,url)
                    Values(%s, %s, %s, 'wallstreetbets', %s)
                """, (submitted_time, stocks[cashtag], submission.title, submission.url))

                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()
