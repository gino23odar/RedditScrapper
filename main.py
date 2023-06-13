import requests
import json
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("API_KEY")
client_id = os.getenv("CLIENT_ID")
# db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")

print(api_key, client_id, db_port, db_user, db_pass)


def run_scraper(num_posts, sub):
    # Define the subreddit and API endpoint
    subreddit = sub
    endpoint = f"https://www.reddit.com/r/{subreddit}/top.json?sort=top&t=all&limit={num_posts}"

    # Set the authentication parameters
    headers = {
        "User-Agent": client_id,
        "Authorization": api_key
    }

    # Send a GET request to the API endpoint
    response = requests.get(endpoint, headers=headers)

    # Parse the response as JSON
    data = json.loads(response.text)

    # Connect to the database
    conn = psycopg2.connect(
        host='localhost',
        port=db_port,
        user=db_user,
        password=db_pass,
        database="Reddit_English"
    )

    # Create a cursor
    cur = conn.cursor()

    # Keep track of the number of saved entries
    saved_entries = 0

    # Iterate through the list of posts
    for post in data["data"]["children"]:
       #if saved_entries >= 20:
            #break
        # Extract the author, content, and upvotes
        author = post["data"]["author"]
        print(author)
        title = post["data"]["title"]
        content = post["data"]["selftext"]
        upvotes = post["data"]["ups"]

        # Check if the post already exists in the database
        cur.execute(f"SELECT 1 FROM {subreddit} WHERE author = %s and content = %s", (author, content))
        if cur.fetchone():
            continue
        # Insert the data into the database
        cur.execute(f"INSERT INTO {subreddit} (author, title, content, upvotes) VALUES (%s, %s, %s, %s)",
                    (author,title, content, upvotes))
        saved_entries += 1

    # Commit the changes to the database
    conn.commit()

    # Print the number of saved entries
    print(f"{saved_entries} entries saved to the database.")

    # Close the cursor and connection
    cur.close()
    conn.close()


def postgres_test():

    try:
        conn = psycopg2.connect(
            host='localhost',
            port=db_port,
            user=db_user,
            password=db_pass,
            database="Reddit_English")
        conn.close()
        print('connected')
        return True
    except:
        print('Connection error')
        return False


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    postgres_test()
    run_scraper(200, 'showerthoughts')


