import requests
import json
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ.get("API_KEY")
client_id = os.environ.get("CLIENT_ID")
# db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")
db_user = os.environ.get("DB_USER")
db_pass = os.environ.get("DB_PASSWORD")


def run_scraper():
    # Define the subreddit and API endpoint
    subreddit = "relationship_advice"
    endpoint = f"https://www.reddit.com/r/{subreddit}/top.json?sort=top&t=all"

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
        if saved_entries >= 10:
            break
        # Extract the author, content, and upvotes
        author = post["data"]["author"]
        content = post["data"]["selftext"]
        upvotes = post["data"]["ups"]

        # Check if the post already exists in the database
        cur.execute("SELECT 1 FROM relationship_advice WHERE author = %s and content = %s", (author, content))
        if cur.fetchone():
            continue
        # Insert the data into the database
        cur.execute("INSERT INTO relationship_advice (author, content, upvotes) VALUES (%s, %s, %s)", (author, content, upvotes))
        saved_entries += 1

    # Commit the changes to the database
    conn.commit()

    # Print the number of saved entries
    print(f"{saved_entries} entries saved to the database.")

    # Close the cursor and connection
    cur.close()
    conn.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run_scraper()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
