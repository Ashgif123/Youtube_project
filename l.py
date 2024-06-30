import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError  # Import HttpError
import pymysql
from datetime import datetime

# Replace with your YouTube Data API key
api_key = "AIzaSyDlQk3ABh2Kn6ZVKpW5Sv6qGho3LfTpCLQ"

# Function to convert ISO 8601 datetime string to MySQL datetime format
def convert_to_mysql_datetime(dt_str):
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        st.error(f"Error converting datetime: {e}")
        return None

# Function to establish MySQL database connection
def get_db_connection():
    try:
        return pymysql.connect(
            host="localhost",
            user="root",
            password="Mysql123!",
            database="youtube_project"
        )
    except pymysql.MySQLError as e:
        st.error(f"Error connecting to database: {e}")
        return None

# Function to create tables in MySQL database
def create_tables():
    conn = get_db_connection()
    if not conn:
        st.error("Failed to connect to the database.")
        return False
    
    cursor = conn.cursor()
    
    try:
        # Create 'channell' table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channell (
                channel_id VARCHAR(50) PRIMARY KEY,
                channel_name VARCHAR(255),
                description TEXT,
                published_date DATETIME,
                subscriber_count BIGINT,
                view_count BIGINT,
                video_count BIGINT
            )
        """)
        
        # Create 'videoss' table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS videoss (
                video_id VARCHAR(50) PRIMARY KEY,
                channel_name VARCHAR(255),
                title VARCHAR(255),
                tags TEXT,
                description TEXT,
                published_at DATETIME,
                view_count BIGINT,
                like_count BIGINT,
                dislike_count BIGINT,
                comment_count BIGINT
            )
        """)
        
        # Create 'commentss' table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS commentss (
                comment_id VARCHAR(50) PRIMARY KEY,
                video_id VARCHAR(50),
                author_name VARCHAR(255),
                published_at DATETIME,
                top_level_comment TEXT,
                FOREIGN KEY (video_id) REFERENCES videoss(video_id)
            )
        """)
        
        conn.commit()
        return True
        
    except pymysql.MySQLError as e:
        st.error(f"Error creating tables: {e}")
        return False
        
    finally:
        conn.close()

# Function to fetch channel data from YouTube API
def get_channel_data(channel_id):
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()

        if 'items' in response and response['items']:
            channel_data = response['items'][0]
            return {
                "Channel ID": channel_data['id'],
                "Channel Name": channel_data['snippet']['title'],
                "Description": channel_data['snippet'].get('description', ''),
                "Published Date": channel_data['snippet']['publishedAt'],
                "Subscriber Count": channel_data['statistics']['subscriberCount'],
                "View Count": channel_data['statistics']['viewCount'],
                "Video Count": channel_data['statistics']['videoCount'],
                "Channel Logo URL": channel_data['snippet']['thumbnails']['default']['url']
            }
        else:
            st.error("No channel data found for the given channel ID.")
            return None
    except Exception as e:
        st.error(f"Error fetching channel data: {e}")
        return None

# Function to fetch all channel data from the database
def fetch_all_channel_data():
    conn = get_db_connection()
    if not conn:
        st.error("Failed to connect to the database.")
        return None
    
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM channell")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    except pymysql.MySQLError as e:
        st.error(f"MySQL error occurred: {e}")
        return None
    finally:
        conn.close()

# Function to fetch all channel IDs and names from the database
def fetch_all_channel_ids_names():
    conn = get_db_connection()
    if not conn:
        st.error("Failed to connect to the database.")
        return []
    
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT channel_id, channel_name FROM channell")
        rows = cursor.fetchall()
        return rows
    except pymysql.MySQLError as e:
        st.error(f"MySQL error occurred: {e}")
        return []
    finally:
        conn.close()

# Function to fetch video data from YouTube API
# Function to fetch video data from YouTube API
def get_video_data(channel_id):
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50,
            order="date"
        )
        response = request.execute()
        videos = []
        for item in response.get('items', []):
            if 'id' in item and 'videoId' in item['id']:
                video_data = {
                    "Video ID": item['id']['videoId'],
                    "Channel Name": item['snippet']['channelTitle'],
                    "Title": item['snippet']['title'],
                    "Description": item['snippet']['description'],
                    "Published At": item['snippet']['publishedAt'],
                    "Tags": ", ".join(item['snippet'].get('tags', []))
                }
                videos.append(video_data)
        return videos
    except Exception as e:
        st.error(f"Error fetching video data: {e}")
        return []

# Function to fetch comment data from YouTube API
def get_comment_data(video_id):
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            order="relevance"
        )
        response = request.execute()
        comments = []
        for item in response.get('items', []):
            if 'snippet' in item and 'topLevelComment' in item['snippet'] and 'snippet' in item['snippet']['topLevelComment']:
                comment_data = {
                    "Comment ID": item['id'],
                    "Video ID": video_id,
                    "Author Name": item['snippet']['topLevelComment']['snippet'].get('authorDisplayName', ''),
                    "Published At": item['snippet']['topLevelComment']['snippet'].get('publishedAt', ''),
                    "Top Level Comment": item['snippet']['topLevelComment']['snippet'].get('textOriginal', '')
                }
                comments.append(comment_data)
        return comments
    except HttpError as e:
        if e.resp.status == 403 and "commentsDisabled" in str(e):
            st.warning(f"Comments are disabled for video ID: {video_id}")
            return []
        else:
            st.error(f"Error fetching comment data: {e}")
            return []
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return []

# Function to save video data to the database
def save_video_data(videos):
    conn = get_db_connection()
    if not conn:
        st.error("Failed to connect to the database.")
        return False
    
    cursor = conn.cursor()
    try:
        for video in videos:
            sql = """
                INSERT INTO videoss (video_id, channel_name, title, tags, description, published_at, view_count, like_count, dislike_count, comment_count)
                VALUES (%s, %s, %s, %s, %s, %s, 0, 0, 0, 0)
            """
            values = (
                video['Video ID'],
                video['Channel Name'],
                video['Title'],
                video['Tags'],
                video['Description'],
                convert_to_mysql_datetime(video['Published At'])
            )
            cursor.execute(sql, values)
        conn.commit()
        return True
    except pymysql.MySQLError as e:
        st.error(f"MySQL error occurred: {e}")
        return False
    finally:
        conn.close()

# Function to save comment data to the database
def save_comment_data(comments):
    conn = get_db_connection()
    if not conn:
        st.error("Failed to connect to the database.")
        return False
    
    cursor = conn.cursor()
    try:
        for comment in comments:
            sql = """
                INSERT INTO commentss (comment_id, video_id, author_name, published_at, top_level_comment)
                VALUES (%s, %s, %s, %s, %s)
            """
            values = (
                comment['Comment ID'],
                comment['Video ID'],
                comment['Author Name'],
                convert_to_mysql_datetime(comment['Published At']),
                comment['Top Level Comment']
            )
            cursor.execute(sql, values)
        conn.commit()
        return True
    except pymysql.MySQLError as e:
        st.error(f"MySQL error occurred: {e}")
        return False
    finally:
        conn.close()

# Function to execute SQL query
def execute_query(query_option):
    conn = get_db_connection()
    if not conn:
        st.error("Failed to connect to the database.")
        return None

    cursor = conn.cursor()
    results = None

    try:
        if query_option == "Names of all videos and their corresponding channels":
            cursor.execute("""                SELECT v.title, c.channel_name
                FROM videoss v
                INNER JOIN channell c ON v.channel_name = c.channel_name
            """)
            results = cursor.fetchall()

        elif query_option == "Top 10 most viewed videos and their respective channels":
            cursor.execute("""
                SELECT v.title, c.channel_name
                FROM videoss v
                INNER JOIN channell c ON v.channel_name = c.channel_name
                ORDER BY v.view_count DESC
                LIMIT 10
            """)
            results = cursor.fetchall()

        elif query_option == "Number of comments on each video":
            cursor.execute("""
                SELECT v.title, COUNT(c.comment_id) AS comment_count
                FROM videoss v
                LEFT JOIN commentss c ON v.video_id = c.video_id
                GROUP BY v.title
            """)
            results = cursor.fetchall()

        elif query_option == "Names of all channels and their respective count of videos, views, likes, and comments":
            cursor.execute("""
                SELECT c.channel_name, COUNT(v.video_id) AS video_count, SUM(v.view_count) AS total_views,
                       SUM(v.like_count) AS total_likes, COUNT(com.comment_id) AS total_comments
                FROM channell c
                LEFT JOIN videoss v ON c.channel_name = v.channel_name
                LEFT JOIN commentss com ON v.video_id = com.video_id
                GROUP BY c.channel_name
            """)
            results = cursor.fetchall()

        elif query_option == "Videos with the highest number of likes":
            cursor.execute("""
                SELECT v.title, v.like_count
                FROM videoss v
                ORDER BY v.like_count DESC
            """)
            results = cursor.fetchall()

        elif query_option == "Videos with the highest number of comments":
            cursor.execute("""
                SELECT v.title, COUNT(c.comment_id) AS comment_count
                FROM videoss v
                LEFT JOIN commentss c ON v.video_id = c.video_id
                GROUP BY v.title
                ORDER BY comment_count DESC
            """)
            results = cursor.fetchall()

        elif query_option == "Total number of videos for each channel":
            cursor.execute("""
                SELECT c.channel_name, COUNT(v.video_id) AS video_count
                FROM channell c
                LEFT JOIN videoss v ON c.channel_name = v.channel_name
                GROUP BY c.channel_name
            """)
            results = cursor.fetchall()

        elif query_option == "Channels with the highest number of videos":
            cursor.execute("""
                SELECT c.channel_name, COUNT(v.video_id) AS video_count
                FROM channell c
                LEFT JOIN videoss v ON c.channel_name = v.channel_name
                GROUP BY c.channel_name
                ORDER BY video_count DESC
            """)
            results = cursor.fetchall()

        else:
            st.error("Invalid query option selected.")

        if results:
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(results, columns=columns)
        else:
            st.warning("No results found for the selected query.")

    except pymysql.MySQLError as e:
        st.error(f"MySQL error occurred: {e}")
        return None

    finally:
        conn.close()

# Streamlit UI
st.title("YouTube Data Harvesting and Warehousing")

# Sidebar for navigation
st.sidebar.title("Navigation")
options = ["HOME","Data Harvesting", "Data Warehousing", "Select and Execute SQL Queries"]
choice = st.sidebar.radio("Go to", options)

if choice == "HOME":
    st.write("Welcome to the YouTube Data Harvesting and Warehousing app. Use the sidebar to navigate.")

# Data Harvesting Page
if choice == "Data Harvesting":
    st.header("Data Harvesting")
    channel_id = st.text_input("Enter YouTube Channel ID")
    if st.button("Get Channel Data"):
        if channel_id:
            channel_data = get_channel_data(channel_id)
            if channel_data:
                st.subheader("Channel Data")
                st.image(channel_data["Channel Logo URL"])
                st.write("**Channel ID:**", channel_data["Channel ID"])
                st.write("**Channel Name:**", channel_data["Channel Name"])
                st.write("**Description:**", channel_data["Description"])
                st.write("**Published Date:**", channel_data["Published Date"])
                st.write("**Subscriber Count:**", channel_data["Subscriber Count"])
                st.write("**View Count:**", channel_data["View Count"])
                st.write("**Video Count:**", channel_data["Video Count"])
        else:
            st.warning("Please enter a valid YouTube Channel ID.")

    # Fetch and display channel data
    channel_data = fetch_all_channel_data()
    #if channel_data is not None:
     #   st.subheader("Stored Channel Data")
      #  st.write(channel_data)

    # Fetch channel IDs and names for selection
    channel_ids_names = fetch_all_channel_ids_names()
    #channel_options = {name: cid for cid, name in channel_ids_names}
    #selected_channel_name = st.selectbox("Select a Channel", options=list(channel_options.keys()))

    if st.button("Save Channel Data"):
        #channel_id = channel_options[selected_channel_name]
        channel_data = get_channel_data(channel_id)
        if channel_data:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                try:
                    sql = """
                        INSERT INTO channell (channel_id, channel_name, description, published_date, subscriber_count, view_count, video_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                        channel_data["Channel ID"],
                        channel_data["Channel Name"],
                        channel_data["Description"],
                        convert_to_mysql_datetime(channel_data["Published Date"]),
                        channel_data["Subscriber Count"],
                        channel_data["View Count"],
                        channel_data["Video Count"]
                    )
                    cursor.execute(sql, values)
                    conn.commit()
                    st.success("Channel data saved successfully.")
                except pymysql.IntegrityError:
                    st.info("Channel data already uploaded.")
                except pymysql.MySQLError as e:
                    st.error(f"MySQL error occurred: {e}")
                finally:
                    conn.close()

# Data Warehousing Page
elif choice == "Data Warehousing":
    st.header("Data Warehousing")

    if create_tables():
        st.success("Tables created successfully or already exist.")

    # Fetch and display channel data
    channel_data = fetch_all_channel_data()
    if channel_data is not None:
        st.subheader("Stored Channel Data")
        st.write(channel_data)

    # Fetch channel IDs and names for selection
    channel_ids_names = fetch_all_channel_ids_names()
    channel_options = {name: cid for cid, name in channel_ids_names}
    selected_channel_name = st.selectbox("Select a Channel", options=list(channel_options.keys()))

    if st.button("Save Video Data"):
        channel_id = channel_options[selected_channel_name]
        videos = get_video_data(channel_id)
        if videos and save_video_data(videos):
            st.success("Video data saved successfully.")

    if st.button("Save Comment Data"):
        channel_id = channel_options[selected_channel_name]
        videos = get_video_data(channel_id)
        if videos:
            all_comments_saved = True
            for video in videos:
                comments = get_comment_data(video['Video ID'])
                if comments and not save_comment_data(comments):
                    all_comments_saved = False
            if all_comments_saved:
                st.success("All comment data saved successfully.")
            else:
                st.error("Some comment data could not be saved.")

# Select and Execute SQL Queries Page
elif choice == "Select and Execute SQL Queries":
    st.header("Select and Execute SQL Queries")

    query_options = [
        "Names of all videos and their corresponding channels",
        "Top 10 most viewed videos and their respective channels",
        "Number of comments on each video",
        "Names of all channels and their respective count of videos, views, likes, and comments",
        "Videos with the highest number of likes",
        "Videos with the highest number of comments",
        "Total number of videos for each channel",
        "Channels with the highest number of videos"
    ]
    
    query_option = st.selectbox("Select a query to execute", query_options)
    if st.button("Execute Query"):
        query_results = execute_query(query_option)
        if query_results is not None:
            st.subheader("Query Results")
            st.write(query_results)


