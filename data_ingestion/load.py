import os
from googleapiclient.discovery import build
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv()
youtube_api_key = os.getenv("YOUTUBE_API_KEY")
mongo_uri = os.getenv("DATABASE_URL")

if not youtube_api_key:
    print("ERROR: YOUTUBE_API_KEY not found in .env file!")
    print("Make sure you have a .env file with YOUTUBE_API_KEY=your_key_here")
    exit(1)

youtube = build('youtube', 'v3', developerKey=youtube_api_key)

mongo_client = MongoClient(mongo_uri)
db = mongo_client.social_media_db
videos_collection = db.youtube_videos
comments_collection = db.youtube_comments

print("Starting YouTube data load")

search_queries = [
    "Data",
    "AI",
    "Statistics"
]

total_videos = 0
total_comments = 0

for query in search_queries:
    print(f"\nSearching for: {query}")
    
    try:
        search_request = youtube.search().list(
            part="snippet",
            q=query,
            type="video",
            maxResults=50,
            relevanceLanguage="en",
            order="date",  
        )
        search_response = search_request.execute()
        
        if not search_response.get('items'):
            print(f"No results found for '{query}'")
            continue
        
        video_ids = [item['id']['videoId'] for item in search_response['items']]
        print(f"Found {len(video_ids)} videos")
        
        videos_request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=','.join(video_ids)
        )
        videos_response = videos_request.execute()
        
        for video in videos_response['items']:
            video_doc = {
                "platform": "YouTube",
                "video_id": video['id'],
                "title": video['snippet']['title'],
                "description": video['snippet']['description'],
                "channel": video['snippet']['channelTitle'],
                "channel_id": video['snippet']['channelId'],
                "published_at": video['snippet']['publishedAt'],
                "views": int(video['statistics'].get('viewCount', 0)),
                "likes": int(video['statistics'].get('likeCount', 0)),
                "comments_count": int(video['statistics'].get('commentCount', 0)),
                "tags": video['snippet'].get('tags', []),
                "category_id": video['snippet']['categoryId'],
                "duration": video['contentDetails']['duration'],
                "thumbnail": video['snippet']['thumbnails']['high']['url'],
                "search_query": query,
                "ingested_at": datetime.utcnow()
            }
            
            videos_collection.update_one(
                {"video_id": video['id']},
                {"$set": video_doc},
                upsert=True
            )
            total_videos += 1
        
        print(f"Saved {len(videos_response['items'])} videos to MongoDB")
        
        videos_with_comments = [v for v in videos_response['items'] 
                               if int(v['statistics'].get('commentCount', 0)) > 0]
        
        for video in videos_with_comments[:5]:
            video_id = video['id']
            
            try:
                comments_request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50,  
                    order="relevance" 
                )
                comments_response = comments_request.execute()
                
                video_comments = 0
                for item in comments_response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    comment_doc = {
                        "platform": "YouTube",
                        "video_id": video_id,
                        "video_title": video['snippet']['title'],
                        "comment_id": item['id'],
                        "text": comment['textDisplay'],
                        "author": comment['authorDisplayName'],
                        "likes": comment['likeCount'],
                        "published_at": comment['publishedAt'],
                        "search_query": query,
                        "ingested_at": datetime.utcnow()
                    }
                    
                    # Update if exists, insert if new
                    comments_collection.update_one(
                        {"comment_id": item['id']},
                        {"$set": comment_doc},
                        upsert=True
                    )
                    video_comments += 1
                    total_comments += 1
                
                print(f" Saved {video_comments} comments from '{video['snippet']['title'][:50]}...'")
                
            except Exception as e:
                if "commentsDisabled" in str(e):
                    print(f"Comments disabled for video")
                else:
                    print(f" Error fetching comments: {str(e)}")
        
        time.sleep(1)
        
    except Exception as e:
        print(f" Error with query '{query}': {str(e)}")
        continue

print(f"\nLoad complete!")
print(f"Total videos ingested: {total_videos}")
print(f"Total comments ingested: {total_comments}")
print(f"Database: {db.name}")
print(f"Collections: {videos_collection.name}, {comments_collection.name}")

print("\n Quick Stats:")
print(f"  Videos in DB: {videos_collection.count_documents({})}")
print(f"  Comments in DB: {comments_collection.count_documents({})}")

mongo_client.close()