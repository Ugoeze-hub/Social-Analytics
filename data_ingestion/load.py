import os
from googleapiclient.discovery import build
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import time
import openpyxl
from openpyxl import Workbook

import re

load_dotenv()
youtube_api_key = os.getenv("YOUTUBE_API_KEY")
mongo_uri = os.getenv("DATABASE_URL")

if not youtube_api_key:
    print("ERROR: YOUTUBE_API_KEY not found in .env file!")
    exit(1)

youtube = build('youtube', 'v3', developerKey=youtube_api_key)

mongo_client = MongoClient(mongo_uri)
db = mongo_client.social_media_db
videos_collection = db.youtube_videos
comments_collection = db.youtube_comments

print("Starting YouTube data load")

search_queries = [
    "Data Science",
    "Artificial Intelligence",
    "Statistics"
]

wb = Workbook()
ws = wb.active
ws.title = "Social Media Data"

headers = [
    'post_id', 'platform', 'text', 'hashtags','created_at',
    'likes', 'comments','permalink', 'url', 'topic_tag', 'engagement'
]

ws.append(headers)

for cell in ws[1]:
    cell.font = openpyxl.styles.Font(bold=True)

total_videos = 0
VIDEOS_PER_QUERY = 200

def extract_hashtags(text):
    """Extract hashtags from text"""
    return ','.join(re.findall(r'#\w+', text))

for query in search_queries:
    print(f"\nSearching for: {query}")
    
    try:
        all_video_ids = []
        next_page_token = None
        videos_collected = 0
        
        while videos_collected < VIDEOS_PER_QUERY:
            search_request = youtube.search().list(
                part="snippet",
                q=query,
                type="video",
                maxResults=50,
                pageToken=next_page_token,
                relevanceLanguage="en",
                order="date",
            )
            search_response = search_request.execute()
            
            if not search_response.get('items'):
                break
            
            page_video_ids = [item['id']['videoId'] for item in search_response['items']]
            all_video_ids.extend(page_video_ids)
            videos_collected += len(page_video_ids)
            
            print(f"  Collected {videos_collected} videos...")
            
            next_page_token = search_response.get('nextPageToken')
            if not next_page_token:
                break
            
            time.sleep(0.5)
        
        print(f"Found {len(all_video_ids)} videos for '{query}'")
        
        for i in range(0, len(all_video_ids), 50):
            batch_ids = all_video_ids[i:i+50]
            
            videos_request = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=','.join(batch_ids)
            )
            videos_response = videos_request.execute()
            
            for video in videos_response['items']:
                video_id = video['id']
                title = video['snippet']['title']
                description = video['snippet']['description']
                full_text = f"{title}. {description}"
                
                hashtags = extract_hashtags(full_text)
                if not hashtags and video['snippet'].get('tags'):
                    hashtags = ','.join(['#' + tag for tag in video['snippet']['tags'][:5]])
                
                created_at = video['snippet']['publishedAt']
                likes = int(video['statistics'].get('likeCount', 0))
                comments = int(video['statistics'].get('commentCount', 0))  
                views = int(video['statistics'].get('viewCount', 0))
                
                engagement = round(((likes + comments) / views * 100), 2) if views > 0 else 0
                
                url = f"https://www.youtube.com/watch?v={video_id}"
                
                ws.append([
                    video_id,                    
                    'YouTube',                   
                    full_text[:500],            
                    hashtags,                    
                    created_at,                 
                    likes,                      
                    comments,                        
                    url,                       
                    url,                       
                    query,                     
                    engagement                 
                ])
                
                video_doc = {
                    "platform": "YouTube",
                    "video_id": video_id,
                    "title": title,
                    "description": description,
                    "channel": video['snippet']['channelTitle'],
                    "published_at": created_at,
                    "views": views,
                    "likes": likes,
                    "comments_count": comments,
                    "tags": video['snippet'].get('tags', []),
                    "url": url,
                    "topic_tag": query,
                    "engagement": engagement,
                    "ingested_at": datetime.utcnow()
                }
                
                videos_collection.update_one(
                    {"video_id": video_id},
                    {"$set": video_doc},
                    upsert=True
                )
                
                total_videos += 1
            
            print(f"Saved batch {i//50 + 1}")
        
        time.sleep(1)
        
    except Exception as e:
        print(f" Error with query '{query}': {str(e)}")
        continue

for column in ws.columns:
    max_length = 0
    column_letter = column[0].column_letter
    for cell in column:
        try:
            if len(str(cell.value)) > max_length:
                max_length = len(str(cell.value))
        except:
            pass
    adjusted_width = min(max_length + 2, 50) 
    ws.column_dimensions[column_letter].width = adjusted_width

excel_filename = "exports/social_media_data.xlsx"
wb.save(excel_filename)

print(f"\nLoad complete!")
print(f"Total videos collected: {total_videos}")
print(f"Data saved to: {excel_filename}")
print(f"\nMongoDB Stats:")
print(f" Videos in DB: {videos_collection.count_documents({})}")

mongo_client.close()