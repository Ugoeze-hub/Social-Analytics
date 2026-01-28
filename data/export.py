from pymongo import MongoClient
import csv
from dotenv import load_dotenv
import os

load_dotenv()
mongo_uri = os.getenv("DATABASE_URL")

client = MongoClient(mongo_uri)
db = client.social_media_db

def export_collection_to_csv(collection_name, filename, fields=None):
    """Export MongoDB collection to CSV"""
    collection = db[collection_name]
    data = list(collection.find({}, {'_id': 0}))  
    
    if not data:
        print(f"No data in {collection_name}")
        return
    
    if not fields:
        fields = list(data[0].keys())
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Exported {len(data)} records from {collection_name} to {filename}")
    return len(data)

videos_count = export_collection_to_csv('youtube_videos', 'youtube_videos_export.csv')
comments_count = export_collection_to_csv('youtube_comments', 'youtube_comments_export.csv')

print(f"\nTotal: {videos_count} videos, {comments_count} comments exported")
client.close()