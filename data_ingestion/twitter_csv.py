import os
from datetime import datetime, timezone
import pandas as pd
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from tqdm import tqdm
import re

# Load environment variables
load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "social_analytics")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "posts")

