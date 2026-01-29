import json
import pandas as pd

rows = []
with open("reddit_topics.jsonl", "r", encoding="utf-8") as f:
    for line in f:
        rows.append(json.loads(line))

df = pd.DataFrame(rows)
print(df.head())
print("Total rows:", len(df))

df["engagement"] = df["likes"].fillna(0) + df["comments"].fillna(0)
top = df.sort_values("engagement", ascending=False).head(10)
print(top[["topic_tag", "subreddit", "engagement", "created_at", "text"]].to_string(index=False))
