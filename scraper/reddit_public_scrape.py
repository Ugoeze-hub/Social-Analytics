import time
import json
import re
import csv
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
import os
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

# -----------------------------
# Helpers: extract hashtags/mentions from posts
# -----------------------------
HASHTAG_RE = re.compile(r"(?:^|[^0-9A-Z_])#([A-Z0-9_]+)", re.IGNORECASE)
MENTION_RE = re.compile(r"(?:^|[^0-9A-Z_])@([A-Z0-9_]+)", re.IGNORECASE)

def extract_hashtags(text: str) -> List[str]:
    return sorted({m.group(1).lower() for m in HASHTAG_RE.finditer(text or "")})

def extract_mentions(text: str) -> List[str]:
    return sorted({m.group(1).lower() for m in MENTION_RE.finditer(text or "")})

def iso_utc(ts_utc: float) -> str:
    return datetime.fromtimestamp(ts_utc, tz=timezone.utc).isoformat().replace("+00:00", "Z")

# --------------------------------------------
# Transform Reddit JSON into ingestion schema
# --------------------------------------------
def to_schema(post: Dict[str, Any]) -> Dict[str, Any]:
    post_id = post.get("name") or post.get("id")
    title = post.get("title") or ""
    body = post.get("selftext") or ""
    text = (title + "\n\n" + body).strip() if body else title.strip()

    likes = int(post.get("score", 0) or 0)
    comments = int(post.get("num_comments", 0) or 0)

    return {
        "post_id": post_id,
        "platform": "reddit",
        "text": text,
        "hashtags": extract_hashtags(text),
        "mentions": extract_mentions(text),
        "created_at": iso_utc(float(post.get("created_utc", 0) or 0)),
        "likes": likes,
        "comments": comments,
        "shares": 0,          # Reddit doesn't expose reliable "shares"
        "language": "en",     # Reddit doesn't provide; detect later if needed
        "location": None,     # Reddit not geo-tagged
        "subreddit": post.get("subreddit"),
        "permalink": "https://www.reddit.com" + (post.get("permalink") or ""),
        "url": post.get("url"),
    }


# HTTP: polite fetch with retry/backoff
def fetch_json(
    url: str,
    params: Dict[str, Any],
    user_agent: str,
    timeout: int = 30,
    max_retries: int = 6,
) -> Dict[str, Any]:
    headers = {"User-Agent": user_agent}

    for attempt in range(max_retries):
        r = requests.get(url, params=params, headers=headers, timeout=timeout)

        # Rate limit
        if r.status_code in (429, 503):
            retry_after = r.headers.get("retry-after")
            sleep_for = int(retry_after) if retry_after and retry_after.isdigit() else (2 ** attempt)
            sleep_for = min(max(sleep_for, 2), 60)
            print(f"[{r.status_code}] Rate-limited. Sleeping {sleep_for}s then retrying...")
            time.sleep(sleep_for)
            continue

        # Some regions/accounts may see 403 occasionally
        if r.status_code == 403 and attempt < max_retries - 1:
            sleep_for = min(2 ** attempt, 30)
            print(f"[403] Forbidden (temporary). Sleeping {sleep_for}s then retrying...")
            time.sleep(sleep_for)
            continue

        r.raise_for_status()
        return r.json()

    raise RuntimeError(f"Failed to fetch after {max_retries} retries: {url}")

# Search function (sitewide or subreddit)
def search_reddit(
    query: str,
    limit_total: int = 300,
    subreddit: Optional[str] = None,
    sort: str = "top",
    t: str = "week",
    sleep_s: float = 1.4,
    user_agent: str = "TopicScraper/1.0 (contact: you@example.com)",
) -> List[Dict[str, Any]]:
    """
    query: keyword expression
    subreddit: None for sitewide, or "technology", "Nigeria", etc.
    sort: 'new', 'top', 'relevance', 'comments'
    t: 'day','week','month','year','all'
    """
    if subreddit:
        base = f"https://www.reddit.com/r/{subreddit}/search.json"
        params = {"q": query, "restrict_sr": 1, "sort": sort, "t": t, "limit": 100}
    else:
        base = "https://www.reddit.com/search.json"
        params = {"q": query, "sort": sort, "t": t, "limit": 100}

    all_rows: List[Dict[str, Any]] = []
    after = None

    while len(all_rows) < limit_total:
        if after:
            params["after"] = after

        data = fetch_json(base, params=params, user_agent=user_agent)
        children = data.get("data", {}).get("children", [])
        if not children:
            break

        for child in children:
            post = child.get("data", {})
            all_rows.append(to_schema(post))
            if len(all_rows) >= limit_total:
                break

        after = data.get("data", {}).get("after")
        if not after:
            break

        time.sleep(sleep_s)

    return all_rows

# Output helpers: JSONL + CSV
def save_jsonl(rows: List[Dict[str, Any]], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def save_csv(rows: List[Dict[str, Any]], path: str) -> None:
    # Flatten lists for CSV
    fieldnames = [
        "post_id","platform","text","hashtags","mentions","created_at",
        "likes","comments","shares","language","location","subreddit",
        "permalink","url","topic_tag","engagement"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            row = dict(r)
            row["hashtags"] = ",".join(row.get("hashtags") or [])
            row["mentions"] = ",".join(row.get("mentions") or [])
            w.writerow({k: row.get(k) for k in fieldnames})


# MAIN: Topics + scrape high-engagement posts
if __name__ == "__main__":
    topics = {
        "ai_tools": '("AI tool" OR "AI tools" OR "ChatGPT alternative" OR "ChatGPT alternatives" OR "best AI" OR "AI for students" OR "AI for work")',
        "phone_launches": '(iPhone OR Samsung OR "Galaxy S" OR "new iPhone" OR "phone launch" OR "battery life" OR camera) (vs OR comparison OR review)',
        "gadgets_gaming": '(PS5 OR Xbox OR PlayStation OR "GTA 6" OR "GTA VI" OR "mobile gaming" OR "gaming phone" OR Nintendo OR Steam)',
        "cybersecurity_scams": '(scam OR "got scammed" OR hacked OR "account hacked" OR phishing OR "SIM swap" OR "bank alert" OR "crypto scam")',

        "side_hustles": '("side hustle" OR "make money online" OR freelancing OR "online business" OR "passive income" OR "extra income")',
        "job_hunting": '("job search" OR "job hunting" OR interview OR "CV" OR resume OR "cover letter" OR "tech interview")',
        "crypto_forex": '(crypto OR bitcoin OR BTC OR ethereum OR ETH OR altcoin OR forex OR "FX" OR "trading") (crash OR pump OR dip OR rally OR news OR prediction)',
        "personal_finance": '(budget OR budgeting OR saving OR savings OR investing OR "what would you do with" OR "how do I save") (money OR naira OR ₦ OR income)',

        "dating_advice": '(dating OR relationship OR "relationship advice" OR breakup OR "talking stage" OR "situationship")',
        "marriage_red_flags": '(marriage OR "red flag" OR "red flags" OR "green flag" OR spouse OR husband OR wife OR "long term relationship")',
        "wellness_productivity": '(productivity OR "study routine" OR burnout OR motivation OR "time management" OR "focus" OR "discipline" OR "self improvement")',

        "music_celebrity": '(album OR "new song" OR "music video" OR "music drop" OR "tracklist" OR celebrity OR "celebrity gist" OR controversy OR "beef")',
        "movies_series": '(Netflix OR "TV show" OR series OR movie OR Marvel OR anime OR "season finale" OR ending OR review)',
        "football": '(football OR soccer OR "Premier League" OR "Champions League" OR transfer OR "who is better" OR "GOAT" OR "match thread")',
        "big_events": '(awards OR "red carpet" OR "reality show" OR "BBNaija" OR "Grammy" OR "Oscars" OR "Met Gala" OR controversy)',

        "cost_of_living": '("cost of living" OR inflation OR rent OR transport OR groceries OR "food prices" OR "fuel price" OR "electricity tariff")',
        "education": '(exam OR "study tips" OR university OR college OR "course choice" OR "best course" OR CGPA OR "final year")',
        "nigeria_tech": '(Nigeria OR Lagos OR Abuja) (fintech OR POS OR "bank transfer" OR "network issues" OR MTN OR Airtel OR Glo OR "data plan" OR "mobile money")',
    }

    USER_AGENT = os.getenv("USER_AGENT")
    LIMIT_PER_TOPIC = 200
    SORT = "top"
    TIME_FILTER = "week"
    SLEEP_S = 1.4

    combined: List[Dict[str, Any]] = []
    seen_ids = set()

    for tag, q in topics.items():
        rows = search_reddit(
            query=q,
            limit_total=LIMIT_PER_TOPIC,
            subreddit=None,
            sort=SORT,
            t=TIME_FILTER,
            sleep_s=SLEEP_S,
            user_agent=USER_AGENT,
        )

        added = 0
        for r in rows:
            pid = r.get("post_id")
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)
            r["topic_tag"] = tag
            r["engagement"] = int(r.get("likes", 0) or 0) + int(r.get("comments", 0) or 0)
            combined.append(r)
            added += 1

        print(f"{tag}: fetched={len(rows)} added={added}")

    save_jsonl(combined, "reddit_topics.jsonl")
    save_csv(combined, "reddit_topics.csv")
    print(f"Saved {len(combined)} unique posts -> reddit_topics.jsonl and reddit_topics.csv")
