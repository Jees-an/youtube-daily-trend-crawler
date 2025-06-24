import os
import re
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def clean_text(text):
    text = re.sub(r'[\n\r]', ' ', text)
    text = re.sub(r'<br>', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def get_comments_and_replies(youtube, video_id, max_comments=100):
    top_comments = []
    replies = []
    next_page_token = None

    while len(top_comments) < max_comments:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token,
            textFormat="plainText"
        )
        response = request.execute()

        for item in response.get("items", []):
            top_snippet = item["snippet"]["topLevelComment"]["snippet"]
            top_comment_id = item["snippet"]["topLevelComment"]["id"]

            top_comments.append({
                "video_id": video_id,
                "comment_id": top_comment_id,
                "author": clean_text(top_snippet.get("authorDisplayName", "")),
                "author_channel_id": top_snippet.get("authorChannelId", {}).get("value", ""),
                "text": clean_text(top_snippet.get("textDisplay", "")),
                "likeCount": top_snippet.get("likeCount", 0),
                "publishedAt": top_snippet.get("publishedAt", ""),
                "updatedAt": top_snippet.get("updatedAt", ""),
                "replyCount": item["snippet"].get("totalReplyCount", 0)
            })

            # 답글 수집
            if item["snippet"].get("totalReplyCount", 0) > 0:
                replies_list = item.get("replies", {}).get("comments", [])
                for reply in replies_list:
                    rep_snippet = reply["snippet"]
                    replies.append({
                        "video_id": video_id,
                        "parent_comment_id": top_comment_id,
                        "reply_id": reply["id"],
                        "author": clean_text(rep_snippet.get("authorDisplayName", "")),
                        "author_channel_id": rep_snippet.get("authorChannelId", {}).get("value", ""),
                        "text": clean_text(rep_snippet.get("textDisplay", "")),
                        "likeCount": rep_snippet.get("likeCount", 0),
                        "publishedAt": rep_snippet.get("publishedAt", ""),
                        "updatedAt": rep_snippet.get("updatedAt", "")
                    })

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return top_comments[:max_comments], replies

def main():
    API_KEY = os.environ['YOUTUBE_API_KEY']
    BASE_DIR = './output'
    TRENDING_DIR = os.path.join(BASE_DIR, 'trending')
    COMMENTS_DIR = os.path.join(BASE_DIR, 'comments')
    LOG_DIR = os.path.join(BASE_DIR, 'log')
    os.makedirs(LOG_DIR, exist_ok=True)

    # ✅ 오늘 날짜 기준
    target_date = datetime.now(timezone.utc).astimezone()
    date_str = target_date.strftime('%Y%m%d')

    trending_path = os.path.join(TRENDING_DIR, f'{date_str}.csv')
    output_dir = os.path.join(COMMENTS_DIR, date_str)
    os.makedirs(output_dir, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f'{date_str}_comments.log')

    try:
        if not os.path.exists(trending_path):
            raise FileNotFoundError(f"📁 인급동 파일 없음: {trending_path}")

        df = pd.read_csv(trending_path)
        video_ids = df['video_id'].dropna().astype(str).tolist()

        youtube = build("youtube", "v3", developerKey=API_KEY)
        total_comment_count = 0
        total_reply_count = 0
        failed = []

        for video_id in video_ids:
            try:
                comments, replies = get_comments_and_replies(youtube, video_id)

                if comments:
                    df_c = pd.DataFrame(comments)
                    df_c.to_csv(os.path.join(output_dir, f'video_{video_id}_comments.csv'),
                                index=False, encoding='utf-8-sig')
                    total_comment_count += len(df_c)

                if replies:
                    df_r = pd.DataFrame(replies)
                    df_r.to_csv(os.path.join(output_dir, f'video_{video_id}_replies.csv'),
                                index=False, encoding='utf-8-sig')
                    total_reply_count += len(df_r)

                print(f"✅ 수집 완료: {video_id} ({len(comments)} 댓글, {len(replies)} 답글)")

            except Exception as e:
                print(f"❌ 실패: {video_id} - {e}")
                failed.append((video_id, str(e)))

        # 로그 작성
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"[{date_str}] ✅ 댓글 수집: {total_comment_count}개\n")
            f.write(f"[{date_str}] ✅ 답글 수집: {total_reply_count}개\n")
            if failed:
                f.write(f"[{date_str}] ❌ 실패한 영상: {len(failed)}개\n")
                for vid, err in failed:
                    f.write(f" - {vid}: {err}\n")

    except Exception as e:
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"[{date_str}] ❌ 전체 예외 발생: {e}\n")
        print(f"❌ 전체 예외 발생: {e}")

if __name__ == "__main__":
    main()