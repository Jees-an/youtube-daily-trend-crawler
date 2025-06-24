import os
import re
import json
import pandas as pd
from datetime import datetime, timezone
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
    REGION = 'KR'
    MAX_RESULTS = 50

    BASE_DIR = './output'
    CSV_DIR = os.path.join(BASE_DIR, 'trending')
    JSON_DIR = os.path.join(BASE_DIR, 'json')
    COMMENTS_DIR = os.path.join(BASE_DIR, 'comments')
    LOG_DIR = os.path.join(BASE_DIR, 'log')

    for d in [CSV_DIR, JSON_DIR, COMMENTS_DIR, LOG_DIR]:
        os.makedirs(d, exist_ok=True)

    now = datetime.now(timezone.utc).astimezone()
    now_str = now.strftime('%Y%m%d_%H%M%S')
    date_str = now.strftime('%Y%m%d')
    collect_time = now.isoformat()

    csv_path = os.path.join(CSV_DIR, f'{date_str}.csv')
    json_backup_path = os.path.join(JSON_DIR, f'{date_str}_response.json')
    log_path = os.path.join(LOG_DIR, f'{date_str}.log')
    comment_output_dir = os.path.join(COMMENTS_DIR, date_str)
    os.makedirs(comment_output_dir, exist_ok=True)

    try:
        youtube = build('youtube', 'v3', developerKey=API_KEY)
        request = youtube.videos().list(
            part='snippet,statistics',
            chart='mostPopular',
            regionCode=REGION,
            maxResults=MAX_RESULTS
        )
        response = request.execute()

        with open(json_backup_path, 'w', encoding='utf-8') as f:
            json.dump(response, f, ensure_ascii=False, indent=2)

        items = response.get('items', [])
        data = []

        for item in items:
            try:
                video_id = item.get('id', '')
                snippet = item.get('snippet', {})
                stats = item.get('statistics', {})
                tags = snippet.get('tags', [])
                tags_str = ', '.join(tags) if isinstance(tags, list) else ''

                category_id = snippet.get('categoryId', '')
                category_ko = {
                    "1": "영화/애니메이션", "2": "자동차/차량", "10": "음악",
                    "15": "애완동물/동물", "17": "스포츠", "18": "단편 영화",
                    "19": "여행/이벤트", "20": "게임", "21": "비디오 블로깅",
                    "22": "인물/블로그", "23": "코미디", "24": "엔터테인먼트",
                    "25": "뉴스/정치", "26": "사용법/스타일", "27": "교육",
                    "28": "과학/기술", "29": "비영리/사회운동"
                }.get(category_id, '기타')

                view_count = stats.get('viewCount', '')
                like_count = stats.get('likeCount', '')
                comment_count = stats.get('commentCount', '')

                data.append({
                    'video_id': video_id,
                    'title': snippet.get('title', ''),
                    'channelTitle': snippet.get('channelTitle', ''),
                    'publishedAt': snippet.get('publishedAt', ''),
                    'viewCount': view_count,
                    'likeCount': like_count,
                    'commentCount': comment_count,
                    'categoryId': category_id,
                    'categoryName_ko': category_ko,
                    'tags': tags_str,
                    'collectedAt': collect_time
                })

            except Exception as e:
                print(f"⚠️ 항목 처리 오류 (video_id: {item.get('id', 'N/A')}): {e}")
                continue

        if data:
            df = pd.DataFrame(data)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(f"[{collect_time}] ✅ 수집 성공: {len(df)}개 영상\n")
            print(f"✅ 수집 완료: {len(df)}개 영상 → {csv_path}")
        else:
            raise ValueError("❌ 유효한 데이터가 없음")

        # ✅ 댓글 수집 시작
        total_comment_count = 0
        total_reply_count = 0
        failed = []

        video_ids = [item.get('id', '') for item in items if item.get('id', '')]

        for video_id in video_ids:
            try:
                comments, replies = get_comments_and_replies(youtube, video_id)

                if comments:
                    df_c = pd.DataFrame(comments)
                    df_c.to_csv(os.path.join(comment_output_dir, f'video_{video_id}_comments.csv'),
                                index=False, encoding='utf-8-sig')
                    total_comment_count += len(df_c)

                if replies:
                    df_r = pd.DataFrame(replies)
                    df_r.to_csv(os.path.join(comment_output_dir, f'video_{video_id}_replies.csv'),
                                index=False, encoding='utf-8-sig')
                    total_reply_count += len(df_r)

                print(f"✅ 댓글 수집 완료: {video_id} ({len(comments)} 댓글, {len(replies)} 답글)")

            except Exception as e:
                print(f"❌ 댓글 수집 실패: {video_id} - {e}")
                failed.append((video_id, str(e)))

        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"[{date_str}] ✅ 댓글 수집: {total_comment_count}개\n")
            f.write(f"[{date_str}] ✅ 답글 수집: {total_reply_count}개\n")
            if failed:
                f.write(f"[{date_str}] ❌ 실패한 영상: {len(failed)}개\n")
                for vid, err in failed:
                    f.write(f" - {vid}: {err}\n")

    except HttpError as e:
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"[{collect_time}] ❌ API 요청 실패: {e}\n")
        print(f"❌ API 요청 실패: {e}")

    except Exception as e:
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"[{collect_time}] ❌ 예외 발생: {e}\n")
        print(f"❌ 예외 발생: {e}")

if __name__ == "__main__":
    main()