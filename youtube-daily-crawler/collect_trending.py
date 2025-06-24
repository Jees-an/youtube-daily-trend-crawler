import os
import json
import pandas as pd
from datetime import datetime, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def main():
    # ✅ [1] 설정
    API_KEY = os.environ['YOUTUBE_API_KEY']
    REGION = 'KR'
    MAX_RESULTS = 50

    # ✅ [2] 저장 디렉토리 설정
    BASE_DIR = './output'
    CSV_DIR = os.path.join(BASE_DIR, 'trending')
    JSON_DIR = os.path.join(BASE_DIR, 'json')
    LOG_DIR = os.path.join(BASE_DIR, 'log')

    for d in [CSV_DIR, JSON_DIR, LOG_DIR]:
        os.makedirs(d, exist_ok=True)

    # ✅ [3] 카테고리 ID → 한글 이름 매핑
    CATEGORY_MAP = {
        "1": "영화/애니메이션", "2": "자동차/차량", "10": "음악",
        "15": "애완동물/동물", "17": "스포츠", "18": "단편 영화",
        "19": "여행/이벤트", "20": "게임", "21": "비디오 블로깅",
        "22": "인물/블로그", "23": "코미디", "24": "엔터테인먼트",
        "25": "뉴스/정치", "26": "사용법/스타일", "27": "교육",
        "28": "과학/기술", "29": "비영리/사회운동"
    }

    # ✅ [4] 시간 및 파일 경로 설정
    now = datetime.now(timezone.utc).astimezone()
    now_str = now.strftime('%Y%m%d_%H%M%S')
    date_str = now.strftime('%Y%m%d')
    collect_time = now.isoformat()

    csv_path = os.path.join(CSV_DIR, f'{date_str}.csv')
    json_backup_path = os.path.join(JSON_DIR, f'{date_str}_response.json')
    log_path = os.path.join(LOG_DIR, f'{date_str}.log')

    # ✅ [5] 수집 시작
    try:
        youtube = build('youtube', 'v3', developerKey=API_KEY)
        request = youtube.videos().list(
            part='snippet,statistics',
            chart='mostPopular',
            regionCode=REGION,
            maxResults=MAX_RESULTS
        )
        response = request.execute()

        # ✅ [6] JSON 응답 백업
        with open(json_backup_path, 'w', encoding='utf-8') as f:
            json.dump(response, f, ensure_ascii=False, indent=2)

        # ✅ [7] 데이터 처리
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
                category_ko = CATEGORY_MAP.get(category_id, '기타')

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

        # ✅ [8] CSV 저장
        if data:
            df = pd.DataFrame(data)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')

            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(f"[{collect_time}] ✅ 수집 성공: {len(df)}개 영상\n")

            print(f"✅ 수집 완료: {len(df)}개 영상 → {csv_path}")
        else:
            raise ValueError("❌ 유효한 데이터가 없음")

    # ✅ [9] 예외 처리
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