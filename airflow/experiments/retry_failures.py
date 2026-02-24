# 이 코드는 naver_blog_latest_dump.py에 있는 fetch_post_content()를 재사용해야 하니까, 그 파일을 import해서 씀

import json
from pathlib import Path
from datetime import datetime, timezone

# naver_blog_latest_dump.py 안의 함수 재사용
from airflow.naver_blog_latest_dump import fetch_post_content

# 이미 실행해서 만들어진 run 결과 폴더를 가리킴
RUN_DIR = "run_20260223_153011"

def main():
    posts_path = Path(RUN_DIR) / "posts.json"
    data = json.load(open(posts_path, "r", encoding="utf-8"))

    failures = data.get("failures", [])
    if not failures:
        print("No failures to retry.")
        return

    # 실패 URL 목록
    urls = [f["url"] for f in failures]
    print("Retrying:", len(urls))

    recovered = []
    still_failed = []

    for url in urls:
        try:
            post = fetch_post_content(url)
            recovered.append({
                "url": url,
                "method": post.get("method"),
                "final_url": post.get("final_url"),
                "title": post.get("title"),
                "content_text": post.get("content_text"),
                "fallback_reason": post.get("fallback_reason"),
                "retried_at": datetime.now(timezone.utc).isoformat(),
            })
            print("[OK]", url)
        except Exception as e:
            still_failed.append({"url": url, "error": str(e)})
            print("[FAIL]", url, str(e))

    # 기존 posts에 붙이기
    data.setdefault("posts", [])
    data["posts"].extend(recovered)

    # failures는 still_failed로 교체
    data["failures"] = still_failed

    # 카운트 갱신
    data["saved_count"] = len(data["posts"])
    data["failed_count"] = len(data["failures"])
    data["saved_at"] = datetime.now(timezone.utc).isoformat()

    with open(posts_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("\nUpdated:", str(posts_path))
    print("saved_count:", data["saved_count"])
    print("failed_count:", data["failed_count"])

if __name__ == "__main__":
    main()