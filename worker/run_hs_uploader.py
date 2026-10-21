"""hotscope → namevids uploader. Remux HLS (video_1.m3u8) via ffmpeg + caption pscp18rs."""
from __future__ import annotations

import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from namevids_client import (
    build_namevids_caption_link,
    clear_pending_drafts,
    delete_draft_fid,
    fetch_api_key,
    login,
    parse_account_metadata,
    publish,
    sanitize_namevids_title,
    upload_stream,
)
from peach_db import db_conn, get_setting, get_upload_account
from pipeline_config import MAX_PER_RUN, NAMEVIDS_DAILY_CAP
from telegram_links import telegram_url

DEFAULT_HS_CAPTION_LINK = telegram_url("pscp18rs")

STUCK_UPLOADING_MINUTES = int(os.environ.get("STUCK_UPLOADING_MINUTES", "15"))
MIN_DOWNLOAD_BYTES = 50 * 1024
GAP_SEC = float(os.environ.get("UPLOADER_GAP_SEC", "30"))
MAX_DURATION_SEC = int(os.environ.get("HS_MAX_DURATION_SEC", "1200"))
FFMPEG_TIMEOUT_SEC = int(os.environ.get("HS_FFMPEG_TIMEOUT_SEC", "600"))


def _reset_stuck_uploading(conn) -> int:
    cur = conn.execute(
        """
        UPDATE videos
        SET status = 'parsed',
            error_message = 'reset: previous upload timed out',
            updated_at = now()
        WHERE status = 'uploading'
          AND updated_at < now() - make_interval(mins => %s)
        RETURNING id
        """,
        (STUCK_UPLOADING_MINUTES,),
    )
    ids = [str(row["id"]) for row in cur.fetchall()]
    if ids:
        print(f"uploader: reset stuck uploading: {', '.join(ids)}")
    return len(ids)


def _claim_videos(conn, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        UPDATE videos
        SET status = 'uploading', updated_at = now()
        WHERE id IN (
            SELECT id FROM videos
            WHERE status = 'parsed'
              AND id LIKE 'hs%%'
              AND (
                error_message IS NULL
                OR error_message = ''
                OR (
                  error_message NOT LIKE '%%429%%'
                  AND error_message NOT LIKE '%%too fast%%'
                )
                OR updated_at < now() - interval '60 minutes'
              )
            ORDER BY
              CASE
                WHEN error_message IS NULL OR error_message = '' THEN 0
                ELSE 1
              END,
              duration_seconds ASC NULLS LAST,
              parsed_at ASC,
              id ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def _release_unprocessed(queue: list[dict[str, Any]], processed: int) -> None:
    remaining = [str(row["id"]) for row in queue[processed:]]
    if not remaining:
        return
    with db_conn() as conn:
        conn.execute(
            """
            UPDATE videos
            SET status = 'parsed', updated_at = now()
            WHERE id = ANY(%s) AND status = 'uploading'
            """,
            (remaining,),
        )
        conn.commit()
    print(f"uploader: released {len(remaining)} unprocessed claim(s)")


def _mark_published(video_id: str, *, file_id: str, namevids_account_id: int) -> None:
    with db_conn() as conn:
        conn.execute(
            """
            UPDATE videos SET
              status = 'published',
              namevids_file_id = %s,
              namevids_account_id = %s,
              published_at = now(),
              updated_at = now(),
              error_message = NULL
            WHERE id = %s
            """,
            (file_id, namevids_account_id, video_id),
        )
        conn.commit()


def _mark_failed(video_id: str, error: str) -> None:
    with db_conn() as conn:
        conn.execute(
            """
            UPDATE videos
            SET status = 'failed', error_message = %s, updated_at = now()
            WHERE id = %s AND status = 'uploading'
            """,
            (error[:500], video_id),
        )
        conn.commit()


def _mark_skipped(video_id: str, reason: str) -> None:
    with db_conn() as conn:
        conn.execute(
            """
            UPDATE videos
            SET status = 'skipped', error_message = %s, updated_at = now()
            WHERE id = %s
            """,
            (reason[:500], video_id),
        )
        conn.commit()


def _namevids_published_today(conn, namevids_account_id: int) -> int:
    row = conn.execute(
        """
        SELECT count(*)::int AS n
        FROM videos
        WHERE status = 'published'
          AND namevids_account_id = %s
          AND coalesce(published_at, updated_at) >= date_trunc('day', now() AT TIME ZONE 'UTC')
        """,
        (namevids_account_id,),
    ).fetchone()
    return int(row["n"] if row else 0)


def _recent_namevids_rate_limits(conn) -> int:
    row = conn.execute(
        """
        SELECT count(*)::int AS n
        FROM videos
        WHERE error_message ILIKE '%%too fast%%'
          AND updated_at > now() - interval '30 minutes'
        """
    ).fetchone()
    return int(row["n"] if row else 0)


def _remux_hls(hls_url: str, dest: Path) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        hls_url,
        "-c",
        "copy",
        "-bsf:a",
        "aac_adtstoasc",
        str(dest),
    ]
    print(f"uploader: ffmpeg remux {hls_url}")
    try:
        subprocess.run(cmd, check=True, timeout=FFMPEG_TIMEOUT_SEC)
    except FileNotFoundError as exc:
        raise RuntimeError("ffmpeg not found on runner") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"ffmpeg timeout after {FFMPEG_TIMEOUT_SEC}s") from exc
    size = dest.stat().st_size if dest.is_file() else 0
    if size < MIN_DOWNLOAD_BYTES:
        raise RuntimeError(f"Remux too small ({size} bytes)")
    print(f"uploader: clip size {size / (1024 * 1024):.1f} MB")


def _hls_url(row: dict[str, Any]) -> str:
    preview = str(row.get("preview_path") or "").strip()
    if preview.endswith(".m3u8"):
        return preview
    # rebuild from video path /video/{slug}
    path = str(row.get("video_path") or "").strip()
    slug = path.rstrip("/").rsplit("/", 1)[-1] if path else ""
    if not slug:
        raise RuntimeError("No HLS url / video slug")
    return f"https://cdn.hotscope.tv/videos/{slug}/video_1.m3u8"


def _upload_one(
    row: dict[str, Any],
    *,
    namevids_acc: dict[str, Any],
    settings: dict[str, str],
) -> None:
    vid = str(row["id"])
    duration = row.get("duration_seconds")
    if duration is not None and int(duration) > MAX_DURATION_SEC:
        _mark_skipped(vid, f"skip: duration {duration}s > {MAX_DURATION_SEC}s")
        print(f"uploader: {vid} skipped long video ({duration}s)")
        return

    hls = _hls_url(row)
    meta = parse_account_metadata(namevids_acc.get("metadata"))
    nv_client = login(
        str(namevids_acc["login"]),
        str(namevids_acc["secret"]),
        metadata=meta,
    )
    file_id = ""
    api_key = ""
    try:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = Path(tmp) / f"{vid}.mp4"
            _remux_hls(hls, clip_path)
            api_key = fetch_api_key(nv_client)
            file_id = upload_stream(nv_client, api_key, str(clip_path), f"{vid}.mp4")
            title = sanitize_namevids_title(
                title_ru=str(row.get("title_ru") or ""),
                title_en=str(row.get("title_en") or ""),
                video_id=vid,
            )
            link = build_namevids_caption_link(settings, vid)
            # caption = just the TG link (user asked for one link in description)
            caption = link
            print(f"uploader: {vid} namevids title={title!r} link={link}")
            publish(
                nv_client,
                api_key,
                file_id,
                title,
                caption,
                fallback_caption=link or ".",
                fallback_title=vid,
            )
        _mark_published(
            vid,
            file_id=file_id,
            namevids_account_id=int(namevids_acc["id"]),
        )
        print(f"uploader: published {vid}")
    except Exception:
        if file_id and api_key:
            try:
                delete_draft_fid(nv_client, api_key, file_id)
            except Exception as cleanup_exc:
                print(f"uploader: {vid} draft cleanup failed: {cleanup_exc}")
        raise
    finally:
        nv_client.close()


def run_uploader() -> int:
    done = 0
    with db_conn() as conn:
        namevids_acc = get_upload_account(conn, "namevids")
        if not namevids_acc:
            raise RuntimeError("No enabled namevids account in upload_accounts")
        caption_link = (
            get_setting(conn, "namevids.caption_link", "").strip() or DEFAULT_HS_CAPTION_LINK
        )
        settings = {"namevids.caption_link": caption_link}
        _reset_stuck_uploading(conn)
        published_today = _namevids_published_today(conn, int(namevids_acc["id"]))
        if published_today >= NAMEVIDS_DAILY_CAP:
            print(
                f"uploader: namevids daily cap {NAMEVIDS_DAILY_CAP} reached "
                f"({published_today} today on account {namevids_acc['login']}) — skip run"
            )
            return 0
        recent_rl = _recent_namevids_rate_limits(conn)
        if recent_rl >= 3:
            print(f"uploader: namevids rate-limited ({recent_rl} recent) — skip run")
            return 0
        remaining_today = NAMEVIDS_DAILY_CAP - published_today
        batch_size = 1 if recent_rl >= 1 else MAX_PER_RUN
        batch_size = min(batch_size, remaining_today)
        if batch_size < 1:
            return 0
        queue = _claim_videos(conn, batch_size)
        conn.commit()

    if not queue:
        print("uploader: queue empty")
        return 0

    prep_client = login(
        str(namevids_acc["login"]),
        str(namevids_acc["secret"]),
        metadata=parse_account_metadata(namevids_acc.get("metadata")),
    )
    try:
        prep_key = fetch_api_key(prep_client)
        clear_pending_drafts(prep_client, prep_key)
    finally:
        prep_client.close()

    processed = 0
    try:
        for index, row in enumerate(queue):
            vid = str(row["id"])
            try:
                _upload_one(row, namevids_acc=dict(namevids_acc), settings=settings)
                done += 1
            except Exception as exc:
                print(f"uploader: {vid} failed: {exc}")
                _mark_failed(vid, str(exc))
            processed += 1
            if index + 1 < len(queue) and GAP_SEC > 0:
                time.sleep(GAP_SEC)
    finally:
        _release_unprocessed(queue, processed)

    print(f"uploader: done {done} video(s)")
    return done


if __name__ == "__main__":
    run_uploader()
