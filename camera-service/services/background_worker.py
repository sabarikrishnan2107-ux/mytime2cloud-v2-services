"""Background face detection worker.

Runs independently of the frontend — connects to camera proxy streams,
detects faces, matches against employee embeddings, and logs attendance
automatically. Starts on service launch and manages all active cameras.
"""

import asyncio
import cv2
import httpx
import numpy as np
import websockets

from config import (
    LARAVEL_API_URL,
    LARAVEL_API_TOKEN,
    FACE_SIMILARITY_THRESHOLD,
    FACE_DETECT_INTERVAL,
    AUTO_LOG_RECOGNIZED_FACES,
    HOST,
)
from services import arcface
from routers.detect import (
    get_embeddings,
    refresh_embeddings,
    fetch_device_details,
    auto_log_camera_attendance,
)

PROXY_URL = "ws://localhost:8501"
CAMERA_POLL_INTERVAL = 60  # Check for new cameras every 60 seconds
RECONNECT_DELAY = 10  # Wait 10 seconds before reconnecting a failed camera
FRAME_DETECT_INTERVAL = FACE_DETECT_INTERVAL  # Same speed as live view

# Track running workers
_active_workers = {}  # {device_id: asyncio.Task}
_worker_running = False


def log(msg):
    print(f"[BG-WORKER] {msg}", flush=True)


def fetch_active_cameras() -> list:
    """Fetch all cameras with RTSP configured directly from database."""
    from database.connection import get_connection
    from psycopg2.extras import RealDictCursor

    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, camera_rtsp_ip, company_id, branch_id
                FROM devices
                WHERE camera_rtsp_ip IS NOT NULL
                  AND camera_rtsp_ip != ''
                  AND status_id = 1
                ORDER BY id
            """)
            cameras = cur.fetchall()
        conn.close()

        log(f"Found {len(cameras)} active cameras with RTSP configured")
        return [
            {"id": cam["id"], "name": cam["name"] or f"Camera {cam['id']}"}
            for cam in cameras
        ]
    except Exception as e:
        log(f"Error fetching cameras: {e}")
        return []


async def detect_worker(device_id: str):
    """Background detection worker for a single camera.

    Connects to the camera proxy, runs ArcFace on frames,
    and logs attendance for recognized employees.
    Reconnects automatically on failure.
    """
    log(f"Starting worker for device {device_id}")

    while _worker_running:
        proxy_ws = None
        try:
            # Get device metadata and load embeddings
            device_details = await fetch_device_details(str(device_id))
            company_id = device_details["company_id"]
            branch_id = device_details.get("branch_id")
            device_name = device_details["device_name"]
            device_serial = device_details.get("device_serial")
            device_type = (device_details.get("device_type") or "all").lower()
            attendance_enabled = device_type in ("all", "attendance", "auto", "camera")

            stored = get_embeddings(company_id, branch_id)
            log(f"Device {device_id} ({device_name}): loaded {len(stored)} employee embeddings")

            if not stored:
                log(f"Device {device_id}: no embeddings for this branch, sleeping...")
                await asyncio.sleep(RECONNECT_DELAY * 3)
                continue

            # Connect to camera proxy
            proxy_ws = await websockets.connect(
                f"{PROXY_URL}/stream/{device_id}",
                max_size=10 * 1024 * 1024,
            )
            log(f"Device {device_id}: connected to proxy, detection active")

            frame_count = 0
            loop = asyncio.get_event_loop()

            async for message in proxy_ws:
                if not _worker_running:
                    break

                if isinstance(message, bytes):
                    frame_count += 1

                    if frame_count % FRAME_DETECT_INTERVAL != 0:
                        continue

                    # Decode frame
                    img_array = np.frombuffer(message, dtype=np.uint8)
                    frame = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
                    if frame is None:
                        continue

                    # Detect faces
                    faces = await loop.run_in_executor(None, arcface.detect_faces, frame)

                    for face in faces:
                        embedding = arcface.get_embedding(face)
                        match = arcface.compare_embedding(
                            embedding, stored, FACE_SIMILARITY_THRESHOLD
                        )

                        if match and AUTO_LOG_RECOGNIZED_FACES and attendance_enabled:
                            try:
                                result = auto_log_camera_attendance(
                                    device_id=str(device_id),
                                    device_name=device_name,
                                    employee_id=match["employee_id"],
                                    device_serial=device_serial,
                                )
                                if result.get("status") == "created":
                                    log(f"Device {device_id}: attendance logged for {match['name']} ({match['confidence']:.2f})")
                            except Exception as e:
                                log(f"Device {device_id}: attendance log error: {e}")

                    # Refresh embeddings periodically
                    if frame_count % 300 == 0:
                        refresh_embeddings(company_id, branch_id)
                        stored = get_embeddings(company_id, branch_id)

                elif isinstance(message, str):
                    log(f"Device {device_id}: proxy message: {message}")

        except websockets.exceptions.ConnectionClosed:
            log(f"Device {device_id}: proxy connection closed")
        except ConnectionRefusedError:
            log(f"Device {device_id}: proxy not available")
        except Exception as e:
            log(f"Device {device_id}: worker error: {type(e).__name__}: {e}")
        finally:
            if proxy_ws is not None:
                try:
                    await proxy_ws.close()
                except Exception:
                    pass

        if _worker_running:
            log(f"Device {device_id}: reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)


async def camera_manager():
    """Periodically polls for active cameras and starts/stops workers."""
    global _worker_running
    _worker_running = True

    log("Camera manager started — polling for active cameras")

    while _worker_running:
        try:
            cameras = fetch_active_cameras()
            active_ids = {str(cam["id"]) for cam in cameras}

            # Start workers for new cameras
            for cam in cameras:
                did = str(cam["id"])
                if did not in _active_workers or _active_workers[did].done():
                    log(f"Starting detection worker for {cam['name']} (device {did})")
                    _active_workers[did] = asyncio.create_task(detect_worker(did))

            # Stop workers for removed cameras
            for did in list(_active_workers.keys()):
                if did not in active_ids:
                    log(f"Stopping worker for device {did} (camera removed)")
                    _active_workers[did].cancel()
                    del _active_workers[did]

        except Exception as e:
            log(f"Camera manager error: {e}")

        await asyncio.sleep(CAMERA_POLL_INTERVAL)


async def start_background_detection():
    """Entry point — called from FastAPI startup event."""
    log("Background detection system starting...")
    asyncio.create_task(camera_manager())


async def stop_background_detection():
    """Shutdown — called from FastAPI shutdown event."""
    global _worker_running
    _worker_running = False

    log("Stopping all background detection workers...")
    for did, task in _active_workers.items():
        task.cancel()
    _active_workers.clear()
    log("All workers stopped")
