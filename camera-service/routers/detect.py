import asyncio
import cv2
import httpx
import numpy as np
from datetime import datetime, timedelta
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from config import (
    LARAVEL_API_URL,
    LARAVEL_API_TOKEN,
    FACE_SIMILARITY_THRESHOLD,
    FACE_DETECT_INTERVAL,
    AUTO_LOG_RECOGNIZED_FACES,
    ATTENDANCE_DUPLICATE_WINDOW_SECONDS,
)
from services import arcface
from database.connection import load_all_embeddings
from database import attendance as attendance_db
from services.attendance import AttendanceCalculationOptions, AttendanceLogEvent, calculate_daily_attendance

router = APIRouter()

# In-memory cache of employee embeddings, keyed by (company_id, branch_id)
_embeddings_cache = {}

# Camera proxy WebSocket URL
PROXY_URL = "ws://localhost:8501"
_recent_attendance_cache = {}


def log(msg):
    print(msg, flush=True)


def get_embeddings(company_id: int, branch_id: int | None = None) -> dict:
    """Get cached embeddings or load from DB."""
    cache_key = (company_id, branch_id)
    if cache_key not in _embeddings_cache:
        _embeddings_cache[cache_key] = load_all_embeddings(company_id, branch_id)
    return _embeddings_cache[cache_key]


def refresh_embeddings(company_id: int, branch_id: int | None = None):
    """Force refresh embeddings from DB."""
    if branch_id is None:
        stale_keys = [key for key in _embeddings_cache if key[0] == company_id]
        for stale_key in stale_keys:
            _embeddings_cache.pop(stale_key, None)
    else:
        _embeddings_cache.pop((company_id, branch_id), None)

    _embeddings_cache[(company_id, branch_id)] = load_all_embeddings(company_id, branch_id)


async def fetch_device_details(device_id: str) -> dict:
    """Fetch camera/device details from Laravel API."""
    try:
        headers = {}
        if LARAVEL_API_TOKEN and LARAVEL_API_TOKEN.strip():
            headers["Authorization"] = f"Bearer {LARAVEL_API_TOKEN}"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{LARAVEL_API_URL}/camera/{device_id}/credentials",
                headers=headers,
            )
            data = response.json()
            if data.get("status"):
                payload = data["data"]
                return {
                    "company_id": payload.get("company_id") or 1,
                    "branch_id": payload.get("branch_id"),
                    "branch_name": payload.get("branch_name"),
                    "device_name": payload.get("device_name") or f"Camera {device_id}",
                    "device_serial": payload.get("device_serial"),
                    "device_type": payload.get("device_type", "all"),
                }
    except Exception as e:
        log(f"Error fetching device info: {e}")
    return {
        "company_id": 1,
        "branch_id": None,
        "branch_name": None,
        "device_name": f"Camera {device_id}",
        "device_serial": None,
        "device_type": "all",
    }


def _is_night_shift(shift: dict | None) -> bool:
    """Check if shift crosses midnight (off_duty < on_duty means overnight)."""
    if not shift:
        return False
    on = shift.get("on_duty_time", "")
    off = shift.get("off_duty_time", "")
    if not on or not off or on == "---" or off == "---":
        return False
    return off < on  # e.g., on=18:00, off=08:00 → 08:00 < 18:00 = True


def build_attendance_record_preview(employee_id: int, attendance_date, options: AttendanceCalculationOptions) -> dict:
    shift = attendance_db.fetch_employee_shift(employee_id, attendance_date)
    night = _is_night_shift(shift)
    raw_logs = attendance_db.fetch_employee_logs(employee_id, attendance_date, is_night_shift=night)

    events = [
        AttendanceLogEvent(
            id=row["id"],
            employee_id=row["employee_id"],
            log_timestamp=row["log_timestamp"],
            source=row["source"],
            camera_name=row["camera_name"],
        )
        for row in raw_logs
    ]

    record = calculate_daily_attendance(
        employee_id=employee_id,
        attendance_date=attendance_date,
        logs=events,
        shift_start=shift.get("on_duty_time") if shift else None,
        shift_end=shift.get("off_duty_time") if shift else None,
        shift_start2=shift.get("on_duty_time1") if shift else None,
        shift_end2=shift.get("off_duty_time1") if shift else None,
        shift_type_id=shift.get("shift_type_id") if shift else None,
        min_session_minutes=shift.get("minimum_session_duration", 30) if shift else 30,
        options=options,
    )

    return record.to_dict()


def auto_log_camera_attendance(device_id: str, device_name: str, employee_id: int, device_serial: str | None = None) -> dict:
    """Log camera attendance via Laravel API.

    Calls the Laravel endpoint which creates the log via Eloquent,
    triggering the Observer that recalculates the employee's daily
    attendance using the existing shift controllers.
    """
    event_time = datetime.now()
    cache_key = (device_id, employee_id)
    cached = _recent_attendance_cache.get(cache_key)

    if cached and cached["expires_at"] >= event_time:
        return {
            **cached["payload"],
            "cached": True,
        }

    # Get employee info for the API call
    from database.connection import get_connection
    from psycopg2.extras import RealDictCursor
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT system_user_id, company_id, branch_id FROM employees WHERE id = %s",
                (employee_id,),
            )
            emp = cur.fetchone()
    finally:
        conn.close()

    if not emp:
        return {"status": "error", "reason": "employee_not_found"}

    # Call Laravel API to create the log (triggers Observer + shift recalculation)
    import httpx
    try:
        response = httpx.post(
            f"{LARAVEL_API_URL}/camera/attendance-log",
            json={
                "user_id": emp["system_user_id"],
                "company_id": emp["company_id"],
                "branch_id": emp["branch_id"] or 0,
                "log_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
                "camera_name": device_name,
                "device_serial": device_serial,
            },
            timeout=10.0,
        )
        result = response.json()
    except Exception as e:
        log(f"Laravel attendance API error: {e}")
        return {"status": "error", "reason": str(e)}

    # Also keep a record in attendance_event_logs for camera-service tracking
    attendance_db.insert_attendance_log_if_not_recent(
        employee_id=employee_id,
        log_timestamp=event_time,
        source="camera",
        camera_name=device_name,
        duplicate_window=timedelta(seconds=ATTENDANCE_DUPLICATE_WINDOW_SECONDS),
    )

    payload = {
        "status": result.get("reason", "created") if result.get("inserted") else "duplicate_ignored",
        "reason": result.get("reason", "unknown"),
        "log_id": result.get("log_id"),
        "log_timestamp": event_time.isoformat(),
        "camera_name": device_name,
        "cached": False,
    }

    _recent_attendance_cache[cache_key] = {
        "expires_at": event_time + timedelta(seconds=ATTENDANCE_DUPLICATE_WINDOW_SECONDS),
        "payload": payload,
    }

    return payload


@router.websocket("/detect/{device_id}")
async def detect_stream(websocket: WebSocket, device_id: str):
    """Detection via camera proxy frames — no separate RTSP connection needed.

    Connects to the camera-proxy WebSocket to receive JPEG frames,
    runs ArcFace detection on them, and sends results back to the client.
    This avoids the Hikvision RTSP session limit issue.
    """
    await websocket.accept()
    log(f"=== Detection connected for device {device_id} ===")

    proxy_ws = None
    try:
        await websocket.send_json({"status": "connecting", "message": "Loading face data..."})

        # Get company_id and device metadata, then load embeddings
        device_details = await fetch_device_details(device_id)
        company_id = device_details["company_id"]
        branch_id = device_details.get("branch_id")
        branch_name = device_details.get("branch_name")
        device_name = device_details["device_name"]
        device_type = (device_details.get("device_type") or "all").lower()

        # Only log attendance if device_type allows it
        attendance_enabled = device_type in ("all", "attendance", "auto", "camera")
        if not attendance_enabled:
            log(f"Device {device_id}: device_type='{device_type}' — attendance logging DISABLED")
        stored = get_embeddings(company_id, branch_id)
        scope = f"company {company_id}"
        if branch_id is not None:
            scope += f", branch {branch_name or branch_id}"
        log(f"Loaded embeddings for {scope}: {len(stored)} employees")

        await websocket.send_json({"status": "connecting", "message": "Connecting to camera..."})

        # Connect to camera proxy WebSocket to get frames
        import websockets
        proxy_ws = await websockets.connect(
            f"{PROXY_URL}/stream/{device_id}",
            max_size=10 * 1024 * 1024,  # 10MB max frame size
        )

        await websocket.send_json({"status": "detecting", "message": "Face detection active"})
        log("Detection stream active — reading frames from camera proxy")

        frame_count = 0
        loop = asyncio.get_event_loop()

        async for message in proxy_ws:
            # Proxy sends binary JPEG frames
            if isinstance(message, bytes):
                frame_count += 1

                if frame_count % FACE_DETECT_INTERVAL != 0:
                    continue

                # Decode JPEG to OpenCV frame
                img_array = np.frombuffer(message, dtype=np.uint8)
                frame = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
                if frame is None:
                    continue

                # Run ArcFace detection in background thread
                faces = await loop.run_in_executor(None, arcface.detect_faces, frame)

                detections = []
                for face in faces:
                    embedding = arcface.get_embedding(face)
                    bbox = arcface.face_to_bbox(face)

                    match = arcface.compare_embedding(
                        embedding, stored, FACE_SIMILARITY_THRESHOLD
                    )

                    if match:
                        attendance_log = {
                            "status": "disabled",
                            "reason": "attendance_not_enabled_for_device",
                            "camera_name": device_name,
                        }

                        if AUTO_LOG_RECOGNIZED_FACES and attendance_enabled:
                            try:
                                attendance_log = auto_log_camera_attendance(
                                    device_id=device_id,
                                    device_name=device_name,
                                    employee_id=match["employee_id"],
                                    device_serial=device_details.get("device_serial"),
                                )
                            except Exception as log_error:
                                attendance_log = {
                                    "status": "error",
                                    "reason": str(log_error),
                                    "camera_name": device_name,
                                }

                        detections.append({
                            "name": match["name"],
                            "employee_id": match["employee_id"],
                            "confidence": match["confidence"],
                            "bbox": bbox,
                            "recognized": True,
                            "attendance_log": attendance_log,
                        })
                    else:
                        detections.append({
                            "name": "Unknown",
                            "employee_id": None,
                            "confidence": 0.0,
                            "bbox": bbox,
                            "recognized": False,
                            "attendance_log": {
                                "status": "not_logged",
                                "reason": "face_not_recognized",
                            },
                        })

                await websocket.send_json({
                    "detections": detections,
                    "count": len(detections),
                    "frameWidth": frame.shape[1],
                    "frameHeight": frame.shape[0],
                })

                if frame_count % 30 == 0:
                    recognized = sum(1 for d in detections if d["recognized"])
                    log(f"Frame #{frame_count}: {len(detections)} faces ({recognized} recognized)")

            elif isinstance(message, str):
                # Text message from proxy (error/info)
                log(f"Proxy message: {message}")

    except WebSocketDisconnect:
        log("Detection client disconnected")
    except Exception as e:
        log(f"Detection error: {type(e).__name__}: {e}")
        try:
            await websocket.send_json({"error": str(e)})
        except:
            pass
    finally:
        if proxy_ws is not None:
            await proxy_ws.close()
        log("=== Detection stream closed ===")
