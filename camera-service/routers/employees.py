import cv2
import httpx
import numpy as np
from fastapi import APIRouter, Query

from routers.detect import refresh_embeddings, get_embeddings
from database.connection import (
    get_employees_with_photos,
    get_employees_without_embeddings,
    save_embedding,
    get_embedding_count,
)
from services import arcface

router = APIRouter()

PHOTO_BASE_URL = "https://backend.mytime2cloud.com/media/employee/profile_picture"


def log(msg):
    print(msg, flush=True)


async def download_image(url: str) -> np.ndarray | None:
    """Download an image from URL and return as OpenCV BGR array."""
    try:
        async with httpx.AsyncClient(timeout=10.0, verify=False) as client:
            response = await client.get(url)
            if response.status_code != 200:
                return None
            img_array = np.frombuffer(response.content, dtype=np.uint8)
            frame = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
            return frame
    except Exception as e:
        log(f"Failed to download {url}: {e}")
        return None


@router.get("/employees/sync")
async def sync_embeddings(
    company_id: int = Query(default=1),
    branch_id: int | None = Query(default=None),
):
    """Force reload employee embeddings from the database into memory."""
    refresh_embeddings(company_id, branch_id)
    stored = get_embeddings(company_id, branch_id)

    total_embeddings = sum(len(v["embeddings"]) for v in stored.values())

    return {
        "status": True,
        "message": f"Synced {total_embeddings} embeddings for {len(stored)} employees",
        "data": {
            "company_id": company_id,
            "branch_id": branch_id,
            "employees": len(stored),
            "total_embeddings": total_embeddings,
        }
    }


@router.post("/employees/generate-embeddings")
async def generate_embeddings_from_photos(
    company_id: int = Query(default=1),
    branch_id: int | None = Query(default=None),
    force: bool = Query(default=False),
):
    """Auto-generate face embeddings from existing employee profile photos.

    Downloads each employee's profile photo, runs ArcFace to detect face
    and extract embedding, then stores it in the database.

    Args:
        company_id: Company ID to process
        branch_id: Optional branch ID to scope processing
        force: If True, regenerate for ALL employees (even those with embeddings).
               If False, only process employees missing embeddings.
    """
    if force:
        employees = get_employees_with_photos(company_id, branch_id)
    else:
        employees = get_employees_without_embeddings(company_id, branch_id)

    if not employees:
        return {
            "status": True,
            "message": "All employees already have embeddings",
            "data": {"processed": 0, "success": 0, "failed": 0, "no_face": 0},
        }

    log(f"Generating embeddings for {len(employees)} employees...")

    success = 0
    failed = 0
    no_face = 0
    results = []

    for emp in employees:
        photo_filename = emp["profile_picture"]
        # Handle if profile_picture already contains full URL or just filename
        if photo_filename.startswith("http"):
            photo_url = photo_filename
        else:
            photo_url = f"{PHOTO_BASE_URL}/{photo_filename}"

        log(f"Processing {emp['name']} (ID: {emp['id']})...")

        # Download photo
        frame = await download_image(photo_url)
        if frame is None:
            log(f"  SKIP — could not download photo: {photo_url}")
            failed += 1
            results.append({"id": emp["id"], "name": emp["name"], "status": "download_failed"})
            continue

        # Detect face
        faces = arcface.detect_faces(frame)
        if len(faces) == 0:
            log(f"  SKIP — no face detected in photo")
            no_face += 1
            results.append({"id": emp["id"], "name": emp["name"], "status": "no_face"})
            continue

        # Use the largest/most prominent face
        face = max(faces, key=lambda f: (f.bbox[2] - f.bbox[0]) * (f.bbox[3] - f.bbox[1]))
        embedding = arcface.get_embedding(face)

        # Save to database
        save_embedding(emp["id"], company_id, embedding)
        success += 1
        log(f"  OK — embedding saved")
        results.append({"id": emp["id"], "name": emp["name"], "status": "success"})

    # Refresh in-memory cache
    refresh_embeddings(company_id, branch_id)

    log(f"Done: {success} success, {no_face} no face, {failed} download failed")

    return {
        "status": True,
        "message": f"Generated {success} embeddings from profile photos",
        "data": {
            "company_id": company_id,
            "branch_id": branch_id,
            "processed": len(employees),
            "success": success,
            "failed": failed,
            "no_face": no_face,
            "results": results,
        }
    }
