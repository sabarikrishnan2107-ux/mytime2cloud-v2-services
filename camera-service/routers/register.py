import base64
import cv2
import numpy as np
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services import arcface
from database.connection import save_embedding, get_embedding_count, get_employee_name

router = APIRouter()


def log(msg):
    print(msg, flush=True)


class RegisterRequest(BaseModel):
    frame: str  # base64 encoded JPEG image
    company_id: int


@router.post("/register/{employee_id}")
async def register_face(employee_id: int, body: RegisterRequest):
    """Register an employee's face from a camera capture."""
    name = get_employee_name(employee_id)
    if not name:
        raise HTTPException(status_code=404, detail="Employee not found")

    try:
        img_bytes = base64.b64decode(body.frame)
        img_array = np.frombuffer(img_bytes, dtype=np.uint8)
        frame = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid image data")

    if frame is None:
        raise HTTPException(status_code=400, detail="Could not decode image")

    faces = arcface.detect_faces(frame)

    if len(faces) == 0:
        raise HTTPException(status_code=400, detail="No face detected in the image. Ensure face is clearly visible.")

    if len(faces) > 1:
        raise HTTPException(status_code=400, detail=f"Multiple faces detected ({len(faces)}). Only one person should be in frame during registration.")

    face = faces[0]
    embedding = arcface.get_embedding(face)

    row_id = save_embedding(employee_id, body.company_id, embedding)
    total = get_embedding_count(employee_id)

    log(f"Registered face for {name} (employee {employee_id}), total embeddings: {total}")

    return {
        "status": True,
        "message": f"Face registered for {name}",
        "data": {
            "employee_id": employee_id,
            "name": name,
            "embedding_id": row_id,
            "total_embeddings": total,
        }
    }


@router.delete("/register/{employee_id}")
async def delete_faces(employee_id: int):
    """Delete all face embeddings for an employee."""
    from database.connection import delete_embeddings

    name = get_employee_name(employee_id)
    if not name:
        raise HTTPException(status_code=404, detail="Employee not found")

    count = delete_embeddings(employee_id)

    return {
        "status": True,
        "message": f"Deleted {count} embeddings for {name}",
        "data": {"employee_id": employee_id, "deleted": count},
    }
