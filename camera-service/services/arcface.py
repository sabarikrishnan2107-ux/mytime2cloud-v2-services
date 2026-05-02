import os
import numpy as np

_app = None


def log(msg):
    print(msg, flush=True)


def get_app():
    """Lazy-load the InsightFace app (downloads model on first run ~300MB)."""
    global _app
    if _app is None:
        from insightface.app import FaceAnalysis

        model_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models")
        os.makedirs(model_dir, exist_ok=True)

        log("Loading ArcFace model (buffalo_l)... first run downloads ~300MB")
        _app = FaceAnalysis(
            name="buffalo_l",
            root=model_dir,
            providers=["CPUExecutionProvider"],
        )
        _app.prepare(ctx_id=-1, det_size=(640, 640))
        log("ArcFace model loaded successfully")

    return _app


def detect_faces(frame: np.ndarray) -> list:
    """Detect all faces in a frame.

    Args:
        frame: BGR image as numpy array from OpenCV

    Returns:
        List of insightface Face objects with bbox, embedding, etc.
    """
    app = get_app()
    faces = app.get(frame)
    return faces


def get_embedding(face) -> np.ndarray:
    """Extract the 512-dim embedding from a detected face.

    Args:
        face: InsightFace Face object from detect_faces()

    Returns:
        512-dim float32 numpy array (normalized)
    """
    return face.normed_embedding


def face_to_bbox(face) -> list:
    """Convert InsightFace face bbox to [x, y, w, h] format.

    Args:
        face: InsightFace Face object

    Returns:
        [x, y, width, height] as integers
    """
    box = face.bbox.astype(int)
    x1, y1, x2, y2 = box[0], box[1], box[2], box[3]
    return [int(x1), int(y1), int(x2 - x1), int(y2 - y1)]


def compare_embedding(
    embedding: np.ndarray,
    stored_embeddings: dict,
    threshold: float = 0.30,
) -> dict | None:
    """Compare a face embedding against all stored employee embeddings.

    Args:
        embedding: 512-dim float32 array from the detected face
        stored_embeddings: {employee_id: {"name": str, "embeddings": [np.array, ...]}}
        threshold: Minimum cosine similarity to consider a match

    Returns:
        {"employee_id": int, "name": str, "confidence": float} or None if no match
    """
    best_match = None
    best_score = -1.0
    second_best_score = -1.0

    for emp_id, emp_data in stored_embeddings.items():
        for stored_emb in emp_data["embeddings"]:
            # ArcFace normed_embedding is already L2-normalized, so dot product = cosine similarity
            score = float(np.dot(embedding, stored_emb))

            if score > best_score:
                second_best_score = best_score
                best_score = score
                best_match = {
                    "employee_id": emp_id,
                    "name": emp_data["name"],
                    "confidence": round(score, 3),
                }
            elif score > second_best_score:
                second_best_score = score

    # Log top scores for debugging
    if best_match:
        gap = best_score - second_best_score
        log(f"  Best: {best_match['name']} ({best_score:.3f}), 2nd: {second_best_score:.3f}, gap: {gap:.3f}")

    if best_match and best_score >= threshold:
        return best_match

    return None
