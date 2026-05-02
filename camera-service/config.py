import os
from dotenv import load_dotenv

load_dotenv()

# Database
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5433"))
DB_NAME = os.getenv("DB_NAME", "mytime2cloud_dev")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")

# Laravel API
LARAVEL_API_URL = os.getenv("LARAVEL_API_URL", "http://localhost:8000/api")
LARAVEL_API_TOKEN = os.getenv("LARAVEL_API_TOKEN", "")

# ArcFace
FACE_SIMILARITY_THRESHOLD = float(os.getenv("FACE_SIMILARITY_THRESHOLD", "0.30"))
FACE_DETECT_INTERVAL = int(os.getenv("FACE_DETECT_INTERVAL", "3"))
AUTO_LOG_RECOGNIZED_FACES = os.getenv("AUTO_LOG_RECOGNIZED_FACES", "true").strip().lower() in {"1", "true", "yes", "on"}
ATTENDANCE_DUPLICATE_WINDOW_SECONDS = int(os.getenv("ATTENDANCE_DUPLICATE_WINDOW_SECONDS", "120"))

# Service
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8500"))
