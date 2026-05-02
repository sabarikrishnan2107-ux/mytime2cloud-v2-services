import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import HOST, PORT
from routers import attendance, detect, register, employees
from services.background_worker import start_background_detection, stop_background_detection


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: launch background detection workers
    await start_background_detection()
    yield
    # Shutdown: stop all workers
    await stop_background_detection()


app = FastAPI(title="MyTime2Cloud Camera Service", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(detect.router)
app.include_router(register.router)
app.include_router(employees.router)
app.include_router(attendance.router)


@app.get("/health")
async def health():
    from services.background_worker import _active_workers
    return {
        "status": "ok",
        "service": "camera-service",
        "model": "arcface-buffalo_l",
        "active_workers": len(_active_workers),
    }


def log(msg):
    print(msg, flush=True)


if __name__ == "__main__":
    log(f"Starting camera service on {HOST}:{PORT}")
    uvicorn.run("main:app", host=HOST, port=PORT)
