from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from database import attendance as attendance_db
from services.attendance import (
    AttendanceCalculationOptions,
    AttendanceLogEvent,
    calculate_daily_attendance,
    deduplicate_logs,
    normalize_source,
)

router = APIRouter(prefix="/attendance", tags=["attendance"])


def log(message: str):
    print(message, flush=True)


class AttendanceLogPayload(BaseModel):
    employee_id: int
    timestamp: datetime
    source: Literal["camera", "mobile", "device"]
    camera_name: str | None = None


class AttendanceLogBatchRequest(BaseModel):
    logs: list[AttendanceLogPayload] = Field(default_factory=list)
    calculate_attendance: bool = True
    single_log_out_equals_in: bool = False
    fallback_to_first_log_when_no_in_window_match: bool = False


class DailyCalculationRequest(BaseModel):
    employee_id: int
    date: date
    persist: bool = True
    single_log_out_equals_in: bool = False
    fallback_to_first_log_when_no_in_window_match: bool = False


class BulkCalculationRequest(BaseModel):
    date: date
    company_id: int | None = None
    employee_ids: list[int] | None = None
    persist: bool = True
    single_log_out_equals_in: bool = False
    fallback_to_first_log_when_no_in_window_match: bool = False


def build_options(single_log_out_equals_in: bool, fallback_to_first_log_when_no_in_window_match: bool) -> AttendanceCalculationOptions:
    return AttendanceCalculationOptions(
        single_log_out_equals_in=single_log_out_equals_in,
        fallback_to_first_log_when_no_in_window_match=fallback_to_first_log_when_no_in_window_match,
    )


def serialize_record_payload(record: dict) -> dict:
    payload = {
        key: value
        for key, value in record.items()
        if key != "attendance_date"
    }

    payload["date"] = record["attendance_date"].isoformat()
    payload["in_time"] = record["in_time"].isoformat() if record["in_time"] else None
    payload["out_time"] = record["out_time"].isoformat() if record["out_time"] else None
    payload["shift_start"] = record["shift_start"].isoformat() if record["shift_start"] else None
    payload["shift_window_start"] = record["shift_window_start"].isoformat() if record["shift_window_start"] else None
    payload["shift_window_end"] = record["shift_window_end"].isoformat() if record["shift_window_end"] else None
    return payload


def serialize_db_record(record: dict) -> dict:
    return {
        "employee_id": record["employee_id"],
        "date": record["attendance_date"].isoformat() if record.get("attendance_date") else None,
        "status": record.get("status"),
        "in_time": record["in_time"].isoformat() if record.get("in_time") else None,
        "out_time": record["out_time"].isoformat() if record.get("out_time") else None,
        "in_source": record.get("in_source"),
        "out_source": record.get("out_source"),
        "in_camera_name": record.get("in_camera_name"),
        "out_camera_name": record.get("out_camera_name"),
        "sources": record.get("sources") or [],
        "camera_names": record.get("camera_names") or [],
        "shift_start": record["shift_start"].isoformat() if record.get("shift_start") else None,
        "shift_window_start": record["shift_window_start"].isoformat() if record.get("shift_window_start") else None,
        "shift_window_end": record["shift_window_end"].isoformat() if record.get("shift_window_end") else None,
        "total_logs": record.get("total_logs", 0),
        "deduplicated_logs": record.get("deduplicated_logs", 0),
        "single_log": record.get("single_log", False),
        "in_window_matched": record.get("in_window_matched", False),
        "remark": record.get("remark"),
        "created_at": record["created_at"].isoformat() if record.get("created_at") else None,
        "updated_at": record["updated_at"].isoformat() if record.get("updated_at") else None,
    }


def calculate_one_employee(
    employee_id: int,
    attendance_date: date,
    options: AttendanceCalculationOptions,
) -> tuple[dict, list[dict], list[dict]]:
    shift_start = attendance_db.fetch_employee_shift_start(employee_id, attendance_date)
    raw_logs = attendance_db.fetch_employee_logs(employee_id, attendance_date)

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
        shift_start=shift_start,
        options=options,
    )

    record_payload = {
        "employee_id": record.employee_id,
        "attendance_date": record.attendance_date,
        "status": record.status,
        "in_time": record.in_time,
        "out_time": record.out_time,
        "in_source": record.in_source,
        "out_source": record.out_source,
        "in_camera_name": record.in_camera_name,
        "out_camera_name": record.out_camera_name,
        "sources": record.sources,
        "camera_names": record.camera_names,
        "shift_start": record.shift_start,
        "shift_window_start": record.shift_window_start,
        "shift_window_end": record.shift_window_end,
        "total_logs": record.total_logs,
        "deduplicated_logs": record.deduplicated_logs,
        "single_log": record.single_log,
        "in_window_matched": record.in_window_matched,
        "remark": record.remark,
    }

    raw_logs_response = [
        {
            "id": row["id"],
            "employee_id": row["employee_id"],
            "timestamp": row["log_timestamp"].isoformat(),
            "source": row["source"],
            "camera_name": row["camera_name"],
        }
        for row in raw_logs
    ]

    deduplicated_events = deduplicate_logs(events, options)
    deduped_logs_response = [
        {
            "id": event.id,
            "employee_id": event.employee_id,
            "timestamp": event.log_timestamp.isoformat(),
            "source": event.source,
            "camera_name": event.camera_name,
        }
        for event in deduplicated_events
    ]

    return record_payload, raw_logs_response, deduped_logs_response


@router.post("/logs")
async def ingest_logs(body: AttendanceLogBatchRequest):
    if not body.logs:
        raise HTTPException(status_code=422, detail="At least one log payload is required.")

    rows = []
    for item in body.logs:
        normalized_source = normalize_source(item.source)
        if normalized_source != "camera" and item.camera_name:
            raise HTTPException(status_code=422, detail="camera_name is only valid for camera logs.")

        rows.append(
            {
                "employee_id": item.employee_id,
                "log_timestamp": item.timestamp.replace(tzinfo=None),
                "source": normalized_source,
                "camera_name": item.camera_name,
            }
        )

    inserted = attendance_db.insert_attendance_logs(rows)
    log(f"Inserted {inserted} attendance event log(s)")

    attendance_records = []
    if body.calculate_attendance:
        options = build_options(
            single_log_out_equals_in=body.single_log_out_equals_in,
            fallback_to_first_log_when_no_in_window_match=body.fallback_to_first_log_when_no_in_window_match,
        )

        affected_employee_dates = sorted(
            {
                (item.employee_id, item.timestamp.date())
                for item in body.logs
            }
        )

        for employee_id, attendance_date in affected_employee_dates:
            record_payload, _, _ = calculate_one_employee(employee_id, attendance_date, options)
            attendance_db.upsert_daily_attendance_records([record_payload])
            attendance_records.append(serialize_record_payload(record_payload))

    return {
        "status": True,
        "message": f"Inserted {inserted} attendance event log(s).",
        "data": {
            "inserted": inserted,
            "attendance_records": attendance_records,
        },
    }


@router.get("/logs/{employee_id}/{attendance_date}")
async def get_logs(employee_id: int, attendance_date: date):
    options = AttendanceCalculationOptions()
    record_payload, raw_logs, deduped_logs = calculate_one_employee(employee_id, attendance_date, options)

    return {
        "status": True,
        "message": "Fetched attendance logs and calculation preview.",
        "data": {
            "record_preview": serialize_record_payload(record_payload),
            "raw_logs": raw_logs,
            "deduplicated_logs": deduped_logs,
        },
    }


@router.post("/calculate/daily")
async def calculate_daily(body: DailyCalculationRequest):
    options = build_options(
        single_log_out_equals_in=body.single_log_out_equals_in,
        fallback_to_first_log_when_no_in_window_match=body.fallback_to_first_log_when_no_in_window_match,
    )

    record_payload, raw_logs, deduped_logs = calculate_one_employee(body.employee_id, body.date, options)

    if body.persist:
        attendance_db.upsert_daily_attendance_records([record_payload])

    return {
        "status": True,
        "message": "Attendance calculated successfully.",
        "data": {
            "record": serialize_record_payload(record_payload),
            "raw_logs_count": len(raw_logs),
            "deduplicated_logs_count": len(deduped_logs),
            "persisted": body.persist,
        },
    }


@router.post("/calculate/bulk")
async def calculate_bulk(body: BulkCalculationRequest):
    options = build_options(
        single_log_out_equals_in=body.single_log_out_equals_in,
        fallback_to_first_log_when_no_in_window_match=body.fallback_to_first_log_when_no_in_window_match,
    )

    shift_map = attendance_db.fetch_employees_shift_map(
        attendance_date=body.date,
        company_id=body.company_id,
        employee_ids=body.employee_ids,
    )

    if not shift_map:
        raise HTTPException(status_code=404, detail="No employees matched the bulk calculation request.")

    raw_logs = attendance_db.fetch_logs_for_employees(list(shift_map.keys()), body.date)
    logs_by_employee: dict[int, list[AttendanceLogEvent]] = defaultdict(list)

    for row in raw_logs:
        logs_by_employee[row["employee_id"]].append(
            AttendanceLogEvent(
                id=row["id"],
                employee_id=row["employee_id"],
                log_timestamp=row["log_timestamp"],
                source=row["source"],
                camera_name=row["camera_name"],
            )
        )

    records = []
    summary = {
        "present": 0,
        "absent": 0,
        "single_log": 0,
    }

    for employee_id, shift_start in shift_map.items():
        record = calculate_daily_attendance(
            employee_id=employee_id,
            attendance_date=body.date,
            logs=logs_by_employee.get(employee_id, []),
            shift_start=shift_start,
            options=options,
        )

        records.append(
            {
                "employee_id": record.employee_id,
                "attendance_date": record.attendance_date,
                "status": record.status,
                "in_time": record.in_time,
                "out_time": record.out_time,
                "in_source": record.in_source,
                "out_source": record.out_source,
                "in_camera_name": record.in_camera_name,
                "out_camera_name": record.out_camera_name,
                "sources": record.sources,
                "camera_names": record.camera_names,
                "shift_start": record.shift_start,
                "shift_window_start": record.shift_window_start,
                "shift_window_end": record.shift_window_end,
                "total_logs": record.total_logs,
                "deduplicated_logs": record.deduplicated_logs,
                "single_log": record.single_log,
                "in_window_matched": record.in_window_matched,
                "remark": record.remark,
            }
        )

        summary[record.status] += 1
        if record.single_log:
            summary["single_log"] += 1

    persisted = 0
    if body.persist:
        persisted = attendance_db.upsert_daily_attendance_records(records)

    return {
        "status": True,
        "message": "Bulk attendance calculation completed.",
        "data": {
            "date": body.date.isoformat(),
            "company_id": body.company_id,
            "employees_processed": len(records),
            "persisted_records": persisted,
            "summary": summary,
            "records": [serialize_record_payload(record) for record in records],
        },
    }


@router.get("/records/{employee_id}/{attendance_date}")
async def get_record(employee_id: int, attendance_date: date):
    record = attendance_db.fetch_daily_attendance_record(employee_id, attendance_date)
    if not record:
        raise HTTPException(status_code=404, detail="Attendance record not found.")

    return {
        "status": True,
        "message": "Attendance record fetched successfully.",
        "data": serialize_db_record(record),
    }
