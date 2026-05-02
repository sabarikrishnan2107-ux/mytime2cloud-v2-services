from __future__ import annotations

from datetime import date, datetime, timedelta

from psycopg2.extras import RealDictCursor, execute_values

from database.connection import get_connection


def log(message: str):
    print(message, flush=True)


def insert_attendance_logs(log_rows: list[dict]) -> int:
    if not log_rows:
        return 0

    values = [
        (
            row["employee_id"],
            row["log_timestamp"],
            row["source"],
            row.get("camera_name"),
        )
        for row in log_rows
    ]

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO attendance_event_logs (
                    employee_id,
                    log_timestamp,
                    source,
                    camera_name,
                    created_at,
                    updated_at
                ) VALUES %s
                """,
                values,
                template="(%s, %s, %s, %s, NOW(), NOW())",
            )
        conn.commit()
        return len(values)
    finally:
        conn.close()


def _get_employee_info(cur, employee_id: int) -> dict | None:
    """Get employee system_user_id, company_id, branch_id for attendance_logs."""
    cur.execute(
        "SELECT system_user_id, company_id, branch_id FROM employees WHERE id = %s",
        (employee_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def insert_attendance_log_if_not_recent(
    employee_id: int,
    log_timestamp: datetime,
    source: str,
    camera_name: str | None = None,
    duplicate_window: timedelta = timedelta(minutes=2),
) -> dict:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check duplicate in attendance_event_logs
            cur.execute(
                """
                SELECT
                    id,
                    employee_id,
                    log_timestamp,
                    source,
                    camera_name
                FROM attendance_event_logs
                WHERE employee_id = %s
                  AND log_timestamp >= %s
                  AND log_timestamp <= %s
                ORDER BY log_timestamp ASC, id ASC
                LIMIT 1
                """,
                (
                    employee_id,
                    log_timestamp - duplicate_window,
                    log_timestamp,
                ),
            )
            existing = cur.fetchone()

            if existing:
                return {
                    "inserted": False,
                    "reason": "duplicate_within_window",
                    "log": dict(existing),
                }

            # Insert into attendance_event_logs (camera service table)
            cur.execute(
                """
                INSERT INTO attendance_event_logs (
                    employee_id,
                    log_timestamp,
                    source,
                    camera_name,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                RETURNING id, employee_id, log_timestamp, source, camera_name
                """,
                (employee_id, log_timestamp, source, camera_name),
            )
            inserted = cur.fetchone()

            # NOTE: attendance_logs insert is handled by Laravel API now
            # (via POST /camera/attendance-log which triggers the Observer)

        conn.commit()
        return {
            "inserted": True,
            "reason": "created",
            "log": dict(inserted) if inserted else None,
        }
    finally:
        conn.close()


def fetch_employee_shift_start(employee_id: int, attendance_date: date) -> str | None:
    """Fetch shift on_duty_time for backward compatibility."""
    shift = fetch_employee_shift(employee_id, attendance_date)
    return shift.get("on_duty_time") if shift else None


def fetch_employee_shift(employee_id: int, attendance_date: date) -> dict | None:
    """Fetch full shift details including session 2 for dual/split shifts.

    Returns:
        {"on_duty_time", "off_duty_time", "on_duty_time1", "off_duty_time1", "shift_type_id", "shift_name"}
        or None if no shift found.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT active_shift.*
                FROM employees e
                LEFT JOIN LATERAL (
                    SELECT s.on_duty_time::text AS on_duty_time,
                           s.off_duty_time::text AS off_duty_time,
                           s.on_duty_time1::text AS on_duty_time1,
                           s.off_duty_time1::text AS off_duty_time1,
                           s.shift_type_id,
                           s.name AS shift_name,
                           s.minimum_session_duration,
                           e.branch_id
                    FROM schedule_employees se
                    JOIN shifts s ON s.id = se.shift_id
                    WHERE se.employee_id = e.system_user_id
                      AND (se.from_date IS NULL OR se.from_date <= %s)
                      AND (se.to_date IS NULL OR se.to_date >= %s)
                      AND (s.from_date IS NULL OR s.from_date <= %s)
                      AND (s.to_date IS NULL OR s.to_date >= %s)
                    ORDER BY
                        COALESCE(se.updated_at, se.created_at) DESC NULLS LAST,
                        COALESCE(se.from_date, s.from_date) DESC NULLS LAST,
                        se.id DESC
                    LIMIT 1
                ) active_shift ON TRUE
                WHERE e.id = %s
                """,
                (attendance_date, attendance_date, attendance_date, attendance_date, employee_id),
            )
            row = cur.fetchone()
            return dict(row) if row and row.get("on_duty_time") else None
    finally:
        conn.close()


def fetch_employee_logs(employee_id: int, attendance_date: date, is_night_shift: bool = False) -> list[dict]:
    day_start = datetime.combine(attendance_date, datetime.min.time())
    # Night shift: extend window to next day noon to capture early morning OUT
    day_end = day_start + timedelta(days=1, hours=12) if is_night_shift else day_start + timedelta(days=1)

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    id,
                    employee_id,
                    log_timestamp,
                    source,
                    camera_name
                FROM attendance_event_logs
                WHERE employee_id = %s
                  AND log_timestamp >= %s
                  AND log_timestamp < %s
                ORDER BY log_timestamp ASC, id ASC
                """,
                (employee_id, day_start, day_end),
            )
            return list(cur.fetchall())
    finally:
        conn.close()


def fetch_employees_shift_map(
    attendance_date: date,
    company_id: int | None = None,
    employee_ids: list[int] | None = None,
) -> dict[int, str | None]:
    query = """
        SELECT
            e.id AS employee_id,
            active_shift.shift_start
        FROM employees e
        LEFT JOIN LATERAL (
            SELECT s.on_duty_time::text AS shift_start
            FROM schedule_employees se
            JOIN shifts s ON s.id = se.shift_id
            WHERE se.employee_id = e.system_user_id
              AND (se.from_date IS NULL OR se.from_date <= %s)
              AND (se.to_date IS NULL OR se.to_date >= %s)
              AND (s.from_date IS NULL OR s.from_date <= %s)
              AND (s.to_date IS NULL OR s.to_date >= %s)
            ORDER BY
                COALESCE(se.updated_at, se.created_at) DESC NULLS LAST,
                COALESCE(se.from_date, s.from_date) DESC NULLS LAST,
                se.id DESC
            LIMIT 1
        ) active_shift ON TRUE
        WHERE e.status = 1
    """

    params: list = [attendance_date, attendance_date, attendance_date, attendance_date]

    if company_id is not None:
        query += " AND e.company_id = %s"
        params.append(company_id)

    if employee_ids:
        query += " AND e.id = ANY(%s)"
        params.append(employee_ids)

    query += " ORDER BY e.id ASC"

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            return {row["employee_id"]: row["shift_start"] for row in rows}
    finally:
        conn.close()


def fetch_logs_for_employees(employee_ids: list[int], attendance_date: date) -> list[dict]:
    if not employee_ids:
        return []

    day_start = datetime.combine(attendance_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    id,
                    employee_id,
                    log_timestamp,
                    source,
                    camera_name
                FROM attendance_event_logs
                WHERE employee_id = ANY(%s)
                  AND log_timestamp >= %s
                  AND log_timestamp < %s
                ORDER BY employee_id ASC, log_timestamp ASC, id ASC
                """,
                (employee_ids, day_start, day_end),
            )
            return list(cur.fetchall())
    finally:
        conn.close()


def upsert_daily_attendance_records(records: list[dict]) -> int:
    if not records:
        return 0

    values = [
        (
            record["employee_id"],
            record["attendance_date"],
            record["status"],
            record["in_time"],
            record["out_time"],
            record["in_source"],
            record["out_source"],
            record["in_camera_name"],
            record["out_camera_name"],
            record["sources"],
            record["camera_names"],
            record["shift_start"],
            record["shift_window_start"],
            record["shift_window_end"],
            record["total_logs"],
            record["deduplicated_logs"],
            record["single_log"],
            record["in_window_matched"],
            record["remark"],
        )
        for record in records
    ]

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO attendance_daily_records (
                    employee_id,
                    attendance_date,
                    status,
                    in_time,
                    out_time,
                    in_source,
                    out_source,
                    in_camera_name,
                    out_camera_name,
                    sources,
                    camera_names,
                    shift_start,
                    shift_window_start,
                    shift_window_end,
                    total_logs,
                    deduplicated_logs,
                    single_log,
                    in_window_matched,
                    remark,
                    created_at,
                    updated_at
                ) VALUES %s
                ON CONFLICT (employee_id, attendance_date)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    in_time = EXCLUDED.in_time,
                    out_time = EXCLUDED.out_time,
                    in_source = EXCLUDED.in_source,
                    out_source = EXCLUDED.out_source,
                    in_camera_name = EXCLUDED.in_camera_name,
                    out_camera_name = EXCLUDED.out_camera_name,
                    sources = EXCLUDED.sources,
                    camera_names = EXCLUDED.camera_names,
                    shift_start = EXCLUDED.shift_start,
                    shift_window_start = EXCLUDED.shift_window_start,
                    shift_window_end = EXCLUDED.shift_window_end,
                    total_logs = EXCLUDED.total_logs,
                    deduplicated_logs = EXCLUDED.deduplicated_logs,
                    single_log = EXCLUDED.single_log,
                    in_window_matched = EXCLUDED.in_window_matched,
                    remark = EXCLUDED.remark,
                    updated_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())",
            )
        conn.commit()
        return len(values)
    finally:
        conn.close()


def fetch_daily_attendance_record(employee_id: int, attendance_date: date) -> dict | None:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    employee_id,
                    attendance_date,
                    status,
                    in_time,
                    out_time,
                    in_source,
                    out_source,
                    in_camera_name,
                    out_camera_name,
                    sources,
                    camera_names,
                    shift_start,
                    shift_window_start,
                    shift_window_end,
                    total_logs,
                    deduplicated_logs,
                    single_log,
                    in_window_matched,
                    remark,
                    created_at,
                    updated_at
                FROM attendance_daily_records
                WHERE employee_id = %s
                  AND attendance_date = %s
                """,
                (employee_id, attendance_date),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()
