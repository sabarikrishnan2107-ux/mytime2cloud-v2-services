from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Iterable


ALLOWED_SOURCES = {"camera", "mobile", "device"}


@dataclass(frozen=True)
class AttendanceLogEvent:
    employee_id: int
    log_timestamp: datetime
    source: str
    camera_name: str | None = None
    id: int | None = None


@dataclass(frozen=True)
class AttendanceCalculationOptions:
    duplicate_window: timedelta = timedelta(minutes=2)
    in_window_before: timedelta = timedelta(hours=1)
    in_window_after: timedelta = timedelta(hours=1)
    single_log_out_equals_in: bool = False
    fallback_to_first_log_when_no_in_window_match: bool = False


@dataclass
class SessionRecord:
    """IN/OUT for a single shift session."""
    session_name: str
    session_start: time | None
    session_end: time | None
    in_time: datetime | None
    out_time: datetime | None
    in_source: str | None
    out_source: str | None
    in_camera_name: str | None
    out_camera_name: str | None
    log_count: int

    def to_dict(self) -> dict:
        return {
            "session_name": self.session_name,
            "session_start": self.session_start.isoformat() if self.session_start else None,
            "session_end": self.session_end.isoformat() if self.session_end else None,
            "in_time": self.in_time.isoformat() if self.in_time else None,
            "out_time": self.out_time.isoformat() if self.out_time else None,
            "in_source": self.in_source,
            "out_source": self.out_source,
            "in_camera_name": self.in_camera_name,
            "out_camera_name": self.out_camera_name,
            "log_count": self.log_count,
        }


@dataclass
class AttendanceRecord:
    employee_id: int
    attendance_date: date
    status: str
    in_time: datetime | None
    out_time: datetime | None
    in_source: str | None
    out_source: str | None
    in_camera_name: str | None
    out_camera_name: str | None
    sources: list[str]
    camera_names: list[str]
    shift_start: time | None
    shift_window_start: datetime | None
    shift_window_end: datetime | None
    total_logs: int
    deduplicated_logs: int
    single_log: bool
    in_window_matched: bool
    remark: str | None
    sessions: list[SessionRecord] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "employee_id": self.employee_id,
            "date": self.attendance_date.isoformat(),
            "status": self.status,
            "in_time": self.in_time.isoformat() if self.in_time else None,
            "out_time": self.out_time.isoformat() if self.out_time else None,
            "in_source": self.in_source,
            "out_source": self.out_source,
            "in_camera_name": self.in_camera_name,
            "out_camera_name": self.out_camera_name,
            "sources": self.sources,
            "camera_names": self.camera_names,
            "shift_start": self.shift_start.isoformat() if self.shift_start else None,
            "shift_window_start": self.shift_window_start.isoformat() if self.shift_window_start else None,
            "shift_window_end": self.shift_window_end.isoformat() if self.shift_window_end else None,
            "total_logs": self.total_logs,
            "deduplicated_logs": self.deduplicated_logs,
            "single_log": self.single_log,
            "in_window_matched": self.in_window_matched,
            "remark": self.remark,
            "sessions": [s.to_dict() for s in self.sessions],
        }


def normalize_source(source: str) -> str:
    normalized = (source or "").strip().lower()
    if normalized not in ALLOWED_SOURCES:
        raise ValueError(f"Unsupported source '{source}'. Allowed: {sorted(ALLOWED_SOURCES)}")
    return normalized


def parse_shift_start(value: str | time | datetime | None) -> time | None:
    if value is None:
        return None

    if isinstance(value, time):
        return value.replace(tzinfo=None)

    if isinstance(value, datetime):
        return value.time().replace(tzinfo=None)

    text = str(value).strip()
    if not text or text == "---":
        return None

    for fmt in ("%H:%M:%S", "%H:%M", "%I:%M %p", "%I:%M:%S %p"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue

    raise ValueError(f"Could not parse shift start time '{value}'")


def build_shift_window(attendance_date: date, shift_start: time | None, options: AttendanceCalculationOptions) -> tuple[datetime | None, datetime | None]:
    if shift_start is None:
        return None, None

    shift_anchor = datetime.combine(attendance_date, shift_start)
    return shift_anchor - options.in_window_before, shift_anchor + options.in_window_after


def deduplicate_logs(logs: Iterable[AttendanceLogEvent], options: AttendanceCalculationOptions) -> list[AttendanceLogEvent]:
    deduplicated: list[AttendanceLogEvent] = []

    for log in sorted(logs, key=lambda item: (item.log_timestamp, item.id or 0, item.source)):
        if not deduplicated:
            deduplicated.append(log)
            continue

        previous = deduplicated[-1]
        if (log.log_timestamp - previous.log_timestamp) <= options.duplicate_window:
            continue

        deduplicated.append(log)

    return deduplicated


def _build_session_window(
    attendance_date: date,
    session_start: time,
    session_end: time,
    buffer_before: timedelta,
    buffer_after: timedelta,
) -> tuple[datetime, datetime]:
    """Build the time window for a shift session with buffer."""
    start_dt = datetime.combine(attendance_date, session_start) - buffer_before
    end_dt = datetime.combine(attendance_date, session_end) + buffer_after
    # Handle overnight sessions
    if session_end <= session_start:
        end_dt += timedelta(days=1)
    return start_dt, end_dt


def _calculate_session(
    session_name: str,
    session_start: time | None,
    session_end: time | None,
    logs: list[AttendanceLogEvent],
    attendance_date: date,
    options: AttendanceCalculationOptions,
) -> tuple[SessionRecord, list[AttendanceLogEvent]]:
    """Calculate IN/OUT for a single session.

    Returns the session record and the logs that were consumed by this session.
    """
    if session_start is None or session_end is None:
        return SessionRecord(
            session_name=session_name,
            session_start=session_start,
            session_end=session_end,
            in_time=None, out_time=None,
            in_source=None, out_source=None,
            in_camera_name=None, out_camera_name=None,
            log_count=0,
        ), []

    # Build window: session_start - 1hr to session_end + 1hr
    window_start, window_end = _build_session_window(
        attendance_date, session_start, session_end,
        options.in_window_before, options.in_window_after,
    )

    # Find logs within this session window
    session_logs = [
        log for log in logs
        if window_start <= log.log_timestamp <= window_end
    ]

    if not session_logs:
        return SessionRecord(
            session_name=session_name,
            session_start=session_start,
            session_end=session_end,
            in_time=None, out_time=None,
            in_source=None, out_source=None,
            in_camera_name=None, out_camera_name=None,
            log_count=0,
        ), []

    # IN = first log in session window
    in_log = session_logs[0]

    # OUT = last log AFTER session end time
    session_end_dt = datetime.combine(attendance_date, session_end)
    if session_end <= session_start:
        session_end_dt += timedelta(days=1)

    after_end_logs = [
        log for log in session_logs
        if log.log_timestamp >= session_end_dt
    ]

    out_log = after_end_logs[-1] if after_end_logs else None

    return SessionRecord(
        session_name=session_name,
        session_start=session_start,
        session_end=session_end,
        in_time=in_log.log_timestamp,
        out_time=out_log.log_timestamp if out_log else None,
        in_source=in_log.source,
        out_source=out_log.source if out_log else None,
        in_camera_name=in_log.camera_name,
        out_camera_name=out_log.camera_name if out_log else None,
        log_count=len(session_logs),
    ), session_logs


def _calculate_multi_shift_sessions(
    deduplicated_logs: list[AttendanceLogEvent],
    min_session_minutes: int = 30,
) -> list[SessionRecord]:
    """Build paired IN/OUT sessions from logs for multi-shift.

    Odd logs = IN, Even logs = OUT.
    E.g.: log1=IN, log2=OUT, log3=IN, log4=OUT...
    Sessions shorter than min_session_minutes are filtered out.
    """
    sessions = []
    i = 0
    session_num = 1

    while i < len(deduplicated_logs):
        in_log = deduplicated_logs[i]
        out_log = deduplicated_logs[i + 1] if i + 1 < len(deduplicated_logs) else None

        # Check minimum session duration
        if out_log:
            duration = (out_log.log_timestamp - in_log.log_timestamp).total_seconds() / 60
            if duration < min_session_minutes:
                # Skip this pair (accidental short punch)
                i += 2
                continue

        sessions.append(SessionRecord(
            session_name=f"Session {session_num}",
            session_start=in_log.log_timestamp.time(),
            session_end=out_log.log_timestamp.time() if out_log else None,
            in_time=in_log.log_timestamp,
            out_time=out_log.log_timestamp if out_log else None,
            in_source=in_log.source,
            out_source=out_log.source if out_log else None,
            in_camera_name=in_log.camera_name,
            out_camera_name=out_log.camera_name if out_log else None,
            log_count=2 if out_log else 1,
        ))

        session_num += 1
        i += 2

    return sessions


def calculate_daily_attendance(
    employee_id: int,
    attendance_date: date,
    logs: Iterable[AttendanceLogEvent],
    shift_start: str | time | datetime | None = None,
    shift_end: str | time | datetime | None = None,
    shift_start2: str | time | datetime | None = None,
    shift_end2: str | time | datetime | None = None,
    shift_type_id: int | None = None,
    min_session_minutes: int = 30,
    options: AttendanceCalculationOptions | None = None,
) -> AttendanceRecord:
    options = options or AttendanceCalculationOptions()
    normalized_shift_start = parse_shift_start(shift_start)
    normalized_shift_end = parse_shift_start(shift_end)
    normalized_shift_start2 = parse_shift_start(shift_start2)
    normalized_shift_end2 = parse_shift_start(shift_end2)

    is_dual_shift = normalized_shift_start2 is not None and normalized_shift_end2 is not None
    is_multi_shift = shift_type_id == 2

    normalized_logs = [
        AttendanceLogEvent(
            employee_id=log.employee_id,
            log_timestamp=log.log_timestamp,
            source=normalize_source(log.source),
            camera_name=log.camera_name,
            id=log.id,
        )
        for log in logs
    ]

    deduplicated_logs = deduplicate_logs(normalized_logs, options)
    window_start, window_end = build_shift_window(attendance_date, normalized_shift_start, options)

    if not deduplicated_logs:
        return AttendanceRecord(
            employee_id=employee_id,
            attendance_date=attendance_date,
            status="absent",
            in_time=None, out_time=None,
            in_source=None, out_source=None,
            in_camera_name=None, out_camera_name=None,
            sources=[], camera_names=[],
            shift_start=normalized_shift_start,
            shift_window_start=window_start,
            shift_window_end=window_end,
            total_logs=0, deduplicated_logs=0,
            single_log=False, in_window_matched=False,
            remark="No logs found for the employee on the requested date.",
        )

    sources = list(dict.fromkeys(log.source for log in deduplicated_logs))
    camera_names = list(dict.fromkeys(log.camera_name for log in deduplicated_logs if log.camera_name))

    sessions = []

    if is_multi_shift:
        # Multi shift: pair logs as IN/OUT, IN/OUT, IN/OUT...
        sessions = _calculate_multi_shift_sessions(deduplicated_logs, min_session_minutes)

        if sessions:
            in_log_time = sessions[0].in_time
            in_log_source = sessions[0].in_source
            in_log_camera = sessions[0].in_camera_name
            last_session = sessions[-1]
            out_log_time = last_session.out_time
            out_log_source = last_session.out_source
            out_log_camera = last_session.out_camera_name

            total_mins = sum(
                (s.out_time - s.in_time).total_seconds() / 60
                for s in sessions if s.out_time
            )
            remark = f"Multi shift. {len(sessions)} sessions, total: {int(total_mins // 60)}h {int(total_mins % 60)}m."
        else:
            in_log_time = deduplicated_logs[0].log_timestamp
            in_log_source = deduplicated_logs[0].source
            in_log_camera = deduplicated_logs[0].camera_name
            out_log_time = None
            out_log_source = None
            out_log_camera = None
            remark = "Multi shift. All sessions filtered (below minimum duration)."

    elif is_dual_shift:
        # Dual/Split shift: calculate each session separately
        session1, s1_logs = _calculate_session(
            "Session 1",
            normalized_shift_start, normalized_shift_end,
            deduplicated_logs, attendance_date, options,
        )
        sessions.append(session1)

        session2, s2_logs = _calculate_session(
            "Session 2",
            normalized_shift_start2, normalized_shift_end2,
            deduplicated_logs, attendance_date, options,
        )
        sessions.append(session2)

        # Overall IN = Session 1 IN, Overall OUT = Session 2 OUT (or Session 1 OUT if no S2)
        in_log_time = session1.in_time
        in_log_source = session1.in_source
        in_log_camera = session1.in_camera_name
        out_log_time = session2.out_time or session2.in_time or session1.out_time
        out_log_source = session2.out_source or session2.in_source or session1.out_source
        out_log_camera = session2.out_camera_name or session2.in_camera_name or session1.out_camera_name

        remark = f"Dual shift. S1: {session1.log_count} logs, S2: {session2.log_count} logs."

    else:
        # Single shift: IN = first log, OUT = last log AFTER shift end time
        first_log = deduplicated_logs[0]

        in_log_time = first_log.log_timestamp
        in_log_source = first_log.source
        in_log_camera = first_log.camera_name

        # Find OUT: last log after shift end time
        out_log_time = None
        out_log_source = None
        out_log_camera = None

        if normalized_shift_end is not None:
            shift_end_dt = datetime.combine(attendance_date, normalized_shift_end)
            # Handle night shift (shift end is next day)
            if normalized_shift_end <= normalized_shift_start if normalized_shift_start else False:
                shift_end_dt += timedelta(days=1)

            # Find logs after shift end time
            after_shift_logs = [
                log for log in deduplicated_logs
                if log.log_timestamp >= shift_end_dt
            ]
            if after_shift_logs:
                out_log = after_shift_logs[-1]  # Last log after shift end
                out_log_time = out_log.log_timestamp
                out_log_source = out_log.source
                out_log_camera = out_log.camera_name
                remark = "IN = first log, OUT = last log after shift end."
            else:
                remark = "IN recorded. Waiting for OUT after shift end."
        elif len(deduplicated_logs) > 1:
            # No shift end defined — fallback to last log
            last_log = deduplicated_logs[-1]
            out_log_time = last_log.log_timestamp
            out_log_source = last_log.source
            out_log_camera = last_log.camera_name
            remark = "IN = first log, OUT = last log (no shift end defined)."
        else:
            remark = "Single log. IN recorded, waiting for OUT."

        # Add single session for consistency
        sessions.append(SessionRecord(
            session_name="Session 1",
            session_start=normalized_shift_start,
            session_end=normalized_shift_end,
            in_time=in_log_time,
            out_time=out_log_time,
            in_source=in_log_source,
            out_source=out_log_source,
            in_camera_name=in_log_camera,
            out_camera_name=out_log_camera,
            log_count=len(deduplicated_logs),
        ))

    # Check if any logs fall within shift window
    in_window_matched = normalized_shift_start is None
    if window_start and window_end:
        in_window_matched = any(
            window_start <= log.log_timestamp <= window_end
            for log in deduplicated_logs
        )

    return AttendanceRecord(
        employee_id=employee_id,
        attendance_date=attendance_date,
        status="present",
        in_time=in_log_time,
        out_time=out_log_time,
        in_source=in_log_source,
        out_source=out_log_source,
        in_camera_name=in_log_camera,
        out_camera_name=out_log_camera,
        sources=sources,
        camera_names=camera_names,
        shift_start=normalized_shift_start,
        shift_window_start=window_start,
        shift_window_end=window_end,
        total_logs=len(normalized_logs),
        deduplicated_logs=len(deduplicated_logs),
        single_log=len(deduplicated_logs) == 1,
        in_window_matched=in_window_matched,
        remark=remark,
        sessions=sessions,
    )
