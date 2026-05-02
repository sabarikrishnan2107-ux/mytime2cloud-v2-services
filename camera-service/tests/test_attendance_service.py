from datetime import date, datetime
import unittest

from services.attendance import (
    AttendanceCalculationOptions,
    AttendanceLogEvent,
    calculate_daily_attendance,
)


class AttendanceServiceTest(unittest.TestCase):
    def test_absent_when_no_logs_exist(self):
        record = calculate_daily_attendance(
            employee_id=10,
            attendance_date=date(2026, 4, 1),
            logs=[],
            shift_start="08:00:00",
        )

        self.assertEqual(record.status, "absent")
        self.assertIsNone(record.in_time)
        self.assertIsNone(record.out_time)

    def test_single_log_sets_in_and_leaves_out_null(self):
        record = calculate_daily_attendance(
            employee_id=10,
            attendance_date=date(2026, 4, 1),
            logs=[
                AttendanceLogEvent(
                    employee_id=10,
                    log_timestamp=datetime(2026, 4, 1, 8, 5),
                    source="mobile",
                )
            ],
            shift_start="08:00:00",
        )

        self.assertEqual(record.status, "present")
        self.assertEqual(record.in_time, datetime(2026, 4, 1, 8, 5))
        self.assertIsNone(record.out_time)
        self.assertTrue(record.single_log)

    def test_deduplicates_logs_within_two_minutes(self):
        record = calculate_daily_attendance(
            employee_id=10,
            attendance_date=date(2026, 4, 1),
            logs=[
                AttendanceLogEvent(10, datetime(2026, 4, 1, 8, 0), "camera", "Front Gate"),
                AttendanceLogEvent(10, datetime(2026, 4, 1, 8, 1), "mobile"),
                AttendanceLogEvent(10, datetime(2026, 4, 1, 17, 0), "device"),
            ],
            shift_start="08:00:00",
        )

        self.assertEqual(record.total_logs, 3)
        self.assertEqual(record.deduplicated_logs, 2)
        self.assertEqual(record.in_time, datetime(2026, 4, 1, 8, 0))
        self.assertEqual(record.out_time, datetime(2026, 4, 1, 17, 0))
        self.assertEqual(record.sources, ["camera", "device"])

    def test_in_uses_first_log_inside_shift_window(self):
        record = calculate_daily_attendance(
            employee_id=10,
            attendance_date=date(2026, 4, 1),
            logs=[
                AttendanceLogEvent(10, datetime(2026, 4, 1, 6, 30), "device"),
                AttendanceLogEvent(10, datetime(2026, 4, 1, 8, 5), "camera", "Reception"),
                AttendanceLogEvent(10, datetime(2026, 4, 1, 17, 10), "mobile"),
            ],
            shift_start="08:00:00",
        )

        self.assertEqual(record.in_time, datetime(2026, 4, 1, 8, 5))
        self.assertEqual(record.in_source, "camera")
        self.assertEqual(record.out_time, datetime(2026, 4, 1, 17, 10))
        self.assertTrue(record.in_window_matched)

    def test_can_fallback_to_first_log_when_in_window_has_no_match(self):
        record = calculate_daily_attendance(
            employee_id=10,
            attendance_date=date(2026, 4, 1),
            logs=[
                AttendanceLogEvent(10, datetime(2026, 4, 1, 11, 30), "device"),
                AttendanceLogEvent(10, datetime(2026, 4, 1, 18, 30), "camera", "Back Door"),
            ],
            shift_start="08:00:00",
            options=AttendanceCalculationOptions(
                fallback_to_first_log_when_no_in_window_match=True,
            ),
        )

        self.assertEqual(record.in_time, datetime(2026, 4, 1, 11, 30))
        self.assertEqual(record.out_time, datetime(2026, 4, 1, 18, 30))
        self.assertFalse(record.in_window_matched)


if __name__ == "__main__":
    unittest.main()
