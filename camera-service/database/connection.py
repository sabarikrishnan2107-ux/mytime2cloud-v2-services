import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS


def log(msg):
    print(msg, flush=True)


def get_connection():
    """Get a PostgreSQL connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def load_all_embeddings(company_id: int, branch_id: int | None = None) -> dict:
    """Load employee face embeddings for a company, optionally scoped to a branch.

    Returns:
        {employee_id: {"name": str, "embeddings": [np.array, ...]}}
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT e.id as employee_id,
                       CONCAT(e.first_name, ' ', e.last_name) as name,
                       ef.embedding
                FROM employee_face_embeddings ef
                JOIN employees e ON e.id = ef.employee_id
                WHERE ef.company_id = %s
            """
            params = [company_id]

            if branch_id is not None:
                query += """
                  AND e.branch_id = %s
                """
                params.append(branch_id)

            query += """
                ORDER BY e.id
            """

            cur.execute(query, params)

            rows = cur.fetchall()

        result = {}
        for row in rows:
            eid = row["employee_id"]
            raw = row["embedding"]

            if raw is None:
                continue

            if isinstance(raw, memoryview):
                raw = bytes(raw)
            embedding = np.frombuffer(raw, dtype=np.float32)

            if eid not in result:
                result[eid] = {"name": row["name"], "embeddings": []}
            result[eid]["embeddings"].append(embedding)

        scope = f"company {company_id}"
        if branch_id is not None:
            scope += f", branch {branch_id}"
        log(
            f"Loaded {sum(len(v['embeddings']) for v in result.values())} embeddings for {len(result)} employees in {scope}"
        )
        return result

    finally:
        conn.close()


def save_embedding(employee_id: int, company_id: int, embedding: np.ndarray) -> int:
    """Save a face embedding to the database.

    Returns:
        Inserted row ID
    """
    conn = get_connection()
    try:
        embedding_bytes = embedding.astype(np.float32).tobytes()

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO employee_face_embeddings (employee_id, company_id, embedding, created_at, updated_at)
                VALUES (%s, %s, %s, NOW(), NOW())
                RETURNING id
            """, (employee_id, company_id, psycopg2.Binary(embedding_bytes)))

            row_id = cur.fetchone()[0]

        conn.commit()
        log(f"Saved embedding for employee {employee_id} (row {row_id})")
        return row_id

    finally:
        conn.close()


def delete_embeddings(employee_id: int) -> int:
    """Delete all embeddings for an employee.

    Returns:
        Number of rows deleted
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM employee_face_embeddings WHERE employee_id = %s",
                (employee_id,),
            )
            count = cur.rowcount
        conn.commit()
        log(f"Deleted {count} embeddings for employee {employee_id}")
        return count
    finally:
        conn.close()


def get_embedding_count(employee_id: int) -> int:
    """Get number of stored embeddings for an employee."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM employee_face_embeddings WHERE employee_id = %s",
                (employee_id,),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def get_employee_name(employee_id: int) -> str | None:
    """Fetch employee full name."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT CONCAT(first_name, ' ', last_name) FROM employees WHERE id = %s",
                (employee_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def get_employees_with_photos(company_id: int, branch_id: int | None = None) -> list:
    """Fetch all employees that have a profile picture.

    Returns:
        List of dicts: [{"id": int, "name": str, "profile_picture": str}]
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT id, CONCAT(first_name, ' ', last_name) as name, profile_picture
                FROM employees
                WHERE company_id = %s
                  AND profile_picture IS NOT NULL
                  AND profile_picture != ''
            """
            params = [company_id]

            if branch_id is not None:
                query += " AND branch_id = %s"
                params.append(branch_id)

            cur.execute(query, params)
            return cur.fetchall()
    finally:
        conn.close()


def get_employees_without_embeddings(company_id: int, branch_id: int | None = None) -> list:
    """Fetch employees that have profile photos but no embeddings yet.

    Returns:
        List of dicts: [{"id": int, "name": str, "profile_picture": str}]
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT e.id, CONCAT(e.first_name, ' ', e.last_name) as name, e.profile_picture
                FROM employees e
                LEFT JOIN employee_face_embeddings ef ON e.id = ef.employee_id
                WHERE e.company_id = %s
                  AND e.profile_picture IS NOT NULL
                  AND e.profile_picture != ''
                  AND ef.id IS NULL
            """
            params = [company_id]

            if branch_id is not None:
                query += " AND e.branch_id = %s"
                params.append(branch_id)

            cur.execute(query, params)
            return cur.fetchall()
    finally:
        conn.close()
