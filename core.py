from flask import session, redirect, url_for, request, flash
from contextlib import contextmanager
import atexit
import sqlite3
import threading
from queue import Empty, Full, LifoQueue
from functools import wraps
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import hashlib
from werkzeug.security import generate_password_hash, check_password_hash

# 怨듯넻 ?묒뾽??紐⑸줉 (臾쇰쪟 ?묒뾽?μ? ?쇰컲 ?좏깮 紐⑸줉?먯꽌 ?쒖쇅)
WORKPLACES = ['\u0031\ub3d9 \uc870\ubbf8', '\u0031\ub3d9 \uc790\ubc18', '\u0032\ub3d9 \uc2e0\uad00 \u0031\uce35', '\u0032\ub3d9 \uc2e0\uad00 \u0032\uce35']
LOGISTICS_WORKPLACE = '\ubb3c\ub958'
SHARED_WORKPLACE = '공통'
SHARED_MATERIAL_CATEGORIES = {'기름', '소금', '실리카', '트레이'}
WORKPLACE_ALIASES = {
    '1동 조미 작업장': '1동 조미',
    '1동 자반 작업장': '1동 자반',
    '2동 1층': '2동 신관 1층',
    '2동1층': '2동 신관 1층',
    '2동 1층 작업장': '2동 신관 1층',
    '2동 신관 1층 작업장': '2동 신관 1층',
    '2동 2층': '2동 신관 2층',
    '2동2층': '2동 신관 2층',
    '2동 2층 작업장': '2동 신관 2층',
    '2동 신관 2층 작업장': '2동 신관 2층',
    '물류 작업장': LOGISTICS_WORKPLACE,
}

_user_schema_checked = False
_purchase_schema_checked = False
_materials_schema_checked = False
_materials_shared_checked = False
_audit_schema_checked = False
_production_schema_checked = False
_products_schema_checked = False
_raw_material_schema_checked = False
_material_lot_schema_checked = False
_logistics_schema_checked = False
_log_retention_checked = False
_import_schema_checked = False
_backup_schema_checked = False
_dashboard_todo_schema_checked = False
USER_ROLE_OPTIONS = ('readonly', 'production', 'rawmat', 'purchase', 'logistics', 'admin')
_DB_POOL_MAX_SIZE = 6
_db_pool = LifoQueue(maxsize=_DB_POOL_MAX_SIZE)
_db_pool_lock = threading.Lock()


class PooledSQLiteConnection(sqlite3.Connection):
    """SQLite connection that returns to the local pool on close()."""

    def close(self):
        if getattr(self, '_pool_closed', False):
            return
        _return_connection_to_pool(self)

    def close_physical(self):
        if getattr(self, '_pool_closed', False):
            return
        try:
            super().close()
        finally:
            self._pool_closed = True
            self._pool_in_pool = False


def _configure_db_connection(conn):
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=60000')
    conn.execute('PRAGMA cache_size=-64000')


def _prepare_db_connection(conn):
    _configure_db_connection(conn)
    _ensure_user_schema(conn)
    _ensure_purchase_schema(conn)
    _ensure_materials_schema(conn)
    _ensure_audit_schema(conn)
    _ensure_shared_materials(conn)
    _ensure_production_schema(conn)
    _ensure_products_schema(conn)
    _ensure_raw_material_schema(conn)
    _ensure_material_lot_schema(conn)
    _ensure_logistics_schema(conn)
    _ensure_import_schema(conn)
    _ensure_backup_schema(conn)
    _ensure_dashboard_todo_schema(conn)
    _cleanup_old_logs(conn)
    return conn


def _create_db_connection():
    conn = sqlite3.connect(
        str(DB_PATH),
        timeout=60.0,
        isolation_level=None,
        check_same_thread=False,
        factory=PooledSQLiteConnection,
    )
    conn._pool_closed = False
    conn._pool_in_pool = False
    return _prepare_db_connection(conn)


def _acquire_pooled_connection():
    while True:
        try:
            conn = _db_pool.get_nowait()
        except Empty:
            return _create_db_connection()

        conn._pool_in_pool = False
        if getattr(conn, '_pool_closed', False):
            continue
        try:
            _configure_db_connection(conn)
            conn.execute('SELECT 1')
            return conn
        except sqlite3.Error:
            conn.close_physical()


def _return_connection_to_pool(conn):
    if conn is None or getattr(conn, '_pool_closed', False):
        return
    if getattr(conn, '_pool_in_pool', False):
        return

    try:
        if conn.in_transaction:
            conn.rollback()
    except Exception:
        conn.close_physical()
        return

    conn._pool_in_pool = True
    try:
        _db_pool.put_nowait(conn)
    except Full:
        conn._pool_in_pool = False
        conn.close_physical()


def _close_all_pooled_connections():
    with _db_pool_lock:
        while True:
            try:
                conn = _db_pool.get_nowait()
            except Empty:
                break
            conn.close_physical()


atexit.register(_close_all_pooled_connections)


def normalize_user_role(role_value):
    role = (role_value or 'readonly').strip().lower()
    if role not in USER_ROLE_OPTIONS:
        return 'readonly'
    return role


def normalize_workplace_name(workplace_value):
    value = str(workplace_value or '').strip()
    if not value:
        return ''
    return WORKPLACE_ALIASES.get(value, value)


def parse_workplace_roles(raw_value):
    if isinstance(raw_value, dict):
        source = raw_value
    else:
        text = (raw_value or '').strip()
        if not text:
            return {}
        try:
            source = json.loads(text)
        except Exception:
            return {}
    normalized = {}
    for workplace, role_value in (source or {}).items():
        wp = normalize_workplace_name(workplace)
        if not wp or wp not in WORKPLACES:
            continue
        normalized[wp] = normalize_user_role(role_value)
    return normalized


def dump_workplace_roles(role_map, workplaces=None):
    allowed = set(workplaces or WORKPLACES)
    normalized = {}
    for workplace, role_value in (role_map or {}).items():
        wp = normalize_workplace_name(workplace)
        if not wp or wp not in allowed:
            continue
        normalized[wp] = normalize_user_role(role_value)
    return json.dumps(normalized, ensure_ascii=False)


def get_effective_user_role(user=None, workplace=None):
    user = user or session.get('user') or {}
    if not user:
        return 'readonly'
    if bool(user.get('is_admin')):
        return 'admin'
    current_workplace = normalize_workplace_name(workplace or session.get('workplace'))
    workplace_roles = parse_workplace_roles(user.get('workplace_roles'))
    if current_workplace and current_workplace in workplace_roles:
        return workplace_roles[current_workplace]
    return normalize_user_role(user.get('base_role') or user.get('role'))


def can_access_integrated_management(user=None):
    user = user or session.get('user') or {}
    if not user:
        return False
    if bool(user.get('is_admin')):
        return True
    if bool(user.get('can_integrated_management')):
        return True
    username = (user.get('username') or '').strip().lower()
    return username == 'test'


def build_session_user(user_row, current_workplace=None):
    workplaces = user_row.get('workplaces') or []
    if isinstance(workplaces, str):
        workplaces = [value.strip() for value in workplaces.split(',') if value.strip()]
    workplaces = [normalize_workplace_name(value) for value in workplaces if normalize_workplace_name(value)]
    if not workplaces:
        workplaces = ['1??議곕?']
    base_role = normalize_user_role(user_row.get('role') or ('admin' if user_row.get('is_admin') else 'readonly'))
    workplace_roles = parse_workplace_roles(user_row.get('workplace_roles'))
    payload = {
        'id': user_row.get('id'),
        'username': user_row.get('username'),
        'is_admin': bool(user_row.get('is_admin')),
        'name': user_row.get('name') or user_row.get('username'),
        'phone': user_row.get('phone'),
        'email': user_row.get('email'),
        'department': user_row.get('department'),
        'workplace1': user_row.get('workplace1'),
        'workplace2': user_row.get('workplace2'),
        'workplaces': workplaces,
        'base_role': base_role,
        'workplace_roles': workplace_roles,
        'can_integrated_management': bool(user_row.get('can_integrated_management')),
    }
    payload['role'] = get_effective_user_role(payload, normalize_workplace_name(current_workplace or session.get('workplace')))
    return payload

# ?곗씠?곕쿋?댁뒪 ?곌껐

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / 'yemat.db'
try:
    APP_TIMEZONE = ZoneInfo('Asia/Seoul')
except ZoneInfoNotFoundError:
    APP_TIMEZONE = timezone(timedelta(hours=9), name='Asia/Seoul')


def now_local():
    return datetime.now(APP_TIMEZONE)


def today_local():
    return now_local().date()


def today_local_str():
    return now_local().strftime('%Y-%m-%d')


def hash_password(password):
    return generate_password_hash(password or '', method='pbkdf2:sha256', salt_length=16)


def verify_password(stored_hash, password):
    stored = (stored_hash or '').strip()
    candidate = password or ''
    if not stored:
        return False
    if stored.startswith('pbkdf2:') or stored.startswith('scrypt:'):
        try:
            return check_password_hash(stored, candidate)
        except Exception:
            return False
    legacy_hash = hashlib.sha256(candidate.encode()).hexdigest()
    return stored == legacy_hash


def password_needs_rehash(stored_hash):
    stored = (stored_hash or '').strip()
    return bool(stored) and not (stored.startswith('pbkdf2:') or stored.startswith('scrypt:'))


def get_db():
    """Return a pooled SQLite connection configured for this app."""
    with _db_pool_lock:
        return _acquire_pooled_connection()


def close_db(conn):
    """Safely close a database connection."""
    if conn is None:
        return
    try:
        if isinstance(conn, PooledSQLiteConnection):
            conn.close()
        else:
            conn.close()
    except Exception:
        pass


def commit_db(conn):
    """Commit the current transaction if a connection exists."""
    if conn is None:
        return
    conn.commit()


def rollback_db(conn):
    """Rollback the current transaction if a connection exists."""
    if conn is None:
        return
    try:
        conn.rollback()
    except Exception:
        pass


def begin_db_transaction(conn, mode='DEFERRED'):
    """Start an explicit SQLite transaction with a supported lock mode."""
    if conn is None:
        raise ValueError('Database connection is required.')

    normalized_mode = (mode or 'DEFERRED').strip().upper()
    if normalized_mode not in {'DEFERRED', 'IMMEDIATE', 'EXCLUSIVE'}:
        raise ValueError(f'Unsupported transaction mode: {mode}')

    conn.execute(f'BEGIN {normalized_mode}')
    return conn


@contextmanager
def db_connection():
    """Yield a managed connection for reads or manual commit flows."""
    conn = get_db()
    try:
        yield conn
    finally:
        close_db(conn)


@contextmanager
def db_transaction(mode='DEFERRED'):
    """Yield a managed connection wrapped in a rollback-safe transaction."""
    conn = get_db()
    try:
        begin_db_transaction(conn, mode=mode)
        yield conn
        commit_db(conn)
    except Exception:
        rollback_db(conn)
        raise
    finally:
        close_db(conn)


def _ensure_import_schema(conn):
    """Excel import staging tables."""
    global _import_schema_checked
    if _import_schema_checked:
        return
    try:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS import_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                stored_file_name TEXT,
                import_type TEXT NOT NULL,
                uploaded_by TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_rows INTEGER DEFAULT 0,
                ok_rows INTEGER DEFAULT 0,
                warning_rows INTEGER DEFAULT 0,
                error_rows INTEGER DEFAULT 0,
                status TEXT DEFAULT 'uploaded',
                column_mapping_json TEXT,
                applied_result_json TEXT,
                applied_at TIMESTAMP
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS import_raw_rows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                sheet_name TEXT,
                row_no INTEGER NOT NULL,
                raw_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS import_parsed_rows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                sheet_name TEXT,
                row_no INTEGER NOT NULL,
                target_type TEXT NOT NULL,
                matched_material_id INTEGER,
                matched_raw_material_id INTEGER,
                supplier_id INTEGER,
                supplier_name TEXT,
                code TEXT,
                name TEXT,
                category TEXT,
                spec TEXT,
                unit TEXT,
                qty REAL,
                received_quantity REAL,
                current_quantity REAL,
                received_date TEXT,
                manufacture_date TEXT,
                expiry_date TEXT,
                lot TEXT,
                lot_seq INTEGER,
                supplier_lot TEXT,
                ja_ho TEXT,
                sheets_per_sok INTEGER,
                car_number TEXT,
                workplace TEXT,
                unit_price REAL DEFAULT 0,
                status TEXT DEFAULT 'ERROR',
                error_message TEXT,
                warning_message TEXT,
                applied_at TIMESTAMP
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS material_name_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_text TEXT NOT NULL UNIQUE,
                material_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS raw_material_name_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_text TEXT NOT NULL UNIQUE,
                raw_material_code TEXT,
                raw_material_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_import_batches_status ON import_batches(status, uploaded_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_import_raw_rows_batch ON import_raw_rows(batch_id, row_no)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_import_parsed_rows_batch ON import_parsed_rows(batch_id, status, row_no)")
    except Exception:
        pass
    _import_schema_checked = True


def _ensure_backup_schema(conn):
    """Database backup settings table."""
    global _backup_schema_checked
    if _backup_schema_checked:
        return
    try:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS db_backup_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                auto_backup_enabled INTEGER DEFAULT 0,
                auto_backup_time TEXT,
                auto_retention_days INTEGER DEFAULT 60,
                manual_keep_count INTEGER DEFAULT 10,
                last_auto_backup_at TEXT,
                last_auto_backup_name TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute(
            '''
            INSERT INTO db_backup_settings (
                id,
                auto_backup_enabled,
                auto_retention_days,
                manual_keep_count
            )
            VALUES (1, 0, 60, 10)
            ON CONFLICT(id) DO NOTHING
            '''
        )
        cols = [row['name'] for row in conn.execute("PRAGMA table_info(db_backup_settings)").fetchall()]
        if 'auto_retention_days' not in cols:
            conn.execute("ALTER TABLE db_backup_settings ADD COLUMN auto_retention_days INTEGER DEFAULT 60")
        if 'manual_keep_count' not in cols:
            conn.execute("ALTER TABLE db_backup_settings ADD COLUMN manual_keep_count INTEGER DEFAULT 10")
        if 'last_auto_backup_at' not in cols:
            conn.execute("ALTER TABLE db_backup_settings ADD COLUMN last_auto_backup_at TEXT")
        if 'last_auto_backup_name' not in cols:
            conn.execute("ALTER TABLE db_backup_settings ADD COLUMN last_auto_backup_name TEXT")
        if 'updated_at' not in cols:
            conn.execute("ALTER TABLE db_backup_settings ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        conn.execute(
            '''
            UPDATE db_backup_settings
            SET auto_retention_days = 60
            WHERE auto_retention_days IS NULL OR auto_retention_days < 1
            '''
        )
        conn.execute(
            '''
            UPDATE db_backup_settings
            SET manual_keep_count = 10
            WHERE manual_keep_count IS NULL OR manual_keep_count < 1
            '''
        )
    except Exception:
        pass
    _backup_schema_checked = True


def _ensure_dashboard_todo_schema(conn):
    """Dashboard to-do table and indexes."""
    global _dashboard_todo_schema_checked
    if _dashboard_todo_schema_checked:
        return
    try:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS dashboard_todos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workplace TEXT NOT NULL,
                title TEXT NOT NULL,
                detail TEXT,
                importance TEXT,
                due_date TEXT,
                todo_status TEXT NOT NULL DEFAULT 'processing',
                is_done INTEGER NOT NULL DEFAULT 0,
                is_announcement INTEGER NOT NULL DEFAULT 0,
                is_private INTEGER NOT NULL DEFAULT 0,
                created_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                done_by TEXT,
                done_at TIMESTAMP
            )
            '''
        )
        todo_cols = [row['name'] for row in conn.execute("PRAGMA table_info(dashboard_todos)").fetchall()]
        if 'detail' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN detail TEXT")
        if 'importance' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN importance TEXT")
        if 'due_date' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN due_date TEXT")
        if 'todo_status' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN todo_status TEXT NOT NULL DEFAULT 'processing'")
        if 'is_done' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN is_done INTEGER NOT NULL DEFAULT 0")
        if 'is_announcement' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN is_announcement INTEGER NOT NULL DEFAULT 0")
        if 'is_private' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN is_private INTEGER NOT NULL DEFAULT 0")
        if 'created_by' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN created_by TEXT")
        if 'created_at' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        if 'done_by' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN done_by TEXT")
        if 'done_at' not in todo_cols:
            conn.execute("ALTER TABLE dashboard_todos ADD COLUMN done_at TIMESTAMP")
        conn.execute(
            """
            UPDATE dashboard_todos
            SET todo_status = CASE
                WHEN COALESCE(is_done, 0) = 1 THEN 'completed'
                WHEN COALESCE(TRIM(todo_status), '') IN ('processing', 'info_needed', 'completed') THEN TRIM(todo_status)
                ELSE 'processing'
            END
            """
        )
        conn.execute(
            """
            UPDATE dashboard_todos
            SET is_done = CASE WHEN todo_status = 'completed' THEN 1 ELSE 0 END
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dashboard_todos_workplace_status_due ON dashboard_todos(workplace, todo_status, due_date, created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dashboard_todos_workplace_done_at ON dashboard_todos(workplace, done_at DESC, created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dashboard_todos_announcement_status ON dashboard_todos(is_announcement, todo_status, due_date, created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dashboard_todos_private_owner_status ON dashboard_todos(is_private, created_by, todo_status, due_date, created_at DESC)"
        )
    except Exception:
        pass
    _dashboard_todo_schema_checked = True


def _ensure_logistics_schema(conn):
    """Logistics hub inventory and issue request tables."""
    global _logistics_schema_checked
    if _logistics_schema_checked:
        return
    try:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS logistics_stocks (
                material_code TEXT PRIMARY KEY,
                material_name TEXT,
                unit TEXT,
                quantity REAL DEFAULT 0,
                updated_by TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS logistics_issue_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                material_id INTEGER NOT NULL,
                material_code TEXT,
                material_name TEXT,
                unit TEXT,
                requester_workplace TEXT NOT NULL,
                requested_quantity REAL NOT NULL DEFAULT 0,
                approved_quantity REAL DEFAULT 0,
                request_type TEXT NOT NULL DEFAULT 'ISSUE',
                reason TEXT,
                reason_detail TEXT,
                material_lot_id INTEGER,
                status TEXT NOT NULL DEFAULT '?붿껌',
                note TEXT,
                requested_by TEXT,
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_by TEXT,
                processed_at TIMESTAMP,
                process_note TEXT
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS logistics_defect_stocks (
                material_code TEXT PRIMARY KEY,
                material_name TEXT,
                unit TEXT,
                quantity REAL DEFAULT 0,
                updated_by TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        # 湲곗〈 DB 留덉씠洹몃젅?댁뀡
        li_cols = [row['name'] for row in conn.execute("PRAGMA table_info(logistics_issue_requests)").fetchall()]
        if 'request_type' not in li_cols:
            conn.execute("ALTER TABLE logistics_issue_requests ADD COLUMN request_type TEXT NOT NULL DEFAULT 'ISSUE'")
        if 'reason' not in li_cols:
            conn.execute("ALTER TABLE logistics_issue_requests ADD COLUMN reason TEXT")
        if 'reason_detail' not in li_cols:
            conn.execute("ALTER TABLE logistics_issue_requests ADD COLUMN reason_detail TEXT")
        if 'material_lot_id' not in li_cols:
            conn.execute("ALTER TABLE logistics_issue_requests ADD COLUMN material_lot_id INTEGER")
        if 'requester_username' not in li_cols:
            conn.execute("ALTER TABLE logistics_issue_requests ADD COLUMN requester_username TEXT")
        if 'rejected_reason' not in li_cols:
            conn.execute("ALTER TABLE logistics_issue_requests ADD COLUMN rejected_reason TEXT")
        if 'rejected_by' not in li_cols:
            conn.execute("ALTER TABLE logistics_issue_requests ADD COLUMN rejected_by TEXT")
        if 'rejected_at' not in li_cols:
            conn.execute("ALTER TABLE logistics_issue_requests ADD COLUMN rejected_at TIMESTAMP")
        if 'receipt_updated_at' not in li_cols:
            conn.execute("ALTER TABLE logistics_issue_requests ADD COLUMN receipt_updated_at TIMESTAMP")
        if 'original_approved_quantity' not in li_cols:
            conn.execute("ALTER TABLE logistics_issue_requests ADD COLUMN original_approved_quantity REAL")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logistics_issue_status ON logistics_issue_requests(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logistics_issue_workplace ON logistics_issue_requests(requester_workplace)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logistics_issue_type ON logistics_issue_requests(request_type)")
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS user_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT,
                link TEXT,
                is_read INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                read_at TIMESTAMP
            )
            '''
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_notifications_username ON user_notifications(username, is_read, created_at DESC)")
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS user_dynamic_notification_reads (
                username TEXT NOT NULL,
                notification_key TEXT NOT NULL,
                signature TEXT NOT NULL,
                read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (username, notification_key)
            )
            '''
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_dynamic_notification_reads_username ON user_dynamic_notification_reads(username)")
    except Exception:
        pass
    _logistics_schema_checked = True


def add_user_notification(conn, username, title, body='', link=None):
    """Insert a lightweight in-app notification for a user."""
    if not username or not title:
        return
    try:
        conn.execute(
            '''
            INSERT INTO user_notifications (username, title, body, link)
            VALUES (?, ?, ?, ?)
            ''',
            (username, title, body, link),
        )
    except Exception:
        pass


def get_usernames_for_notification(conn, roles=None, include_admin=False):
    roles = [r for r in (roles or []) if r]
    params = ['approved']
    clauses = ["status = ?"]
    role_parts = []
    if include_admin:
        role_parts.append("COALESCE(is_admin, 0) = 1")
    if roles:
        placeholders = ','.join(['?'] * len(roles))
        role_parts.append(f"role IN ({placeholders})")
        params.extend(roles)
    if role_parts:
        clauses.append('(' + ' OR '.join(role_parts) + ')')
    sql = f"SELECT username FROM users WHERE {' AND '.join(clauses)}"
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [row['username'] for row in rows if row['username']]


def _ensure_user_schema(conn):
    """Ensure required columns exist on the users table."""
    global _user_schema_checked
    if _user_schema_checked:
        return
    try:
        cols = [row['name'] for row in conn.execute("PRAGMA table_info(users)").fetchall()]
        if 'role' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN role TEXT")
        if 'workplaces' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN workplaces TEXT")
        if 'workplace_roles' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN workplace_roles TEXT")
        if 'can_integrated_management' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN can_integrated_management INTEGER DEFAULT 0")
        if 'status' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'approved'")
        if 'recovery_question' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN recovery_question TEXT")
        if 'recovery_answer_hash' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN recovery_answer_hash TEXT")

        # 湲곗〈 ?ъ슜??湲곕낯媛?蹂댁젙
        conn.execute("UPDATE users SET status='approved' WHERE status IS NULL")
        # workplaces 鍮꾩뼱 ?덉쑝硫?workplace1/2 湲곗??쇰줈 梨꾩?
        conn.execute(
            """
            UPDATE users
            SET workplaces = TRIM(
                COALESCE(workplace1, '') ||
                CASE WHEN workplace2 IS NOT NULL AND workplace2 != '' THEN ',' || workplace2 ELSE '' END
            )
            WHERE (workplaces IS NULL OR workplaces = '')
            """
        )
        # 洹몃옒??鍮꾨㈃ 湲곕낯 ?묒뾽??
        conn.execute("UPDATE users SET workplaces='1??議곕?' WHERE workplaces IS NULL OR workplaces = ''")
        conn.execute("UPDATE users SET can_integrated_management = 1 WHERE COALESCE(is_admin, 0) = 1")
        conn.execute("UPDATE users SET can_integrated_management = 1 WHERE LOWER(COALESCE(username, '')) = 'test'")
    except Exception:
        pass
    _user_schema_checked = True


def _ensure_shared_materials(conn):
    """Normalize shared material categories into the shared workplace."""
    global _materials_shared_checked
    if _materials_shared_checked:
        return
    try:
        cols = [row['name'] for row in conn.execute("PRAGMA table_info(materials)").fetchall()]
        if 'workplace' not in cols:
            _materials_shared_checked = True
            return
        if not SHARED_MATERIAL_CATEGORIES:
            _materials_shared_checked = True
            return

        placeholders = ','.join(['?'] * len(SHARED_MATERIAL_CATEGORIES))
        sql = f"""
            UPDATE materials
            SET workplace = ?
            WHERE category IN ({placeholders})
              AND (workplace IS NULL OR workplace != ?)
        """
        params = [SHARED_WORKPLACE, *SHARED_MATERIAL_CATEGORIES, SHARED_WORKPLACE]
        conn.execute(sql, params)
    except Exception:
        pass
    _materials_shared_checked = True


def _ensure_materials_schema(conn):
    global _materials_schema_checked
    if _materials_schema_checked:
        return
    try:
        cols = [row['name'] for row in conn.execute("PRAGMA table_info(materials)").fetchall()]
        if 'upper_unit' not in cols:
            conn.execute("ALTER TABLE materials ADD COLUMN upper_unit TEXT")
        if 'upper_unit_qty' not in cols:
            conn.execute("ALTER TABLE materials ADD COLUMN upper_unit_qty REAL")
        conn.execute("UPDATE materials SET unit = 'EA' WHERE COALESCE(TRIM(unit), '') = '媛?")
        conn.execute("UPDATE logistics_stocks SET unit = 'EA' WHERE COALESCE(TRIM(unit), '') = '媛?")
        conn.execute("UPDATE logistics_defect_stocks SET unit = 'EA' WHERE COALESCE(TRIM(unit), '') = '媛?")
        conn.execute("UPDATE logistics_issue_requests SET unit = 'EA' WHERE COALESCE(TRIM(unit), '') = '媛?")
    except Exception:
        pass
    _materials_schema_checked = True


def _ensure_purchase_schema(conn):
    """Ensure purchase request audit columns exist."""
    global _purchase_schema_checked
    if _purchase_schema_checked:
        return
    try:
        cols = [row['name'] for row in conn.execute("PRAGMA table_info(purchase_requests)").fetchall()]
        if 'ordered_by' not in cols:
            conn.execute("ALTER TABLE purchase_requests ADD COLUMN ordered_by TEXT")
        if 'requester_username' not in cols:
            conn.execute("ALTER TABLE purchase_requests ADD COLUMN requester_username TEXT")
        if 'received_by' not in cols:
            conn.execute("ALTER TABLE purchase_requests ADD COLUMN received_by TEXT")
        if 'logistics_closed' not in cols:
            conn.execute("ALTER TABLE purchase_requests ADD COLUMN logistics_closed INTEGER DEFAULT 0")
        if 'logistics_closed_at' not in cols:
            conn.execute("ALTER TABLE purchase_requests ADD COLUMN logistics_closed_at TEXT")
        if 'logistics_close_note' not in cols:
            conn.execute("ALTER TABLE purchase_requests ADD COLUMN logistics_close_note TEXT")
        if 'logistics_close_type' not in cols:
            conn.execute("ALTER TABLE purchase_requests ADD COLUMN logistics_close_type TEXT")
    except Exception:
        pass
    _purchase_schema_checked = True


def _ensure_audit_schema(conn):
    """Ensure the audit log table exists."""
    global _audit_schema_checked
    if _audit_schema_checked:
        return
    try:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                entity TEXT NOT NULL,
                entity_id INTEGER,
                data TEXT,
                username TEXT,
                name TEXT,
                workplace TEXT,
                ip TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
    except Exception:
        pass
    _audit_schema_checked = True


def _ensure_production_schema(conn):
    """Ensure production table support columns exist."""
    global _production_schema_checked
    if _production_schema_checked:
        return
    try:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS export_schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workplace TEXT NOT NULL,
                product_id INTEGER NOT NULL,
                export_quantity INTEGER NOT NULL DEFAULT 0,
                boxes_per_container INTEGER NOT NULL DEFAULT 0,
                container_count INTEGER NOT NULL DEFAULT 0,
                container_box_quantities TEXT NOT NULL DEFAULT '',
                unit_mode TEXT NOT NULL DEFAULT 'container',
                production_start_date TEXT NOT NULL,
                production_end_date TEXT,
                cutoff_date TEXT NOT NULL,
                line TEXT,
                note TEXT,
                created_by TEXT,
                updated_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS export_schedule_containers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_schedule_id INTEGER NOT NULL,
                container_no INTEGER NOT NULL,
                po_number TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(export_schedule_id, container_no)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS set_schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workplace TEXT NOT NULL,
                finished_product_id INTEGER NOT NULL,
                finished_product_quantities TEXT NOT NULL DEFAULT '',
                production_start_date TEXT NOT NULL,
                production_end_date TEXT NOT NULL,
                excluded_dates TEXT NOT NULL DEFAULT '',
                note TEXT,
                created_by TEXT,
                updated_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS set_schedule_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                set_schedule_id INTEGER NOT NULL,
                component_product_id INTEGER NOT NULL,
                required_quantity INTEGER NOT NULL DEFAULT 0,
                priority INTEGER NOT NULL DEFAULT 1,
                line TEXT NOT NULL DEFAULT '',
                workplace TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(set_schedule_id, component_product_id)
            )
            '''
        )
        export_cols = [row['name'] for row in conn.execute("PRAGMA table_info(export_schedules)").fetchall()]
        if 'workplace' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN workplace TEXT NOT NULL DEFAULT ''")
        if 'product_id' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN product_id INTEGER NOT NULL DEFAULT 0")
        if 'export_quantity' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN export_quantity INTEGER NOT NULL DEFAULT 0")
        if 'boxes_per_container' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN boxes_per_container INTEGER NOT NULL DEFAULT 0")
        if 'container_count' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN container_count INTEGER NOT NULL DEFAULT 0")
        if 'container_box_quantities' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN container_box_quantities TEXT NOT NULL DEFAULT ''")
        if 'unit_mode' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN unit_mode TEXT NOT NULL DEFAULT 'container'")
        if 'production_start_date' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN production_start_date TEXT")
        if 'production_end_date' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN production_end_date TEXT")
        if 'cutoff_date' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN cutoff_date TEXT")
        if 'line' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN line TEXT")
        if 'note' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN note TEXT")
        if 'excluded_dates' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN excluded_dates TEXT NOT NULL DEFAULT ''")
        if 'created_by' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN created_by TEXT")
        if 'updated_by' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN updated_by TEXT")
        if 'created_at' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        if 'updated_at' not in export_cols:
            conn.execute("ALTER TABLE export_schedules ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        conn.execute(
            "UPDATE export_schedules SET production_end_date = COALESCE(NULLIF(production_end_date, ''), cutoff_date) "
            "WHERE COALESCE(NULLIF(production_end_date, ''), '') = ''"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_export_schedules_workplace_dates ON export_schedules(workplace, production_start_date, cutoff_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_export_schedules_workplace_product ON export_schedules(workplace, product_id, cutoff_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_export_schedule_containers_schedule ON export_schedule_containers(export_schedule_id, container_no)"
        )
        set_schedule_cols = [row['name'] for row in conn.execute("PRAGMA table_info(set_schedules)").fetchall()]
        if 'workplace' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN workplace TEXT NOT NULL DEFAULT ''")
        if 'finished_product_id' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN finished_product_id INTEGER NOT NULL DEFAULT 0")
        if 'finished_product_quantities' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN finished_product_quantities TEXT NOT NULL DEFAULT ''")
        if 'production_start_date' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN production_start_date TEXT NOT NULL DEFAULT ''")
        if 'production_end_date' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN production_end_date TEXT NOT NULL DEFAULT ''")
        if 'excluded_dates' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN excluded_dates TEXT NOT NULL DEFAULT ''")
        if 'note' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN note TEXT")
        if 'created_by' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN created_by TEXT")
        if 'updated_by' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN updated_by TEXT")
        if 'created_at' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        if 'updated_at' not in set_schedule_cols:
            conn.execute("ALTER TABLE set_schedules ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        set_schedule_item_cols = [row['name'] for row in conn.execute("PRAGMA table_info(set_schedule_items)").fetchall()]
        if 'set_schedule_id' not in set_schedule_item_cols:
            conn.execute("ALTER TABLE set_schedule_items ADD COLUMN set_schedule_id INTEGER NOT NULL DEFAULT 0")
        if 'component_product_id' not in set_schedule_item_cols:
            conn.execute("ALTER TABLE set_schedule_items ADD COLUMN component_product_id INTEGER NOT NULL DEFAULT 0")
        if 'required_quantity' not in set_schedule_item_cols:
            conn.execute("ALTER TABLE set_schedule_items ADD COLUMN required_quantity INTEGER NOT NULL DEFAULT 0")
        if 'priority' not in set_schedule_item_cols:
            conn.execute("ALTER TABLE set_schedule_items ADD COLUMN priority INTEGER NOT NULL DEFAULT 1")
        if 'line' not in set_schedule_item_cols:
            conn.execute("ALTER TABLE set_schedule_items ADD COLUMN line TEXT NOT NULL DEFAULT ''")
        if 'workplace' not in set_schedule_item_cols:
            conn.execute("ALTER TABLE set_schedule_items ADD COLUMN workplace TEXT NOT NULL DEFAULT ''")
        if 'created_at' not in set_schedule_item_cols:
            conn.execute("ALTER TABLE set_schedule_items ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_set_schedules_workplace_dates ON set_schedules(workplace, production_start_date, production_end_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_set_schedule_items_schedule_priority ON set_schedule_items(set_schedule_id, priority, id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_set_schedule_items_workplace ON set_schedule_items(workplace, set_schedule_id)"
        )

        schedule_cols = [row['name'] for row in conn.execute("PRAGMA table_info(production_schedules)").fetchall()]
        if 'line' not in schedule_cols:
            conn.execute("ALTER TABLE production_schedules ADD COLUMN line TEXT")
        if 'line_usage_disabled' not in schedule_cols:
            conn.execute("ALTER TABLE production_schedules ADD COLUMN line_usage_disabled INTEGER NOT NULL DEFAULT 0")
        if 'workplace' not in schedule_cols:
            conn.execute("ALTER TABLE production_schedules ADD COLUMN workplace TEXT")
        if 'production_id' not in schedule_cols:
            conn.execute("ALTER TABLE production_schedules ADD COLUMN production_id INTEGER")
        if 'schedule_source' not in schedule_cols:
            conn.execute("ALTER TABLE production_schedules ADD COLUMN schedule_source TEXT DEFAULT 'manual'")
        if 'export_schedule_id' not in schedule_cols:
            conn.execute("ALTER TABLE production_schedules ADD COLUMN export_schedule_id INTEGER")
        if 'set_schedule_id' not in schedule_cols:
            conn.execute("ALTER TABLE production_schedules ADD COLUMN set_schedule_id INTEGER")
        if 'set_schedule_item_id' not in schedule_cols:
            conn.execute("ALTER TABLE production_schedules ADD COLUMN set_schedule_item_id INTEGER")
        if 'export_container_no' not in schedule_cols:
            conn.execute("ALTER TABLE production_schedules ADD COLUMN export_container_no INTEGER")
        if 'export_container_label' not in schedule_cols:
            conn.execute("ALTER TABLE production_schedules ADD COLUMN export_container_label TEXT")
        conn.execute("UPDATE production_schedules SET schedule_source = 'manual' WHERE schedule_source IS NULL OR TRIM(schedule_source) = ''")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_production_schedules_export_schedule ON production_schedules(export_schedule_id, scheduled_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_production_schedules_set_schedule ON production_schedules(set_schedule_id, scheduled_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_production_schedules_workplace_date ON production_schedules(workplace, scheduled_date)"
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS schedule_special_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workplace TEXT NOT NULL,
                note_date TEXT NOT NULL,
                note_color TEXT NOT NULL DEFAULT 'blue',
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                created_by TEXT,
                updated_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        note_cols = [row['name'] for row in conn.execute("PRAGMA table_info(schedule_special_notes)").fetchall()]
        if 'workplace' not in note_cols:
            conn.execute("ALTER TABLE schedule_special_notes ADD COLUMN workplace TEXT NOT NULL DEFAULT ''")
        if 'note_date' not in note_cols:
            conn.execute("ALTER TABLE schedule_special_notes ADD COLUMN note_date TEXT NOT NULL DEFAULT ''")
        if 'note_color' not in note_cols:
            conn.execute("ALTER TABLE schedule_special_notes ADD COLUMN note_color TEXT NOT NULL DEFAULT 'blue'")
        if 'title' not in note_cols:
            conn.execute("ALTER TABLE schedule_special_notes ADD COLUMN title TEXT NOT NULL DEFAULT ''")
        if 'content' not in note_cols:
            conn.execute("ALTER TABLE schedule_special_notes ADD COLUMN content TEXT NOT NULL DEFAULT ''")
        if 'created_by' not in note_cols:
            conn.execute("ALTER TABLE schedule_special_notes ADD COLUMN created_by TEXT")
        if 'updated_by' not in note_cols:
            conn.execute("ALTER TABLE schedule_special_notes ADD COLUMN updated_by TEXT")
        if 'created_at' not in note_cols:
            conn.execute("ALTER TABLE schedule_special_notes ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        if 'updated_at' not in note_cols:
            conn.execute("ALTER TABLE schedule_special_notes ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_schedule_special_notes_workplace_date ON schedule_special_notes(workplace, note_date)"
        )

        cols = [row['name'] for row in conn.execute("PRAGMA table_info(productions)").fetchall()]
        if 'schedule_id' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN schedule_id INTEGER")
        if 'workplace' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN workplace TEXT")
        if 'export_schedule_id' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN export_schedule_id INTEGER")
        if 'set_schedule_id' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN set_schedule_id INTEGER")
        if 'set_schedule_item_id' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN set_schedule_item_id INTEGER")
        if 'supply_line' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN supply_line TEXT")
        if 'line_usage_disabled' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN line_usage_disabled INTEGER NOT NULL DEFAULT 0")
        if 'supply_people' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN supply_people INTEGER")
        if 'packing_line' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN packing_line TEXT")
        if 'packing_people' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN packing_people INTEGER")
        if 'outer_packing_line' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN outer_packing_line TEXT")
        if 'outer_packing_people' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN outer_packing_people INTEGER")
        if 'work_time' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN work_time TEXT")
        if 'personnel_note' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN personnel_note TEXT")
        if 'expiry_date' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN expiry_date TEXT")
        if 'expiry_date_2' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN expiry_date_2 TEXT")
        if 'expiry_date_3' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN expiry_date_3 TEXT")
        if 'expiry_boxes_1' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN expiry_boxes_1 REAL")
        if 'expiry_boxes_2' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN expiry_boxes_2 REAL")
        if 'expiry_boxes_3' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN expiry_boxes_3 REAL")
        if 'sample_excluded_boxes_1' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN sample_excluded_boxes_1 REAL")
        if 'sample_excluded_boxes_2' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN sample_excluded_boxes_2 REAL")
        if 'sample_excluded_boxes_3' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN sample_excluded_boxes_3 REAL")
        if 'raw_sok_mode' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN raw_sok_mode INTEGER DEFAULT 1")
        if 'entry_mode' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN entry_mode TEXT NOT NULL DEFAULT 'standard'")
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS production_workplace_settings (
                workplace TEXT PRIMARY KEY,
                material_management_disabled INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute("UPDATE productions SET raw_sok_mode = 1 WHERE raw_sok_mode IS NULL OR raw_sok_mode < 1")
        conn.execute(
            "UPDATE productions SET entry_mode = 'standard' "
            "WHERE COALESCE(NULLIF(TRIM(entry_mode), ''), '') = ''"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_productions_export_schedule ON productions(export_schedule_id, production_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_productions_set_schedule ON productions(set_schedule_id, production_date)"
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS production_personnel_note_hidden (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workplace TEXT NOT NULL,
                note_text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(workplace, note_text)
            )
            '''
        )
        usage_cols = [row['name'] for row in conn.execute("PRAGMA table_info(production_material_usage)").fetchall()]
        if 'component_product_id' not in usage_cols:
            conn.execute("ALTER TABLE production_material_usage ADD COLUMN component_product_id INTEGER")
        if 'usage_note' not in usage_cols:
            conn.execute("ALTER TABLE production_material_usage ADD COLUMN usage_note TEXT")
        if 'override_receiving_date' not in usage_cols:
            conn.execute("ALTER TABLE production_material_usage ADD COLUMN override_receiving_date TEXT")
        if 'override_expiry_date' not in usage_cols:
            conn.execute("ALTER TABLE production_material_usage ADD COLUMN override_expiry_date TEXT")
        if 'override_manufacture_date' not in usage_cols:
            conn.execute("ALTER TABLE production_material_usage ADD COLUMN override_manufacture_date TEXT")
        if 'override_car_number' not in usage_cols:
            conn.execute("ALTER TABLE production_material_usage ADD COLUMN override_car_number TEXT")
        if 'raw_material_code_snapshot' not in usage_cols:
            conn.execute("ALTER TABLE production_material_usage ADD COLUMN raw_material_code_snapshot TEXT")
        if 'register_lot_deferred' not in usage_cols:
            conn.execute("ALTER TABLE production_material_usage ADD COLUMN register_lot_deferred INTEGER NOT NULL DEFAULT 0")
        # 등록 모드는 재고 원초를 실제로 소비한 기록이 아니라 과거 생산 내역을
        # 수기로 남기는 용도다. 기존 등록 모드 이력에 남은 원초 FK도 스냅샷으로
        # 보존한 뒤 분리해 원초 재고 삭제/사용량에 영향을 주지 않게 한다.
        conn.execute(
            '''
            UPDATE production_material_usage
            SET raw_material_code_snapshot = COALESCE(
                NULLIF(TRIM(raw_material_code_snapshot), ''),
                (
                    SELECT COALESCE(NULLIF(TRIM(rm.code), ''), '')
                    FROM raw_materials rm
                    WHERE rm.id = production_material_usage.raw_material_id
                ),
                ''
            )
            WHERE raw_material_id IS NOT NULL
              AND production_id IN (
                  SELECT id FROM productions
                  WHERE LOWER(COALESCE(entry_mode, '')) = 'register'
              )
            '''
        )
        conn.execute(
            '''
            UPDATE production_material_usage
            SET raw_material_id = NULL
            WHERE raw_material_id IS NOT NULL
              AND production_id IN (
                  SELECT id FROM productions
                  WHERE LOWER(COALESCE(entry_mode, '')) = 'register'
              )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS production_component_lot_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                production_usage_id INTEGER NOT NULL,
                component_production_id INTEGER,
                component_product_id INTEGER NOT NULL,
                receiving_date TEXT,
                expiry_date TEXT,
                quantity REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (production_usage_id) REFERENCES production_material_usage(id),
                FOREIGN KEY (component_production_id) REFERENCES productions(id)
            )
            '''
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pclu_usage_id ON production_component_lot_usage(production_usage_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pclu_component_output ON production_component_lot_usage(component_production_id, expiry_date)"
        )
    except Exception:
        pass
    _production_schema_checked = True


def _ensure_products_schema(conn):
    """Ensure product helper columns exist."""
    global _products_schema_checked
    if _products_schema_checked:
        return
    try:
        cols = [row['name'] for row in conn.execute("PRAGMA table_info(products)").fetchall()]
        if 'expiry_months' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN expiry_months INTEGER DEFAULT 12")
        if 'sok_per_box_2' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN sok_per_box_2 REAL")
        if 'sok_per_box_3' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN sok_per_box_3 REAL")
        if 'sheets_per_pack_2' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN sheets_per_pack_2 INTEGER")
        if 'sheets_per_pack_3' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN sheets_per_pack_3 INTEGER")
        if 'spec_sheet_file_name' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN spec_sheet_file_name TEXT")
        if 'spec_sheet_stored_name' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN spec_sheet_stored_name TEXT")
        if 'spec_sheet_uploaded_at' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN spec_sheet_uploaded_at TIMESTAMP")
        if 'selected_silica_material_id' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN selected_silica_material_id INTEGER")
        if 'selected_pouch_material_id' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN selected_pouch_material_id INTEGER")
        if 'set_item_type' not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN set_item_type TEXT DEFAULT ''")
        conn.execute(
            "UPDATE products SET set_item_type = '' "
            "WHERE set_item_type IS NULL"
        )
        conn.execute("UPDATE products SET expiry_months = 12 WHERE expiry_months IS NULL")
        bom_cols = [row['name'] for row in conn.execute("PRAGMA table_info(bom)").fetchall()]
        if bom_cols and 'quantity_per_box_expr' not in bom_cols:
            conn.execute("ALTER TABLE bom ADD COLUMN quantity_per_box_expr TEXT")
        if bom_cols and 'component_product_id' not in bom_cols:
            conn.execute("ALTER TABLE bom ADD COLUMN component_product_id INTEGER")
        if bom_cols:
            conn.execute(
                '''
                INSERT INTO bom (product_id, component_product_id, quantity_per_box)
                SELECT finished.id, component.id, 3
                FROM products finished
                JOIN products component
                  ON component.code = 'C-KR-R003'
                WHERE finished.code = 'A-KR-Z002V1'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bom existing
                      WHERE existing.product_id = finished.id
                        AND existing.component_product_id = component.id
                  )
                '''
            )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS product_stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                workplace TEXT NOT NULL,
                current_stock REAL NOT NULL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(product_id, workplace)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS product_stock_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                workplace TEXT NOT NULL,
                action TEXT NOT NULL,
                quantity REAL NOT NULL DEFAULT 0,
                note TEXT,
                production_id INTEGER,
                created_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        stock_cols = [row['name'] for row in conn.execute("PRAGMA table_info(product_stocks)").fetchall()]
        if 'updated_at' not in stock_cols:
            conn.execute("ALTER TABLE product_stocks ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_product_stocks_product_workplace ON product_stocks(product_id, workplace)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_product_stock_logs_product_workplace ON product_stock_logs(product_id, workplace, created_at)"
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS set_schedule_stock_deductions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                set_schedule_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                workplace TEXT NOT NULL,
                quantity REAL NOT NULL,
                reason TEXT NOT NULL,
                created_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_set_schedule_stock_deductions_schedule ON set_schedule_stock_deductions(set_schedule_id, created_at DESC)"
        )
    except Exception:
        pass
    _products_schema_checked = True


def _ensure_raw_material_schema(conn):
    """Ensure raw material code and lot columns exist."""
    global _raw_material_schema_checked
    if _raw_material_schema_checked:
        return
    try:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS raw_materials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                code TEXT,
                lot TEXT,
                sheets_per_sok REAL DEFAULT 0,
                receiving_date TEXT,
                ja_ho TEXT,
                car_number TEXT,
                total_stock REAL DEFAULT 0,
                current_stock REAL DEFAULT 0,
                used_quantity REAL DEFAULT 0,
                workplace TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        cols = [row['name'] for row in conn.execute("PRAGMA table_info(raw_materials)").fetchall()]
        if 'code' not in cols:
            conn.execute("ALTER TABLE raw_materials ADD COLUMN code TEXT")
        if 'lot' not in cols:
            conn.execute("ALTER TABLE raw_materials ADD COLUMN lot TEXT")
        if 'ja_ho' not in cols:
            conn.execute("ALTER TABLE raw_materials ADD COLUMN ja_ho TEXT")

        conn.execute(
            '''
            UPDATE raw_materials
            SET ja_ho = TRIM(car_number)
            WHERE (ja_ho IS NULL OR TRIM(ja_ho) = '')
              AND car_number IS NOT NULL
              AND TRIM(car_number) != ''
            '''
        )
        conn.execute(
            '''
            UPDATE raw_materials
            SET car_number = TRIM(ja_ho)
            WHERE (car_number IS NULL OR TRIM(car_number) = '')
              AND ja_ho IS NOT NULL
              AND TRIM(ja_ho) != ''
            '''
        )

        conn.execute(
            '''
            UPDATE raw_materials
            SET code = printf('RM%05d', id)
            WHERE code IS NULL OR TRIM(code) = ''
            '''
        )
        conn.execute(
            '''
            UPDATE raw_materials
            SET lot = (
                COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id))
                || '-' ||
                CASE
                    WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN '00000000'
                    ELSE REPLACE(TRIM(receiving_date), '-', '')
                END
                || '-' ||
                CASE
                    WHEN COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')) IS NULL THEN 'NO_CAR'
                    ELSE REPLACE(
                        REPLACE(
                            REPLACE(COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')), ' ', ''),
                            '-',
                            ''
                        ),
                        '/',
                        ''
                    )
                END
            )
            WHERE lot IS NULL OR TRIM(lot) = ''
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS raw_material_checksheet_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_material_id INTEGER NOT NULL,
                use_date TEXT NOT NULL,
                note TEXT,
                created_by TEXT,
                updated_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(raw_material_id, use_date),
                FOREIGN KEY (raw_material_id) REFERENCES raw_materials(id)
            )
            '''
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_checksheet_notes_raw_date ON raw_material_checksheet_notes(raw_material_id, use_date)")
    except Exception:
        pass
    _raw_material_schema_checked = True


def _ensure_material_lot_schema(conn):
    """Ensure material lot and material lot log tables exist."""
    global _material_lot_schema_checked
    if _material_lot_schema_checked:
        try:
            cols = [row['name'] for row in conn.execute("PRAGMA table_info(material_lots)").fetchall()]
            if 'current_quantity' in cols and 'received_quantity' in cols:
                return
        except Exception:
            pass
    try:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS material_lots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                material_id INTEGER NOT NULL,
                lot TEXT UNIQUE NOT NULL,
                lot_seq INTEGER DEFAULT 1,
                receiving_date TEXT,
                manufacture_date TEXT,
                manufacture_date_unknown INTEGER DEFAULT 0,
                expiry_date TEXT,
                expiry_date_unknown INTEGER DEFAULT 0,
                unit_price REAL DEFAULT 0,
                received_quantity REAL DEFAULT 0,
                current_quantity REAL DEFAULT 0,
                supplier_lot TEXT,
                is_disposed INTEGER DEFAULT 0,
                disposed_at TEXT,
                quantity REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (material_id) REFERENCES materials(id)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS material_lot_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                material_lot_id INTEGER,
                material_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                quantity REAL DEFAULT 0,
                note TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (material_lot_id) REFERENCES material_lots(id),
                FOREIGN KEY (material_id) REFERENCES materials(id)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS logistics_issue_receipt_lots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                material_lot_id INTEGER NOT NULL,
                quantity REAL NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (request_id) REFERENCES logistics_issue_requests(id),
                FOREIGN KEY (material_lot_id) REFERENCES material_lots(id)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS production_material_lot_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                production_id INTEGER NOT NULL,
                production_usage_id INTEGER,
                material_id INTEGER NOT NULL,
                material_lot_id INTEGER NOT NULL,
                quantity REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (production_id) REFERENCES productions(id),
                FOREIGN KEY (production_usage_id) REFERENCES production_material_usage(id),
                FOREIGN KEY (material_id) REFERENCES materials(id),
                FOREIGN KEY (material_lot_id) REFERENCES material_lots(id)
            )
            '''
        )
        conn.execute('CREATE INDEX IF NOT EXISTS idx_material_lots_material_id ON material_lots(material_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_material_lot_logs_material_id ON material_lot_logs(material_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_logistics_issue_receipt_lots_request_id ON logistics_issue_receipt_lots(request_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_pmlu_production_id ON production_material_lot_usage(production_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_pmlu_lot_id ON production_material_lot_usage(material_lot_id)')
        cols = [row['name'] for row in conn.execute("PRAGMA table_info(material_lots)").fetchall()]
        pmlu_cols = [row['name'] for row in conn.execute("PRAGMA table_info(production_material_lot_usage)").fetchall()]
        if 'lot_seq' not in cols:
            conn.execute("ALTER TABLE material_lots ADD COLUMN lot_seq INTEGER DEFAULT 1")
        if 'received_quantity' not in cols:
            conn.execute("ALTER TABLE material_lots ADD COLUMN received_quantity REAL DEFAULT 0")
        if 'current_quantity' not in cols:
            conn.execute("ALTER TABLE material_lots ADD COLUMN current_quantity REAL DEFAULT 0")
        if 'manufacture_date_unknown' not in cols:
            conn.execute("ALTER TABLE material_lots ADD COLUMN manufacture_date_unknown INTEGER DEFAULT 0")
        if 'expiry_date_unknown' not in cols:
            conn.execute("ALTER TABLE material_lots ADD COLUMN expiry_date_unknown INTEGER DEFAULT 0")
        if 'supplier_lot' not in cols:
            conn.execute("ALTER TABLE material_lots ADD COLUMN supplier_lot TEXT")
        if 'is_disposed' not in cols:
            conn.execute("ALTER TABLE material_lots ADD COLUMN is_disposed INTEGER DEFAULT 0")
        if 'disposed_at' not in cols:
            conn.execute("ALTER TABLE material_lots ADD COLUMN disposed_at TEXT")
        if 'location_id' not in pmlu_cols:
            conn.execute("ALTER TABLE production_material_lot_usage ADD COLUMN location_id INTEGER")
        conn.execute("UPDATE material_lots SET received_quantity = COALESCE(received_quantity, quantity, 0) WHERE received_quantity IS NULL OR received_quantity = 0")
        conn.execute("UPDATE material_lots SET current_quantity = COALESCE(current_quantity, quantity, 0) WHERE current_quantity IS NULL")
        conn.execute("UPDATE material_lots SET quantity = COALESCE(current_quantity, quantity, 0)")
    except Exception:
        pass
    _material_lot_schema_checked = True


def _cleanup_old_logs(conn):
    """Delete log records older than 2 years."""
    global _log_retention_checked
    if _log_retention_checked:
        return
    try:
        conn.execute("DELETE FROM production_material_lot_usage WHERE created_at < datetime('now', '-2 years')")
        conn.execute("DELETE FROM production_material_usage WHERE created_at < datetime('now', '-2 years')")
        conn.execute("DELETE FROM material_lot_logs WHERE created_at < datetime('now', '-2 years')")
        conn.execute("DELETE FROM raw_material_logs WHERE created_at < datetime('now', '-2 years')")
        conn.execute("DELETE FROM production_logs WHERE created_at < datetime('now', '-2 years')")
        conn.execute("DELETE FROM material_history WHERE created_at < datetime('now', '-2 years')")
        conn.execute("DELETE FROM audit_logs WHERE created_at < datetime('now', '-2 years')")
        conn.execute(
            '''
            DELETE FROM production_material_lot_usage
            WHERE production_usage_id IS NOT NULL
              AND production_usage_id NOT IN (SELECT id FROM production_material_usage)
            '''
        )
    except Exception:
        pass
    _log_retention_checked = True


def audit_log(conn, action, entity, entity_id=None, data=None):
    """Write an audit log entry within the current transaction."""
    try:
        user = session.get('user', {}) if session else {}
        username = user.get('username')
        name = user.get('name')
        workplace = session.get('workplace') if session else None
        ip = request.remote_addr if request else None
        payload = json.dumps(data, ensure_ascii=False) if data is not None else None
        conn.execute(
            '''
            INSERT INTO audit_logs (action, entity, entity_id, data, username, name, workplace, ip, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                action,
                entity,
                entity_id,
                payload,
                username,
                name,
                workplace,
                ip,
                now_local().strftime('%Y-%m-%d %H:%M:%S'),
            ),
        )
    except Exception:
        # 濡쒓퉭 ?ㅽ뙣???낅Т ?먮쫫??留됱? ?딆쓬
        pass


# ?묒뾽???ы띁 ?⑥닔

def get_workplace():
    """Return the currently selected workplace from the session."""
    workplace = normalize_workplace_name(session.get('workplace', '1??議곕?'))
    if workplace and session.get('workplace') != workplace:
        session['workplace'] = workplace
    return workplace


def require_workplace(f):
    """Persist the selected workplace in the session."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'workplace' not in session:
            return redirect(url_for('main.select_workplace'))
        return f(*args, **kwargs)
    return decorated_function


def rows_to_dict(rows):
    """Convert sqlite3.Row iterables into plain dict lists."""
    if not rows:
        return []
    return [dict(row) for row in rows]


# 濡쒓렇??沅뚰븳 ?곗퐫?덉씠??

def has_role(*roles):
    """Check whether the current user has one of the given roles."""
    user = session.get('user', {})
    if not user:
        return False
    role = get_effective_user_role(user)
    if role == 'admin':
        return True
    return role in roles


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('auth.login'))
        user = session.get('user') or {}
        if not isinstance(user, dict):
            session.pop('user', None)
            session.pop('workplace', None)
            flash('濡쒓렇???뺣낫媛 留뚮즺?섏뼱 ?ㅼ떆 濡쒓렇?명빐 二쇱꽭??', 'warning')
            return redirect(url_for('auth.login'))
        user_workplaces = user.get('workplaces') or []
        if not user_workplaces:
            legacy_workplaces = []
            workplace1 = (user.get('workplace1') or '').strip() if isinstance(user, dict) else ''
            workplace2 = (user.get('workplace2') or '').strip() if isinstance(user, dict) else ''
            if workplace1:
                legacy_workplaces.append(workplace1)
            if workplace2 and workplace2 not in legacy_workplaces:
                legacy_workplaces.append(workplace2)
            if legacy_workplaces:
                user_workplaces = legacy_workplaces
                if isinstance(user, dict):
                    user = dict(user)
                    user['workplaces'] = legacy_workplaces
                    session['user'] = user
        effective_role = get_effective_user_role(user)
        if (user.get('role') or '').strip() != effective_role:
            user = dict(user)
            user['role'] = effective_role
            session['user'] = user
        if (
            len(user_workplaces) > 1
            and not session.get('workplace')
            and request.endpoint not in {'main.select_workplace', 'main.set_workplace', 'auth.logout'}
        ):
            flash('\uc791\uc5c5\uc7a5\uc744 \uba3c\uc800 \uc120\ud0dd\ud574\uc8fc\uc138\uc694. \uc791\uc5c5\uc7a5 \uc120\ud0dd \ud6c4 \uc815\uc0c1\uc801\uc73c\ub85c \uc0ac\uc6a9\ud560 \uc218 \uc788\uc2b5\ub2c8\ub2e4.', 'warning')
            return redirect(url_for('main.select_workplace'))
        return f(*args, **kwargs)
    return decorated_function


# 愿由ъ옄 沅뚰븳 ?꾩슂 ?곗퐫?덉씠??

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('auth.login'))
        if not session['user'].get('is_admin'):
            return redirect(url_for('main.index'))
        return f(*args, **kwargs)
    return decorated_function


def role_required(*roles):
    """Role-based access decorator.

    Admin users are always allowed. Other users must match one of `roles`.
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user' not in session:
                return redirect(url_for('auth.login'))
            user_role = get_effective_user_role(session['user'])
            # admin? 紐⑤뱺 沅뚰븳 ?듦낵
            if user_role == 'admin':
                return f(*args, **kwargs)
            # ?덉슜??role?대㈃ ?듦낵
            if user_role in roles:
                return f(*args, **kwargs)
            # 沅뚰븳 ?놁쓬 ????쒕낫?쒕줈
            return redirect(url_for('main.index'))
        return decorated_function
    return decorator


