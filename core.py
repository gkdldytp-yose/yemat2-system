from flask import session, redirect, url_for, request, flash
import sqlite3
from functools import wraps
import json
from pathlib import Path

# 공통 작업장 목록 (물류 작업장은 일반 선택 목록에서 제외)
WORKPLACES = ['\u0031\ub3d9 \uc870\ubbf8', '\u0031\ub3d9 \uc790\ubc18', '\u0032\ub3d9 \uc2e0\uad00 \u0031\uce35', '\u0032\ub3d9 \uc2e0\uad00 \u0032\uce35', '\uae30\ud0c0']
LOGISTICS_WORKPLACE = '\ubb3c\ub958'
SHARED_WORKPLACE = '공통'
SHARED_MATERIAL_CATEGORIES = {'기름', '소금', '실리카', '트레이'}

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

# 데이터베이스 연결

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / 'yemat.db'

def get_db():
    """데이터베이스 연결 - WAL 모드 + 긴 타임아웃"""
    conn = sqlite3.connect(
        str(DB_PATH),
        timeout=60.0,
        isolation_level=None,  # autocommit 모드
        check_same_thread=False
    )
    conn.row_factory = sqlite3.Row

    # WAL 모드 및 최적화 (매번 설정)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=60000')
    conn.execute('PRAGMA cache_size=-64000')

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
    _cleanup_old_logs(conn)
    return conn


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
                status TEXT NOT NULL DEFAULT '요청',
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
        # 기존 DB 마이그레이션
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
    """users 테이블에 필요한 컬럼이 없으면 추가"""
    global _user_schema_checked
    if _user_schema_checked:
        return
    try:
        cols = [row['name'] for row in conn.execute("PRAGMA table_info(users)").fetchall()]
        if 'role' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN role TEXT")
        if 'workplaces' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN workplaces TEXT")
        if 'status' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'approved'")

        # 기존 사용자 기본값 보정
        conn.execute("UPDATE users SET status='approved' WHERE status IS NULL")
        # workplaces 비어 있으면 workplace1/2 기준으로 채움
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
        # 그래도 비면 기본 작업장
        conn.execute("UPDATE users SET workplaces='1동 조미' WHERE workplaces IS NULL OR workplaces = ''")
    except Exception:
        pass
    _user_schema_checked = True


def _ensure_shared_materials(conn):
    """공통 부자재 카테고리를 공통(workplace)으로 정규화"""
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
    except Exception:
        pass
    _materials_schema_checked = True


def _ensure_purchase_schema(conn):
    """purchase_requests 테이블에 사용자 추적 컬럼 추가"""
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
    """감사 로그 테이블 생성"""
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
    """productions 테이블 인원관리 컬럼 보정"""
    global _production_schema_checked
    if _production_schema_checked:
        return
    try:
        cols = [row['name'] for row in conn.execute("PRAGMA table_info(productions)").fetchall()]
        if 'supply_line' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN supply_line TEXT")
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
        if 'raw_sok_mode' not in cols:
            conn.execute("ALTER TABLE productions ADD COLUMN raw_sok_mode INTEGER DEFAULT 1")
        conn.execute("UPDATE productions SET raw_sok_mode = 1 WHERE raw_sok_mode IS NULL OR raw_sok_mode < 1")
        usage_cols = [row['name'] for row in conn.execute("PRAGMA table_info(production_material_usage)").fetchall()]
        if 'usage_note' not in usage_cols:
            conn.execute("ALTER TABLE production_material_usage ADD COLUMN usage_note TEXT")
    except Exception:
        pass
    _production_schema_checked = True


def _ensure_products_schema(conn):
    """products 테이블 상품 보조 컬럼 보정"""
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
        conn.execute("UPDATE products SET expiry_months = 12 WHERE expiry_months IS NULL")
    except Exception:
        pass
    _products_schema_checked = True


def _ensure_raw_material_schema(conn):
    """raw_materials 테이블에 코드/로트 컬럼을 보정한다."""
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
    """부자재 로트/로트 로그 테이블 생성"""
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
    """감사 로그 기록 (동일 트랜잭션 내에서 사용)"""
    try:
        user = session.get('user', {}) if session else {}
        username = user.get('username')
        name = user.get('name')
        workplace = session.get('workplace') if session else None
        ip = request.remote_addr if request else None
        payload = json.dumps(data, ensure_ascii=False) if data is not None else None
        conn.execute(
            '''
            INSERT INTO audit_logs (action, entity, entity_id, data, username, name, workplace, ip)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (action, entity, entity_id, payload, username, name, workplace, ip),
        )
    except Exception:
        # 로깅 실패는 업무 흐름을 막지 않음
        pass


# 작업장 헬퍼 함수

def get_workplace():
    """현재 세션의 작업장 반환"""
    return session.get('workplace', '1동 조미')


def require_workplace(f):
    """작업장 선택을 요구하는 데코레이터"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'workplace' not in session:
            return redirect(url_for('main.select_workplace'))
        return f(*args, **kwargs)
    return decorated_function


def rows_to_dict(rows):
    """sqlite3.Row 객체 리스트를 딕셔너리 리스트로 변환"""
    if not rows:
        return []
    return [dict(row) for row in rows]


# 로그인/권한 데코레이터

def has_role(*roles):
    """현재 사용자 role 체크 (헬퍼 함수)"""
    user = session.get('user', {})
    if not user:
        return False
    role = user.get('role', 'readonly')
    if role == 'admin':
        return True
    return role in roles


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('auth.login'))
        user = session.get('user') or {}
        user_workplaces = user.get('workplaces') or []
        role = (user.get('role') or '').strip()
        if (
            len(user_workplaces) > 1
            and not session.get('workplace')
            and request.endpoint not in {'main.select_workplace', 'main.set_workplace', 'auth.logout'}
        ):
            flash('\uc791\uc5c5\uc7a5\uc744 \uba3c\uc800 \uc120\ud0dd\ud574\uc8fc\uc138\uc694. \uc791\uc5c5\uc7a5 \uc120\ud0dd \ud6c4 \uc815\uc0c1\uc801\uc73c\ub85c \uc0ac\uc6a9\ud560 \uc218 \uc788\uc2b5\ub2c8\ub2e4.', 'warning')
            return redirect(url_for('main.select_workplace'))
        return f(*args, **kwargs)
    return decorated_function


# 관리자 권한 필요 데코레이터

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
    """역할 기반 접근 제어 데코레이터.
    admin은 항상 통과. roles에 해당 role이 있으면 통과.
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user' not in session:
                return redirect(url_for('auth.login'))
            user_role = session['user'].get('role', 'readonly')
            # admin은 모든 권한 통과
            if user_role == 'admin':
                return f(*args, **kwargs)
            # 허용된 role이면 통과
            if user_role in roles:
                return f(*args, **kwargs)
            # 권한 없음 → 대시보드로
            return redirect(url_for('main.index'))
        return decorated_function
    return decorator
