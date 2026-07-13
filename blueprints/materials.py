from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify, abort
import sqlite3
from datetime import datetime
import json

from core import (
    add_user_notification,
    begin_db_transaction,
    close_db,
    commit_db,
    db_connection,
    db_transaction,
    get_db,
    get_usernames_for_notification,
    get_workplace,
    login_required,
    role_required,
    rollback_db,
    WORKPLACES,
    LOGISTICS_WORKPLACE,
    SHARED_WORKPLACE,
    SHARED_MATERIAL_CATEGORIES,
    audit_log,
    now_local,
    today_local,
    today_local_str,
)

bp = Blueprint('materials', __name__)


def _local_timestamp_str():
    return now_local().strftime('%Y-%m-%d %H:%M:%S')


def _normalize_production_status(status_value):
    s = (status_value or '').strip()
    done = '완료'
    planned = '예정'
    in_progress = '진행중'
    status_hex = s.encode('utf-8', errors='ignore').hex().upper() if s else ''
    if not s:
        return planned
    if s == done or status_hex in {'EC9984EBA38C', '3FEABEA8ECA6BA'} or s == '?꾨즺' or '꾨즺' in s:
        return done
    if s == in_progress:
        return in_progress
    if s == planned or status_hex == 'EC9888ECA095' or s == '계획' or s == '?덈젙' or '덈젙' in s:
        return planned
    return s


def _parse_manual_processed_at(raw_value):
    value = (raw_value or '').strip()
    if not value:
        return _local_timestamp_str()
    for fmt in ('%Y-%m-%dT%H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            parsed = datetime.strptime(value, fmt)
            if fmt == '%Y-%m-%d':
                return f"{parsed.strftime('%Y-%m-%d')} 00:00:00"
            return parsed.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
    raise ValueError('실제 처리 일시 형식이 올바르지 않습니다.')


def _parse_manual_processed_from_form(form):
    processed_date = (form.get('processed_date') or '').strip()
    processed_time = (form.get('processed_time') or '').strip()
    combined = (form.get('processed_at') or '').strip()

    if processed_date:
        if processed_time:
            return _parse_manual_processed_at(f'{processed_date} {processed_time}')
        return f'{processed_date} {now_local().strftime("%H:%M:%S")}'

    return _parse_manual_processed_at(combined)

PURCHASE_STATUS_NEEDED = '\ubc1c\uc8fc\ud544\uc694'
PURCHASE_STATUS_ORDERED = '\ubc1c\uc8fc\uc911'
PURCHASE_STATUS_RECEIVED = '\uc785\uace0\uc644\ub8cc'
ISSUE_STATUS_REQUESTED = '\uc694\uccad'
ISSUE_STATUS_COMPLETED = '\uc644\ub8cc'
ISSUE_STATUS_REJECTED = '\ubc18\ub824'

WORKPLACE_SORT_ORDER = {
    '1동 조미': 1,
    '2동 신관 1층': 2,
    '2동 신관 2층': 3,
    '1동 자반': 4,
    '공통': 5,
    '물류': 6,
}

CATEGORY_SORT_ORDER = {
    '내포': 1,
    '외포': 2,
    '박스': 3,
    '기름': 4,
    '소금': 5,
    '실리카': 6,
    '트레이': 7,
}


def _is_logistics_manager():
    user = session.get('user') or {}
    role = user.get('role', 'readonly')
    workplace = (session.get('workplace') or '').strip()
    return bool(user.get('is_admin')) or role != 'readonly' or workplace == LOGISTICS_WORKPLACE


def _can_manage_material_lots():
    user = session.get('user') or {}
    return bool(user.get('is_admin'))


def _can_manage_material_master():
    user = session.get('user') or {}
    role = user.get('role', 'readonly')
    return bool(user.get('is_admin')) or role != 'readonly'


def _alert_back(message):
    safe_message = json.dumps(str(message or ''), ensure_ascii=False)
    return f"<meta charset='utf-8'><script>alert({safe_message}); history.back();</script>"


def _alert_redirect(message, target_url):
    safe_message = json.dumps(str(message or ''), ensure_ascii=False)
    safe_target = json.dumps(str(target_url or ''), ensure_ascii=False)
    return f"<meta charset='utf-8'><script>alert({safe_message}); location.href={safe_target};</script>"


def _notify_users(conn, usernames, title, body='', link=None):
    seen = set()
    for username in usernames or []:
        key = (username or '').strip()
        if not key or key in seen:
            continue
        seen.add(key)
        add_user_notification(conn, key, title, body, link)


def _group_request_rows_by_date(rows, date_field):
    grouped = []
    bucket = {}
    for row in rows or []:
        item = dict(row)
        raw_date = str(item.get(date_field) or item.get('processed_at') or item.get('requested_at') or '')[:10]
        if not raw_date:
            raw_date = '날짜 미상'
        if raw_date not in bucket:
            bucket[raw_date] = {'date': raw_date, 'items': []}
            grouped.append(bucket[raw_date])
        bucket[raw_date]['items'].append(item)
    return grouped


def _format_issue_receipt_update_note(workplace, before_qty, after_qty):
    workplace_text = (workplace or '').strip()
    before_value = _round_to_1_decimal(before_qty or 0)
    after_value = _round_to_1_decimal(after_qty or 0)
    if abs(before_value - after_value) <= 0.01:
        return f'{workplace_text} 실입고 수정'
    return f'{workplace_text} 실입고 수정 {before_value:.1f} > {after_value:.1f}'


def _format_receipt_row_date_value(row, field_name, unknown_field=None):
    row = row or {}
    if unknown_field and int(row.get(unknown_field) or 0) == 1:
        return '-'
    return (row.get(field_name) or '').strip() or '-'


def _build_issue_receipt_update_note(workplace, before_qty, after_qty, before_row=None, after_row=None):
    workplace_text = (workplace or '').strip() or '실입고'
    before_value = _round_to_1_decimal(before_qty or 0)
    after_value = _round_to_1_decimal(after_qty or 0)
    changes = []
    if abs(before_value - after_value) > 0.01:
        changes.append(f'입고수량 {before_value:.1f} > {after_value:.1f}')

    field_specs = [
        ('receiving_date', '입고일', None),
        ('manufacture_date', '제조일', 'manufacture_date_unknown'),
        ('expiry_date', '소비기한', 'expiry_date_unknown'),
    ]
    for field_name, label, unknown_field in field_specs:
        before_text = _format_receipt_row_date_value(before_row, field_name, unknown_field)
        after_text = _format_receipt_row_date_value(after_row, field_name, unknown_field)
        if before_text != after_text:
            changes.append(f'{label} {before_text} > {after_text}')

    if not changes:
        return f'{workplace_text} 실입고 수정'
    return f"{workplace_text} 실입고 수정 / {' / '.join(changes)}"


def _ledger_workplaces():
    return list(WORKPLACES)


def _normalize_ledger_code(code_value, fallback_prefix, fallback_id):
    code = (code_value or '').strip()
    if code:
        return code
    try:
        iid = int(fallback_id or 0)
    except Exception:
        iid = 0
    return f"{fallback_prefix}{iid:05d}" if iid > 0 else '-'


def _normalize_date_token(value):
    raw = (value or '').strip()
    if not raw:
        return '00000000'
    return raw.replace('-', '')


def _round_to_1_decimal(value):
    return round(float(value or 0) + 1e-9, 1)


def _normalize_stock_quantity(value, digits=6):
    qty = round(float(value or 0), digits)
    if abs(qty) < 1e-6:
        return 0.0
    return qty


def _has_enough_quantity(available, required):
    return _round_to_1_decimal(available) + 1e-9 >= _round_to_1_decimal(required)


def _normalize_material_unit(unit):
    raw = (unit or '').strip()
    if not raw:
        return raw
    key = raw.lower().replace(' ', '')
    aliases = {
        '1kg': 'kg',
        '1g': 'g',
        '1l': 'L',
        '1ml': 'ml',
        'ea': 'EA',
        '개': 'EA',
    }
    return aliases.get(key, raw)


def _material_workplace_sort_key(value):
    name = (value or '').strip()
    return (WORKPLACE_SORT_ORDER.get(name, 99), name)


def _material_category_sort_key(value):
    name = (value or '').strip()
    return (CATEGORY_SORT_ORDER.get(name, 99), name)


def _material_row_sort_key(row):
    if not row:
        return (99, 99, '', '')
    workplace = row.get('workplace') if isinstance(row, dict) else ''
    category = row.get('category') if isinstance(row, dict) else ''
    code = (row.get('code') or '').strip() if isinstance(row, dict) else ''
    name = (row.get('name') or '').strip() if isinstance(row, dict) else ''
    return (
        _material_workplace_sort_key(workplace)[0],
        _material_category_sort_key(category)[0],
        code,
        name,
    )


def _pool_code_from_row(row):
    if not row:
        return 'UNKNOWN'
    if isinstance(row, dict):
        code = (row.get('code') or '').strip()
        mid = int(row.get('id') or 0)
    else:
        keys = set(row.keys()) if hasattr(row, 'keys') else set()
        code = (row['code'] or '').strip() if 'code' in keys else ''
        mid = int(row['id'] or 0) if 'id' in keys else 0
    if code:
        return code
    return f"M{mid:05d}" if mid else 'UNKNOWN'


def _increase_logistics_stock(cursor, material_code, material_name, unit, qty_delta, updated_by=None):
    qty = float(qty_delta or 0)
    if qty <= 0:
        return
    cursor.execute('SELECT material_code, quantity FROM logistics_stocks WHERE material_code = ?', (material_code,))
    row = cursor.fetchone()
    if row:
        cursor.execute(
            '''
            UPDATE logistics_stocks
            SET quantity = COALESCE(quantity, 0) + ?, material_name = ?, unit = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
            WHERE material_code = ?
            ''',
            (qty, material_name, unit, updated_by, material_code),
        )
    else:
        cursor.execute(
            '''
            INSERT INTO logistics_stocks (material_code, material_name, unit, quantity, updated_by)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (material_code, material_name, unit, qty, updated_by),
        )


def _get_inventory_location_id(cursor, name):
    target_name = (name or '').strip()
    if target_name in {LOGISTICS_WORKPLACE, '물류창고'}:
        row = cursor.execute(
            '''
            SELECT id
            FROM inv_locations
            WHERE name = '물류창고'
               OR (loc_type = 'WAREHOUSE' AND COALESCE(workplace_code, '') = 'WH')
               OR COALESCE(workplace_code, '') = ?
            ORDER BY CASE WHEN name = '물류창고' THEN 0
                          WHEN COALESCE(workplace_code, '') = 'WH' THEN 1
                          WHEN COALESCE(workplace_code, '') = ? THEN 2
                          ELSE 3 END,
                     id
            LIMIT 1
            ''',
            (LOGISTICS_WORKPLACE, LOGISTICS_WORKPLACE),
        ).fetchone()
        return int(row['id']) if row else None
    row = cursor.execute(
        '''
        SELECT id
        FROM inv_locations
        WHERE name = ?
           OR COALESCE(workplace_code, '') = ?
        ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, id
        LIMIT 1
        ''',
        (name, name, name),
    ).fetchone()
    return int(row['id']) if row else None


def _unused_broken_get_inventory_location_ids(cursor, name):
    target_name = (name or '').strip()
    if not target_name:
        return []
    if target_name in {LOGISTICS_WORKPLACE, '臾쇰쪟李쎄퀬'}:
        rows = cursor.execute(
            '''
            SELECT id
            FROM inv_locations
            WHERE name = '臾쇰쪟李쎄퀬'
               OR (loc_type = 'WAREHOUSE' AND COALESCE(workplace_code, '') = 'WH')
               OR COALESCE(workplace_code, '') = ?
            ORDER BY CASE WHEN name = '臾쇰쪟李쎄퀬' THEN 0
                          WHEN COALESCE(workplace_code, '') = 'WH' THEN 1
                          WHEN COALESCE(workplace_code, '') = ? THEN 2
                          ELSE 3 END,
                     id
            ''',
            (LOGISTICS_WORKPLACE, LOGISTICS_WORKPLACE),
        ).fetchall()
        return [int(row['id']) for row in rows if int(row['id'] or 0) > 0]
    rows = cursor.execute(
        '''
        SELECT id
        FROM inv_locations
        WHERE name = ?
           OR COALESCE(workplace_code, '') = ?
           OR REPLACE(COALESCE(name, ''), ' ', '') = REPLACE(?, ' ', '')
           OR REPLACE(COALESCE(workplace_code, ''), ' ', '') = REPLACE(?, ' ', '')
        ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, id
        ''',
        (target_name, target_name, target_name, target_name, target_name),
    ).fetchall()
    return [int(row['id']) for row in rows if int(row['id'] or 0) > 0]


def _unused_broken_get_workplace_lot_quantity(cursor, workplace, material_id, lot_id):
    location_ids = _get_inventory_location_ids(cursor, workplace)
    if not location_ids:
        return 0.0
    placeholders = ','.join(['?'] * len(location_ids))
    row = cursor.execute(
        f'''
        SELECT COALESCE(SUM(b.qty), 0) AS qty
        FROM inv_material_lot_balances b
        JOIN material_lots ml ON ml.id = b.material_lot_id
        WHERE ml.id = ?
          AND ml.material_id = ?
          AND b.location_id IN ({placeholders})
          AND COALESCE(ml.is_disposed, 0) = 0
          AND COALESCE(b.qty, 0) > 0
        ''',
        [lot_id, material_id, *location_ids],
    ).fetchone()
    return float((row['qty'] if row else 0) or 0)


def _unused_broken_consume_workplace_lot_quantity(cursor, workplace, material_lot_id, quantity):
    qty_needed = float(quantity or 0)
    if qty_needed <= 0:
        return 0.0
    location_ids = _get_inventory_location_ids(cursor, workplace)
    if not location_ids:
        return 0.0
    placeholders = ','.join(['?'] * len(location_ids))
    rows = cursor.execute(
        f'''
        SELECT location_id, COALESCE(qty, 0) AS qty
        FROM inv_material_lot_balances
        WHERE material_lot_id = ?
          AND location_id IN ({placeholders})
          AND COALESCE(qty, 0) > 0
        ORDER BY qty DESC, location_id
        ''',
        [material_lot_id, *location_ids],
    ).fetchall()
    consumed = 0.0
    for row in rows:
        available = float(row['qty'] or 0)
        if available <= 0:
            continue
        take_qty = min(available, qty_needed - consumed)
        if take_qty <= 0:
            break
        cursor.execute(
            '''
            UPDATE inv_material_lot_balances
            SET qty = qty - ?, updated_at = CURRENT_TIMESTAMP
            WHERE material_lot_id = ?
              AND location_id = ?
            ''',
            (take_qty, material_lot_id, int(row['location_id'])),
        )
        cursor.execute(
            '''
            DELETE FROM inv_material_lot_balances
            WHERE material_lot_id = ?
              AND location_id = ?
              AND COALESCE(qty, 0) <= 0
            ''',
            (material_lot_id, int(row['location_id'])),
        )
        consumed += take_qty
        if consumed >= qty_needed:
            break
    return consumed


def _get_material_stock_map_for_location(cursor, material_ids, location_name):
    ids = []
    for raw_id in material_ids or []:
        try:
            material_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if material_id > 0:
            ids.append(material_id)
    if not ids:
        return {}

    target_name = (location_name or '').strip()
    if not target_name:
        return {}
    placeholders = ','.join(['?'] * len(ids))
    if target_name in {LOGISTICS_WORKPLACE, '물류창고'}:
        rows = cursor.execute(
            f'''
            SELECT ml.material_id, COALESCE(SUM(b.qty), 0) AS qty
            FROM inv_material_lot_balances b
            JOIN material_lots ml ON ml.id = b.material_lot_id
            JOIN inv_locations loc ON loc.id = b.location_id
            WHERE (
                    COALESCE(loc.name, '') = '물류창고'
                 OR COALESCE(loc.workplace_code, '') IN ('WH', ?)
                 OR COALESCE(loc.loc_type, '') = 'WAREHOUSE'
            )
              AND ml.material_id IN ({placeholders})
              AND COALESCE(ml.is_disposed, 0) = 0
            GROUP BY ml.material_id
            ''',
            [LOGISTICS_WORKPLACE, *ids],
        ).fetchall()
        return {int(row['material_id']): float(row['qty'] or 0) for row in rows}
    rows = cursor.execute(
        f'''
        SELECT ml.material_id, COALESCE(SUM(b.qty), 0) AS qty
        FROM inv_material_lot_balances b
        JOIN material_lots ml ON ml.id = b.material_lot_id
        JOIN inv_locations loc ON loc.id = b.location_id
        WHERE (
                COALESCE(loc.name, '') = ?
             OR COALESCE(loc.workplace_code, '') = ?
             OR REPLACE(COALESCE(loc.name, ''), ' ', '') = REPLACE(?, ' ', '')
             OR REPLACE(COALESCE(loc.workplace_code, ''), ' ', '') = REPLACE(?, ' ', '')
        )
          AND ml.material_id IN ({placeholders})
          AND COALESCE(ml.is_disposed, 0) = 0
        GROUP BY ml.material_id
        ''',
        [target_name, target_name, target_name, target_name, *ids],
    ).fetchall()
    return {int(row['material_id']): float(row['qty'] or 0) for row in rows}


def _upsert_material_lot_balance(cursor, location_id, material_lot_id, qty):
    qty_value = _normalize_stock_quantity(qty)
    existing = cursor.execute(
        '''
        SELECT id
        FROM inv_material_lot_balances
        WHERE location_id = ? AND material_lot_id = ?
        LIMIT 1
        ''',
        (location_id, material_lot_id),
    ).fetchone()
    if qty_value <= 0:
        if existing:
            cursor.execute(
                '''
                DELETE FROM inv_material_lot_balances
                WHERE location_id = ? AND material_lot_id = ?
                ''',
                (location_id, material_lot_id),
            )
        return
    if existing:
        cursor.execute(
            '''
            UPDATE inv_material_lot_balances
            SET qty = ?, updated_at = CURRENT_TIMESTAMP
            WHERE location_id = ? AND material_lot_id = ?
            ''',
            (qty_value, location_id, material_lot_id),
        )
    else:
        cursor.execute(
            '''
            INSERT INTO inv_material_lot_balances (location_id, material_lot_id, qty, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''',
            (location_id, material_lot_id, qty_value),
        )


def _increase_material_lot_balance(cursor, location_id, material_lot_id, qty):
    qty_value = _normalize_stock_quantity(qty)
    if qty_value <= 0:
        return
    existing = cursor.execute(
        '''
        SELECT id, qty
        FROM inv_material_lot_balances
        WHERE location_id = ? AND material_lot_id = ?
        LIMIT 1
        ''',
        (location_id, material_lot_id),
    ).fetchone()
    if existing:
        current_qty = float(existing['qty'] or 0) if 'qty' in existing.keys() else 0.0
        _upsert_material_lot_balance(cursor, location_id, material_lot_id, current_qty + qty_value)
    else:
        _upsert_material_lot_balance(cursor, location_id, material_lot_id, qty_value)


def _move_lot_balance_between_locations(cursor, material_lot_id, from_location_id, to_location_id, qty):
    qty_value = _normalize_stock_quantity(qty)
    if qty_value <= 0:
        return 0.0

    from_row = cursor.execute(
        '''
        SELECT qty
        FROM inv_material_lot_balances
        WHERE location_id = ? AND material_lot_id = ?
        LIMIT 1
        ''',
        (from_location_id, material_lot_id),
    ).fetchone()
    from_qty = float(from_row['qty'] or 0) if from_row else 0.0
    if from_qty < qty_value:
        raise ValueError('?? ??? ?????.')

    _upsert_material_lot_balance(cursor, from_location_id, material_lot_id, from_qty - qty_value)

    existing_to = cursor.execute(
        '''
        SELECT qty
        FROM inv_material_lot_balances
        WHERE location_id = ? AND material_lot_id = ?
        LIMIT 1
        ''',
        (to_location_id, material_lot_id),
    ).fetchone()
    to_qty = float(existing_to['qty'] or 0) if existing_to else 0.0
    _upsert_material_lot_balance(cursor, to_location_id, material_lot_id, to_qty + qty_value)
    return qty_value


def _transfer_logistics_stock_to_workplace(cursor, material_id, workplace, qty):
    qty_need = float(qty or 0)
    if qty_need <= 0:
        return 0.0

    logistics_location_id = _get_inventory_location_id(cursor, '\ubb3c\ub958\ucc3d\uace0')
    workplace_location_id = _get_inventory_location_id(cursor, workplace)
    if not logistics_location_id or not workplace_location_id:
        raise ValueError('?? ?? ??? ?? ? ????.')

    lots = cursor.execute(
        '''
        SELECT b.material_lot_id, b.qty, ml.receiving_date, ml.id
        FROM inv_material_lot_balances b
        JOIN material_lots ml ON ml.id = b.material_lot_id
        WHERE b.location_id = ?
          AND ml.material_id = ?
          AND b.qty > 0
          AND COALESCE(ml.is_disposed, 0) = 0
        ORDER BY COALESCE(ml.receiving_date, ''), ml.id
        ''',
        (logistics_location_id, material_id),
    ).fetchall()

    available = sum(float(row['qty'] or 0) for row in lots)
    if available < qty_need:
        raise ValueError('?? ?? ??? ?????.')

    moved = 0.0
    remaining = qty_need
    for lot in lots:
        if remaining <= 0:
            break
        lot_qty = float(lot['qty'] or 0)
        move_qty = min(lot_qty, remaining)
        _move_lot_balance_between_locations(
            cursor,
            int(lot['material_lot_id']),
            logistics_location_id,
            workplace_location_id,
            move_qty,
        )
        moved += move_qty
        remaining -= move_qty
    return moved


def _sync_logistics_stock_for_material(cursor, material_id, material_code, material_name, unit, updated_by=None):
    logistics_location_id = _get_inventory_location_id(cursor, '물류창고')
    if not logistics_location_id:
        return 0.0
    row = cursor.execute(
        '''
        SELECT COALESCE(SUM(b.qty), 0) AS qty
        FROM inv_material_lot_balances b
        JOIN material_lots ml ON ml.id = b.material_lot_id
        WHERE b.location_id = ?
          AND ml.material_id = ?
          AND COALESCE(ml.is_disposed, 0) = 0
        ''',
        (logistics_location_id, material_id),
    ).fetchone()
    qty = float(row['qty'] or 0) if row else 0.0
    existing = cursor.execute(
        'SELECT material_code FROM logistics_stocks WHERE material_code = ?',
        (material_code,),
    ).fetchone()
    if existing:
        cursor.execute(
            '''
            UPDATE logistics_stocks
            SET quantity = ?, material_name = ?, unit = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
            WHERE material_code = ?
            ''',
            (qty, material_name, unit, updated_by, material_code),
        )
    else:
        cursor.execute(
            '''
            INSERT INTO logistics_stocks (material_code, material_name, unit, quantity, updated_by)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (material_code, material_name, unit, qty, updated_by),
        )
    return qty


def _sync_missing_logistics_lot_balances(conn):
    cursor = conn.cursor()
    logistics_location_id = _get_inventory_location_id(cursor, '\ubb3c\ub958\ucc3d\uace0')
    if not logistics_location_id:
        return 0

    cursor.execute(
        '''
        SELECT ml.id, COALESCE(ml.current_quantity, ml.quantity, 0) AS qty
        FROM material_lots ml
        WHERE COALESCE(ml.is_disposed, 0) = 0
          AND NOT EXISTS (
                SELECT 1
                FROM inv_material_lot_balances b
                WHERE b.material_lot_id = ml.id
          )
        '''
    )
    rows = cursor.fetchall()
    synced = 0
    for row in rows:
        qty = float(row['qty'] or 0)
        if qty <= 0:
            continue
        _upsert_material_lot_balance(cursor, logistics_location_id, int(row['id']), qty)
        synced += 1
    if synced:
        commit_db(conn)
    return synced


def _increase_logistics_defect_stock(cursor, material_code, material_name, unit, qty_delta, updated_by=None):
    qty = float(qty_delta or 0)
    if qty <= 0:
        return
    cursor.execute('SELECT material_code, quantity FROM logistics_defect_stocks WHERE material_code = ?', (material_code,))
    row = cursor.fetchone()
    if row:
        cursor.execute(
            '''
            UPDATE logistics_defect_stocks
            SET quantity = COALESCE(quantity, 0) + ?, material_name = ?, unit = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
            WHERE material_code = ?
            ''',
            (qty, material_name, unit, updated_by, material_code),
        )
    else:
        cursor.execute(
            '''
            INSERT INTO logistics_defect_stocks (material_code, material_name, unit, quantity, updated_by)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (material_code, material_name, unit, qty, updated_by),
        )


def _cleanup_orphan_material_refs(conn):
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT pr.id
        FROM purchase_requests pr
        LEFT JOIN materials m ON m.id = pr.material_id
        WHERE m.id IS NULL
        '''
    )
    orphan_purchase_ids = [int(row['id']) for row in cursor.fetchall()]
    if orphan_purchase_ids:
        placeholders = ','.join(['?'] * len(orphan_purchase_ids))
        cursor.execute(f'DELETE FROM purchase_requests WHERE id IN ({placeholders})', orphan_purchase_ids)

    cursor.execute(
        '''
        SELECT lir.id
        FROM logistics_issue_requests lir
        LEFT JOIN materials m ON m.id = lir.material_id
        WHERE m.id IS NULL
        '''
    )
    orphan_issue_ids = [int(row['id']) for row in cursor.fetchall()]
    if orphan_issue_ids:
        placeholders = ','.join(['?'] * len(orphan_issue_ids))
        cursor.execute(f'DELETE FROM logistics_issue_requests WHERE id IN ({placeholders})', orphan_issue_ids)

    if orphan_purchase_ids or orphan_issue_ids:
        conn.commit()

    return {
        'purchase_requests': len(orphan_purchase_ids),
        'issue_requests': len(orphan_issue_ids),
    }


def _register_export_request_row(cursor, workplace, req_user, req_username, material_id, lot_id, quantity, reason, reason_detail, note):
    quantity = _round_to_1_decimal(quantity)
    if material_id <= 0 or lot_id <= 0 or quantity <= 0:
        raise ValueError('자재, 로트, 반출 수량을 확인해주세요.')
    if reason not in ('정리', '불량', '기타'):
        raise ValueError('반출 사유를 선택해주세요.')
    if reason in ('불량', '기타') and not reason_detail:
        raise ValueError('불량/기타 사유는 상세 내용을 입력해주세요.')

    cursor.execute('SELECT id, code, name, unit FROM materials WHERE id = ?', (material_id,))
    mat = cursor.fetchone()
    if not mat:
        raise ValueError('자재를 찾을 수 없습니다.')

    workplace_location_ids = _get_inventory_location_ids(cursor, workplace)
    if not workplace_location_ids:
        raise ValueError('작업장 재고 위치를 찾을 수 없습니다.')

    cursor.execute(
        '''
        SELECT
            ml.id,
            ml.material_id,
            ml.lot
        FROM material_lots ml
        WHERE ml.id = ?
          AND ml.material_id = ?
          AND COALESCE(ml.is_disposed, 0) = 0
        ''',
        (lot_id, material_id),
    )
    lot = cursor.fetchone()
    if not lot:
        raise ValueError('선택한 로트를 찾을 수 없습니다.')

    lot_qty = _round_to_1_decimal(_get_workplace_lot_quantity(cursor, workplace, material_id, lot_id))
    workplace_stock_map = _get_material_stock_map_for_location(cursor, [material_id], workplace)
    mat_qty = _round_to_1_decimal(workplace_stock_map.get(int(material_id), 0) or 0)
    if not _has_enough_quantity(lot_qty, quantity):
        raise ValueError('선택한 로트 재고가 부족합니다.')
    if not _has_enough_quantity(mat_qty, quantity):
        raise ValueError('작업장 재고가 부족합니다.')

    pool_code = _pool_code_from_row(mat)
    requested_at = _local_timestamp_str()
    cursor.execute(
        '''
        INSERT INTO logistics_issue_requests
        (material_id, material_code, material_name, unit, requester_workplace, requested_quantity, approved_quantity,
         request_type, reason, reason_detail, material_lot_id, status, note, requested_by, requester_username, requested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'RETURN', ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            material_id,
            pool_code,
            mat['name'],
            mat['unit'],
            workplace,
            quantity,
            0,
            reason,
            reason_detail or None,
            lot_id,
            ISSUE_STATUS_REQUESTED,
            note,
            req_user,
            req_username,
            requested_at,
        ),
    )
    cursor.execute(
        '''
        INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
        VALUES (?, ?, 'export_request_pending', ?, ?)
        ''',
        (lot_id, material_id, quantity, f'{workplace} 반출 요청 ({reason})'),
    )
    return cursor.lastrowid, pool_code, lot['lot']


def _describe_export_request_item(cursor, material_id, lot_id):
    cursor.execute(
        '''
        SELECT m.code, m.name, ml.lot
        FROM materials m
        LEFT JOIN material_lots ml ON ml.id = ?
        WHERE m.id = ?
        ''',
        (lot_id, material_id),
    )
    row = cursor.fetchone()
    if not row:
        return f'자재 ID {material_id}'
    code = (row['code'] or '').strip()
    name = (row['name'] or '').strip() or f'자재 ID {material_id}'
    lot = (row['lot'] or '').strip()
    prefix = f'[{code}] ' if code else ''
    suffix = f' / 로트:{lot}' if lot else ''
    return f'{prefix}{name}{suffix}'


def _describe_export_request_by_id(cursor, req_id):
    cursor.execute(
        '''
        SELECT material_id, material_lot_id
        FROM logistics_issue_requests
        WHERE id = ?
        ''',
        (req_id,),
    )
    row = cursor.fetchone()
    if not row:
        return f'요청 ID {req_id}'
    return _describe_export_request_item(cursor, int(row['material_id'] or 0), int(row['material_lot_id'] or 0))


def _get_inventory_location_ids(cursor, name):
    target_name = (name or '').strip()
    if not target_name:
        return []
    if target_name in {LOGISTICS_WORKPLACE, '臾쇰쪟李쎄퀬'}:
        rows = cursor.execute(
            '''
            SELECT id
            FROM inv_locations
            WHERE name = '臾쇰쪟李쎄퀬'
               OR (loc_type = 'WAREHOUSE' AND COALESCE(workplace_code, '') = 'WH')
               OR COALESCE(workplace_code, '') = ?
            ORDER BY CASE WHEN name = '臾쇰쪟李쎄퀬' THEN 0
                          WHEN COALESCE(workplace_code, '') = 'WH' THEN 1
                          WHEN COALESCE(workplace_code, '') = ? THEN 2
                          ELSE 3 END,
                     id
            ''',
            (LOGISTICS_WORKPLACE, LOGISTICS_WORKPLACE),
        ).fetchall()
        return [int(row['id']) for row in rows if int(row['id'] or 0) > 0]
    rows = cursor.execute(
        '''
        SELECT id
        FROM inv_locations
        WHERE name = ?
           OR COALESCE(workplace_code, '') = ?
           OR REPLACE(COALESCE(name, ''), ' ', '') = REPLACE(?, ' ', '')
           OR REPLACE(COALESCE(workplace_code, ''), ' ', '') = REPLACE(?, ' ', '')
        ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, id
        ''',
        (target_name, target_name, target_name, target_name, target_name),
    ).fetchall()
    return [int(row['id']) for row in rows if int(row['id'] or 0) > 0]


def _get_workplace_lot_quantity(cursor, workplace, material_id, lot_id):
    location_ids = _get_inventory_location_ids(cursor, workplace)
    if not location_ids:
        return 0.0
    placeholders = ','.join(['?'] * len(location_ids))
    row = cursor.execute(
        f'''
        SELECT COALESCE(SUM(b.qty), 0) AS qty
        FROM inv_material_lot_balances b
        JOIN material_lots ml ON ml.id = b.material_lot_id
        WHERE ml.id = ?
          AND ml.material_id = ?
          AND b.location_id IN ({placeholders})
          AND COALESCE(ml.is_disposed, 0) = 0
          AND COALESCE(b.qty, 0) > 0
        ''',
        [lot_id, material_id, *location_ids],
    ).fetchone()
    return float((row['qty'] if row else 0) or 0)


def _consume_workplace_lot_quantity(cursor, workplace, material_lot_id, quantity):
    qty_needed = float(quantity or 0)
    if qty_needed <= 0:
        return 0.0
    location_ids = _get_inventory_location_ids(cursor, workplace)
    if not location_ids:
        return 0.0
    placeholders = ','.join(['?'] * len(location_ids))
    rows = cursor.execute(
        f'''
        SELECT location_id, COALESCE(qty, 0) AS qty
        FROM inv_material_lot_balances
        WHERE material_lot_id = ?
          AND location_id IN ({placeholders})
          AND COALESCE(qty, 0) > 0
        ORDER BY qty DESC, location_id
        ''',
        [material_lot_id, *location_ids],
    ).fetchall()
    consumed = 0.0
    for row in rows:
        available = float(row['qty'] or 0)
        if available <= 0:
            continue
        take_qty = min(available, qty_needed - consumed)
        if take_qty <= 0:
            break
        cursor.execute(
            '''
            UPDATE inv_material_lot_balances
            SET qty = qty - ?, updated_at = CURRENT_TIMESTAMP
            WHERE material_lot_id = ?
              AND location_id = ?
            ''',
            (take_qty, material_lot_id, int(row['location_id'])),
        )
        cursor.execute(
            '''
            DELETE FROM inv_material_lot_balances
            WHERE material_lot_id = ?
              AND location_id = ?
              AND COALESCE(qty, 0) <= 0
            ''',
            (material_lot_id, int(row['location_id'])),
        )
        consumed += take_qty
        if consumed >= qty_needed:
            break
    return consumed


def _unused_broken_register_export_request_row(cursor, workplace, req_user, req_username, material_id, lot_id, quantity, reason, reason_detail, note):
    if material_id <= 0 or lot_id <= 0 or quantity <= 0:
        raise ValueError('?먯옱, 濡쒗듃, 諛섏텧 ?섎웾???뺤씤?댁＜?몄슂.')
    if reason not in ('?뺣━', '遺덈웾', '湲고?'):
        raise ValueError('諛섏텧 ?ъ쑀瑜??좏깮?댁＜?몄슂.')
    if reason in ('遺덈웾', '湲고?') and not reason_detail:
        raise ValueError('遺덈웾/湲고? ?ъ쑀???곸꽭 ?댁슜???낅젰?댁＜?몄슂.')

    cursor.execute('SELECT id, code, name, unit FROM materials WHERE id = ?', (material_id,))
    mat = cursor.fetchone()
    if not mat:
        raise ValueError('?먯옱瑜?李얠쓣 ???놁뒿?덈떎.')

    workplace_location_ids = _get_inventory_location_ids(cursor, workplace)
    if not workplace_location_ids:
        raise ValueError('?묒뾽???ш퀬 ?꾩튂瑜?李얠쓣 ???놁뒿?덈떎.')

    cursor.execute(
        '''
        SELECT ml.id, ml.material_id, ml.lot
        FROM material_lots ml
        WHERE ml.id = ?
          AND ml.material_id = ?
          AND COALESCE(ml.is_disposed, 0) = 0
        ''',
        (lot_id, material_id),
    )
    lot = cursor.fetchone()
    if not lot:
        raise ValueError('?좏깮??濡쒗듃瑜?李얠쓣 ???놁뒿?덈떎.')

    lot_qty = _get_workplace_lot_quantity(cursor, workplace, material_id, lot_id)
    workplace_stock_map = _get_material_stock_map_for_location(cursor, [material_id], workplace)
    mat_qty = float(workplace_stock_map.get(int(material_id), 0) or 0)
    if lot_qty < quantity:
        raise ValueError('?좏깮??濡쒗듃 ?ш퀬媛 遺議깊빀?덈떎.')
    if mat_qty < quantity:
        raise ValueError('?묒뾽???ш퀬媛 遺議깊빀?덈떎.')

    pool_code = _pool_code_from_row(mat)
    cursor.execute(
        '''
        INSERT INTO logistics_issue_requests
        (material_id, material_code, material_name, unit, requester_workplace, requested_quantity, approved_quantity,
         request_type, reason, reason_detail, material_lot_id, status, note, requested_by, requester_username)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'RETURN', ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            material_id,
            pool_code,
            mat['name'],
            mat['unit'],
            workplace,
            quantity,
            0,
            reason,
            reason_detail or None,
            lot_id,
            ISSUE_STATUS_REQUESTED,
            note,
            req_user,
            req_username,
        ),
    )
    cursor.execute(
        '''
        INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note, created_at)
        VALUES (?, ?, 'export_request_pending', ?, ?, ?)
        ''',
        (lot_id, material_id, quantity, f'{workplace} 諛섏텧 ?붿껌 ({reason})', requested_at),
    )
    return cursor.lastrowid, pool_code, lot['lot']


def _build_material_lot(material_code, receiving_date, lot_seq):
    code = (material_code or '').strip() or 'NO_CODE'
    seq = int(lot_seq or 1)
    return f"{code}-{_normalize_date_token(receiving_date)}-{seq:03d}"


def _normalize_ja_ho_token(value):
    raw = (value or '').strip()
    if not raw:
        return 'NO_CAR'
    return raw.replace(' ', '').replace('-', '').replace('/', '')


def _build_raw_material_lot(raw_code, receiving_date, ja_ho):
    code = (raw_code or '').strip() or 'NO_CODE'
    date_token = _normalize_date_token(receiving_date)
    short_date = date_token[-6:] if len(date_token) >= 6 else date_token
    car_token = _normalize_ja_ho_token(ja_ho)
    short_car = 'NC' if car_token == 'NO_CAR' else (car_token[-4:] if len(car_token) > 4 else car_token)
    return f"{code}-{short_date}-{short_car}"


def _ensure_raw_code_and_lot(cursor, raw_material_id, code, receiving_date, ja_ho):
    final_code = (code or '').strip() or f"RM{int(raw_material_id):05d}"
    lot = _build_raw_material_lot(final_code, receiving_date, ja_ho)
    cursor.execute(
        '''
        UPDATE raw_materials
        SET code = ?, lot = ?, ja_ho = ?, car_number = ?
        WHERE id = ?
        ''',
        (final_code, lot, ja_ho, ja_ho, raw_material_id),
    )
    return final_code, lot


def _next_lot_seq(cursor, material_id, receiving_date):
    cursor.execute(
        "SELECT COALESCE(MAX(lot_seq), 0) + 1 AS next_seq FROM material_lots WHERE material_id = ? AND receiving_date = ?",
        (material_id, receiving_date),
    )
    row = cursor.fetchone()
    return int(row['next_seq'] or 1) if row else 1


def _next_unique_material_lot(cursor, material_id, material_code, receiving_date):
    lot_seq = _next_lot_seq(cursor, material_id, receiving_date)
    while True:
        lot = _build_material_lot(material_code, receiving_date, lot_seq)
        cursor.execute('SELECT 1 FROM material_lots WHERE lot = ? LIMIT 1', (lot,))
        if not cursor.fetchone():
            return lot, lot_seq
        lot_seq += 1


def _find_matching_material_lot(
    cursor,
    material_id,
    receiving_date,
    manufacture_date=None,
    expiry_date=None,
    manufacture_date_unknown=0,
    expiry_date_unknown=0,
    supplier_lot='',
):
    cursor.execute(
        '''
        SELECT id, lot, lot_seq
        FROM material_lots
        WHERE material_id = ?
          AND COALESCE(receiving_date, '') = COALESCE(?, '')
          AND COALESCE(manufacture_date, '') = COALESCE(?, '')
          AND COALESCE(expiry_date, '') = COALESCE(?, '')
          AND COALESCE(manufacture_date_unknown, 0) = ?
          AND COALESCE(expiry_date_unknown, 0) = ?
          AND COALESCE(supplier_lot, '') = COALESCE(?, '')
          AND COALESCE(is_disposed, 0) = 0
        ORDER BY id DESC
        LIMIT 1
        ''',
        (
            material_id,
            receiving_date or '',
            manufacture_date or '',
            expiry_date or '',
            int(manufacture_date_unknown or 0),
            int(expiry_date_unknown or 0),
            supplier_lot or '',
        ),
    )
    return cursor.fetchone()


def _sync_material_stock_with_lots(conn, material_id=None):
    cursor = conn.cursor()
    if material_id is not None:
        cursor.execute(
            '''
            UPDATE materials
            SET current_stock = (
                SELECT COALESCE(SUM(COALESCE(b.qty, 0)), 0)
                FROM inv_material_lot_balances b
                JOIN material_lots ml ON ml.id = b.material_lot_id
                JOIN inv_locations loc ON loc.id = b.location_id
                WHERE ml.material_id = materials.id
                  AND COALESCE(ml.is_disposed, 0) = 0
                  AND COALESCE(loc.loc_type, '') = 'WORKPLACE'
            )
            WHERE id = ?
            ''',
            (material_id,),
        )
        return

    cursor.execute(
        '''
        UPDATE materials
        SET current_stock = (
            SELECT COALESCE(SUM(COALESCE(b.qty, 0)), 0)
            FROM inv_material_lot_balances b
            JOIN material_lots ml ON ml.id = b.material_lot_id
            JOIN inv_locations loc ON loc.id = b.location_id
            WHERE ml.material_id = materials.id
              AND COALESCE(ml.is_disposed, 0) = 0
              AND COALESCE(loc.loc_type, '') = 'WORKPLACE'
        )
        '''
    )


def _clear_material_lot_balances(cursor, material_lot_id):
    cursor.execute(
        '''
        DELETE FROM inv_material_lot_balances
        WHERE material_lot_id = ?
        ''',
        (material_lot_id,),
    )


def _create_request_receipt_lot(
    cursor,
    material_id,
    material_code,
    quantity,
    workplace,
    receiving_date=None,
    manufacture_date=None,
    expiry_date=None,
    manufacture_date_unknown=0,
    expiry_date_unknown=0,
    log_action='issue_request_complete',
    log_note=None,
    created_at=None,
):
    created_at = (created_at or '').strip() or _local_timestamp_str()
    receiving_date = (receiving_date or '').strip() or today_local_str()
    manufacture_date = (manufacture_date or '').strip() or None
    expiry_date = (expiry_date or '').strip() or None
    matched_lot = _find_matching_material_lot(
        cursor,
        int(material_id),
        receiving_date,
        manufacture_date=manufacture_date,
        expiry_date=expiry_date,
        manufacture_date_unknown=manufacture_date_unknown,
        expiry_date_unknown=expiry_date_unknown,
        supplier_lot='',
    )
    if matched_lot:
        lot_id = int(matched_lot['id'])
        lot = matched_lot['lot']
        cursor.execute(
            '''
            UPDATE material_lots
            SET received_quantity = COALESCE(received_quantity, 0) + ?,
                current_quantity = COALESCE(current_quantity, 0) + ?,
                quantity = COALESCE(quantity, 0) + ?,
                manufacture_date = COALESCE(manufacture_date, ?),
                expiry_date = COALESCE(expiry_date, ?),
                manufacture_date_unknown = CASE WHEN ? != 0 THEN 1 ELSE COALESCE(manufacture_date_unknown, 0) END,
                expiry_date_unknown = CASE WHEN ? != 0 THEN 1 ELSE COALESCE(expiry_date_unknown, 0) END
            WHERE id = ?
            ''',
            (
                quantity,
                quantity,
                quantity,
                manufacture_date,
                expiry_date,
                int(manufacture_date_unknown or 0),
                int(expiry_date_unknown or 0),
                lot_id,
            ),
        )
    else:
        lot, lot_seq = _next_unique_material_lot(cursor, int(material_id), material_code, receiving_date)
        cursor.execute(
            '''
            INSERT INTO material_lots
            (material_id, lot, lot_seq, receiving_date, manufacture_date, manufacture_date_unknown, expiry_date, expiry_date_unknown, unit_price, received_quantity, current_quantity, supplier_lot, quantity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, '', ?)
            ''',
            (
                material_id,
                lot,
                lot_seq,
                receiving_date,
                manufacture_date,
                int(manufacture_date_unknown or 0),
                expiry_date,
                int(expiry_date_unknown or 0),
                quantity,
                quantity,
                quantity,
            ),
        )
        lot_id = cursor.lastrowid
    location_id = _get_inventory_location_id(cursor, workplace)
    if location_id:
        _increase_material_lot_balance(cursor, location_id, lot_id, quantity)
    cursor.execute(
        '''
        INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (
            lot_id,
            material_id,
            (log_action or 'issue_request_complete'),
            quantity,
            log_note or f'{workplace} 실입고 확인',
            created_at,
        ),
    )
    return lot_id, lot


def _record_issue_receipt_lot(cursor, request_id, material_lot_id, quantity):
    cursor.execute(
        '''
        INSERT INTO logistics_issue_receipt_lots (request_id, material_lot_id, quantity)
        VALUES (?, ?, ?)
        ''',
        (int(request_id), int(material_lot_id), float(quantity or 0)),
    )


def _load_issue_receipt_lots(cursor, request_id):
    rows = cursor.execute(
        '''
        SELECT ril.material_lot_id, ml.material_id, ril.quantity
        FROM logistics_issue_receipt_lots ril
        JOIN material_lots ml ON ml.id = ril.material_lot_id
        WHERE ril.request_id = ?
        ORDER BY ril.id
        ''',
        (int(request_id),),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_issue_receipt_lot_details(cursor, request_id):
    rows = cursor.execute(
        '''
        SELECT
            ril.material_lot_id,
            ml.material_id,
            ril.quantity,
            COALESCE(ml.lot, '') AS lot,
            COALESCE(ml.receiving_date, '') AS receiving_date,
            COALESCE(ml.manufacture_date, '') AS manufacture_date,
            COALESCE(ml.expiry_date, '') AS expiry_date,
            COALESCE(ml.manufacture_date_unknown, 0) AS manufacture_date_unknown,
            COALESCE(ml.expiry_date_unknown, 0) AS expiry_date_unknown,
            COALESCE(ml.is_disposed, 0) AS is_disposed
        FROM logistics_issue_receipt_lots ril
        JOIN material_lots ml ON ml.id = ril.material_lot_id
        WHERE ril.request_id = ?
        ORDER BY ril.id
        ''',
        (int(request_id),),
    ).fetchall()
    return [dict(row) for row in rows]


def _receipt_rows_include_disposed(rows):
    for row in rows or []:
        if int(row.get('is_disposed') or 0) == 1:
            return True
    return False


def _recalculate_material_lot_quantity_from_balances(cursor, material_lot_id):
    total_row = cursor.execute(
        '''
        SELECT COALESCE(SUM(qty), 0) AS total_qty
        FROM inv_material_lot_balances
        WHERE material_lot_id = ?
        ''',
        (int(material_lot_id),),
    ).fetchone()
    total_qty = _round_to_1_decimal((total_row['total_qty'] if total_row else 0) or 0)
    cursor.execute(
        '''
        UPDATE material_lots
        SET current_quantity = ?, quantity = ?
        WHERE id = ?
        ''',
        (total_qty, total_qty, int(material_lot_id)),
    )
    return total_qty


def _rebuild_material_fifo_for_workplace(conn, cursor, material_id, workplace, date_from='', reconsume=True):
    material_id = int(material_id or 0)
    workplace = (workplace or '').strip()
    date_from = (date_from or '').strip()
    if material_id <= 0 or not workplace:
        return

    from blueprints.production import _consume_material_fifo

    target_rows = cursor.execute(
        '''
        SELECT
            pmlu.id,
            pmlu.production_id,
            pmlu.production_usage_id,
            pmlu.material_lot_id,
            pmlu.quantity,
            pmlu.location_id,
            pr.production_date
        FROM production_material_lot_usage pmlu
        JOIN productions pr ON pr.id = pmlu.production_id
        LEFT JOIN material_lots ml ON ml.id = pmlu.material_lot_id
        WHERE pmlu.material_id = ?
          AND COALESCE(pr.workplace, '') = ?
          AND COALESCE(pr.status, '') = '완료'
          AND (? = '' OR COALESCE(pr.production_date, '') >= ?)
        ORDER BY pr.production_date DESC, pmlu.production_id DESC, pmlu.id DESC
        ''',
        (material_id, workplace, date_from, date_from),
    ).fetchall()
    if not target_rows and not reconsume:
        _sync_material_stock_with_lots(conn, material_id)
        return

    for row in target_rows:
        lot_row = cursor.execute(
            'SELECT COALESCE(is_disposed, 0) AS is_disposed FROM material_lots WHERE id = ?',
            (int(row['material_lot_id'] or 0),),
        ).fetchone()
        if lot_row and int(lot_row['is_disposed'] or 0) == 1:
            raise ValueError('폐기된 로트 사용 이력이 포함되어 있어 자동 FIFO 재계산을 진행할 수 없습니다.')

    for row in target_rows:
        qty = float(row['quantity'] or 0)
        if qty <= 0:
            continue
        location_id = int(row['location_id'] or 0)
        lot_id = int(row['material_lot_id'] or 0)
        if location_id > 0 and lot_id > 0:
            bal = cursor.execute(
                '''
                SELECT COALESCE(qty, 0) AS qty
                FROM inv_material_lot_balances
                WHERE location_id = ? AND material_lot_id = ?
                ''',
                (location_id, lot_id),
            ).fetchone()
            current_bal = float((bal['qty'] if bal else 0) or 0)
            _upsert_material_lot_balance(cursor, location_id, lot_id, current_bal + qty)
            _recalculate_material_lot_quantity_from_balances(cursor, lot_id)
        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'rollback', ?, ?)
            ''',
            (lot_id, material_id, qty, f'issue_receipt_edit:fifo rollback:{row["production_id"]}'),
        )

    target_ids = [int(row['id']) for row in target_rows if int(row['id'] or 0) > 0]
    if target_ids:
        placeholders = ','.join(['?'] * len(target_ids))
        cursor.execute(
            f'DELETE FROM production_material_lot_usage WHERE id IN ({placeholders})',
            target_ids,
        )

    if not reconsume:
        _sync_material_stock_with_lots(conn, material_id)
        return

    usage_rows = cursor.execute(
        '''
        SELECT
            pmu.id,
            pmu.production_id,
            COALESCE(pmu.actual_quantity, 0) AS actual_quantity
        FROM production_material_usage pmu
        JOIN productions pr ON pr.id = pmu.production_id
        WHERE pmu.material_id = ?
          AND COALESCE(pr.workplace, '') = ?
          AND COALESCE(pr.status, '') = '완료'
          AND COALESCE(pmu.actual_quantity, 0) > 0
          AND (? = '' OR COALESCE(pr.production_date, '') >= ?)
        ORDER BY pr.production_date ASC, pmu.production_id ASC, pmu.id ASC
        ''',
        (material_id, workplace, date_from, date_from),
    ).fetchall()
    for usage in usage_rows:
        _consume_material_fifo(
            cursor,
            int(usage['production_id']),
            int(usage['id']),
            material_id,
            float(usage['actual_quantity'] or 0),
            workplace,
        )

    _sync_material_stock_with_lots(conn, material_id)


def _infer_issue_receipt_lots(cursor, req_row):
    processed_at = (req_row['processed_at'] or '').strip()
    workplace = (req_row['requester_workplace'] or '').strip()
    if not processed_at or not workplace:
        return []
    rows = cursor.execute(
        '''
        SELECT material_lot_id, material_id, quantity
        FROM material_lot_logs
        WHERE material_id = ?
          AND action = 'issue_request_complete'
          AND note LIKE ?
          AND created_at BETWEEN datetime(?, '-5 seconds') AND datetime(?, '+5 seconds')
        ORDER BY id
        ''',
        (
            int(req_row['material_id']),
            f'{workplace}%',
            processed_at[:19],
            processed_at[:19],
        ),
    ).fetchall()
    result = [dict(row) for row in rows if int(row['material_lot_id'] or 0) > 0]
    target_qty = _round_to_1_decimal(req_row['approved_quantity'] or req_row['requested_quantity'] or 0)
    total_qty = _round_to_1_decimal(sum(float(row.get('quantity') or 0) for row in result))
    if result and abs(total_qty - target_qty) <= 0.11:
        return result
    return []


def _build_receipt_split_rows(form, total_qty):
    split_enabled = (form.get('split_enabled') or '').strip() == '1'
    if not split_enabled:
        return [
            {
                'quantity': float(total_qty or 0),
                'receiving_date': (form.get('receiving_date') or '').strip(),
                'manufacture_date': (form.get('manufacture_date') or '').strip(),
                'expiry_date': (form.get('expiry_date') or '').strip(),
                'manufacture_date_unknown': 1 if (form.get('manufacture_date_unknown') or '').strip() == '1' else 0,
                'expiry_date_unknown': 1 if (form.get('expiry_date_unknown') or '').strip() == '1' else 0,
            }
        ]

    qty_list = form.getlist('split_quantity[]')
    receiving_list = form.getlist('split_receiving_date[]')
    manufacture_list = form.getlist('split_manufacture_date[]')
    expiry_list = form.getlist('split_expiry_date[]')
    manufacture_unknown_list = form.getlist('split_manufacture_unknown[]')
    expiry_unknown_list = form.getlist('split_expiry_unknown[]')
    row_count = max(
        len(qty_list),
        len(receiving_list),
        len(manufacture_list),
        len(expiry_list),
        len(manufacture_unknown_list),
        len(expiry_unknown_list),
    )
    rows = []
    for idx in range(row_count):
        qty = float((qty_list[idx] if idx < len(qty_list) and qty_list[idx] else 0) or 0)
        receiving_date = (receiving_list[idx] if idx < len(receiving_list) else '').strip()
        manufacture_date = (manufacture_list[idx] if idx < len(manufacture_list) else '').strip()
        expiry_date = (expiry_list[idx] if idx < len(expiry_list) else '').strip()
        manufacture_unknown = 1 if (manufacture_unknown_list[idx] if idx < len(manufacture_unknown_list) else '0').strip() == '1' else 0
        expiry_unknown = 1 if (expiry_unknown_list[idx] if idx < len(expiry_unknown_list) else '0').strip() == '1' else 0
        if qty <= 0 and not receiving_date and not manufacture_date and not expiry_date:
            continue
        rows.append(
            {
                'quantity': qty,
                'receiving_date': receiving_date,
                'manufacture_date': manufacture_date,
                'expiry_date': expiry_date,
                'manufacture_date_unknown': manufacture_unknown,
                'expiry_date_unknown': expiry_unknown,
            }
        )
    return rows


def _rollback_issue_receipt_rows(cursor, req_row, receipt_rows, note_suffix):
    for row in receipt_rows:
        lot_qty = _get_workplace_lot_quantity(
            cursor,
            req_row['requester_workplace'],
            int(row['material_id']),
            int(row['material_lot_id']),
        )
        if not _has_enough_quantity(lot_qty, float(row['quantity'] or 0)):
            raise ValueError('이미 사용되었거나 이동된 lot가 있어 수정할 수 없습니다.')

    for row in receipt_rows:
        rollback_qty = float(row['quantity'] or 0)
        consumed_qty = _consume_workplace_lot_quantity(
            cursor,
            req_row['requester_workplace'],
            int(row['material_lot_id']),
            rollback_qty,
        )
        if not _has_enough_quantity(consumed_qty, rollback_qty):
            raise ValueError('재고가 변경되어 완료건을 다시 계산할 수 없습니다. 새로고침 후 다시 시도해주세요.')
        cursor.execute(
            '''
            UPDATE material_lots
            SET received_quantity = MAX(COALESCE(received_quantity, 0) - ?, 0),
                current_quantity = MAX(COALESCE(current_quantity, 0) - ?, 0),
                quantity = MAX(COALESCE(quantity, 0) - ?, 0)
            WHERE id = ?
            ''',
            (rollback_qty, rollback_qty, rollback_qty, int(row['material_lot_id'])),
        )
        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'issue_request_cancel', ?, ?)
            ''',
            (
                int(row['material_lot_id']),
                int(row['material_id']),
                rollback_qty,
                f"{req_row['requester_workplace']} {note_suffix}",
            ),
        )


def _get_latest_workplace_lot_defaults(cursor, material_id, workplace):
    location_id = _get_inventory_location_id(cursor, workplace)
    if location_id:
        row = cursor.execute(
            '''
            SELECT
                ml.receiving_date,
                ml.manufacture_date,
                COALESCE(ml.manufacture_date_unknown, 0) AS manufacture_date_unknown,
                ml.expiry_date,
                COALESCE(ml.expiry_date_unknown, 0) AS expiry_date_unknown
            FROM inv_material_lot_balances b
            JOIN material_lots ml ON ml.id = b.material_lot_id
            WHERE ml.material_id = ?
              AND b.location_id = ?
              AND COALESCE(ml.is_disposed, 0) = 0
            ORDER BY
                CASE WHEN COALESCE(ml.receiving_date, '') = '' THEN 1 ELSE 0 END,
                ml.receiving_date DESC,
                ml.id DESC
            LIMIT 1
            ''',
            (material_id, location_id),
        ).fetchone()
        if row:
            return {
                'receiving_date': row['receiving_date'] or '',
                'manufacture_date': row['manufacture_date'] or '',
                'manufacture_date_unknown': int(row['manufacture_date_unknown'] or 0),
                'expiry_date': row['expiry_date'] or '',
                'expiry_date_unknown': int(row['expiry_date_unknown'] or 0),
            }

    row = cursor.execute(
        '''
        SELECT
            receiving_date,
            manufacture_date,
            COALESCE(manufacture_date_unknown, 0) AS manufacture_date_unknown,
            expiry_date,
            COALESCE(expiry_date_unknown, 0) AS expiry_date_unknown
        FROM material_lots
        WHERE material_id = ?
          AND COALESCE(is_disposed, 0) = 0
        ORDER BY
            CASE WHEN COALESCE(receiving_date, '') = '' THEN 1 ELSE 0 END,
            receiving_date DESC,
            id DESC
        LIMIT 1
        ''',
        (material_id,),
    ).fetchone()
    return {
        'receiving_date': row['receiving_date'] if row else '',
        'manufacture_date': row['manufacture_date'] if row else '',
        'manufacture_date_unknown': int((row['manufacture_date_unknown'] if row else 0) or 0),
        'expiry_date': row['expiry_date'] if row else '',
        'expiry_date_unknown': int((row['expiry_date_unknown'] if row else 0) or 0),
    }


@bp.route('/suppliers')
@login_required
def suppliers():
    """?? ??"""
    with db_connection() as conn:
        cursor = conn.cursor()
        _cleanup_orphan_material_refs(conn)
        _sync_material_stock_with_lots(conn)

        cursor.execute('''
            SELECT s.*, COUNT(m.id) as material_count
            FROM suppliers s
            LEFT JOIN materials m ON s.id = m.supplier_id
            GROUP BY s.id
            ORDER BY s.name
        ''')
        suppliers = cursor.fetchall()

    return render_template('suppliers.html', user=session['user'], suppliers=suppliers)


@bp.route('/suppliers/add', methods=['POST'])
@role_required('purchase')
def add_supplier():
    """?? ??"""
    name = request.form.get('name')
    contact = request.form.get('contact')
    address = request.form.get('address')
    note = request.form.get('note')

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM suppliers')
        count = cursor.fetchone()[0]
        code = f"S{count+1:05d}"

        cursor.execute('''
            INSERT INTO suppliers (code, name, contact, address, note)
            VALUES (?, ?, ?, ?, ?)
        ''', (code, name, contact, address, note))

    return redirect(url_for('materials.materials', req_tab='issue'))


@bp.route('/materials')
@login_required
def materials():
    """??? ?? - ?? ? ???? ?? ?? ??"""
    workplace = get_workplace()
    is_logistics = False
    user = session.get('user') or {}
    is_logistics_role = False
    selected_category = request.args.get('category', '')
    search_keyword = request.args.get('search', '').strip()
    search_keywords = [token.strip() for token in search_keyword.split(',') if token.strip()]
    shortage_ids_raw = (request.args.get('shortage_ids') or '').strip()
    shortage_material_ids = []
    if shortage_ids_raw:
        seen_shortage_ids = set()
        for token in shortage_ids_raw.split(','):
            token = token.strip()
            if not token:
                continue
            try:
                material_id = int(token)
            except Exception:
                continue
            if material_id <= 0 or material_id in seen_shortage_ids:
                continue
            seen_shortage_ids.add(material_id)
            shortage_material_ids.append(material_id)
    selected_product_id = (request.args.get('product_id') or '').strip()
    selected_material_search_field = (request.args.get('material_search_field') or 'all').strip() or 'all'
    selected_material_type = (request.args.get('material_type') or 'all').strip() or 'all'
    selected_show_all_stock = str(request.args.get('show_all_stock') or '').strip().lower() in {'1', 'true', 'on', 'yes'}
    req_tab = (request.args.get('req_tab') or 'issue').strip().lower()
    if req_tab not in ('issue', 'export'):
        req_tab = 'issue'
    issue_status_tab = (request.args.get('issue_status') or 'pending').strip().lower()
    if issue_status_tab not in ('pending', 'completed'):
        issue_status_tab = 'pending'
    export_status_tab = (request.args.get('export_status') or 'pending').strip().lower()
    if export_status_tab not in ('pending', 'completed', 'rejected'):
        export_status_tab = 'pending'

    conn_context = db_connection()
    conn = conn_context.__enter__()
    cursor = conn.cursor()
    _cleanup_orphan_material_refs(conn)

    if is_logistics_role:
        cursor.execute(
            """
            SELECT DISTINCT category
            FROM materials
            WHERE category IS NOT NULL
            ORDER BY category
            """
        )
    else:
        cursor.execute(
            """
            SELECT DISTINCT category
            FROM materials
            WHERE category IS NOT NULL
              AND (workplace = ? OR workplace = ? OR workplace IS NULL)
            ORDER BY category
            """,
            (workplace, SHARED_WORKPLACE),
        )
    categories = [row['category'] for row in cursor.fetchall()]
    categories = sorted(categories, key=_material_category_sort_key)

    if is_logistics_role:
        cursor.execute(
            '''
            SELECT id, name
            FROM products
            ORDER BY name
            '''
        )
    else:
        cursor.execute(
            '''
            SELECT id, name
            FROM products
            WHERE workplace = ?
            ORDER BY name
            ''',
            (workplace,),
        )
    filter_products = [dict(row) for row in cursor.fetchall()]

    query = '''
        SELECT
            m.*,
            s.name as supplier_name,
            COALESCE(SUM(COALESCE(ml.current_quantity, ml.quantity, 0)), 0) as lot_total_quantity,
            COUNT(ml.id) as lot_count
        FROM materials m
        LEFT JOIN suppliers s ON m.supplier_id = s.id
        LEFT JOIN material_lots ml ON ml.material_id = m.id AND COALESCE(ml.is_disposed, 0) = 0
        WHERE 1=1
    '''
    params = []
    if not is_logistics_role:
        if selected_product_id:
            query += '''
                AND (
                    m.workplace = ?
                    OR m.workplace = ?
                    OR m.workplace IS NULL
                    OR EXISTS (
                        SELECT 1
                        FROM bom b_scope
                        WHERE b_scope.product_id = ?
                          AND b_scope.material_id = m.id
                    )
                )
            '''
            params.extend([workplace, SHARED_WORKPLACE, selected_product_id])
        else:
            query += " AND (m.workplace = ? OR m.workplace = ? OR m.workplace IS NULL)"
            params.extend([workplace, SHARED_WORKPLACE])

    if selected_category:
        query += " AND m.category = ?"
        params.append(selected_category)

    if selected_material_type == 'raw_like':
        query += " AND COALESCE(m.category, '') IN ('기름', '소금')"
    elif selected_material_type == 'material_only':
        query += " AND COALESCE(m.category, '') NOT IN ('기름', '소금')"

    if search_keywords:
        for keyword in search_keywords:
            search_pattern = f"%{keyword}%"
            if selected_material_search_field == 'code':
                query += " AND COALESCE(m.code, '') LIKE ?"
                params.append(search_pattern)
            elif selected_material_search_field == 'name':
                query += " AND COALESCE(m.name, '') LIKE ?"
                params.append(search_pattern)
            elif selected_material_search_field == 'supplier':
                query += " AND COALESCE(s.name, '') LIKE ?"
                params.append(search_pattern)
            elif selected_material_search_field == 'category':
                query += " AND COALESCE(m.category, '') LIKE ?"
                params.append(search_pattern)
            elif selected_material_search_field == 'unit':
                query += " AND COALESCE(m.unit, '') LIKE ?"
                params.append(search_pattern)
            else:
                query += " AND (COALESCE(m.name, '') LIKE ? OR COALESCE(m.code, '') LIKE ? OR COALESCE(s.name, '') LIKE ? OR COALESCE(m.category, '') LIKE ?)"
                params.extend([search_pattern, search_pattern, search_pattern, search_pattern])

    if selected_product_id:
        query += '''
            AND EXISTS (
                SELECT 1
                FROM bom b
                WHERE b.product_id = ?
                  AND b.material_id = m.id
            )
        '''
        params.append(selected_product_id)

    if shortage_material_ids:
        placeholders = ','.join(['?'] * len(shortage_material_ids))
        query += f" AND m.id IN ({placeholders})"
        params.extend(shortage_material_ids)

    query += """
        GROUP BY m.id
        ORDER BY
            CASE COALESCE(m.workplace, '')
                WHEN '1동 조미' THEN 1
                WHEN '2동 신관 1층' THEN 2
                WHEN '2동 신관 2층' THEN 3
                WHEN '1동 자반' THEN 4
                WHEN '공통' THEN 5
                WHEN '물류' THEN 6
                ELSE 99
            END,
            CASE COALESCE(m.category, '')
                WHEN '내포' THEN 1
                WHEN '외포' THEN 2
                WHEN '박스' THEN 3
                WHEN '기름' THEN 4
                WHEN '소금' THEN 5
                WHEN '실리카' THEN 6
                WHEN '트레이' THEN 7
                ELSE 99
            END,
            COALESCE(m.code, ''),
            m.name
    """

    cursor.execute(query, params)
    material_rows = cursor.fetchall()
    material_ids = [int(row['id']) for row in material_rows if row['id']]
    workplace_stock_by_id = _get_material_stock_map_for_location(cursor, material_ids, workplace)

    materials = []
    for row in material_rows:
        item = dict(row)
        workplace_stock = _round_to_1_decimal(workplace_stock_by_id.get(int(item.get('id') or 0), 0) or 0)
        if not selected_show_all_stock and workplace_stock <= 0:
            continue
        item['unit'] = _normalize_material_unit(item.get('unit'))
        item['workplace_stock'] = workplace_stock
        item['logistics_stock'] = 0.0
        item['total_stock'] = workplace_stock
        materials.append(item)
    materials.sort(key=_material_row_sort_key)

    cursor.execute('SELECT id, name FROM suppliers ORDER BY name')
    suppliers = cursor.fetchall()

    # 불출 요청용 목록은 작업장과 무관하게 전체 부자재를 대상으로 검색한다.
    cursor.execute(
        '''
        SELECT id, code, name, unit, upper_unit, upper_unit_qty
        FROM materials
        ORDER BY category, name
        '''
    )
    request_materials = [dict(r) for r in cursor.fetchall()]
    request_materials.sort(key=_material_row_sort_key)
    request_material_id_set = {
        int(row.get('id') or 0)
        for row in request_materials
        if int(row.get('id') or 0) > 0
    }

    if is_logistics_role:
        export_request_materials = []
    else:
        cursor.execute(
            '''
            SELECT DISTINCT
                m.id,
                m.code,
                m.name,
                m.unit,
                m.upper_unit,
                m.upper_unit_qty
            FROM inv_material_lot_balances b
            JOIN material_lots ml ON ml.id = b.material_lot_id
            JOIN materials m ON m.id = ml.material_id
            JOIN inv_locations loc ON loc.id = b.location_id
            WHERE COALESCE(ml.is_disposed, 0) = 0
              AND COALESCE(b.qty, 0) > 0
              AND (
                    COALESCE(loc.name, '') = ?
                 OR COALESCE(loc.workplace_code, '') = ?
                 OR REPLACE(COALESCE(loc.name, ''), ' ', '') = REPLACE(?, ' ', '')
                 OR REPLACE(COALESCE(loc.workplace_code, ''), ' ', '') = REPLACE(?, ' ', '')
              )
            ORDER BY m.category, m.name
            ''',
            (workplace, workplace, workplace, workplace),
        )
        export_request_materials = [dict(r) for r in cursor.fetchall()]
        export_request_materials.sort(key=_material_row_sort_key)
    export_material_id_set = {
        int(row.get('id') or 0)
        for row in export_request_materials
        if int(row.get('id') or 0) > 0
    }

    issue_bom_products = []
    export_bom_products = []
    issue_bom_map = {}
    export_bom_map = {}
    if not is_logistics_role:
        cursor.execute(
            '''
            SELECT
                p.id AS product_id,
                p.name AS product_name,
                m.id AS material_id,
                m.code AS material_code,
                m.name AS material_name,
                m.unit,
                m.upper_unit,
                m.upper_unit_qty,
                COALESCE(b.quantity_per_box, 0) AS quantity_per_box
            FROM bom b
            JOIN products p ON p.id = b.product_id
            JOIN materials m ON m.id = b.material_id
            WHERE p.workplace = ?
              AND b.material_id IS NOT NULL
            ORDER BY p.name, m.category, m.name
            ''',
            (workplace,),
        )
        for row in cursor.fetchall():
            product_id = int(row['product_id'] or 0)
            material_id = int(row['material_id'] or 0)
            qty = float(row['quantity_per_box'] or 0)
            if product_id <= 0 or material_id <= 0 or qty <= 0:
                continue
            item = {
                'material_id': material_id,
                'code': (row['material_code'] or '').strip() if row['material_code'] else _pool_code_from_row(row),
                'name': row['material_name'],
                'unit': _normalize_material_unit(row['unit']),
                'upper_unit': row['upper_unit'] or '',
                'upper_unit_qty': float(row['upper_unit_qty'] or 0),
                'quantity_per_box': qty,
            }
            if material_id in request_material_id_set:
                issue_bom_map.setdefault(product_id, []).append(item)
            if material_id in export_material_id_set:
                export_bom_map.setdefault(product_id, []).append(dict(item))
        issue_bom_products = [
            product for product in filter_products
            if issue_bom_map.get(int(product.get('id') or 0))
        ]
        export_bom_products = [
            product for product in filter_products
            if export_bom_map.get(int(product.get('id') or 0))
        ]

    material_lots_by_material = {}
    if not is_logistics_role:
        cursor.execute(
            '''
            SELECT
                ml.id,
                ml.material_id,
                ml.lot,
                ml.receiving_date,
                COALESCE(SUM(b.qty), 0) AS current_quantity
            FROM material_lots ml
            JOIN inv_material_lot_balances b ON b.material_lot_id = ml.id
            JOIN inv_locations loc ON loc.id = b.location_id
            WHERE COALESCE(ml.is_disposed, 0) = 0
              AND COALESCE(b.qty, 0) > 0
              AND (
                    COALESCE(loc.name, '') = ?
                 OR COALESCE(loc.workplace_code, '') = ?
                 OR REPLACE(COALESCE(loc.name, ''), ' ', '') = REPLACE(?, ' ', '')
                 OR REPLACE(COALESCE(loc.workplace_code, ''), ' ', '') = REPLACE(?, ' ', '')
              )
            GROUP BY ml.id, ml.material_id, ml.lot, ml.receiving_date
            ORDER BY ml.receiving_date ASC, ml.id ASC
            ''',
            (workplace, workplace, workplace, workplace),
        )
        for lot in cursor.fetchall():
            material_lots_by_material.setdefault(int(lot['material_id']), []).append(
                {
                    'id': int(lot['id']),
                    'lot': lot['lot'],
                    'receiving_date': lot['receiving_date'],
                    'current_quantity': float(lot['current_quantity'] or 0),
                }
            )

    if is_logistics_role:
        cursor.execute(
            '''
            SELECT lir.*, ml.lot as material_lot
            FROM logistics_issue_requests lir
            LEFT JOIN material_lots ml ON ml.id = lir.material_lot_id
            WHERE COALESCE(lir.request_type, 'ISSUE') = 'ISSUE'
            ORDER BY COALESCE(lir.processed_at, lir.requested_at) DESC, lir.id DESC
            '''
        )
        all_issue_requests = [dict(r) for r in cursor.fetchall()]
        cursor.execute(
            '''
            SELECT lir.*, ml.lot as material_lot
            FROM logistics_issue_requests lir
            LEFT JOIN material_lots ml ON ml.id = lir.material_lot_id
            WHERE COALESCE(lir.request_type, 'ISSUE') = 'RETURN'
            ORDER BY COALESCE(lir.processed_at, lir.requested_at) DESC, lir.id DESC
            LIMIT 200
            '''
        )
        export_requests = [dict(r) for r in cursor.fetchall()]
    else:
        cursor.execute(
            '''
            SELECT lir.*, ml.lot as material_lot
            FROM logistics_issue_requests lir
            LEFT JOIN material_lots ml ON ml.id = lir.material_lot_id
            WHERE COALESCE(lir.request_type, 'ISSUE') = 'ISSUE' AND lir.requester_workplace = ?
            ORDER BY COALESCE(lir.processed_at, lir.requested_at) DESC, lir.id DESC
            LIMIT 200
            ''',
            (workplace,),
        )
        all_issue_requests = [dict(r) for r in cursor.fetchall()]
        cursor.execute(
            '''
            SELECT lir.*, ml.lot as material_lot
            FROM logistics_issue_requests lir
            LEFT JOIN material_lots ml ON ml.id = lir.material_lot_id
            WHERE COALESCE(lir.request_type, 'ISSUE') = 'RETURN' AND lir.requester_workplace = ?
            ORDER BY COALESCE(lir.processed_at, lir.requested_at) DESC, lir.id DESC
            LIMIT 200
            ''',
            (workplace,),
        )
        export_requests = [dict(r) for r in cursor.fetchall()]

    issue_requests_pending = [row for row in all_issue_requests if (row['status'] or '') == ISSUE_STATUS_REQUESTED]
    issue_requests_completed = [row for row in all_issue_requests if (row['status'] or '') == ISSUE_STATUS_COMPLETED]
    issue_requests_rejected = [row for row in all_issue_requests if (row['status'] or '') == ISSUE_STATUS_REJECTED]
    export_requests_pending = [row for row in export_requests if (row['status'] or '') == ISSUE_STATUS_REQUESTED]
    export_requests_completed = [row for row in export_requests if (row['status'] or '') == ISSUE_STATUS_COMPLETED]
    export_requests_rejected = [row for row in export_requests if (row['status'] or '') == ISSUE_STATUS_REJECTED]
    if not is_logistics_role:
        for row in issue_requests_pending:
            note_text = str(row.get('note') or '').strip()
            if (
                ('??' in note_text)
                or ('�' in note_text)
                or note_text.startswith('[????]')
                or note_text.startswith('[?????]')
            ):
                qty = float(row.get('requested_quantity') or 0)
                unit = (row.get('unit') or '').strip()
                row['note'] = "[자동 불출 등록]"
            defaults = _get_latest_workplace_lot_defaults(cursor, int(row['material_id']), row['requester_workplace'])
            row['receipt_lot_defaults'] = defaults
        for row in issue_requests_completed:
            row['receipt_edit_rows'] = _load_issue_receipt_lot_details(cursor, int(row['id']))
            row['receipt_disposed_locked'] = _receipt_rows_include_disposed(row['receipt_edit_rows'])
            row['receipt_updated'] = bool((row.get('receipt_updated_at') or '').strip())
            original_qty = float(
                row.get('original_approved_quantity')
                or row.get('approved_quantity')
                or 0
            )
            current_qty = float(row.get('approved_quantity') or 0)
            row['original_approved_quantity_display'] = _round_to_1_decimal(original_qty)
            row['approved_quantity_changed'] = abs(original_qty - current_qty) > 0.01
    issue_completed_groups = _group_request_rows_by_date(issue_requests_completed, 'processed_at')
    export_completed_groups = _group_request_rows_by_date(export_requests_completed, 'processed_at')

    conn_context.__exit__(None, None, None)
    current_view_url = request.full_path[:-1] if request.full_path.endswith('?') else request.full_path
    dashboard_issue_prefill = session.pop('dashboard_issue_prefill', [])

    return render_template(
        'materials.html',
        user=session['user'],
        materials=materials,
        categories=categories,
        selected_category=selected_category,
        selected_material_search_field=selected_material_search_field,
        selected_material_type=selected_material_type,
        search_keyword=search_keyword,
        selected_product_id=selected_product_id,
        filter_products=filter_products,
        suppliers=suppliers,
        workplaces=WORKPLACES,
        current_workplace=workplace,
        request_materials=request_materials,
        export_request_materials=export_request_materials,
        issue_bom_products=issue_bom_products,
        export_bom_products=export_bom_products,
        issue_bom_map_json=json.dumps(issue_bom_map, ensure_ascii=False),
        export_bom_map_json=json.dumps(export_bom_map, ensure_ascii=False),
        material_lots_by_material_json=json.dumps(material_lots_by_material, ensure_ascii=False),
        issue_requests=all_issue_requests,
        issue_requests_pending=issue_requests_pending,
        issue_requests_completed=issue_requests_completed,
        issue_completed_groups=issue_completed_groups,
        issue_requests_rejected=issue_requests_rejected,
        export_requests=export_requests,
        export_requests_pending=export_requests_pending,
        export_requests_completed=export_requests_completed,
        export_completed_groups=export_completed_groups,
        export_requests_rejected=export_requests_rejected,
        is_logistics=is_logistics,
        selected_show_all_stock=selected_show_all_stock,
        req_tab=req_tab,
        issue_status_tab=issue_status_tab,
        export_status_tab=export_status_tab,
        dashboard_issue_prefill_json=json.dumps(dashboard_issue_prefill, ensure_ascii=False),
        current_view_url=current_view_url,
    )


@bp.route('/materials/add', methods=['POST'])
@login_required
def add_material():
    if not _can_manage_material_master():
        return "<script>alert('부자재 추가 권한이 없습니다.'); history.back();</script>"
    workplace = get_workplace()

    # 1. 프론트엔드에서 넘어온 code 값을 가져옵니다.
    custom_code = request.form.get('code', '').strip()

    supplier_id = request.form.get('supplier_id')
    name = request.form.get('name')
    category = request.form.get('category')
    category_clean = (category or '').strip()
    spec = request.form.get('spec')
    unit = request.form.get('unit')
    upper_unit = (request.form.get('upper_unit') or '').strip()
    upper_unit_qty_raw = (request.form.get('upper_unit_qty') or '').strip()

    moq = int(request.form.get('moq') or 0)
    lead_time = int(request.form.get('lead_time') or 0)
    unit_price = float(request.form.get('unit_price') or 0)
    current_stock = 0.0
    min_stock = float(request.form.get('min_stock') or 0)
    upper_unit_qty = None
    if upper_unit:
        try:
            upper_unit_qty = float(upper_unit_qty_raw or 0)
        except ValueError:
            return "<script>alert('상위 단위 환산값은 숫자로 입력해 주세요.'); history.back();</script>"
        if upper_unit_qty <= 0:
            return "<script>alert('상위 단위 환산값은 0보다 커야 합니다.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()

    try:
        # 2. 먼저 기본 정보 INSERT (code는 나중에 넣거나 바로 넣거나 결정)
        target_workplace = SHARED_WORKPLACE if category_clean in SHARED_MATERIAL_CATEGORIES else workplace
        cursor.execute(
            '''
            INSERT INTO materials 
            (supplier_id, name, category, spec, unit, upper_unit, upper_unit_qty, moq, lead_time, 
             unit_price, current_stock, min_stock, workplace)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
            (
                supplier_id,
                name,
                category_clean,
                spec,
                unit,
                upper_unit or None,
                upper_unit_qty,
                moq,
                lead_time,
                unit_price,
                current_stock,
                min_stock,
                target_workplace,
            ),
        )

        new_id = cursor.lastrowid

        # 3. 코드 결정 로직
        # 사용자가 입력한 코드가 있으면 그걸 사용, 없으면 기존 M0000X 방식 사용
        final_code = custom_code if custom_code else f"M{new_id:05d}"

        # 4. 결정된 코드로 UPDATE
        cursor.execute(
            '''
            UPDATE materials
            SET code = ?
            WHERE id = ?
        ''',
            (final_code, new_id),
        )

        audit_log(
            conn,
            'create',
            'material',
            new_id,
            {
                'code': final_code,
                'name': name,
                'category': category_clean,
                'unit': unit,
                'upper_unit': upper_unit or None,
                'upper_unit_qty': upper_unit_qty,
                'current_stock': current_stock,
                'min_stock': min_stock,
                'workplace': target_workplace,
            },
        )

        conn.commit()

    except sqlite3.IntegrityError as e:
        rollback_db(conn)
        # UNIQUE 제약 조건(중복 코드) 에러 발생 시 알림
        return (
            f"<script>alert('저장 오류: 이미 존재하는 코드이거나 데이터에 문제가 있습니다. ({str(e)})'); history.back();</script>"
        )

    finally:
        close_db(conn)

    return_url = (request.form.get('return_url') or '').strip()
    if return_url.startswith('/materials'):
        return redirect(return_url)
    return_url = (request.form.get('return_url') or '').strip()
    if return_url.startswith('/materials'):
        return redirect(return_url)
    return_url = (request.form.get('return_url') or '').strip()
    if return_url.startswith('/materials'):
        return redirect(return_url)
    return redirect(url_for('materials.materials'))


@bp.route('/materials/update', methods=['POST'])
@login_required
def update_material():
    """부자재 정보 및 재고 통합 수정"""
    if not _can_manage_material_master():
        return "<script>alert('부자재 수정 권한이 없습니다.'); history.back();</script>"
    # 1. 값 가져오기
    material_id = request.form.get('material_id')
    new_code = request.form.get('code', '').strip()
    name = request.form.get('name')
    category = request.form.get('category')
    category_clean = (category or '').strip()
    unit = request.form.get('unit')
    upper_unit = (request.form.get('upper_unit') or '').strip()
    upper_unit_qty_raw = (request.form.get('upper_unit_qty') or '').strip()
    supplier_id = request.form.get('supplier_id') or None
    moq = request.form.get('moq') or 0

    # 2. 숫자형 데이터 변환 (float 처리로 ValueError 방지)
    try:
        min_stock = float(request.form.get('min_stock') or 0)
        unit_price = float(request.form.get('unit_price') or 0)
        moq_value = float(moq or 0)
        upper_unit_qty = float(upper_unit_qty_raw or 0) if upper_unit else None
    except ValueError:
        return "<script>alert('????? ???????? ?????????????'); history.back();</script>"
    if upper_unit and (upper_unit_qty is None or upper_unit_qty <= 0):
        return "<script>alert('?? ?? ???? 0?? ?? ???.'); history.back();</script>"

    workplace = get_workplace()
    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM materials WHERE id = ?', (material_id,))
            before = cursor.fetchone()
            target_workplace = SHARED_WORKPLACE if category_clean in SHARED_MATERIAL_CATEGORIES else workplace
            cursor.execute(
                '''
                UPDATE materials 
                SET code = ?, name = ?, category = ?, unit = ?, upper_unit = ?, upper_unit_qty = ?,
                    min_stock = ?, unit_price = ?, supplier_id = ?, moq = ?, workplace = ?
                WHERE id = ?
            ''',
                (
                    new_code,
                    name,
                    category_clean,
                    unit,
                    upper_unit or None,
                    upper_unit_qty,
                    min_stock,
                    unit_price,
                    supplier_id,
                    moq_value,
                    target_workplace,
                    material_id,
                ),
            )

            audit_log(
                conn,
                'update',
                'material',
                material_id,
                {
                    'before': dict(before) if before else None,
                    'after': {
                        'code': new_code,
                        'name': name,
                        'category': category_clean,
                        'unit': unit,
                        'upper_unit': upper_unit or None,
                        'upper_unit_qty': upper_unit_qty,
                        'min_stock': min_stock,
                        'unit_price': unit_price,
                        'supplier_id': supplier_id,
                        'moq': moq_value,
                        'workplace': target_workplace,
                    },
                },
            )
    except sqlite3.IntegrityError:
        return "<script>alert('오류: 이미 존재하는 자재 코드입니다.'); history.back();</script>"
    except Exception as e:
        return f"<script>alert('수정 중 오류 발생: {str(e)}'); history.back();</script>"

    return_url = (request.form.get('return_url') or '').strip()
    if return_url.startswith('/materials'):
        return redirect(return_url)
    return redirect(url_for('materials.materials'))


@bp.route('/materials/<int:material_id>/move-workplace', methods=['POST'])
@role_required('purchase')
def move_material_workplace(material_id):
    target_workplace = (request.form.get('target_workplace') or '').strip()
    move_note = (request.form.get('move_note') or '').strip()
    next_url = (request.form.get('next') or '').strip()
    valid_targets = set(WORKPLACES) | {LOGISTICS_WORKPLACE}
    if target_workplace not in valid_targets:
        return "<script>alert('??? ???? ?? ??? ???.'); history.back();</script>"

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, code, name, category, workplace, unit, current_stock FROM materials WHERE id = ?', (material_id,))
            before = cursor.fetchone()
            if not before:
                return "<script>alert('\ub85c\ud2b8\ub97c \ucc3e\uc744 \uc218 \uc5c6\uc2b5\ub2c8\ub2e4.'); history.back();</script>"

            source_workplace = (before['workplace'] or '').strip()
            if target_workplace == LOGISTICS_WORKPLACE:
                final_workplace = LOGISTICS_WORKPLACE
            else:
                final_workplace = SHARED_WORKPLACE if (before['category'] or '').strip() in SHARED_MATERIAL_CATEGORIES else target_workplace
            if source_workplace == final_workplace:
                return "<script>alert('?? ?????? ??? ? ????.'); history.back();</script>"

            cursor.execute('UPDATE materials SET workplace = ? WHERE id = ?', (final_workplace, material_id))
            cursor.execute(
                '''
                INSERT INTO material_history (material_id, type, quantity, reason, note, created_at)
                VALUES (?, 'MOVE_WORKPLACE', 0, ?, ?, datetime('now'))
                ''',
                (
                    material_id,
                    f'{source_workplace} -> {final_workplace}',
                    move_note,
                ),
            )
            audit_log(
                conn,
                'update',
                'material_workplace_move',
                material_id,
                {
                    'code': before['code'],
                    'name': before['name'],
                    'from_workplace': source_workplace,
                    'to_workplace': final_workplace,
                    'quantity': 0,
                    'note': move_note,
                    'stock_unchanged': True,
                },
            )
    except Exception:
        return "<script>alert('??? ?? ? ??? ??????.'); history.back();</script>"

    return redirect(next_url or request.referrer or url_for('materials.materials'))


@bp.route('/materials/<int:material_id>/detail')
@login_required
def material_detail(material_id):
    with db_connection() as conn:
        cursor = conn.cursor()
        _sync_material_stock_with_lots(conn, material_id)
        cursor.execute(
            """
            SELECT m.id, m.code, m.name, m.unit, m.current_stock, m.workplace, s.name as supplier_name
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            WHERE m.id = ?
            """,
            (material_id,),
        )
        material = cursor.fetchone()
        if not material:
            return jsonify({'ok': False, 'message': '부자재 정보를 찾을 수 없습니다.'}), 404

        is_logistics = False
        allowed_locations = []
        if is_logistics:
            cursor.execute(
                """
                SELECT id
                FROM inv_locations
                WHERE COALESCE(loc_type, '') IN ('WORKPLACE', 'WAREHOUSE')
                ORDER BY CASE WHEN name = '물류창고' THEN 0 ELSE 1 END, id
                """
            )
            allowed_locations = [int(row['id']) for row in cursor.fetchall()]
        else:
            workplace_name = (session.get('workplace') or material['workplace'] or '').strip()
            workplace_location_id = _get_inventory_location_id(cursor, workplace_name) if workplace_name else None
            if workplace_location_id:
                allowed_locations.append(workplace_location_id)

        lots = []
        if allowed_locations:
            placeholders = ','.join(['?'] * len(allowed_locations))
            cursor.execute(
                f"""
                SELECT
                    ml.id,
                    ml.lot,
                    ml.lot_seq,
                    ml.receiving_date,
                    ml.manufacture_date,
                    ml.expiry_date,
                    ml.unit_price,
                    ml.received_quantity,
                    ml.current_quantity,
                    ml.supplier_lot,
                    ml.is_disposed,
                    b.location_id AS location_id,
                    l.name AS location_name,
                    COALESCE(b.qty, 0) AS location_quantity
                FROM inv_material_lot_balances b
                JOIN material_lots ml ON ml.id = b.material_lot_id
                JOIN inv_locations l ON l.id = b.location_id
                WHERE ml.material_id = ?
                  AND COALESCE(ml.is_disposed, 0) = 0
                  AND b.location_id IN ({placeholders})
                  AND COALESCE(b.qty, 0) > 0
                  AND COALESCE(l.name, '') <> '????'
                  AND COALESCE(l.loc_type, '') <> 'WAREHOUSE'
                ORDER BY CASE WHEN l.name = '????' THEN 1 ELSE 0 END,
                         CASE WHEN ml.receiving_date IS NULL OR TRIM(ml.receiving_date) = '' THEN 1 ELSE 0 END,
                         ml.receiving_date DESC,
                         ml.lot_seq DESC,
                         ml.id DESC
                """,
                (material_id, *allowed_locations),
            )
            lots = [dict(row) for row in cursor.fetchall()]
        visible_lot_ids = {
            int(row.get('id') or 0)
            for row in lots
            if int(row.get('id') or 0) > 0 and float(row.get('location_quantity') or 0) > 0
        }

        cursor.execute(
            """
SELECT
                COALESCE(p.production_date, substr(pmu.created_at, 1, 10)) as use_date,
                COALESCE(
                    (
                        SELECT al.created_at
                        FROM audit_logs al
                        WHERE al.entity = 'production'
                          AND al.entity_id = pmu.production_id
                        ORDER BY al.id DESC
                        LIMIT 1
                    ),
                    pmu.created_at
                ) as completed_at,
                pmu.id as production_usage_id,
                pmu.production_id as production_id,
                ('PROD-' || pmu.production_id) as production_no,
                COALESCE(prd.name, '-') as product_name,
                COALESCE(ml.lot, '-') as lot,
                COALESCE(pmlu.quantity, pmu.actual_quantity, 0) as used_quantity,
                COALESCE(pmu.actual_quantity, 0) as total_used_quantity,
                CASE
                    WHEN pmlu.id IS NOT NULL THEN
                        COALESCE(ml.received_quantity, 0) - COALESCE((
                            SELECT SUM(COALESCE(pmlu2.quantity, 0))
                            FROM production_material_lot_usage pmlu2
                            JOIN production_material_usage pmu2 ON pmu2.id = pmlu2.production_usage_id
                            LEFT JOIN productions p2 ON p2.id = pmu2.production_id
                            WHERE pmlu2.material_lot_id = pmlu.material_lot_id
                              AND (
                                  COALESCE(p2.production_date, substr(pmu2.created_at, 1, 10)) < COALESCE(p.production_date, substr(pmu.created_at, 1, 10))
                                  OR (
                                      COALESCE(p2.production_date, substr(pmu2.created_at, 1, 10)) = COALESCE(p.production_date, substr(pmu.created_at, 1, 10))
                                      AND (
                                          pmu2.production_id < pmu.production_id
                                          OR (pmu2.production_id = pmu.production_id AND pmlu2.id <= pmlu.id)
                                      )
                                  )
                              )
                        ), 0)
                    ELSE COALESCE(m.current_stock, 0)
                END as remaining_quantity,
                COALESCE(
                    (
                        SELECT NULLIF(al.name, '')
                        FROM audit_logs al
                        WHERE al.entity = 'production'
                          AND al.entity_id = pmu.production_id
                        ORDER BY al.id DESC
                        LIMIT 1
                    ),
                    (
                        SELECT NULLIF(al.username, '')
                        FROM audit_logs al
                        WHERE al.entity = 'production'
                          AND al.entity_id = pmu.production_id
                        ORDER BY al.id DESC
                        LIMIT 1
                    ),
                    '-'
                ) as user_name
            FROM production_material_usage pmu
            LEFT JOIN productions p ON p.id = pmu.production_id
            LEFT JOIN products prd ON prd.id = p.product_id
            LEFT JOIN materials m ON m.id = pmu.material_id
            LEFT JOIN production_material_lot_usage pmlu
              ON pmlu.production_usage_id = pmu.id
             AND pmlu.material_id = pmu.material_id
            LEFT JOIN material_lots ml ON ml.id = pmlu.material_lot_id
            WHERE pmu.material_id = ?
              AND COALESCE(pmu.actual_quantity, 0) > 0
              AND COALESCE(p.status, '') = '완료'
            ORDER BY
                COALESCE(p.production_date, pmu.created_at) DESC,
                pmu.id DESC,
                COALESCE(pmlu.id, 0) DESC
            LIMIT 200
            """,
            (material_id,),
        )
        usage_logs = [dict(row) for row in cursor.fetchall()]

        workplace_name = (session.get('workplace') or material['workplace'] or '').strip()
        cursor.execute(
            """
            SELECT
                substr(COALESCE(lir.processed_at, lir.requested_at, ''), 1, 10) AS use_date,
                COALESCE(lir.processed_at, lir.requested_at, '') AS completed_at,
                ('EXPORT-' || lir.id) AS usage_group_id,
                lir.id AS export_request_id,
                '반출' AS production_no,
                TRIM(
                    COALESCE(NULLIF(lir.reason, ''), '반출')
                    || CASE
                        WHEN COALESCE(NULLIF(lir.reason_detail, ''), '') <> '' THEN ' / ' || lir.reason_detail
                        ELSE ''
                    END
                ) AS product_name,
                COALESCE(ml.lot, '-') AS lot,
                COALESCE(lir.approved_quantity, lir.requested_quantity, 0) AS used_quantity,
                COALESCE(lir.approved_quantity, lir.requested_quantity, 0) AS total_used_quantity,
                0 AS remaining_quantity,
                COALESCE(NULLIF(lir.processed_by, ''), NULLIF(lir.requested_by, ''), NULLIF(lir.requester_username, ''), '-') AS user_name,
                'export' AS usage_type
            FROM logistics_issue_requests lir
            LEFT JOIN material_lots ml ON ml.id = lir.material_lot_id
            WHERE lir.material_id = ?
              AND COALESCE(lir.request_type, 'ISSUE') = 'RETURN'
              AND COALESCE(lir.status, '') = ?
              AND COALESCE(lir.requester_workplace, '') = ?
              AND COALESCE(lir.approved_quantity, lir.requested_quantity, 0) > 0
            ORDER BY COALESCE(lir.processed_at, lir.requested_at) DESC, lir.id DESC
            LIMIT 200
            """,
            (material_id, ISSUE_STATUS_COMPLETED, workplace_name),
        )
        export_usage_logs = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
                mll.id as log_id,
                mll.material_lot_id as material_lot_id,
                COALESCE(ml.receiving_date, substr(mll.created_at, 1, 10)) as receive_date,
                mll.created_at as received_at,
                COALESCE(ml.lot, '-') as lot,
                COALESCE(ml.manufacture_date, '-') as manufacture_date,
                COALESCE(ml.expiry_date, '-') as expiry_date,
                COALESCE(mll.quantity, 0) as received_quantity,
                COALESCE(ml.is_disposed, 0) as is_disposed,
                COALESCE(mll.action, '') as action,
                CASE
                    WHEN COALESCE(mll.action, '') IN ('issue_request_complete', 'issue_request_update') THEN COALESCE(
                        (
                            SELECT lir.id
                            FROM logistics_issue_requests lir
                            WHERE lir.material_id = mll.material_id
                              AND substr(COALESCE(lir.processed_at, ''), 1, 19) = substr(COALESCE(mll.created_at, ''), 1, 19)
                              AND (
                                  COALESCE(lir.requester_workplace, '') = ''
                                  OR instr(COALESCE(mll.note, ''), COALESCE(lir.requester_workplace, '')) > 0
                              )
                            ORDER BY lir.id DESC
                            LIMIT 1
                        ),
                        (
                            SELECT ril.request_id
                            FROM logistics_issue_receipt_lots ril
                            JOIN material_lots link_ml ON link_ml.id = ril.material_lot_id
                            WHERE ril.material_lot_id = mll.material_lot_id
                              AND COALESCE(link_ml.material_id, 0) = COALESCE(mll.material_id, 0)
                            ORDER BY ril.id DESC
                            LIMIT 1
                        ),
                        (
                            SELECT nearest.id
                            FROM (
                                SELECT
                                    lir.id,
                                    ABS(strftime('%s', COALESCE(lir.processed_at, '')) - strftime('%s', COALESCE(mll.created_at, ''))) AS time_diff
                                FROM logistics_issue_requests lir
                                WHERE lir.material_id = mll.material_id
                                  AND substr(COALESCE(lir.processed_at, ''), 1, 10) = substr(COALESCE(mll.created_at, ''), 1, 10)
                                  AND (
                                      COALESCE(lir.requester_workplace, '') = ''
                                      OR instr(COALESCE(mll.note, ''), COALESCE(lir.requester_workplace, '')) > 0
                                  )
                            ) nearest
                            ORDER BY nearest.time_diff ASC, nearest.id DESC
                            LIMIT 1
                        )
                    )
                    ELSE NULL
                END as issue_request_id,
                CASE COALESCE(mll.action, '')
                    WHEN 'create' THEN '신규 입고'
                    WHEN 'issue_request_complete' THEN '불출 입고 완료'
                    WHEN 'issue_request_update' THEN '입고 수정'
                    ELSE COALESCE(mll.action, '-')
                END as action_label,
                COALESCE(mll.note, '-') as note
            FROM material_lot_logs mll
            LEFT JOIN material_lots ml ON ml.id = mll.material_lot_id
            WHERE mll.material_id = ?
              AND COALESCE(mll.action, '') IN ('create', 'issue_request_complete', 'issue_request_update')
              AND COALESCE(mll.quantity, 0) > 0
            ORDER BY
                COALESCE(ml.receiving_date, substr(mll.created_at, 1, 10)) DESC,
                mll.id DESC
            LIMIT 200
            """,
            (material_id,),
        )
        receive_logs = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            """
            SELECT
                mll.id as log_id,
                mll.material_lot_id as material_lot_id,
                COALESCE(ml.receiving_date, substr(mll.created_at, 1, 10)) as receive_date,
                mll.created_at as received_at,
                COALESCE(ml.lot, '-') as lot,
                COALESCE(ml.manufacture_date, '-') as manufacture_date,
                COALESCE(ml.expiry_date, '-') as expiry_date,
                ABS(COALESCE(mll.quantity, 0)) as received_quantity,
                COALESCE(ml.is_disposed, 0) as is_disposed,
                COALESCE(mll.action, '') as action,
                CASE
                    WHEN COALESCE(mll.action, '') = 'issue_request_complete' THEN COALESCE(
                        (
                            SELECT lir.id
                            FROM logistics_issue_requests lir
                            WHERE lir.material_id = mll.material_id
                              AND substr(COALESCE(lir.processed_at, ''), 1, 19) = substr(COALESCE(mll.created_at, ''), 1, 19)
                              AND (
                                  COALESCE(lir.requester_workplace, '') = ''
                                  OR instr(COALESCE(mll.note, ''), COALESCE(lir.requester_workplace, '')) > 0
                              )
                            ORDER BY lir.id DESC
                            LIMIT 1
                        ),
                        (
                            SELECT ril.request_id
                            FROM logistics_issue_receipt_lots ril
                            JOIN material_lots link_ml ON link_ml.id = ril.material_lot_id
                            WHERE ril.material_lot_id = mll.material_lot_id
                              AND COALESCE(link_ml.material_id, 0) = COALESCE(mll.material_id, 0)
                            ORDER BY ril.id DESC
                            LIMIT 1
                        )
                    )
                    ELSE NULL
                END as issue_request_id,
                '최초 재고 등록' as action_label,
                CASE
                    WHEN COALESCE(mll.note, '') = 'inventory_audit_apply_workplace_plus' THEN '재고조사 반영'
                    ELSE COALESCE(NULLIF(mll.note, ''), '-')
                END as note
            FROM material_lot_logs mll
            LEFT JOIN material_lots ml ON ml.id = mll.material_lot_id
            WHERE mll.material_id = ?
              AND COALESCE(mll.quantity, 0) > 0
              AND (
                    COALESCE(mll.action, '') IN ('create', 'issue_request_complete')
                 OR (
                        COALESCE(mll.action, '') = 'adjustment'
                    AND COALESCE(mll.note, '') = 'inventory_audit_apply_workplace_plus'
                 )
              )
            ORDER BY
                COALESCE(ml.receiving_date, substr(mll.created_at, 1, 10)) ASC,
                mll.id ASC
            LIMIT 1
            """,
            (material_id,),
        )
        initial_stock_row = cursor.fetchone()
        initial_stock_log = dict(initial_stock_row) if initial_stock_row else None
        cursor.execute(
            """
            SELECT
                id,
                lot,
                receiving_date,
                manufacture_date,
                expiry_date,
                COALESCE(received_quantity, 0) as received_quantity,
                COALESCE(current_quantity, 0) as current_quantity,
                COALESCE(quantity, 0) as legacy_quantity
            FROM material_lots
            WHERE material_id = ?
            ORDER BY COALESCE(receiving_date, '') DESC, id DESC
            """,
            (material_id,),
        )
        material_lot_rows = [dict(row) for row in cursor.fetchall()]
        visible_material_lot_rows = material_lot_rows
        if visible_lot_ids:
            visible_material_lot_rows = [row for row in material_lot_rows if int(row.get('id') or 0) in visible_lot_ids]
        cursor.execute(
            """
            SELECT DISTINCT material_lot_id
            FROM production_material_lot_usage
            WHERE material_id = ?
              AND material_lot_id IS NOT NULL
            """,
            (material_id,),
        )
        historical_lot_ids = {
            int(row['material_lot_id'] or 0)
            for row in cursor.fetchall()
            if int(row['material_lot_id'] or 0) > 0
        }
        cursor.execute(
            """
            SELECT
                material_lot_id,
                COALESCE(SUM(quantity), 0) as production_usage_qty
            FROM production_material_lot_usage
            WHERE material_id = ?
              AND material_lot_id IS NOT NULL
            GROUP BY material_lot_id
            """,
            (material_id,),
        )
        production_usage_qty_by_lot = {
            int(row['material_lot_id'] or 0): _round_to_1_decimal(row['production_usage_qty'] or 0)
            for row in cursor.fetchall()
            if int(row['material_lot_id'] or 0) > 0
        }
        cursor.execute(
            """
            SELECT
                material_lot_id,
                COALESCE(SUM(quantity), 0) as export_complete_qty
            FROM material_lot_logs
            WHERE material_id = ?
              AND COALESCE(action, '') = 'export_request_complete'
              AND COALESCE(quantity, 0) > 0
              AND material_lot_id IS NOT NULL
            GROUP BY material_lot_id
            """,
            (material_id,),
        )
        export_complete_qty_by_lot = {
            int(row['material_lot_id'] or 0): _round_to_1_decimal(row['export_complete_qty'] or 0)
            for row in cursor.fetchall()
            if int(row['material_lot_id'] or 0) > 0
        }
        previous_used_lots = []
        for row in material_lot_rows:
            lot_id = int(row.get('id') or 0)
            if lot_id <= 0 or lot_id in visible_lot_ids or lot_id not in historical_lot_ids:
                continue
            previous_used_lots.append(
                {
                    'id': lot_id,
                    'lot': row.get('lot') or '-',
                    'receiving_date': row.get('receiving_date') or '',
                    'manufacture_date': row.get('manufacture_date') or '',
                    'expiry_date': row.get('expiry_date') or '',
                    'received_quantity': _round_to_1_decimal(row.get('received_quantity') or row.get('legacy_quantity') or 0),
                    'current_quantity': _round_to_1_decimal(row.get('current_quantity') or 0),
                    'status_label': '사용 완료',
                }
            )
        previous_used_lots.sort(
            key=lambda row: (
                0 if (row.get('receiving_date') or '').strip() else 1,
                row.get('receiving_date') or '',
                row.get('id') or 0,
            ),
            reverse=True,
        )

        def _receive_log_matches_current_workplace(row):
            if not workplace_name:
                return True
            action = str(row.get('action') or '').strip()
            note = str(row.get('note') or '').strip()
            lot_id = int(row.get('material_lot_id') or 0)
            if action in {'issue_request_complete', 'issue_request_update', 'issue_request_cancel', 'export_request_complete', 'export_request_cancel'}:
                return note.startswith(workplace_name)
            if visible_lot_ids:
                return lot_id in visible_lot_ids
            return False

        receive_logs = [row for row in receive_logs if _receive_log_matches_current_workplace(row)]
        if initial_stock_log and not _receive_log_matches_current_workplace(initial_stock_log):
            initial_stock_log = None
        effective_receive_logs = []
        cancel_qty_by_lot = {}
        positive_receive_logs_by_lot = {}
        for row in receive_logs:
            lot_id = int(row.get('material_lot_id') or 0)
            if lot_id <= 0:
                continue
            positive_receive_logs_by_lot.setdefault(lot_id, []).append(dict(row))
        cursor.execute(
            """
            SELECT
                material_lot_id,
                COALESCE(SUM(quantity), 0) as cancel_qty
            FROM material_lot_logs
            WHERE material_id = ?
              AND COALESCE(action, '') = 'issue_request_cancel'
              AND COALESCE(quantity, 0) > 0
            GROUP BY material_lot_id
            """,
            (material_id,),
        )
        for row in cursor.fetchall():
            lot_id = int(row['material_lot_id'] or 0)
            if lot_id > 0:
                cancel_qty_by_lot[lot_id] = _round_to_1_decimal(row['cancel_qty'] or 0)

        for lot_id, rows in positive_receive_logs_by_lot.items():
            remaining_cancel_qty = _round_to_1_decimal(cancel_qty_by_lot.get(lot_id, 0))
            complete_rows = [row for row in rows if str(row.get('action') or '') == 'issue_request_complete']
            other_rows = [row for row in rows if str(row.get('action') or '') != 'issue_request_complete']
            for row in sorted(complete_rows, key=lambda item: ((item.get('received_at') or ''), int(item.get('log_id') or 0)), reverse=True):
                row_qty = _round_to_1_decimal(row.get('received_quantity') or 0)
                if remaining_cancel_qty > 0:
                    if remaining_cancel_qty >= row_qty - 0.01:
                        remaining_cancel_qty = _round_to_1_decimal(remaining_cancel_qty - row_qty)
                        continue
                    patched = dict(row)
                    patched['received_quantity'] = _round_to_1_decimal(row_qty - remaining_cancel_qty)
                    remaining_cancel_qty = 0.0
                    effective_receive_logs.append(patched)
                else:
                    effective_receive_logs.append(row)
            effective_receive_logs.extend(other_rows)

        effective_lot_ids = {
            int(row.get('material_lot_id') or 0)
            for row in effective_receive_logs
            if int(row.get('material_lot_id') or 0) > 0
        }
        for lot_row in visible_material_lot_rows:
            lot_id = int(lot_row.get('id') or 0)
            if lot_id <= 0 or lot_id in effective_lot_ids:
                continue
            current_qty = _round_to_1_decimal(lot_row.get('current_quantity') or 0)
            base_received_qty = _round_to_1_decimal(lot_row.get('received_quantity') or lot_row.get('legacy_quantity') or 0)
            traced_usage_qty = _round_to_1_decimal(production_usage_qty_by_lot.get(lot_id, 0))
            traced_export_qty = _round_to_1_decimal(export_complete_qty_by_lot.get(lot_id, 0))
            restored_qty = _round_to_1_decimal(current_qty + traced_usage_qty + traced_export_qty)
            received_qty = restored_qty if restored_qty > 0 else base_received_qty
            if received_qty <= 0:
                continue
            if lot_id not in historical_lot_ids and current_qty <= 0:
                continue
            note = '과거 사용 로트 복원'
            if restored_qty > 0 and abs(base_received_qty - restored_qty) >= 0.1:
                note = f'과거 사용 로트 복원 (기록기준 {restored_qty:.1f})'
            effective_receive_logs.append({
                'log_id': -lot_id,
                'material_lot_id': lot_id,
                'receive_date': lot_row.get('receiving_date') or '-',
                'received_at': f"{lot_row.get('receiving_date') or '0000-00-00'} 00:00:00",
                'lot': lot_row.get('lot') or '-',
                'manufacture_date': lot_row.get('manufacture_date') or '-',
                'expiry_date': lot_row.get('expiry_date') or '-',
                'received_quantity': received_qty,
                'is_disposed': 0,
                'action': 'initial_restore',
                'issue_request_id': None,
                'action_label': '최초 재고 등록',
                'note': '과거 사용 로트 복원',
                'show_received_quantity': True,
            })
        effective_receive_logs.sort(
            key=lambda row: ((row.get('received_at') or ''), int(row.get('log_id') or 0)),
            reverse=True,
        )
        effective_receive_total = _round_to_1_decimal(
            sum(float(row.get('received_quantity') or 0) for row in effective_receive_logs)
        )
        active_receive_logs = [row for row in receive_logs if int(row.get('is_disposed') or 0) != 1]
        disposed_receive_logs = [row for row in receive_logs if int(row.get('is_disposed') or 0) == 1]
        receive_complete_logs = [
            row for row in active_receive_logs
            if str(row.get('action') or '') in {'create', 'issue_request_complete'}
        ]
        receive_update_logs = [
            row for row in active_receive_logs
            if str(row.get('action') or '') == 'issue_request_update'
        ]
        for row in receive_update_logs:
            note_text = str(row.get('note') or '').strip()
            row['show_received_quantity'] = '입고수량 ' in note_text
        for row in receive_complete_logs:
            row['show_received_quantity'] = True
        if initial_stock_log:
            initial_stock_log['show_received_quantity'] = True

        payload = dict(material)
        payload['total_quantity'] = sum(float(row.get('location_quantity') or 0) for row in lots)
        payload['can_manage_lots'] = bool(_can_manage_material_lots())
        return jsonify({
            'ok': True,
            'material': payload,
            'lots': lots,
            'previous_used_lots': previous_used_lots,
            'usage_logs': usage_logs,
            'export_usage_logs': export_usage_logs,
            'receive_logs': receive_complete_logs,
            'receive_complete_logs': receive_complete_logs,
            'receive_update_logs': receive_update_logs,
            'disposed_receive_logs': disposed_receive_logs,
            'initial_stock_log': initial_stock_log,
            'effective_receive_logs': effective_receive_logs,
            'effective_receive_total': effective_receive_total,
        })


@bp.route('/materials/material-lots/add', methods=['POST'])
@login_required
def add_material_lot():
    if not _can_manage_material_lots():
        return jsonify({'ok': False, 'message': '\ub85c\ud2b8 \uad00\ub9ac \uad8c\ud55c\uc774 \uc5c6\uc2b5\ub2c8\ub2e4.'}), 403
    material_id = request.form.get('material_id')
    receiving_date = request.form.get('receiving_date')
    manufacture_date = (request.form.get('manufacture_date') or '').strip()
    expiry_date = (request.form.get('expiry_date') or '').strip()
    unit_price = float(request.form.get('unit_price') or 0)
    received_quantity = _round_to_1_decimal(request.form.get('received_quantity') or request.form.get('quantity') or 0)
    current_quantity = _round_to_1_decimal(request.form.get('current_quantity') or received_quantity)
    supplier_lot = (request.form.get('supplier_lot') or '').strip()
    location_id = request.form.get('location_id')

    if not manufacture_date and not expiry_date:
        return jsonify({'ok': False, 'message': '\uc81c\uc870\uc77c \ub610\ub294 \uc18c\ube44\uae30\ud55c \uc911 \ud558\ub098\ub294 \uc785\ub825\ud574\uc8fc\uc138\uc694.'}), 400

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, code, name, unit, workplace FROM materials WHERE id = ?', (material_id,))
            material = cursor.fetchone()
            if not material:
                return jsonify({'ok': False, 'message': '???? ?? ? ????.'}), 404

            lot_seq = _next_lot_seq(cursor, int(material_id), receiving_date)
            lot = _build_material_lot(material['code'], receiving_date, lot_seq)
            cursor.execute(
                '''
                INSERT INTO material_lots
                (material_id, lot, lot_seq, receiving_date, manufacture_date, expiry_date, unit_price, received_quantity, current_quantity, supplier_lot, quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
                (material_id, lot, lot_seq, receiving_date, manufacture_date, expiry_date, unit_price, received_quantity, current_quantity, supplier_lot, current_quantity),
            )
            lot_id = cursor.lastrowid
            action = 'create'

            cursor.execute('UPDATE materials SET current_stock = current_stock + ?, unit_price = ? WHERE id = ?', (current_quantity, unit_price, material_id))

            current_workplace = (session.get('workplace') or '').strip()
            target_location_name = '\ubb3c\ub958\ucc3d\uace0' if current_workplace == LOGISTICS_WORKPLACE else (current_workplace or (material['workplace'] or '').strip())
            target_location_id = _get_inventory_location_id(cursor, target_location_name)
            if target_location_id:
                _upsert_material_lot_balance(cursor, target_location_id, lot_id, current_quantity)

            if current_workplace == LOGISTICS_WORKPLACE:
                _increase_logistics_stock(
                    cursor,
                    material['code'],
                    material['name'],
                    material['unit'],
                    current_quantity,
                    (session.get('user') or {}).get('username'),
                )

            cursor.execute(
                '''
                INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
                VALUES (?, ?, ?, ?, ?)
            ''',
                (lot_id, material_id, action, current_quantity, lot),
            )
            return jsonify({'ok': True, 'lot': lot, 'lot_id': lot_id})
    except Exception:
        return jsonify({'ok': False, 'message': '?? ?? ? ??? ??????.'}), 500

@bp.route('/materials/material-lots/<int:lot_id>/update', methods=['POST'])
@login_required
def update_material_lot(lot_id):
    if not _can_manage_material_lots():
        return jsonify({'ok': False, 'message': '\ub85c\ud2b8 \uad00\ub9ac \uad8c\ud55c\uc774 \uc5c6\uc2b5\ub2c8\ub2e4.'}), 403
    receiving_date = request.form.get('receiving_date')
    manufacture_date = (request.form.get('manufacture_date') or '').strip()
    expiry_date = (request.form.get('expiry_date') or '').strip()
    unit_price = float(request.form.get('unit_price') or 0)
    received_quantity = _round_to_1_decimal(request.form.get('received_quantity') or request.form.get('quantity') or 0)
    current_quantity = _round_to_1_decimal(request.form.get('current_quantity') or received_quantity)
    supplier_lot = (request.form.get('supplier_lot') or '').strip()
    location_id = request.form.get('location_id')

    if not manufacture_date and not expiry_date:
        return jsonify({'ok': False, 'message': '\uc81c\uc870\uc77c \ub610\ub294 \uc18c\ube44\uae30\ud55c \uc911 \ud558\ub098\ub294 \uc785\ub825\ud574\uc8fc\uc138\uc694.'}), 400

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM material_lots WHERE id = ?', (lot_id,))
            before = cursor.fetchone()
            if not before:
                return jsonify({'ok': False, 'message': '\ub85c\ud2b8\ub97c \ucc3e\uc744 \uc218 \uc5c6\uc2b5\ub2c8\ub2e4.'}), 404
            if int(before['is_disposed'] or 0) == 1:
                return jsonify({'ok': False, 'message': '폐기된 로트는 수정할 수 없습니다.'}), 400

            cursor.execute('SELECT code FROM materials WHERE id = ?', (before['material_id'],))
            material = cursor.fetchone()
            lot = _build_material_lot(material['code'] if material else '', receiving_date, before['lot_seq'])

        cursor.execute('SELECT id FROM material_lots WHERE lot = ? AND id != ?', (lot, lot_id))
        if cursor.fetchone():
            return jsonify({'ok': False, 'message': '\uc774\ubbf8 \uc874\uc7ac\ud558\ub294 \ub85c\ud2b8\uc785\ub2c8\ub2e4.'}), 400

        previous_lot_quantity = float(before['current_quantity'] or before['quantity'] or 0)
        cursor.execute(
            '''
            UPDATE material_lots
            SET lot = ?, receiving_date = ?, manufacture_date = ?, expiry_date = ?, unit_price = ?, received_quantity = ?, supplier_lot = ?, is_disposed = 0, disposed_at = NULL
            WHERE id = ?
        ''',
            (lot, receiving_date, manufacture_date, expiry_date, unit_price, received_quantity, supplier_lot, lot_id),
        )

        if location_id:
            try:
                location_id_int = int(location_id)
            except (TypeError, ValueError):
                location_id_int = None
            if location_id_int:
                _upsert_material_lot_balance(cursor, location_id_int, lot_id, current_quantity)

        cursor.execute(
            'SELECT COALESCE(SUM(qty), 0) AS total_qty FROM inv_material_lot_balances WHERE material_lot_id = ?',
            (lot_id,),
        )
        total_row = cursor.fetchone()
        lot_total = _round_to_1_decimal((total_row['total_qty'] if total_row else 0) or 0)
        cursor.execute('UPDATE material_lots SET current_quantity = ?, quantity = ? WHERE id = ?', (lot_total, lot_total, lot_id))
        cursor.execute('UPDATE materials SET current_stock = current_stock + ?, unit_price = ? WHERE id = ?', (lot_total - previous_lot_quantity, unit_price, before['material_id']))

        if location_id:
            cursor.execute('SELECT name FROM inv_locations WHERE id = ?', (location_id,))
            loc_row = cursor.fetchone()
            if loc_row and (loc_row['name'] or '').strip() == '\ubb3c\ub958\ucc3d\uace0':
                cursor.execute('SELECT code, name, unit FROM materials WHERE id = ?', (before['material_id'],))
                material_info = cursor.fetchone()
                if material_info:
                    cursor.execute(
                        '''
                        INSERT INTO logistics_stocks (material_code, material_name, unit, quantity, updated_by)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(material_code) DO UPDATE SET
                            material_name = excluded.material_name,
                            unit = excluded.unit,
                            quantity = excluded.quantity,
                            updated_by = excluded.updated_by,
                            updated_at = CURRENT_TIMESTAMP
                        ''',
                        (material_info['code'], material_info['name'], material_info['unit'], lot_total, (session.get('user') or {}).get('username')),
                    )

        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'update', ?, ?)
        ''',
            (lot_id, before['material_id'], lot_total, lot),
        )
        audit_log(conn, 'update', 'material_lot', lot_id, {'before': dict(before), 'after': {'lot': lot, 'received_quantity': received_quantity, 'current_quantity': lot_total, 'supplier_lot': supplier_lot}})
        return jsonify({'ok': True, 'lot': lot})
    except Exception as e:
        return jsonify({'ok': False, 'message': f'\ub85c\ud2b8 \uc218\uc815 \uc911 \uc624\ub958\uac00 \ubc1c\uc0dd\ud588\uc2b5\ub2c8\ub2e4: {e}'}), 500


@bp.route('/materials/material-lots/<int:lot_id>/delete', methods=['POST'])
@login_required
def delete_material_lot(lot_id):
    if not _can_manage_material_lots():
        return jsonify({'ok': False, 'message': '로트 관리 권한이 없습니다.'}), 403
    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM material_lots WHERE id = ?', (lot_id,))
            lot = cursor.fetchone()
            if not lot:
                return jsonify({'ok': False, 'message': '로트를 찾을 수 없습니다.'}), 404

            current_qty = float(lot['current_quantity'] or lot['quantity'] or 0)
            cursor.execute(
                '''
                UPDATE material_lots
                SET is_disposed = 1, disposed_at = CURRENT_TIMESTAMP, current_quantity = 0, quantity = 0
                WHERE id = ?
            ''',
                (lot_id,),
            )
            _clear_material_lot_balances(cursor, lot_id)
            _sync_material_stock_with_lots(conn, lot['material_id'])
            cursor.execute(
                '''
                INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
                VALUES (?, ?, 'delete', ?, ?)
            ''',
                (lot_id, lot['material_id'], current_qty, lot['lot']),
            )
            return jsonify({'ok': True})
    except Exception:
        return jsonify({'ok': False, 'message': '로트 삭제 중 오류가 발생했습니다.'}), 500


@bp.route('/materials/<int:material_id>/export', methods=['POST'])
@role_required('purchase')
def export_material(material_id):
    # 1. 입력값 검증
    try:
        export_quantity = float(request.form.get('export_quantity', 0))
        if export_quantity <= 0:
            return "유효하지 않은 수량입니다.", 400
    except ValueError:
        return "숫자를 입력해주세요.", 400

    export_reason = request.form.get('export_reason', '')
    note = request.form.get('note', '')

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT name, current_stock FROM materials WHERE id = ?', (material_id,))
            material = cursor.fetchone()

            if not material:
                return redirect(url_for('materials.materials'))

            if material['current_stock'] < export_quantity:
                return "<script>alert('재고 부족'); history.back();</script>"

            cursor.execute(
                '''
                UPDATE materials 
                SET current_stock = current_stock - ?
                WHERE id = ? AND current_stock >= ?
            ''',
                (export_quantity, material_id, export_quantity),
            )

            cursor.execute(
                '''
                INSERT INTO material_history (material_id, type, quantity, reason, note, created_at)
                VALUES (?, 'EXPORT', ?, ?, ?, datetime('now'))
            ''',
                (material_id, export_quantity, export_reason, note),
            )

            audit_log(
                conn,
                'export',
                'material',
                material_id,
                {'quantity': export_quantity, 'reason': export_reason, 'note': note},
            )
    except Exception as e:
        return f"오류 발생: {str(e)}", 500

    return redirect(url_for('materials.materials'))


@bp.route('/materials/<int:material_id>/delete', methods=['POST'])
@login_required
def delete_material(material_id):
    """부자재 삭제"""
    if not _can_manage_material_master():
        return "<script>alert('부자재 삭제 권한이 없습니다.'); history.back();</script>"
    conn_context = db_connection()
    conn = conn_context.__enter__()
    cursor = conn.cursor()

    # BOM에 사용 중인지 확인
    cursor.execute('SELECT COUNT(*) as cnt FROM bom WHERE material_id = ?', (material_id,))
    if cursor.fetchone()['cnt'] > 0:
        return '''
            <script>
                alert('이 부자재는 BOM에 사용 중이므로 삭제할 수 없습니다.');
                window.history.back();
            </script>
        '''

    # 생산에 사용 중인지 확인
    cursor.execute('SELECT COUNT(*) as cnt FROM production_material_usage WHERE material_id = ?', (material_id,))
    if cursor.fetchone()['cnt'] > 0:
        return '''
            <script>
                alert('이 부자재는 생산 기록에 사용 중이므로 삭제할 수 없습니다.');
                window.history.back();
            </script>
        '''

    cursor.execute('SELECT COUNT(*) as cnt FROM purchase_requests WHERE material_id = ?', (material_id,))
    if cursor.fetchone()['cnt'] > 0:
        return '''
            <script>
                alert('이 부자재는 발주 이력이 있어 삭제할 수 없습니다.');
                window.history.back();
            </script>
        '''

    cursor.execute('SELECT COUNT(*) as cnt FROM logistics_issue_requests WHERE material_id = ?', (material_id,))
    if cursor.fetchone()['cnt'] > 0:
        return '''
            <script>
                alert('이 부자재는 불출 이력이 있어 삭제할 수 없습니다.');
                window.history.back();
            </script>
        '''

    cursor.execute('SELECT COUNT(*) as cnt FROM material_lots WHERE material_id = ?', (material_id,))
    if cursor.fetchone()['cnt'] > 0:
        return '''
            <script>
                alert('이 부자재는 입고 로트 이력이 있어 삭제할 수 없습니다.');
                window.history.back();
            </script>
        '''

    cursor.execute('SELECT * FROM materials WHERE id = ?', (material_id,))
    before = cursor.fetchone()

    # 삭제
    cursor.execute('DELETE FROM materials WHERE id = ?', (material_id,))
    audit_log(conn, 'delete', 'material', material_id, {'before': dict(before) if before else None})

    return redirect(url_for('materials.materials'))


# ============== 원초 관리 라우트 ==============

@bp.route('/raw-materials')
@login_required
def raw_materials():
    """?? ?? ??"""
    workplace = get_workplace()
    is_logistics = workplace == LOGISTICS_WORKPLACE
    selected_tab = (request.args.get('tab') or 'active').strip() or 'active'
    if selected_tab not in ('active', 'done'):
        selected_tab = 'active'
    selected_raw_search_field = (request.args.get('raw_search_field') or 'all').strip() or 'all'
    if selected_raw_search_field not in ('all', 'code', 'name', 'car_number', 'receiving_date'):
        selected_raw_search_field = 'all'
    raw_search_keyword = (request.args.get('raw_search_keyword') or '').strip()
    selected_raw_name = (request.args.get('raw_name') or '').strip()
    selected_done_kind = (request.args.get('done_kind') or 'all').strip() or 'all'
    if selected_done_kind not in ('all', 'production', 'export'):
        selected_done_kind = 'all'
    selected_done_scope = (request.args.get('done_scope') or '').strip().lower()
    if selected_done_scope not in ('all', 'month'):
        selected_done_scope = ''
    conn_context = db_connection()
    conn = conn_context.__enter__()
    cursor = conn.cursor()

    # ?? ?? (???? ??)
    selected_done_date = (request.args.get('done_date') or '').strip()
    current_month_key = datetime.now().strftime('%Y-%m')
    selected_done_month = (request.args.get('done_month') or '').strip()
    if selected_done_date and len(selected_done_date) >= 7:
        selected_done_month = selected_done_date[:7]
    elif not selected_done_month:
        selected_done_month = current_month_key
    if len(selected_done_month) != 7:
        selected_done_month = current_month_key
    if selected_done_date:
        selected_done_scope = 'month'
    elif selected_done_scope == '':
        selected_done_scope = 'month'
    logistics_workplace_filter = (request.args.get('logistics_workplace') or '??').strip() if is_logistics else '??'
    logistics_workplace_tabs = ['??'] + [wp for wp in WORKPLACES if wp != LOGISTICS_WORKPLACE]

    raw_query = '''
        SELECT * FROM raw_materials
        WHERE 1=1
    '''
    raw_params = []
    if not is_logistics:
        raw_query += ' AND workplace = ?'
        raw_params.append(workplace)
    if selected_raw_name:
        raw_query += " AND COALESCE(name, '') = ?"
        raw_params.append(selected_raw_name)
    if raw_search_keyword:
        like_q = f'%{raw_search_keyword}%'
        if selected_raw_search_field == 'code':
            raw_query += " AND COALESCE(code, '') LIKE ?"
            raw_params.append(like_q)
        elif selected_raw_search_field == 'name':
            raw_query += " AND COALESCE(name, '') LIKE ?"
            raw_params.append(like_q)
        elif selected_raw_search_field == 'car_number':
            raw_query += " AND COALESCE(COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')), '') LIKE ?"
            raw_params.append(like_q)
        elif selected_raw_search_field == 'receiving_date':
            raw_query += " AND COALESCE(receiving_date, '') LIKE ?"
            raw_params.append(like_q)
        else:
            raw_query += '''
                AND (
                    COALESCE(name, '') LIKE ?
                    OR COALESCE(code, '') LIKE ?
                    OR COALESCE(COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')), '') LIKE ?
                    OR COALESCE(receiving_date, '') LIKE ?
                )
            '''
            raw_params.extend([like_q, like_q, like_q, like_q])
    raw_query += '''
        ORDER BY
            name COLLATE NOCASE ASC,
            CASE WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN 1 ELSE 0 END ASC,
            receiving_date ASC,
            id ASC
    '''
    cursor.execute(raw_query, raw_params)
    raw_materials = [dict(row) for row in cursor.fetchall()]

    def attach_done_meta(rows):
        row_ids = [int(row.get('id') or 0) for row in rows if int(row.get('id') or 0) > 0]
        if not row_ids:
            for row in rows:
                row['done_kind'] = 'production'
                row['done_date'] = ''
                row['done_note'] = ''
                row['_done_kind_match'] = (selected_done_kind == 'all')
            return rows

        row_placeholders = ','.join(['?'] * len(row_ids))
        raw_done_meta = {}
        cursor.execute(
            f'''
            WITH production_done AS (
                SELECT
                    pmu.raw_material_id as raw_material_id,
                    substr(MAX(COALESCE(p.production_date, '')), 1, 10) as done_date,
                    GROUP_CONCAT(DISTINCT pr.name) as product_names
                FROM production_material_usage pmu
                JOIN productions p
                  ON p.id = pmu.production_id
                 AND COALESCE(p.status, '') = '완료'
                LEFT JOIN products pr ON pr.id = p.product_id
                WHERE pmu.raw_material_id IN ({row_placeholders})
                  AND COALESCE(pmu.actual_quantity, 0) > 0
                GROUP BY pmu.raw_material_id
            ),
            export_done AS (
                SELECT
                    rml.raw_material_id as raw_material_id,
                    substr(MAX(COALESCE(rml.created_at, '')), 1, 10) as done_date,
                    ABS(SUM(COALESCE(rml.quantity, 0))) as moved_quantity,
                    MIN(COALESCE(NULLIF(TRIM(rml.note), ''), '')) as export_note
                FROM raw_material_logs rml
                WHERE rml.raw_material_id IN ({row_placeholders})
                  AND COALESCE(rml.type, '') = 'export'
                  AND COALESCE(rml.quantity, 0) < 0
                GROUP BY rml.raw_material_id, substr(COALESCE(rml.created_at, ''), 1, 10), COALESCE(NULLIF(TRIM(rml.note), ''), '')
            ),
            export_done_latest AS (
                SELECT ed.*
                FROM export_done ed
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM export_done newer
                    WHERE newer.raw_material_id = ed.raw_material_id
                      AND (
                          COALESCE(newer.done_date, '') > COALESCE(ed.done_date, '')
                          OR (
                              COALESCE(newer.done_date, '') = COALESCE(ed.done_date, '')
                              AND COALESCE(newer.export_note, '') > COALESCE(ed.export_note, '')
                          )
                      )
                )
            )
            SELECT
                rm.id as raw_material_id,
                COALESCE(pd.done_date, '') as production_done_date,
                COALESCE(pd.product_names, '') as product_names,
                COALESCE(ed.done_date, '') as export_done_date,
                COALESCE(ed.moved_quantity, 0) as moved_quantity,
                COALESCE(ed.export_note, '') as export_note
            FROM raw_materials rm
            LEFT JOIN production_done pd ON pd.raw_material_id = rm.id
            LEFT JOIN export_done_latest ed ON ed.raw_material_id = rm.id
            WHERE rm.id IN ({row_placeholders})
            ''',
            [*row_ids, *row_ids, *row_ids],
        )
        for meta_row in cursor.fetchall():
            item = dict(meta_row)
            raw_id = int(item.get('raw_material_id') or 0)
            if raw_id <= 0:
                continue
            export_date = str(item.get('export_done_date') or '').strip()
            production_date = str(item.get('production_done_date') or '').strip()
            moved_qty = float(item.get('moved_quantity') or 0)
            export_note = str(item.get('export_note') or '').strip()
            product_names = str(item.get('product_names') or '').strip()
            done_kind = 'export' if export_date else 'production'
            done_date = export_date or production_date
            done_note = export_note if export_date else product_names
            if export_date and moved_qty > 0:
                done_note = f'이동 원초량 {moved_qty:,.0f}속 / {export_note}' if export_note else f'이동 원초량 {moved_qty:,.0f}속'
            raw_done_meta[raw_id] = {
                'done_kind': done_kind,
                'done_date': done_date,
                'done_note': done_note,
            }

        for row in rows:
            meta = raw_done_meta.get(int(row.get('id') or 0), {})
            row['done_kind'] = meta.get('done_kind', 'production')
            row['done_date'] = meta.get('done_date', '')
            row['done_note'] = meta.get('done_note', '')
            row['_done_kind_match'] = True if selected_done_kind == 'all' else row.get('done_kind') == selected_done_kind
        return rows

    raw_materials = attach_done_meta(raw_materials)

    done_query = '''
        SELECT * FROM raw_materials
        WHERE 1=1
    '''
    done_params = []
    if not is_logistics:
        done_query += ' AND workplace = ?'
        done_params.append(workplace)
    if selected_raw_name:
        done_query += " AND COALESCE(name, '') = ?"
        done_params.append(selected_raw_name)
    if raw_search_keyword:
        like_q = f'%{raw_search_keyword}%'
        if selected_raw_search_field == 'code':
            done_query += " AND COALESCE(code, '') LIKE ?"
            done_params.append(like_q)
        elif selected_raw_search_field == 'name':
            done_query += " AND COALESCE(name, '') LIKE ?"
            done_params.append(like_q)
        elif selected_raw_search_field == 'car_number':
            done_query += " AND COALESCE(COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')), '') LIKE ?"
            done_params.append(like_q)
        elif selected_raw_search_field == 'receiving_date':
            done_query += " AND COALESCE(receiving_date, '') LIKE ?"
            done_params.append(like_q)
        else:
            done_query += '''
                AND (
                    COALESCE(name, '') LIKE ?
                    OR COALESCE(code, '') LIKE ?
                    OR COALESCE(COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')), '') LIKE ?
                    OR COALESCE(receiving_date, '') LIKE ?
                )
            '''
            done_params.extend([like_q, like_q, like_q, like_q])
    done_query += '''
        AND COALESCE(current_stock, 0) <= 0
        ORDER BY
            name COLLATE NOCASE ASC,
            CASE WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN 1 ELSE 0 END ASC,
            receiving_date ASC,
            id ASC
    '''
    cursor.execute(done_query, done_params)
    done_rows_raw = [dict(row) for row in cursor.fetchall()]
    done_rows_raw = attach_done_meta(done_rows_raw)

    if is_logistics:
        cursor.execute(
            '''
            SELECT DISTINCT TRIM(COALESCE(name, '')) AS name
            FROM raw_materials
            WHERE TRIM(COALESCE(name, '')) <> ''
            ORDER BY name COLLATE NOCASE ASC
        '''
        )
    else:
        cursor.execute(
            '''
            SELECT DISTINCT TRIM(COALESCE(name, '')) AS name
            FROM raw_materials
            WHERE workplace = ?
              AND TRIM(COALESCE(name, '')) <> ''
            ORDER BY name COLLATE NOCASE ASC
        ''',
            (workplace,),
        )
    raw_name_options = [row['name'] for row in cursor.fetchall()]

    if is_logistics:
        cursor.execute(
            '''
            WITH normalized AS (
                SELECT
                    id,
                    COALESCE(NULLIF(TRIM(code), ''), '') as code,
                    name,
                    sheets_per_sok,
                    receiving_date,
                    COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')) as ja_ho
                FROM raw_materials
                WHERE TRIM(COALESCE(code, '')) <> ''
            )
            SELECT
                n.code,
                n.name,
                COALESCE(n.sheets_per_sok, 0) as sheets_per_sok,
                n.receiving_date,
                n.ja_ho
            FROM normalized n
            WHERE NOT EXISTS (
                SELECT 1
                FROM normalized newer
                WHERE newer.code = n.code
                  AND (
                      COALESCE(newer.receiving_date, '') > COALESCE(n.receiving_date, '')
                      OR (
                          COALESCE(newer.receiving_date, '') = COALESCE(n.receiving_date, '')
                          AND newer.id > n.id
                      )
                  )
            )
            ORDER BY code
        '''
        )
    else:
        cursor.execute(
            '''
            WITH normalized AS (
                SELECT
                    id,
                    COALESCE(NULLIF(TRIM(code), ''), '') as code,
                    name,
                    sheets_per_sok,
                    receiving_date,
                    COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')) as ja_ho
                FROM raw_materials
                WHERE workplace = ?
                  AND TRIM(COALESCE(code, '')) <> ''
            )
            SELECT
                n.code,
                n.name,
                COALESCE(n.sheets_per_sok, 0) as sheets_per_sok,
                n.receiving_date,
                n.ja_ho
            FROM normalized n
            WHERE NOT EXISTS (
                SELECT 1
                FROM normalized newer
                WHERE newer.code = n.code
                  AND (
                      COALESCE(newer.receiving_date, '') > COALESCE(n.receiving_date, '')
                      OR (
                          COALESCE(newer.receiving_date, '') = COALESCE(n.receiving_date, '')
                          AND newer.id > n.id
                      )
                  )
            )
            ORDER BY code
        ''',
            (workplace,),
        )
    raw_code_profiles = [dict(r) for r in cursor.fetchall()]

    def _safe_float(value):
        try:
            if value is None:
                return 0.0
            if isinstance(value, str):
                value = value.replace(',', '').strip()
                if not value:
                    return 0.0
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    done_dates = sorted(
        {
            str(row.get('done_date') or '').strip()
            for row in raw_materials
            if (
                _safe_float(row.get('current_stock')) <= 0
                and str(row.get('done_date') or '').strip()
                and bool(row.get('_done_kind_match'))
            )
        },
        reverse=True,
    )

    def build_raw_groups(rows):
        groups = {}
        order = []
        for row in rows:
            total_stock_value = _safe_float(row.get('total_stock'))
            current_stock_value = _safe_float(row.get('current_stock'))
            used_quantity_value = _safe_float(row.get('used_quantity'))
            stock_percent = int((current_stock_value / total_stock_value) * 100) if total_stock_value > 0 else 0
            if stock_percent > 50:
                stock_color = 'good'
            elif stock_percent > 20:
                stock_color = 'warn'
            else:
                stock_color = 'danger'

            row_view = dict(row)
            row_view['total_stock_value'] = total_stock_value
            row_view['current_stock_value'] = current_stock_value
            row_view['used_quantity_value'] = used_quantity_value
            row_view['stock_percent'] = stock_percent
            row_view['stock_color'] = stock_color
            row_view['current_stock_text_color'] = '#e74c3c' if current_stock_value < 10 else '#27ae60'

            code = str(row.get('code') or '').strip()
            key = code or f"NO_CODE_{int(row.get('id') or 0)}"
            if key not in groups:
                order.append(key)
                groups[key] = {
                    'group_key': key,
                    'code': code or '미지정',
                    'name': str(row.get('name') or '').strip() or '원초',
                    'items': [],
                    'total_stock_sum': 0.0,
                    'current_stock_sum': 0.0,
                    'used_quantity_sum': 0.0,
                }
            groups[key]['items'].append(row_view)
            groups[key]['total_stock_sum'] += total_stock_value
            groups[key]['current_stock_sum'] += current_stock_value
            groups[key]['used_quantity_sum'] += used_quantity_value

        grouped_rows = []
        for idx, key in enumerate(order):
            group = groups[key]
            group['color_index'] = idx % 6
            group['item_count'] = len(group['items'])
            grouped_rows.append(group)
        return grouped_rows

    active_raw_groups = build_raw_groups([row for row in raw_materials if _safe_float(row.get('current_stock')) > 0])
    if selected_done_date:
        done_source_rows = [
            row for row in done_rows_raw
            if _safe_float(row.get('current_stock')) <= 0
            and bool(row.get('_done_kind_match'))
            and str(row.get('done_date') or '').strip() == selected_done_date
        ]
    elif selected_done_scope == 'month':
        done_source_rows = [
            row for row in done_rows_raw
            if _safe_float(row.get('current_stock')) <= 0
            and bool(row.get('_done_kind_match'))
            and str(row.get('done_date') or '').strip().startswith(selected_done_month)
        ]
    else:
        done_source_rows = [
            row for row in done_rows_raw
            if _safe_float(row.get('current_stock')) <= 0
            and bool(row.get('_done_kind_match'))
        ]
    done_raw_groups = build_raw_groups(done_source_rows)

    conn_context.__exit__(None, None, None)

    return render_template(
        'raw_materials.html',
        user=session['user'],
        selected_tab=selected_tab,
        raw_materials=raw_materials,
        raw_code_profiles=raw_code_profiles,
        raw_code_profiles_json=json.dumps(raw_code_profiles, ensure_ascii=False),
        done_dates=done_dates,
        selected_done_date=selected_done_date,
        selected_done_kind=selected_done_kind,
        selected_done_scope=selected_done_scope,
        selected_done_month=selected_done_month,
        selected_raw_search_field=selected_raw_search_field,
        raw_search_keyword=raw_search_keyword,
        selected_raw_name=selected_raw_name,
        raw_name_options=raw_name_options,
        logistics_workplace_filter=logistics_workplace_filter,
        logistics_workplace_tabs=logistics_workplace_tabs,
        workplaces=WORKPLACES,
        current_workplace=workplace,
        server_today=today_local().isoformat(),
        active_raw_groups=active_raw_groups,
        done_raw_groups=done_raw_groups,
        done_raw_rows=done_source_rows,
    )


@bp.route('/raw-materials/activity')
@login_required
def raw_materials_activity():
    """원초 사용/입고/반출 일자별 요약"""
    date_param = (request.args.get('date') or '').strip()
    wp_filter = (request.args.get('wp') or 'all').strip()

    if date_param:
        try:
            target_date = datetime.strptime(date_param, '%Y-%m-%d').date()
        except ValueError:
            target_date = today_local()
    else:
        target_date = today_local()

    date_s = target_date.isoformat()

    conn_context = db_connection()
    conn = conn_context.__enter__()
    cursor = conn.cursor()
    try:
        where_clause = ''
        where_params = []
        if wp_filter != 'all':
            where_clause = 'WHERE rm.workplace = ?'
            where_params.append(wp_filter)

        params = [*where_params, date_s, date_s, date_s, date_s, date_s, date_s]
        cursor.execute(
            f'''
            WITH base_rm AS (
                SELECT
                    rm.id,
                    rm.name,
                    rm.code,
                    rm.lot,
                    rm.receiving_date,
                    COALESCE(NULLIF(TRIM(rm.ja_ho), ''), NULLIF(TRIM(rm.car_number), '')) as car_number,
                    rm.workplace,
                    COALESCE(rm.current_stock, 0) as current_stock
                FROM raw_materials rm
                {where_clause}
            ),
            used_rm AS (
                SELECT
                    pmu.raw_material_id,
                    COALESCE(SUM(COALESCE(pmu.actual_quantity, 0)), 0) as period_used_quantity,
                    GROUP_CONCAT(DISTINCT prd.name) as product_names
                FROM production_material_usage pmu
                LEFT JOIN productions p ON p.id = pmu.production_id
                LEFT JOIN products prd ON prd.id = p.product_id
                WHERE p.production_date BETWEEN ? AND ?
                  AND COALESCE(p.status, '') = '완료'
                  AND COALESCE(pmu.actual_quantity, 0) > 0
                  AND pmu.raw_material_id IS NOT NULL
                GROUP BY pmu.raw_material_id
            ),
            export_rm AS (
                SELECT
                    rml.raw_material_id,
                    COALESCE(SUM(COALESCE(rml.quantity, 0)), 0) as period_export_quantity
                FROM raw_material_logs rml
                WHERE substr(rml.created_at, 1, 10) BETWEEN ? AND ?
                  AND COALESCE(rml.type, '') = 'export'
                GROUP BY rml.raw_material_id
            ),
            rm_activity AS (
                SELECT
                    b.id,
                    b.name,
                    b.code,
                    b.lot,
                    b.receiving_date,
                    b.car_number,
                    b.workplace,
                    b.current_stock,
                    COALESCE(u.period_used_quantity, 0) as period_used_quantity,
                    COALESCE(e.period_export_quantity, 0) as period_export_quantity,
                    u.product_names as product_names,
                    CASE WHEN b.receiving_date BETWEEN ? AND ? THEN 1 ELSE 0 END as is_period_received
                FROM base_rm b
                LEFT JOIN used_rm u ON u.raw_material_id = b.id
                LEFT JOIN export_rm e ON e.raw_material_id = b.id
            )
            SELECT
                id, name, code, lot, receiving_date, car_number, workplace, current_stock,
                period_used_quantity, period_export_quantity, product_names, is_period_received
            FROM rm_activity
            WHERE period_used_quantity > 0 OR period_export_quantity > 0 OR is_period_received = 1
            ORDER BY period_used_quantity DESC, period_export_quantity DESC, is_period_received DESC, receiving_date DESC, name
            ''',
            params,
        )
        rows = [dict(r) for r in cursor.fetchall()]
        return jsonify({'ok': True, 'date': date_s, 'rows': rows})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500
    finally:
        conn_context.__exit__(None, None, None)


@bp.route('/raw-materials/<int:raw_material_id>/detail')
@login_required
def raw_material_detail(raw_material_id):
    """원초 로트 상세 조회(원초명 클릭 모달용)"""
    workplace = get_workplace()
    is_logistics = workplace == LOGISTICS_WORKPLACE
    conn_context = db_connection()
    conn = conn_context.__enter__()
    cursor = conn.cursor()
    try:
        if is_logistics:
            cursor.execute(
                '''
                SELECT id, workplace, name, code, sheets_per_sok, total_stock, current_stock, used_quantity
                FROM raw_materials
                WHERE id = ?
                ''',
                (raw_material_id,),
            )
        else:
            cursor.execute(
                '''
                SELECT id, workplace, name, code, sheets_per_sok, total_stock, current_stock, used_quantity
                FROM raw_materials
                WHERE id = ?
                  AND workplace = ?
                ''',
                (raw_material_id, workplace),
            )
        base = cursor.fetchone()
        if not base:
            return jsonify({'ok': False, 'message': 'Raw material not found.'}), 404

        code = (base['code'] or '').strip()
        if code:
            if is_logistics:
                cursor.execute(
                    '''
                    SELECT
                        id, workplace, name, code, lot, receiving_date,
                        COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')) as car_number,
                        COALESCE(sheets_per_sok, 0) as sheets_per_sok,
                        COALESCE(total_stock, 0) as total_stock,
                        COALESCE(current_stock, 0) as current_stock,
                        COALESCE(used_quantity, 0) as used_quantity
                    FROM raw_materials
                    WHERE TRIM(COALESCE(code, '')) = TRIM(COALESCE(?, ''))
                      AND COALESCE(current_stock, 0) > 0
                    ORDER BY
                        CASE WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN 1 ELSE 0 END ASC,
                        receiving_date ASC,
                        id ASC
                    ''',
                    (code,),
                )
            else:
                cursor.execute(
                    '''
                    SELECT
                        id, workplace, name, code, lot, receiving_date,
                        COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')) as car_number,
                        COALESCE(sheets_per_sok, 0) as sheets_per_sok,
                        COALESCE(total_stock, 0) as total_stock,
                        COALESCE(current_stock, 0) as current_stock,
                        COALESCE(used_quantity, 0) as used_quantity
                    FROM raw_materials
                    WHERE workplace = ?
                      AND TRIM(COALESCE(code, '')) = TRIM(COALESCE(?, ''))
                      AND COALESCE(current_stock, 0) > 0
                    ORDER BY
                        CASE WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN 1 ELSE 0 END ASC,
                        receiving_date ASC,
                        id ASC
                    ''',
                    (workplace, code),
                )
        else:
            cursor.execute(
                '''
                SELECT
                    id, workplace, name, code, lot, receiving_date,
                    COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')) as car_number,
                    COALESCE(sheets_per_sok, 0) as sheets_per_sok,
                    COALESCE(total_stock, 0) as total_stock,
                    COALESCE(current_stock, 0) as current_stock,
                    COALESCE(used_quantity, 0) as used_quantity
                FROM raw_materials
                WHERE workplace = ?
                  AND TRIM(COALESCE(name, '')) = TRIM(COALESCE(?, ''))
                  AND COALESCE(current_stock, 0) > 0
                ORDER BY
                    CASE WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN 1 ELSE 0 END ASC,
                    receiving_date ASC,
                    id ASC
                ''',
                (workplace, base['name']),
            )
        lots = [dict(row) for row in cursor.fetchall()]
        raw_ids = [int(row['id']) for row in lots if row.get('id')]
        usage_logs = []
        receive_logs = []
        if raw_ids:
            placeholders = ','.join(['?'] * len(raw_ids))
            cursor.execute(
                f'''
                SELECT
                    COALESCE(p.production_date, substr(rml.created_at, 1, 10)) as log_date,
                    rml.production_id as production_id,
                    COALESCE(pr.name, '-') as product_name,
                    COALESCE(p.production_date, substr(rml.created_at, 1, 10)) as production_date,
                    COALESCE(rm.receiving_date, '-') as receiving_date,
                    COALESCE(NULLIF(TRIM(rm.ja_ho), ''), NULLIF(TRIM(rm.car_number), ''), '-') as car_number,
                    COALESCE(ABS(rml.quantity), 0) as quantity,
                    CASE
                        WHEN COALESCE(pr.name, '') <> '' THEN pr.name || ' 생산 (' || COALESCE(p.production_date, substr(rml.created_at, 1, 10)) || ')'
                        ELSE COALESCE(rml.note, '-')
                    END as note
                FROM raw_material_logs rml
                LEFT JOIN raw_materials rm ON rm.id = rml.raw_material_id
                LEFT JOIN productions p ON rml.production_id = p.id
                LEFT JOIN products pr ON p.product_id = pr.id
                WHERE rml.raw_material_id IN ({placeholders})
                  AND COALESCE(rml.type, '') = 'production'
                  AND COALESCE(p.production_date, '') <> ''
                  AND p.product_id IS NOT NULL
                ORDER BY rml.created_at DESC, rml.id DESC
                LIMIT 200
                ''',
                raw_ids,
            )
            usage_logs = []
            for row in cursor.fetchall():
                item = dict(row)
                note_text = (item.get('note') or '').strip()
                product_name = (item.get('product_name') or '').strip()
                production_date = (item.get('production_date') or '').strip()

                if product_name and product_name != '-':
                    item['note'] = f"{product_name} 생산 ({production_date})" if production_date else f"{product_name} 생산"
                elif note_text == 'production_edit:fifo rollback':
                    item['note'] = '생산 수정: FIFO 재계산 복원'
                elif note_text.startswith('production_resave:'):
                    item['note'] = '생산 수정 후 원초 재적용'
                elif note_text.startswith('production rollback:'):
                    item['note'] = '생산 취소로 원초 복원'
                elif (
                    not note_text
                    or '??' in note_text
                    or '�' in note_text
                    or '?앹궛' in note_text
                    or '李④컧' in note_text
                ):
                    item['note'] = f"생산 차감 ({production_date})" if production_date else '생산 차감'
                else:
                    item['note'] = note_text

                usage_logs.append(item)

            cursor.execute(
                f'''
                SELECT
                    COALESCE(rm.receiving_date, substr(rml.created_at, 1, 10)) as receive_date,
                    COALESCE(NULLIF(TRIM(rm.ja_ho), ''), NULLIF(TRIM(rm.car_number), ''), '-') as car_number,
                    COALESCE(ABS(rml.quantity), 0) as quantity,
                    COALESCE(rml.note, '-') as note
                FROM raw_material_logs rml
                LEFT JOIN raw_materials rm ON rm.id = rml.raw_material_id
                WHERE rml.raw_material_id IN ({placeholders})
                  AND COALESCE(rml.type, '') = 'receive'
                ORDER BY COALESCE(rm.receiving_date, substr(rml.created_at, 1, 10)) DESC, rml.id DESC
                LIMIT 200
                ''',
                raw_ids,
            )
            receive_logs = [dict(row) for row in cursor.fetchall()]
        payload = dict(base)
        payload['lot_count'] = len(lots)
        payload['total_stock_sum'] = sum(float(row.get('total_stock') or 0) for row in lots)
        payload['current_stock_sum'] = sum(float(row.get('current_stock') or 0) for row in lots)
        payload['used_quantity_sum'] = sum(float(row.get('used_quantity') or 0) for row in lots)
        return jsonify({'ok': True, 'raw_material': payload, 'lots': lots, 'usage_logs': usage_logs, 'receive_logs': receive_logs})
    finally:
        conn_context.__exit__(None, None, None)


@bp.route('/raw-materials/<int:raw_material_id>/checksheet-preview')
@login_required
def raw_material_checksheet_preview(raw_material_id):
    workplace = get_workplace()
    production_id = int(request.args.get('production_id') or 0) if str(request.args.get('production_id') or '').strip().isdigit() else 0
    requested_receiving_date = (request.args.get('lot_receiving_date') or '').strip()
    requested_car_number = (request.args.get('lot_car_number') or '').strip()
    conn_context = db_connection()
    conn = conn_context.__enter__()
    cursor = conn.cursor()
    try:
        raw_sql = '''
            SELECT
                rm.id, rm.workplace, rm.name, rm.code, rm.lot, rm.receiving_date,
                COALESCE(NULLIF(TRIM(rm.ja_ho), ''), NULLIF(TRIM(rm.car_number), '')) as car_number,
                COALESCE(rm.sheets_per_sok, 0) as sheets_per_sok,
                COALESCE(rm.total_stock, 0) as total_stock,
                COALESCE(rm.current_stock, 0) as current_stock,
                COALESCE(rm.used_quantity, 0) as used_quantity
            FROM raw_materials rm
            WHERE rm.id = ?
              AND (
                    COALESCE(rm.workplace, '') = ?
                    OR (
                        ? > 0
                        AND EXISTS (
                            SELECT 1
                            FROM production_material_usage pmu
                            JOIN productions p ON p.id = pmu.production_id
                            WHERE pmu.raw_material_id = rm.id
                              AND pmu.production_id = ?
                              AND COALESCE(p.workplace, '') = ?
                        )
                    )
              )
        '''
        cursor.execute(raw_sql, (raw_material_id, workplace, production_id, production_id, workplace))
        raw = cursor.fetchone()
        if not raw:
            abort(404)

        aggregate_register_lot = False
        if production_id > 0:
            cursor.execute('SELECT COALESCE(entry_mode, "") as entry_mode FROM productions WHERE id = ?', (production_id,))
            production_row = cursor.fetchone()
            aggregate_register_lot = (production_row and (production_row['entry_mode'] or '').strip().lower() == 'register')

        raw_receiving_date = requested_receiving_date or (raw['receiving_date'] or '').strip()
        raw_car_number = requested_car_number or (raw['car_number'] or '').strip()
        raw_code = (raw['code'] or '').strip()
        lot_raw_ids = [int(raw_material_id)]
        lot_total_stock = float(raw['total_stock'] or 0)
        lot_current_stock = float(raw['current_stock'] or 0)
        if raw_receiving_date and raw_car_number:
            cursor.execute(
                '''
                SELECT id, COALESCE(total_stock, 0) as total_stock, COALESCE(current_stock, 0) as current_stock
                FROM raw_materials
                WHERE COALESCE(workplace, '') = ?
                  AND COALESCE(receiving_date, '') = ?
                  AND COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), ''), '') = ?
                ''',
                (workplace, raw_receiving_date, raw_car_number),
            )
            lot_rows = [dict(row) for row in cursor.fetchall()]
            if lot_rows:
                lot_raw_ids = [int(row['id']) for row in lot_rows if int(row['id'] or 0) > 0]
                lot_total_stock = sum(float(row.get('total_stock') or 0) for row in lot_rows)
                lot_current_stock = sum(float(row.get('current_stock') or 0) for row in lot_rows)
        if aggregate_register_lot and raw_receiving_date and raw_car_number:
            cursor.execute(
                '''
                SELECT
                    p.id as production_id,
                    substr(p.production_date, 1, 10) as use_date,
                    COALESCE(pmu.actual_quantity, 0) as used_quantity,
                    COALESCE(pr.name, '') as product_name,
                    COALESCE(NULLIF(TRIM(COALESCE(pmu.usage_note, '')), ''), '') as usage_note,
                    COALESCE(rcn.note, '') as checksheet_note,
                    COALESCE(p.status, '') as production_status
                FROM production_material_usage pmu
                JOIN productions p
                  ON p.id = pmu.production_id
                 AND COALESCE(p.production_date, '') <> ''
                LEFT JOIN products pr ON pr.id = p.product_id
                LEFT JOIN raw_materials rm ON rm.id = pmu.raw_material_id
                LEFT JOIN raw_material_checksheet_notes rcn
                  ON rcn.raw_material_id = pmu.raw_material_id
                  AND rcn.use_date = substr(p.production_date, 1, 10)
                WHERE COALESCE(p.workplace, '') = ?
                  AND LOWER(COALESCE(p.entry_mode, '')) = 'register'
                  AND COALESCE(NULLIF(TRIM(pmu.override_receiving_date), ''), rm.receiving_date, '') = ?
                  AND COALESCE(NULLIF(TRIM(pmu.override_car_number), ''), NULLIF(TRIM(rm.ja_ho), ''), NULLIF(TRIM(rm.car_number), ''), '') = ?
                  AND COALESCE(pmu.actual_quantity, 0) > 0
                ORDER BY substr(p.production_date, 1, 10) DESC, p.id DESC, pmu.id DESC
                ''',
                (workplace, raw_receiving_date, raw_car_number),
            )
        else:
            cursor.execute(
                '''
                SELECT
                    p.id as production_id,
                    substr(p.production_date, 1, 10) as use_date,
                    COALESCE(pmu.actual_quantity, 0) as used_quantity,
                    COALESCE(pr.name, '') as product_name,
                    COALESCE(NULLIF(TRIM(COALESCE(pmu.usage_note, '')), ''), '') as usage_note,
                    COALESCE(rcn.note, '') as checksheet_note,
                    COALESCE(p.status, '') as production_status
                FROM production_material_usage pmu
                JOIN productions p
                  ON p.id = pmu.production_id
                 AND COALESCE(p.production_date, '') <> ''
                LEFT JOIN products pr ON pr.id = p.product_id
                LEFT JOIN raw_material_checksheet_notes rcn
                  ON rcn.raw_material_id = pmu.raw_material_id
                  AND rcn.use_date = substr(p.production_date, 1, 10)
                WHERE pmu.raw_material_id = ?
                  AND (? <= 0 OR pmu.production_id = ?)
                  AND COALESCE(pmu.actual_quantity, 0) > 0
                ORDER BY substr(p.production_date, 1, 10) DESC, p.id DESC, pmu.id DESC
                ''',
                (raw_material_id, production_id, production_id),
            )
        production_usage_rows = [dict(row) for row in cursor.fetchall()]
        if aggregate_register_lot and production_id > 0 and not production_usage_rows:
            cursor.execute(
                '''
                SELECT
                    p.id as production_id,
                    substr(p.production_date, 1, 10) as use_date,
                    COALESCE(pmu.actual_quantity, 0) as used_quantity,
                    COALESCE(pr.name, '') as product_name,
                    COALESCE(NULLIF(TRIM(COALESCE(pmu.usage_note, '')), ''), '') as usage_note,
                    COALESCE(rcn.note, '') as checksheet_note,
                    COALESCE(p.status, '') as production_status
                FROM production_material_usage pmu
                JOIN productions p
                  ON p.id = pmu.production_id
                 AND COALESCE(p.production_date, '') <> ''
                LEFT JOIN products pr ON pr.id = p.product_id
                LEFT JOIN raw_material_checksheet_notes rcn
                  ON rcn.raw_material_id = pmu.raw_material_id
                  AND rcn.use_date = substr(p.production_date, 1, 10)
                WHERE pmu.raw_material_id = ?
                  AND pmu.production_id = ?
                  AND COALESCE(pmu.actual_quantity, 0) > 0
                ORDER BY substr(p.production_date, 1, 10) DESC, p.id DESC, pmu.id DESC
                ''',
                (raw_material_id, production_id),
            )
            production_usage_rows = [dict(row) for row in cursor.fetchall()]

        production_usage_map = {}
        register_production_used_total = 0.0
        done_status = _normalize_production_status('완료')
        for row in production_usage_rows:
            is_selected_register_row = aggregate_register_lot and production_id > 0 and int(row.get('production_id') or 0) == production_id
            if _normalize_production_status(row.get('production_status')) != done_status and not is_selected_register_row:
                continue
            if aggregate_register_lot:
                register_production_used_total += float(row.get('used_quantity') or 0)
            use_date = row.get('use_date') or ''
            checksheet_note = (row.get('checksheet_note') or '').strip()
            key = (use_date, checksheet_note)
            entry = production_usage_map.setdefault(
                key,
                {
                    'use_date': use_date,
                    'used_quantity': 0.0,
                    'product_names': [],
                    'usage_notes': [],
                    'checksheet_note': checksheet_note,
                    'sort_group': 1,
                    'log_kind': 'production',
                },
            )
            entry['used_quantity'] += float(row.get('used_quantity') or 0)
            product_name = (row.get('product_name') or '').strip()
            if product_name and product_name not in entry['product_names']:
                entry['product_names'].append(product_name)
            usage_note = (row.get('usage_note') or '').strip()
            if usage_note:
                entry['usage_notes'].append(usage_note)

        lot_placeholders = ','.join(['?'] * len(lot_raw_ids))
        cursor.execute(
            f'''
            SELECT
                substr(rml.created_at, 1, 10) as use_date,
                ABS(COALESCE(SUM(COALESCE(rml.quantity, 0)), 0)) as used_quantity,
                '' as product_names,
                '' as usage_notes,
                COALESCE(NULLIF(TRIM(rml.note), ''), '') as checksheet_note,
                2 as sort_group,
                'export' as log_kind
            FROM raw_material_logs rml
            WHERE rml.raw_material_id IN ({lot_placeholders})
              AND COALESCE(rml.type, '') = 'export'
              AND COALESCE(rml.quantity, 0) < 0
            GROUP BY substr(rml.created_at, 1, 10), COALESCE(NULLIF(TRIM(rml.note), ''), '')
            ''',
            lot_raw_ids,
        )
        export_usage_rows = [dict(row) for row in cursor.fetchall()]

        usage_logs = []
        for entry in production_usage_map.values():
            usage_logs.append(
                {
                    'use_date': entry['use_date'],
                    'used_quantity': entry['used_quantity'],
                    'product_names': ', '.join(entry['product_names']),
                    'usage_notes': chr(31).join(entry['usage_notes']),
                    'checksheet_note': entry['checksheet_note'],
                    'sort_group': entry['sort_group'],
                    'log_kind': entry['log_kind'],
                }
            )
        usage_logs.extend(export_usage_rows)
        usage_logs = [row for row in usage_logs if float(row.get('used_quantity') or 0) > 0]
        usage_logs.sort(key=lambda row: int(row.get('sort_group') or 0))
        usage_logs.sort(key=lambda row: row.get('use_date') or '', reverse=True)
        usage_logs = usage_logs[:14]

        total_stock = lot_total_stock
        current_stock = lot_current_stock
        if aggregate_register_lot:
            total_stock = register_production_used_total
        reconstructed_rows = []
        if aggregate_register_lot:
            display_logs = sorted(usage_logs, key=lambda row: row.get('use_date') or '')
            running_before = total_stock
        else:
            display_logs = usage_logs
            running_after = current_stock
        for log in display_logs:
            used_qty = float(log.get('used_quantity') or 0)
            log_kind = str(log.get('log_kind') or 'production').strip()
            default_note = ', '.join([x.strip() for x in (log.get('product_names') or '').split(',') if x.strip()])
            usage_note_lines = []
            for raw_note in [x.strip() for x in (log.get('usage_notes') or '').split(chr(31)) if x.strip()]:
                for line in raw_note.splitlines():
                    clean_line = line.strip()
                    if clean_line.startswith('원초 변경:') or clean_line.startswith('임시 원초 추가:') or clean_line.startswith('임시 원초 혼용:'):
                        usage_note_lines.append(clean_line)
            note_parts = []
            checksheet_note = (log.get('checksheet_note') or '').strip()
            if checksheet_note:
                note_parts.append(checksheet_note)
            elif default_note:
                note_parts.append(default_note)
            for line in usage_note_lines:
                if line not in note_parts:
                    note_parts.append(line)
            note = ' / '.join(note_parts)
            display_used_qty = used_qty
            if log_kind == 'export':
                display_used_qty = 0.0
                move_text = f'원초 반출 {used_qty:,.0f}속'
                note = f'{move_text} / {note}' if note else move_text
            if aggregate_register_lot:
                row_stock = max(running_before - used_qty, 0)
                running_before = row_stock
            else:
                row_stock = running_after
            reconstructed_rows.append(
                {
                    'use_date': log.get('use_date') or '',
                    'site_stock': f'{row_stock:,.0f}',
                    'warehouse_stock': f'{0:,.0f}',
                    'total_stock': f'{row_stock:,.0f}',
                    'used_quantity': f'{display_used_qty:,.0f}',
                    'note': note,
                }
            )
            if not aggregate_register_lot:
                running_after = max(running_after + used_qty, 0)
        usage_rows = reconstructed_rows if aggregate_register_lot else list(reversed(reconstructed_rows))
        # Keep extra blank rows available so the print template can fill the page
        # and then hide only the overflow rows in fitUsageRowsToSinglePage().
        target_usage_rows = 12
        while len(usage_rows) < target_usage_rows:
            usage_rows.append(
                {
                    'use_date': '',
                    'site_stock': '',
                    'warehouse_stock': '',
                    'total_stock': '',
                    'used_quantity': '',
                    'note': '',
                }
            )

        use_dates = [row['use_date'] for row in usage_rows if row['use_date']]
        if raw_receiving_date or use_dates:
            start_text = raw_receiving_date or min(use_dates)
            end_text = max(use_dates) if use_dates else start_text
            period_text = f"{start_text} ~ {end_text}"
        else:
            period_text = '-'

        sheets_per_sok = float(raw['sheets_per_sok'] or 0)
        if aggregate_register_lot and raw_code:
            cursor.execute(
                '''
                SELECT COALESCE(MAX(COALESCE(sheets_per_sok, 0)), 0) as sheets_per_sok
                FROM raw_materials
                WHERE COALESCE(workplace, '') = ?
                  AND COALESCE(NULLIF(TRIM(code), ''), '') = ?
                  AND COALESCE(sheets_per_sok, 0) > 0
                ''',
                (workplace, raw_code),
            )
            sheet_row = cursor.fetchone()
            sheets_per_sok = float(sheet_row['sheets_per_sok'] or 0) if sheet_row else sheets_per_sok
        box_qty = (total_stock / sheets_per_sok) if sheets_per_sok > 0 else 0
        workplace_title = str(workplace or '').replace('2층', '2F').replace('1층', '1F')
        author_name = (session.get('user', {}) or {}).get('name') or (session.get('user', {}) or {}).get('username') or ''

        intake = {
            'receiving_date': raw_receiving_date,
            'name': raw['name'] or '',
            'car_number': raw_car_number,
            'sheets_per_sok_text': f'{sheets_per_sok:,.0f}' if sheets_per_sok else '',
            'box_quantity_text': f'{box_qty:,.0f}' if box_qty else '',
            'total_stock_text': f'{total_stock:,.0f}' if total_stock else '',
        }

        return render_template(
            'raw_checksheet_preview.html',
            raw_material_id=raw_material_id,
            intake=intake,
            usage_rows=usage_rows[:target_usage_rows],
            period_text=period_text,
            workplace_title=workplace_title,
            author_name=author_name,
        )
    finally:
        conn_context.__exit__(None, None, None)


@bp.route('/raw-materials/<int:raw_material_id>/checksheet-notes', methods=['POST'])
@login_required
def save_raw_material_checksheet_notes(raw_material_id):
    workplace = get_workplace()
    payload = request.get_json(silent=True) or {}
    rows = payload.get('rows') or []

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT id
                FROM raw_materials
                WHERE id = ?
                  AND workplace = ?
                ''',
                (raw_material_id, workplace),
            )
            raw = cursor.fetchone()
            if not raw:
                return jsonify({'ok': False, 'message': '원초를 찾을 수 없습니다.'}), 404

            username = (session.get('user', {}) or {}).get('username') or ''
            cleaned_rows = []
            for row in rows:
                use_date = str((row or {}).get('use_date') or '').strip()
                note = str((row or {}).get('note') or '').rstrip()
                if not use_date:
                    continue
                cleaned_rows.append((use_date, note))

            if cleaned_rows:
                cursor.executemany(
                    '''
                    INSERT INTO raw_material_checksheet_notes
                    (raw_material_id, use_date, note, created_by, updated_by, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(raw_material_id, use_date) DO UPDATE SET
                        note = excluded.note,
                        updated_by = excluded.updated_by,
                        updated_at = CURRENT_TIMESTAMP
                    ''',
                    [(raw_material_id, use_date, note, username, username) for use_date, note in cleaned_rows],
                )
                cursor.executemany(
                    '''
                    DELETE FROM raw_material_checksheet_notes
                    WHERE raw_material_id = ?
                      AND use_date = ?
                      AND COALESCE(TRIM(note), '') = ''
                    ''',
                    [(raw_material_id, use_date) for use_date, _note in cleaned_rows],
                )

            return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


@bp.route('/raw-material-lots/<int:lot_id>/update', methods=['POST'])
@role_required('rawmat')
def update_raw_material_lot(lot_id):
    """원초 로트 수정(모달 inline 편집)"""
    workplace = get_workplace()
    receiving_date = (request.form.get('receiving_date') or '').strip() or None
    ja_ho = (request.form.get('ja_ho') or request.form.get('car_number') or '').strip()
    sheets_per_sok = float(request.form.get('sheets_per_sok') or 0)
    total_stock = float(request.form.get('total_stock') or 0)
    current_stock = float(request.form.get('current_stock') or 0)

    if total_stock < 0 or current_stock < 0:
        return jsonify({'ok': False, 'message': 'Stock must be >= 0.'}), 400
    if current_stock > total_stock:
        return jsonify({'ok': False, 'message': 'Current stock cannot exceed total stock.'}), 400

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM raw_materials WHERE id = ? AND workplace = ?', (lot_id, workplace))
            before = cursor.fetchone()
            if not before:
                return jsonify({'ok': False, 'message': 'Raw lot not found.'}), 404

            used_quantity = total_stock - current_stock
            cursor.execute(
                '''
                UPDATE raw_materials
                SET receiving_date = ?, ja_ho = ?, car_number = ?, sheets_per_sok = ?, total_stock = ?, current_stock = ?, used_quantity = ?
                WHERE id = ?
                ''',
                (receiving_date, ja_ho, ja_ho, sheets_per_sok, total_stock, current_stock, used_quantity, lot_id),
            )
            final_code, lot = _ensure_raw_code_and_lot(cursor, lot_id, before['code'], receiving_date, ja_ho)
            audit_log(
                conn,
                'update',
                'raw_material_lot',
                lot_id,
                {
                    'before': dict(before),
                    'after': {
                        'receiving_date': receiving_date,
                        'ja_ho': ja_ho,
                        'sheets_per_sok': sheets_per_sok,
                        'total_stock': total_stock,
                        'current_stock': current_stock,
                        'used_quantity': used_quantity,
                        'code': final_code,
                        'lot': lot,
                    },
                },
            )
            return jsonify({'ok': True, 'lot': lot})
    except Exception:
        return jsonify({'ok': False, 'message': 'Failed to update raw lot.'}), 500


@bp.route('/raw-materials/add', methods=['POST'])
@role_required('rawmat')
def add_raw_material():
    """원초 추가"""
    workplace = get_workplace()
    code = (request.form.get('code') or '').strip()
    name = request.form.get('name')
    sheets_per_sok = request.form.get('sheets_per_sok')
    receiving_date = request.form.get('receiving_date')
    ja_ho = (request.form.get('ja_ho') or request.form.get('car_number') or '').strip()
    total_stock = request.form.get('total_stock', 0)
    current_stock = request.form.get('current_stock', 0)

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO raw_materials (name, code, lot, sheets_per_sok, receiving_date, ja_ho, car_number, total_stock, current_stock, used_quantity, workplace)
            VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, 0, ?)
        ''',
            (
                name,
                code,
                sheets_per_sok,
                receiving_date if receiving_date else None,
                ja_ho,
                ja_ho,
                total_stock,
                current_stock,
                workplace,
            ),
        )
        raw_id = cursor.lastrowid
        final_code, lot = _ensure_raw_code_and_lot(cursor, raw_id, code, receiving_date, ja_ho)

        audit_log(
            conn,
            'create',
            'raw_material',
            raw_id,
            {
                'name': name,
                'code': final_code,
                'lot': lot,
                'sheets_per_sok': sheets_per_sok,
                'receiving_date': receiving_date,
                'ja_ho': ja_ho,
                'total_stock': total_stock,
                'current_stock': current_stock,
                'workplace': workplace,
            },
        )

    return redirect(url_for('materials.raw_materials'))


@bp.route('/raw-materials/update-basic', methods=['POST'])
@role_required('rawmat')
def update_raw_material_basic():
    raw_id = int(request.form.get('id') or 0)
    workplace = get_workplace()
    code = (request.form.get('code') or '').strip()
    name = (request.form.get('name') or '').strip()
    receiving_date = (request.form.get('receiving_date') or '').strip() or None
    ja_ho = (request.form.get('ja_ho') or request.form.get('car_number') or '').strip()

    try:
        sheets_per_sok = float(request.form.get('sheets_per_sok') or 0)
    except (TypeError, ValueError):
        sheets_per_sok = 0
    try:
        total_stock = float(request.form.get('total_stock') or 0)
    except (TypeError, ValueError):
        total_stock = 0
    try:
        current_stock = float(request.form.get('current_stock') or 0)
    except (TypeError, ValueError):
        current_stock = 0

    if raw_id <= 0:
        return redirect(url_for('materials.raw_materials'))
    if not name:
        return redirect(url_for('materials.raw_materials'))
    if sheets_per_sok < 0:
        sheets_per_sok = 0
    if total_stock < 0:
        total_stock = 0
    if current_stock < 0:
        current_stock = 0
    if current_stock > total_stock:
        current_stock = total_stock
    used_quantity = total_stock - current_stock

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT *
                FROM raw_materials
                WHERE id = ?
                  AND workplace = ?
                ''',
                (raw_id, workplace),
            )
            before = cursor.fetchone()
            if not before:
                return redirect(url_for('materials.raw_materials'))

            cursor.execute(
                '''
                UPDATE raw_materials
                SET code = ?, name = ?, sheets_per_sok = ?, receiving_date = ?, ja_ho = ?, car_number = ?,
                    total_stock = ?, current_stock = ?, used_quantity = ?
                WHERE id = ?
                  AND workplace = ?
                ''',
                (
                    code,
                    name,
                    sheets_per_sok,
                    receiving_date,
                    ja_ho,
                    ja_ho,
                    total_stock,
                    current_stock,
                    used_quantity,
                    raw_id,
                    workplace,
                ),
            )
            final_code, lot = _ensure_raw_code_and_lot(cursor, raw_id, code, receiving_date, ja_ho)
            audit_log(
                conn,
                'update',
                'raw_material',
                raw_id,
                {
                    'before': dict(before),
                    'after': {
                        'name': name,
                        'code': final_code,
                        'lot': lot,
                        'sheets_per_sok': sheets_per_sok,
                        'receiving_date': receiving_date,
                        'ja_ho': ja_ho,
                        'total_stock': total_stock,
                        'current_stock': current_stock,
                        'used_quantity': used_quantity,
                    },
                },
            )
    except Exception:
        pass

    return redirect(url_for('materials.raw_materials'))


@bp.route('/raw-materials/update-stock', methods=['POST'])
@role_required('rawmat')
def update_raw_material_stock():
    """원초 재고 조정 + 로그 기록 (autocommit)"""
    raw_material_id = request.form.get('raw_material_id')
    log_type = request.form.get('log_type', 'adjustment')

    if log_type not in ('receive', 'export'):
        return "<script>alert('원초 조정 기능은 제거되었습니다.'); location.href='/raw-materials';</script>"

    conn = None
    try:
        with db_connection() as conn:
            cursor = conn.cursor()

        # 트랜잭션 시작
        begin_db_transaction(conn, mode='IMMEDIATE')

        # 현재 재고 조회
        cursor.execute(
            '''
            SELECT current_stock, name, code, receiving_date, COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')) as ja_ho
            FROM raw_materials
            WHERE id = ?
            ''',
            (raw_material_id,),
        )
        result = cursor.fetchone()
        old_stock = result['current_stock'] if result else 0
        raw_code = result['code'] if result else None
        lot_receiving_date = result['receiving_date'] if result else None
        lot_car_number = result['ja_ho'] if result else None

        # 로그 타입에 따른 처리
        if log_type == 'receive':
            # 입고
            receive_qty = float(request.form.get('receive_quantity', 0))
            new_stock = old_stock + receive_qty
            quantity_change = receive_qty
            note = request.form.get('note', '입고')

            # 입고 정보 업데이트
            receiving_date = request.form.get('receiving_date')
            ja_ho = (request.form.get('ja_ho') or request.form.get('car_number') or '').strip()
            lot_receiving_date = receiving_date
            lot_car_number = ja_ho

            cursor.execute(
                '''
                UPDATE raw_materials
                SET current_stock = ?, total_stock = total_stock + ?, receiving_date = ?, ja_ho = ?, car_number = ?
                WHERE id = ?
            ''',
                (new_stock, receive_qty, receiving_date, ja_ho, ja_ho, raw_material_id),
            )

        elif log_type == 'export':
            # 반출
            export_qty = float(request.form.get('export_quantity', 0))
            new_stock = old_stock - export_qty
            quantity_change = -export_qty
            export_reason = request.form.get('export_reason', '')
            note = f"반출 - {export_reason}: {request.form.get('note', '')}"

            cursor.execute(
                '''
                UPDATE raw_materials
                SET current_stock = ?
                WHERE id = ?
            ''',
                (new_stock, raw_material_id),
            )

        else:
            # 일반 조정
            new_stock = float(request.form.get('current_stock'))
            quantity_change = new_stock - old_stock
            note = request.form.get('note', '재고 조정')

            cursor.execute(
                '''
                UPDATE raw_materials
                SET current_stock = ?, total_stock = total_stock + ?
                WHERE id = ?
            ''',
                (new_stock, quantity_change, raw_material_id),
            )

        # 로그 기록
        final_code, lot = _ensure_raw_code_and_lot(
            cursor,
            raw_material_id,
            raw_code,
            lot_receiving_date,
            lot_car_number,
        )

        cursor.execute(
            '''
            INSERT INTO raw_material_logs (raw_material_id, type, quantity, note, created_by)
            VALUES (?, ?, ?, ?, ?)
        ''',
            (raw_material_id, log_type, quantity_change, note, session['user']['username']),
        )

        # ★ 재고가 0 이하가 되면 BOM에서 자동 제거
        audit_log(
            conn,
            'update_stock',
            'raw_material',
            raw_material_id,
            {
                'log_type': log_type,
                'old_stock': old_stock,
                'new_stock': new_stock,
                'quantity_change': quantity_change,
                'code': final_code,
                'lot': lot,
                'note': note,
            },
        )

        # 트랜잭션 커밋
        commit_db(conn)

    except Exception as e:
        if conn:
            try:
                rollback_db(conn)
            except Exception:
                pass
        print(f"Error in update_raw_material_stock: {e}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        if conn:
            close_db(conn)

    return redirect(url_for('materials.raw_materials'))


@bp.route('/raw-materials/<int:raw_material_id>/logs-data')
@login_required
def raw_material_logs_data(raw_material_id):
    """원초 로그 JSON 데이터"""
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT rml.*, p.production_date, pr.name as product_name
            FROM raw_material_logs rml
            LEFT JOIN productions p ON rml.production_id = p.id
            LEFT JOIN products pr ON p.product_id = pr.id
            WHERE rml.raw_material_id = ?
            ORDER BY rml.created_at DESC
        ''',
            (raw_material_id,),
        )
        logs = cursor.fetchall()

    # 로그 타입 텍스트 변환
    type_map = {
        'production': '생산 차감',
        'receive': '입고',
        'export': '반출',
        'adjustment': '재고 조정',
    }

    logs_data = []
    for log in logs:
        log_dict = dict(log)
        log_dict['type_text'] = type_map.get(log['type'], log['type'])

        note_text = (log_dict.get('note') or '').strip()
        if log['type'] == 'production':
            if log['product_name']:
                log_dict['note'] = f"{log['product_name']} 생산 ({log['production_date']})"
            else:
                log_dict['note'] = f"생산 차감 ({log['production_date']})" if log['production_date'] else '생산 차감'
        elif note_text == 'production_edit:fifo rollback':
            log_dict['note'] = '생산 수정: FIFO 재계산 복원'
        elif note_text.startswith('생산 삭제:'):
            log_dict['note'] = note_text
        elif not note_text or '??' in note_text or '�' in note_text:
            fallback_map = {
                'receive': '입고',
                'export': '반출',
                'adjustment': '재고 조정',
            }
            log_dict['note'] = fallback_map.get(log['type'], '-')
        else:
            log_dict['note'] = note_text

        logs_data.append(log_dict)

    return {'logs': logs_data}


@bp.route('/raw-materials/<int:raw_material_id>/logs')
@login_required
def raw_material_logs(raw_material_id):
    """원초 사용 로그 조회"""
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM raw_materials WHERE id = ?', (raw_material_id,))
        raw_material = cursor.fetchone()
        cursor.execute(
            '''
            SELECT rml.*, p.production_date, pr.name as product_name
            FROM raw_material_logs rml
            LEFT JOIN productions p ON rml.production_id = p.id
            LEFT JOIN products pr ON p.product_id = pr.id
            WHERE rml.raw_material_id = ?
            ORDER BY rml.created_at DESC
        ''',
            (raw_material_id,),
        )
        logs = cursor.fetchall()

    return render_template('raw_material_logs.html', user=session['user'], raw_material=raw_material, logs=logs)


@bp.route('/raw-materials/<int:raw_material_id>/delete', methods=['POST'])
@role_required('rawmat')
def delete_raw_material(raw_material_id):
    """원초 삭제 (BOM에서도 자동 제거)"""
    with db_transaction() as conn:
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) as cnt FROM bom WHERE raw_material_id = ?', (raw_material_id,))
        bom_count = cursor.fetchone()['cnt']

        cursor.execute(
            '''
            SELECT COUNT(*) as cnt FROM production_material_usage 
            WHERE raw_material_id = ? AND actual_quantity > 0
        ''',
            (raw_material_id,),
        )
        usage_count = cursor.fetchone()['cnt']

        if usage_count > 0:
            return '''
                <script>
                    alert('이 원초는 생산 기록에 사용되었으므로 삭제할 수 없습니다.');
                    window.history.back();
                </script>
            '''

        cursor.execute('SELECT * FROM raw_materials WHERE id = ?', (raw_material_id,))
        before = cursor.fetchone()

        if bom_count > 0:
            cursor.execute('DELETE FROM bom WHERE raw_material_id = ?', (raw_material_id,))

        cursor.execute('DELETE FROM raw_material_logs WHERE raw_material_id = ?', (raw_material_id,))
        cursor.execute('DELETE FROM raw_materials WHERE id = ?', (raw_material_id,))
        audit_log(conn, 'delete', 'raw_material', raw_material_id, {'before': dict(before) if before else None})

    return redirect(url_for('materials.raw_materials'))


# ============== 발주/발주요청 ==============

@bp.route('/purchase-orders')
@login_required
def purchase_orders():
    """?? ?? - ???? ??"""
    workplace = get_workplace()
    is_logistics = False
    return _render_purchase_orders_page(
        workplace=workplace,
        is_logistics=is_logistics,
        page_title='\ubc1c\uc8fc \uad00\ub9ac',
        page_mode='purchase',
        show_logistics_hub=False,
        show_low_stock_tab=True,
        disable_purchase_actions=False,
    )


@bp.route('/logistics-materials')
@login_required
def logistics_materials():
    return redirect(url_for('materials.materials', req_tab='issue'))


def _render_purchase_orders_page(
    workplace,
    is_logistics=False,
    page_title='\ubc1c\uc8fc \uad00\ub9ac',
    page_mode='purchase',
    show_logistics_hub=False,
    show_low_stock_tab=True,
    disable_purchase_actions=False,
):
    conn_context = db_connection()
    conn = conn_context.__enter__()
    cursor = conn.cursor()
    _cleanup_orphan_material_refs(conn)
    _sync_material_stock_with_lots(conn)

    month_param = request.args.get('month', '')
    logistics_workplace_filter = (request.args.get('logistics_workplace') or '전체').strip() if is_logistics else '전체'
    logistics_workplace_tabs = ['전체'] + [wp for wp in WORKPLACES if wp != LOGISTICS_WORKPLACE]

    if is_logistics:
        cursor.execute(
            '''
            SELECT pr.*, m.name as material_name, m.code as material_code, m.unit, m.current_stock, m.min_stock, m.unit_price,
                   m.moq, m.lead_time,
                   s.name as supplier_name
            FROM purchase_requests pr
            JOIN materials m ON pr.material_id = m.id
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            WHERE pr.status IN ('발주필요', '발주중')
              AND pr.received_at IS NULL
            ORDER BY pr.status, pr.requested_at DESC
        '''
        )
    else:
        cursor.execute(
            '''
            SELECT pr.*, m.name as material_name, m.code as material_code, m.unit, m.current_stock, m.min_stock, m.unit_price,
                   m.moq, m.lead_time,
                   s.name as supplier_name
            FROM purchase_requests pr
            JOIN materials m ON pr.material_id = m.id
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            WHERE pr.status IN ('발주필요', '발주중')
              AND pr.received_at IS NULL
            AND pr.workplace IN (?, ?)
            ORDER BY pr.status, pr.requested_at DESC
        ''',
            (workplace, SHARED_WORKPLACE),
        )
    requests_active = cursor.fetchall()
    if is_logistics and logistics_workplace_filter != '전체':
        requests_active = [row for row in requests_active if (row['workplace'] or '') == logistics_workplace_filter]

    if month_param:
        if is_logistics:
            cursor.execute(
                '''
                SELECT pr.*, m.name as material_name, m.code as material_code, m.unit, m.unit_price,
                       s.name as supplier_name
                FROM purchase_requests pr
                JOIN materials m ON pr.material_id = m.id
                LEFT JOIN suppliers s ON m.supplier_id = s.id
                WHERE pr.status = '입고완료'
                AND pr.received_at IS NOT NULL
                AND strftime('%Y-%m', pr.received_at) = ?
                ORDER BY pr.received_at DESC
            ''',
                (month_param,),
            )
        else:
            cursor.execute(
                '''
                SELECT pr.*, m.name as material_name, m.code as material_code, m.unit, m.unit_price,
                       s.name as supplier_name
                FROM purchase_requests pr
                JOIN materials m ON pr.material_id = m.id
                LEFT JOIN suppliers s ON m.supplier_id = s.id
                WHERE pr.status = '입고완료'
                AND pr.received_at IS NOT NULL
                AND strftime('%Y-%m', pr.received_at) = ?
                AND pr.workplace IN (?, ?)
                ORDER BY pr.received_at DESC
            ''',
                (month_param, workplace, SHARED_WORKPLACE),
            )
    else:
        if is_logistics:
            cursor.execute(
                '''
                SELECT pr.*, m.name as material_name, m.code as material_code, m.unit, m.unit_price,
                       s.name as supplier_name
                FROM purchase_requests pr
                JOIN materials m ON pr.material_id = m.id
                LEFT JOIN suppliers s ON m.supplier_id = s.id
                WHERE pr.status = '입고완료'
                AND pr.received_at IS NOT NULL
                ORDER BY pr.received_at DESC
                LIMIT 50
            '''
            )
        else:
            cursor.execute(
                '''
                SELECT pr.*, m.name as material_name, m.code as material_code, m.unit, m.unit_price,
                       s.name as supplier_name
                FROM purchase_requests pr
                JOIN materials m ON pr.material_id = m.id
                LEFT JOIN suppliers s ON m.supplier_id = s.id
                WHERE pr.status = '입고완료'
                AND pr.received_at IS NOT NULL
                AND pr.workplace IN (?, ?)
                ORDER BY pr.received_at DESC
                LIMIT 50
            ''',
                (workplace, SHARED_WORKPLACE),
            )
    requests_done = cursor.fetchall()

    today = now_local()
    month_anchor = today.replace(day=1)
    months = []
    for i in range(6):
        if month_anchor.month - i > 0:
            m = month_anchor.replace(month=month_anchor.month - i)
        else:
            m = month_anchor.replace(year=month_anchor.year - 1, month=12 + (month_anchor.month - i))
        months.append(m.strftime('%Y-%m'))

    if is_logistics:
        cursor.execute(
            '''
            SELECT m.*, s.name as supplier_name,
                   CASE WHEN pr.id IS NOT NULL THEN 1 ELSE 0 END as already_requested
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            LEFT JOIN purchase_requests pr
              ON pr.material_id = m.id
             AND pr.status != '입고완료'
            WHERE m.current_stock < m.min_stock AND m.min_stock > 0
            ORDER BY (m.min_stock - m.current_stock) DESC
        '''
        )
    else:
        cursor.execute(
            '''
            SELECT m.*, s.name as supplier_name,
                   CASE WHEN pr.id IS NOT NULL THEN 1 ELSE 0 END as already_requested
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            LEFT JOIN purchase_requests pr
              ON pr.material_id = m.id
             AND pr.status != '입고완료'
             AND pr.workplace IN (?, ?)
            WHERE m.current_stock < m.min_stock AND m.min_stock > 0
              AND (m.workplace = ? OR m.workplace = ? OR m.workplace IS NULL)
            ORDER BY (m.min_stock - m.current_stock) DESC
        ''',
            (workplace, SHARED_WORKPLACE, workplace, SHARED_WORKPLACE),
        )
    low_stock = cursor.fetchall()

    cursor.execute('SELECT id, name, contact, address FROM suppliers ORDER BY name')
    suppliers = cursor.fetchall()
    if is_logistics:
        cursor.execute(
            '''
            SELECT m.id, m.code, m.name, m.unit, m.moq, m.lead_time, s.name as supplier_name
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            ORDER BY m.category, m.name
        '''
        )
    else:
        cursor.execute(
            '''
            SELECT m.id, m.code, m.name, m.unit, m.moq, m.lead_time, s.name as supplier_name
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            WHERE (m.workplace = ? OR m.workplace = ? OR m.workplace IS NULL)
            ORDER BY m.category, m.name
        ''',
            (workplace, SHARED_WORKPLACE),
        )
    order_materials = cursor.fetchall()

    today_str = today_local_str()
    logistics_focus_date = None
    logistics_focus_items = []
    logistics_pending_receipt_count = 0
    logistics_received_pending_count = 0
    logistics_receive_action_count = 0
    logistics_issue_action_count = 0
    logistics_focus_issue_requests = []
    if show_logistics_hub:
        cursor.execute(
            '''
            SELECT material_code, material_name, unit, quantity
            FROM logistics_stocks
            WHERE COALESCE(quantity, 0) > 0
            ORDER BY material_name
            '''
        )
        logistics_stocks = cursor.fetchall()

        cursor.execute(
            '''
            SELECT expected_delivery_date AS target_date, workplace
            FROM purchase_requests pr
            WHERE expected_delivery_date IS NOT NULL
              AND TRIM(expected_delivery_date) != ''
              AND EXISTS (
                    SELECT 1
                    FROM materials m
                    WHERE m.id = pr.material_id
                  )
              AND pr.received_at IS NULL
              AND COALESCE(pr.ordered_quantity, 0) > 0
            '''
        )
        focus_dates = []
        for row in cursor.fetchall():
            if logistics_workplace_filter != '전체' and (row['workplace'] or '') != logistics_workplace_filter:
                continue
            focus_dates.append(row['target_date'])

        cursor.execute(
            '''
            SELECT substr(requested_at, 1, 10) AS target_date, requester_workplace
            FROM logistics_issue_requests
            WHERE status = ?
              AND COALESCE(request_type, 'ISSUE') = 'ISSUE'
            ''',
            (ISSUE_STATUS_REQUESTED,),
        )
        for row in cursor.fetchall():
            if logistics_workplace_filter != '전체' and (row['requester_workplace'] or '') != logistics_workplace_filter:
                continue
            focus_dates.append(row['target_date'])

        logistics_focus_date = min(focus_dates) if focus_dates else None

        if logistics_focus_date:
            cursor.execute(
                '''
                SELECT pr.id, pr.material_id, pr.status, pr.expected_delivery_date, pr.ordered_quantity, pr.received_quantity,
                       pr.received_at, pr.workplace,
                       m.name AS material_name, m.code AS material_code, m.unit
                FROM purchase_requests pr
                JOIN materials m ON m.id = pr.material_id
                WHERE pr.expected_delivery_date = ?
                  AND TRIM(pr.expected_delivery_date) != ''
                  AND pr.received_at IS NULL
                  AND COALESCE(pr.ordered_quantity, 0) > 0
                ORDER BY m.name
                ''',
                (logistics_focus_date,),
            )
            logistics_focus_items = cursor.fetchall()
            if logistics_workplace_filter != '전체':
                logistics_focus_items = [row for row in logistics_focus_items if (row['workplace'] or '') == logistics_workplace_filter]
            logistics_pending_receipt_count = len(logistics_focus_items)

            cursor.execute(
                '''
                SELECT COUNT(*) AS cnt
                FROM purchase_requests pr
                JOIN materials m ON m.id = pr.material_id
                WHERE pr.expected_delivery_date = ?
                  AND TRIM(pr.expected_delivery_date) != ''
                  AND pr.received_at IS NOT NULL
                ''',
                (logistics_focus_date,),
            )
            received_row = cursor.fetchone()
            logistics_received_pending_count = int((received_row['cnt'] if received_row else 0) or 0)
            if logistics_workplace_filter != '전체':
                cursor.execute(
                    '''
                    SELECT COUNT(*) AS cnt
                    FROM purchase_requests pr
                    JOIN materials m ON m.id = pr.material_id
                    WHERE pr.expected_delivery_date = ?
                      AND TRIM(pr.expected_delivery_date) != ''
                      AND pr.received_at IS NOT NULL
                      AND pr.workplace = ?
                    ''',
                    (logistics_focus_date, logistics_workplace_filter),
                )
                received_row = cursor.fetchone()
                logistics_received_pending_count = int((received_row['cnt'] if received_row else 0) or 0)

            cursor.execute(
                '''
                SELECT *
                FROM logistics_issue_requests
                WHERE status = ?
                  AND COALESCE(request_type, 'ISSUE') = 'ISSUE'
                  AND substr(requested_at, 1, 10) = ?
                ORDER BY requested_at
                ''',
                (ISSUE_STATUS_REQUESTED, logistics_focus_date),
            )
            logistics_focus_issue_requests = cursor.fetchall()
            if logistics_workplace_filter != '전체':
                logistics_focus_issue_requests = [row for row in logistics_focus_issue_requests if (row['requester_workplace'] or '') == logistics_workplace_filter]
            logistics_issue_action_count = len(logistics_focus_issue_requests)

        logistics_due_today = logistics_focus_items if logistics_focus_date == today_str else []
        logistics_overdue = logistics_focus_items if logistics_focus_date and logistics_focus_date < today_str else []

        cursor.execute(
            '''
            SELECT *
            FROM logistics_issue_requests
            WHERE status = ?
              AND COALESCE(request_type, 'ISSUE') = 'ISSUE'
            ORDER BY requested_at
            ''',
            (ISSUE_STATUS_REQUESTED,),
        )
        issue_requests = cursor.fetchall()
        cursor.execute(
            '''
            SELECT lir.*, ml.lot AS material_lot
            FROM logistics_issue_requests lir
            LEFT JOIN material_lots ml ON ml.id = lir.material_lot_id
            WHERE status = ?
              AND COALESCE(lir.request_type, 'ISSUE') = 'RETURN'
            ORDER BY lir.requested_at
            ''',
            (ISSUE_STATUS_REQUESTED,),
        )
        export_issue_requests = cursor.fetchall()
        if logistics_workplace_filter != '전체':
            issue_requests = [row for row in issue_requests if (row['requester_workplace'] or '') == logistics_workplace_filter]
            export_issue_requests = [row for row in export_issue_requests if (row['requester_workplace'] or '') == logistics_workplace_filter]
        logistics_receive_action_count = logistics_pending_receipt_count
    else:
        logistics_stocks = []
        logistics_due_today = []
        logistics_overdue = []
        cursor.execute(
            '''
            SELECT *
            FROM logistics_issue_requests
            WHERE requester_workplace = ?
              AND COALESCE(request_type, 'ISSUE') = 'ISSUE'
            ORDER BY requested_at DESC
            LIMIT 30
            ''',
            (workplace,),
        )
        issue_requests = cursor.fetchall()
        export_issue_requests = []

    conn_context.__exit__(None, None, None)
    receive_next_page = 'logistics' if page_mode == 'logistics' else 'purchase'

    return render_template(
        'purchase_orders.html',
        user=session['user'],
        page_title=page_title,
        page_mode=page_mode,
        requests_active=requests_active,
        requests_done=requests_done,
        low_stock=low_stock,
        suppliers=suppliers,
        order_materials=order_materials,
        months=months,
        selected_month=month_param,
        current_workplace=workplace,
        logistics_stocks=logistics_stocks,
        logistics_due_today=logistics_due_today,
        logistics_overdue=logistics_overdue,
        issue_requests=issue_requests,
        export_issue_requests=export_issue_requests,
        today_str=today_str,
        logistics_focus_date=logistics_focus_date,
        logistics_focus_items=logistics_focus_items,
        logistics_pending_receipt_count=logistics_pending_receipt_count,
        logistics_received_pending_count=logistics_received_pending_count,
        logistics_receive_action_count=logistics_receive_action_count,
        logistics_issue_action_count=logistics_issue_action_count,
        logistics_focus_issue_requests=logistics_focus_issue_requests,
        logistics_workplace_filter=logistics_workplace_filter,
        logistics_workplace_tabs=logistics_workplace_tabs,
        show_logistics_hub=show_logistics_hub,
        show_low_stock_tab=show_low_stock_tab,
        disable_purchase_actions=disable_purchase_actions,
        receive_next_page=receive_next_page,
    )


@bp.route('/logistics-ledger')
@login_required
@role_required('logistics')
def logistics_ledger():
    """물류 전용 원부자재 수불대장"""
    q = (request.args.get('q') or '').strip()
    search_field = (request.args.get('search_field') or 'all').strip()
    category_q = (request.args.get('category') or '').strip()
    item_type_q = (request.args.get('item_type') or '').strip()
    workplace_q = (request.args.get('workplace') or '').strip()
    product_id_q = (request.args.get('product_id') or '').strip()
    workplaces = _ledger_workplaces()
    conn_context = db_connection()
    conn = conn_context.__enter__()
    cursor = conn.cursor()
    try:
        _sync_missing_logistics_lot_balances(conn)
        cursor.execute(
            """
            SELECT id, code, name, workplace
            FROM products
            ORDER BY name
            """
        )
        product_options = cursor.fetchall()

        selected_material_ids = set()
        selected_raw_ids = set()
        if product_id_q:
            try:
                product_id_int = int(product_id_q)
            except (TypeError, ValueError):
                product_id_int = 0
            if product_id_int > 0:
                cursor.execute(
                    """
                    SELECT material_id, raw_material_id
                    FROM bom
                    WHERE product_id = ?
                    """,
                    (product_id_int,),
                )
                for bom_row in cursor.fetchall():
                    if bom_row['material_id']:
                        selected_material_ids.add(int(bom_row['material_id']))
                    if bom_row['raw_material_id']:
                        selected_raw_ids.add(int(bom_row['raw_material_id']))

        cursor.execute(
            """
            SELECT m.id, m.code, m.name, COALESCE(m.category, '') AS category, m.unit, m.unit_price, COALESCE(s.name, '-') AS supplier_name
            FROM materials m
            LEFT JOIN suppliers s ON s.id = m.supplier_id
            ORDER BY m.name
            """
        )
        material_master = cursor.fetchall()

        cursor.execute(
            """
            SELECT ml.material_id, l.name AS location_name, COALESCE(SUM(b.qty), 0) AS qty
            FROM inv_material_lot_balances b
            JOIN material_lots ml ON ml.id = b.material_lot_id
            JOIN inv_locations l ON l.id = b.location_id
            WHERE COALESCE(ml.is_disposed, 0) = 0
            GROUP BY ml.material_id, l.name
            """
        )
        material_balances = {}
        for row in cursor.fetchall():
            material_balances.setdefault(int(row['material_id']), {})[row['location_name']] = float(row['qty'] or 0)

        ledger_rows = []
        for m in material_master:
            by_wp = {}
            for wp in workplaces:
                by_wp[wp] = round(float(material_balances.get(int(m['id']), {}).get(wp, 0) or 0), 2)
            logistics_qty = round(float(material_balances.get(int(m['id']), {}).get('물류창고', 0) or 0), 2)
            total_qty = round(sum(by_wp.values()) + logistics_qty, 2)
            ledger_rows.append(
                {
                    'row_type': 'material',
                    'row_id': int(m['id']),
                    'code': _normalize_ledger_code(m['code'], 'M', m['id']),
                    'name': m['name'],
                    'category': str(m['category'] or '').strip() or '-',
                    'supplier_name': m['supplier_name'] or '-',
                    'item_type': '부자재',
                    'unit': _normalize_material_unit(m['unit']) or '-',
                    'unit_price': float(m['unit_price'] or 0),
                    'by_wp': by_wp,
                    'logistics_qty': logistics_qty,
                    'total_qty': total_qty,
                }
            )

        cursor.execute(
            """
            SELECT id, name, COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id)) AS code, workplace, COALESCE(current_stock, 0) AS current_stock
            FROM raw_materials
            """
        )
        raw_rows = cursor.fetchall()
        raw_agg = {}
        for rr in raw_rows:
            code = _normalize_ledger_code(rr['code'], 'RM', rr['id'])
            key = code
            if key not in raw_agg:
                raw_agg[key] = {
                    'row_type': 'raw_material',
                    'row_id': int(rr['id']),
                    'code': code,
                    'name': rr['name'],
                    'category': '원초',
                    'supplier_name': '-',
                    'item_type': '원초',
                    'unit': '속',
                    'unit_price': 0.0,
                    'by_wp': {wp: 0.0 for wp in workplaces},
                    'logistics_qty': 0.0,
                }
            wp = (rr['workplace'] or '').strip()
            qty = float(rr['current_stock'] or 0)
            if wp in raw_agg[key]['by_wp']:
                raw_agg[key]['by_wp'][wp] = round(raw_agg[key]['by_wp'][wp] + qty, 2)
            elif wp == LOGISTICS_WORKPLACE:
                raw_agg[key]['logistics_qty'] = round(raw_agg[key]['logistics_qty'] + qty, 2)

        for rv in raw_agg.values():
            rv['total_qty'] = round(sum(rv['by_wp'].values()) + rv['logistics_qty'], 2)
            ledger_rows.append(rv)

        def _matches(value, keyword):
            return not keyword or keyword.lower() in str(value or '').lower()

        def _matches_workplace(row):
            if not workplace_q:
                return True
            if product_id_q:
                if row.get('row_type') == 'material' and int(row.get('row_id') or 0) in selected_material_ids:
                    return True
                if row.get('row_type') == 'raw_material' and int(row.get('row_id') or 0) in selected_raw_ids:
                    return True
            if workplace_q == LOGISTICS_WORKPLACE:
                return float(row.get('logistics_qty') or 0) > 0
            return float((row.get('by_wp') or {}).get(workplace_q, 0) or 0) > 0

        def _matches_product(row):
            if not product_id_q:
                return True
            if row.get('row_type') == 'material':
                return int(row.get('row_id') or 0) in selected_material_ids
            if row.get('row_type') == 'raw_material':
                return int(row.get('row_id') or 0) in selected_raw_ids
            return False

        def _matches_keyword(row):
            if not q:
                return True
            if search_field == 'code':
                return _matches(row.get('code'), q)
            if search_field == 'name':
                return _matches(row.get('name'), q)
            if search_field == 'supplier':
                return _matches(row.get('supplier_name'), q)
            if search_field == 'category':
                return _matches(row.get('category'), q)
            if search_field == 'item_type':
                return _matches(row.get('item_type'), q)
            return (
                _matches(row.get('code'), q)
                or _matches(row.get('name'), q)
                or _matches(row.get('supplier_name'), q)
                or _matches(row.get('category'), q)
                or _matches(row.get('item_type'), q)
            )

        if any([q, category_q, item_type_q, workplace_q, product_id_q]):
            ledger_rows = [
                row for row in ledger_rows
                if (
                    _matches_keyword(row)
                    and _matches(row.get('category'), category_q)
                    and _matches(row.get('item_type'), item_type_q)
                    and _matches_workplace(row)
                    and _matches_product(row)
                )
            ]

        ledger_rows.sort(key=lambda x: ((x.get('code') or ''), (x.get('name') or '')))
        category_options = sorted({str((row.get('category') or '')).strip() for row in ledger_rows if str((row.get('category') or '')).strip()})
    finally:
        conn_context.__exit__(None, None, None)

    return render_template(
        'logistics_ledger.html',
        user=session['user'],
        q=q,
        search_field=search_field,
        category_q=category_q,
        item_type_q=item_type_q,
        workplace_q=workplace_q,
        product_id_q=product_id_q,
        product_options=product_options,
        workplace_options=workplaces + [LOGISTICS_WORKPLACE],
        category_options=category_options,
        workplaces=workplaces,
        rows=ledger_rows,
    )


@bp.route('/purchase-orders/add', methods=['GET', 'POST'])
@role_required('purchase')
def add_purchase_order():
    """발주 추가"""
    if request.method == 'GET':
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, name FROM suppliers ORDER BY name')
            suppliers = cursor.fetchall()

        return render_template('purchase_order_add.html', user=session['user'], suppliers=suppliers)

    # POST 처리
    supplier_id = request.form.get('supplier_id')
    order_date = request.form.get('order_date')
    expected_delivery_date = request.form.get('expected_delivery_date')
    requester = request.form.get('requester')
    note = request.form.get('note')

    # 발주 항목 (여러 개)
    material_ids = request.form.getlist('material_id[]')
    quantities = request.form.getlist('quantity[]')
    unit_prices = request.form.getlist('unit_price[]')

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO purchase_orders 
            (supplier_id, order_date, expected_delivery_date, requester, note, status)
            VALUES (?, ?, ?, ?, ?, '발주')
        ''',
            (supplier_id, order_date, expected_delivery_date, requester, note),
        )

        order_id = cursor.lastrowid

        for i in range(len(material_ids)):
            if material_ids[i]:
                quantity = float(quantities[i])
                unit_price = float(unit_prices[i])
                total_price = quantity * unit_price

                cursor.execute(
                    '''
                    INSERT INTO purchase_order_items
                    (purchase_order_id, material_id, quantity, unit_price, total_price)
                    VALUES (?, ?, ?, ?, ?)
                ''',
                    (order_id, material_ids[i], quantity, unit_price, total_price),
                )

    return redirect(request.referrer or url_for('materials.materials', req_tab='issue'))


@bp.route('/purchase-orders/<int:order_id>/receive', methods=['POST'])
@role_required('logistics')
def receive_purchase_order(order_id):
    """입고 처리"""
    if not _is_logistics_manager():
        return "<script>alert('물류관리 권한이 필요합니다.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()

        actual_delivery_date = request.form.get('actual_delivery_date')
        cursor.execute(
            '''
            UPDATE purchase_orders
            SET actual_delivery_date = ?, status = '입고완료'
            WHERE id = ?
        ''',
            (actual_delivery_date, order_id),
        )

        cursor.execute(
            '''
            SELECT id, material_id, quantity
            FROM purchase_order_items
            WHERE purchase_order_id = ?
        ''',
            (order_id,),
        )

        items = cursor.fetchall()

        for item in items:
            item_id = item[0]
            material_id = item[1]
            quantity = item[2]
            received_qty_key = f'received_{item_id}'
            received_qty = request.form.get(received_qty_key, quantity)

            cursor.execute(
                '''
                UPDATE purchase_order_items
                SET received_quantity = ?
                WHERE id = ?
            ''',
                (received_qty, item_id),
            )

            cursor.execute('SELECT id, code, name, unit FROM materials WHERE id = ?', (material_id,))
            mat_row = cursor.fetchone()
            if mat_row:
                pool_code = _pool_code_from_row(mat_row)
                _increase_logistics_stock(
                    cursor,
                    pool_code,
                    mat_row['name'],
                    mat_row['unit'],
                    received_qty,
                    session.get('user', {}).get('name'),
                )

    return redirect(url_for('materials.materials', req_tab='issue', issue_status='pending'))


# ============== 발주 요청 라우트 ==============

@bp.route('/purchase-requests/add', methods=['POST'])
@role_required('purchase')
def add_purchase_request():
    """\ubc1c\uc8fc \uc694\uccad \ub4f1\ub85d (\ubc1c\uc8fc\ud544\uc694 -> \ubc1c\uc8fc\uc911)"""
    workplace = get_workplace()
    material_id = request.form.get('material_id')
    quantity = request.form.get('quantity', 0)
    expected_date = (request.form.get('expected_delivery_date') or '').strip()
    note = request.form.get('note', '')
    if not expected_date:
        return "<script>alert('\uc785\uace0 \uc608\uc815\uc77c\uc744 \ub4f1\ub85d\ud574 \uc8fc\uc138\uc694.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT workplace, category, name, unit FROM materials WHERE id=?",
            (material_id,),
        )
        mat = cursor.fetchone()
        mat_category = (mat['category'] or '').strip() if mat else ''
        mat_workplace = mat['workplace'] if mat else None
        mat_name = (mat['name'] or '\ubd80\uc790\uc7ac') if mat else '\ubd80\uc790\uc7ac'
        mat_unit = (mat['unit'] or '').strip() if mat else ''
        target_workplace = (
            SHARED_WORKPLACE
            if mat_category in SHARED_MATERIAL_CATEGORIES or mat_workplace in (None, SHARED_WORKPLACE)
            else workplace
        )

        cursor.execute(
            "SELECT id FROM purchase_requests WHERE material_id=? AND status!=? AND workplace=?",
            (material_id, PURCHASE_STATUS_RECEIVED, target_workplace),
        )
        existing = cursor.fetchone()
        username = session['user'].get('name') if session.get('user') else None
        requester_username = session['user'].get('username') if session.get('user') else None

        if existing:
            cursor.execute(
                """
                UPDATE purchase_requests
                SET ordered_quantity=?, expected_delivery_date=?, status=?,
                    ordered_at=CURRENT_TIMESTAMP, note=?, ordered_by=?, requester_username=?
                WHERE id=?
            """,
                (quantity, expected_date, PURCHASE_STATUS_ORDERED, note, username, requester_username, existing['id']),
            )
            audit_log(
                conn,
                'update',
                'purchase_request',
                existing['id'],
                {
                    'ordered_quantity': quantity,
                    'expected_delivery_date': expected_date,
                    'status': PURCHASE_STATUS_ORDERED,
                    'note': note,
                    'ordered_by': username,
                    'requester_username': requester_username,
                    'workplace': target_workplace,
                },
            )
        else:
            cursor.execute(
                """
                INSERT INTO purchase_requests
                (material_id, status, requested_quantity, ordered_quantity, expected_delivery_date, note, ordered_at, workplace, ordered_by, requester_username)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?)
            """,
                (material_id, PURCHASE_STATUS_ORDERED, quantity, quantity, expected_date, note, target_workplace, username, requester_username),
            )
            audit_log(
                conn,
                'create',
                'purchase_request',
                cursor.lastrowid,
                {
                    'material_id': material_id,
                    'status': PURCHASE_STATUS_ORDERED,
                    'requested_quantity': quantity,
                    'ordered_quantity': quantity,
                    'expected_delivery_date': expected_date,
                    'note': note,
                    'workplace': target_workplace,
                    'ordered_by': username,
                    'requester_username': requester_username,
                },
            )

        logistics_users = get_usernames_for_notification(conn, roles=['logistics'], include_admin=True)
        change_label = '\ubc1c\uc8fc \uc694\uccad \ubcc0\uacbd' if existing else '\uc0c8 \ubc1c\uc8fc \uc694\uccad'
        _notify_users(
            conn,
            logistics_users,
            f"{change_label}: {mat_name}",
            f"{target_workplace} / \uc218\ub7c9 {float(quantity or 0):g}{mat_unit} / \uc785\uace0\uc608\uc815 {expected_date}",
            '/purchase-orders',
        )
    return redirect(url_for('materials.materials', req_tab='issue', issue_status='pending'))


@bp.route('/purchase-requests/bulk-add-from-materials', methods=['POST'])
@role_required('purchase')
def bulk_add_purchase_requests_from_materials():
    """부자재 목록에서 선택한 항목을 일괄 발주 등록한다."""
    workplace = get_workplace()
    expected_date = (request.form.get('expected_delivery_date') or '').strip()
    note = (request.form.get('note') or '').strip()
    ordered_at = _local_timestamp_str()
    selected_material_ids = [mid for mid in request.form.getlist('material_ids') if (mid or '').strip()]

    if not expected_date:
        return "<script>alert('입고 예정일을 등록해 주세요.'); history.back();</script>"
    if not selected_material_ids:
        return "<script>alert('발주 등록할 부자재를 선택해 주세요.'); history.back();</script>"

    username = session['user'].get('name') if session.get('user') else None
    requester_username = session['user'].get('username') if session.get('user') else None
    created_count = 0
    updated_count = 0

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            for material_id in selected_material_ids:
                qty_raw = (request.form.get(f'qty_{material_id}') or '').strip()
                try:
                    quantity = float(qty_raw or 0)
                except ValueError:
                    continue
                if quantity <= 0:
                    continue

                cursor.execute(
                    "SELECT id, workplace, category FROM materials WHERE id=?",
                    (material_id,),
                )
                mat = cursor.fetchone()
                if not mat:
                    continue

                mat_category = (mat['category'] or '').strip()
                mat_workplace = mat['workplace']
                target_workplace = (
                    SHARED_WORKPLACE
                    if mat_category in SHARED_MATERIAL_CATEGORIES or mat_workplace in (None, SHARED_WORKPLACE)
                    else workplace
                )

                cursor.execute(
                    "SELECT id FROM purchase_requests WHERE material_id=? AND status!=? AND workplace=?",
                    (material_id, PURCHASE_STATUS_RECEIVED, target_workplace),
                )
                existing = cursor.fetchone()

                if existing:
                    cursor.execute(
                        """
                        UPDATE purchase_requests
                        SET requested_quantity=?, ordered_quantity=?, expected_delivery_date=?,
                            status=?, ordered_at=CURRENT_TIMESTAMP, note=?, ordered_by=?, requester_username=?
                        WHERE id=?
                    """,
                        (
                            quantity,
                            quantity,
                            expected_date,
                            PURCHASE_STATUS_ORDERED,
                            note,
                            username,
                            requester_username,
                            existing['id'],
                        ),
                    )
                    updated_count += 1
                else:
                    cursor.execute(
                        """
                        INSERT INTO purchase_requests
                        (material_id, status, requested_quantity, ordered_quantity, expected_delivery_date, note, ordered_at, workplace, ordered_by, requester_username)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?)
                    """,
                        (
                            material_id,
                            PURCHASE_STATUS_ORDERED,
                            quantity,
                            quantity,
                            expected_date,
                            note,
                            target_workplace,
                            username,
                            requester_username,
                        ),
                    )
                    created_count += 1

            if created_count == 0 and updated_count == 0:
                return "<script>alert('선택한 부자재의 발주 수량을 확인해 주세요.'); history.back();</script>"
    except Exception as e:
        return f"<script>alert('일괄 발주 등록 중 오류가 발생했습니다: {str(e)}'); history.back();</script>"

    return redirect(url_for('materials.materials', req_tab='issue'))


@bp.route('/issue-requests/add', methods=['POST'])
@login_required
def add_issue_request():
    workplace = get_workplace()
    if workplace == LOGISTICS_WORKPLACE:
        return "<script>alert('물류 작업장에서는 불출 요청을 등록할 수 없습니다.'); history.back();</script>"

    material_id = request.form.get('material_id')
    requested_qty = float(request.form.get('requested_quantity') or 0)
    note = (request.form.get('note') or '').strip()
    if not material_id or requested_qty <= 0:
        return "<script>alert('불출 요청 수량을 확인해주세요.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, code, name, unit FROM materials WHERE id = ?', (material_id,))
        mat = cursor.fetchone()
        if not mat:
            return "<script>alert('자재를 찾을 수 없습니다.'); history.back();</script>"

        material_code = _pool_code_from_row(mat)
        req_user = session.get('user', {}).get('name') or session.get('user', {}).get('username')
        req_username = session.get('user', {}).get('username')
        cursor.execute(
            '''
            INSERT INTO logistics_issue_requests
            (material_id, material_code, material_name, unit, requester_workplace, requested_quantity, request_type, note, requested_by, requester_username, requested_at)
            VALUES (?, ?, ?, ?, ?, ?, 'ISSUE', ?, ?, ?, ?)
            ''',
            (material_id, material_code, mat['name'], mat['unit'], workplace, requested_qty, note, req_user, req_username, requested_at),
        )
        req_id = cursor.lastrowid
        audit_log(
            conn,
            'create',
            'logistics_issue_request',
            req_id,
            {
                'material_id': material_id,
                'material_code': material_code,
                'material_name': mat['name'],
                'requester_workplace': workplace,
                'requested_quantity': requested_qty,
                'note': note,
                'requested_by': req_user,
                'requester_username': req_username,
            },
        )

    return redirect(url_for('materials.materials', req_tab='issue'))


@bp.route('/issue-requests/bulk-add', methods=['POST'])
@login_required
def bulk_add_issue_request():
    workplace = get_workplace()
    if workplace == LOGISTICS_WORKPLACE:
        return "<script>alert('물류 작업장에서는 불출 요청을 등록할 수 없습니다.'); history.back();</script>"

    material_ids = request.form.getlist('material_id[]')
    requested_quantities = request.form.getlist('requested_quantity[]')
    notes = request.form.getlist('note[]')

    rows = []
    for i, mid in enumerate(material_ids):
        material_id = (mid or '').strip()
        if not material_id:
            continue
        qty_raw = requested_quantities[i] if i < len(requested_quantities) else '0'
        try:
            qty = float(qty_raw or 0)
        except ValueError:
            qty = 0
        if qty <= 0:
            continue
        note = (notes[i] if i < len(notes) else '').strip()
        rows.append((material_id, qty, note))

    if not rows:
        return "<script>alert('요청할 자재와 수량을 입력해주세요.'); history.back();</script>"

    created_count = 0
    req_user = session.get('user', {}).get('name') or session.get('user', {}).get('username')
    req_username = session.get('user', {}).get('username')
    requested_at = _local_timestamp_str()
    with db_transaction() as conn:
        cursor = conn.cursor()
        for material_id, qty, note in rows:
            cursor.execute('SELECT id, code, name, unit FROM materials WHERE id = ?', (material_id,))
            mat = cursor.fetchone()
            if not mat:
                continue
            material_code = _pool_code_from_row(mat)
            cursor.execute(
                '''
                INSERT INTO logistics_issue_requests
                (material_id, material_code, material_name, unit, requester_workplace, requested_quantity, request_type, note, requested_by, requester_username, requested_at)
                VALUES (?, ?, ?, ?, ?, ?, 'ISSUE', ?, ?, ?, ?)
                ''',
                (material_id, material_code, mat['name'], mat['unit'], workplace, qty, note, req_user, req_username, requested_at),
            )
            req_id = cursor.lastrowid
            created_count += 1
            audit_log(
                conn,
                'create',
                'logistics_issue_request',
                req_id,
                {
                    'material_id': material_id,
                    'material_code': material_code,
                    'material_name': mat['name'],
                    'requester_workplace': workplace,
                    'requested_quantity': qty,
                    'note': note,
                    'requested_by': req_user,
                    'requester_username': req_username,
                    'bulk': True,
                },
            )

    if created_count == 0:
        return "<script>alert('요청 가능한 자재가 없습니다.'); history.back();</script>"
    return redirect(url_for('materials.materials', req_tab='issue'))


@bp.route('/export-requests/add', methods=['POST'])
@login_required
def add_export_request():
    workplace = get_workplace()
    if workplace == LOGISTICS_WORKPLACE:
        return _alert_back('물류 작업장에서는 반출 요청을 등록할 수 없습니다.')

    material_id = int(request.form.get('material_id') or 0)
    lot_id = int(request.form.get('material_lot_id') or 0)
    reason = (request.form.get('reason') or '').strip()
    reason_detail = (request.form.get('reason_detail') or '').strip()
    note = (request.form.get('note') or '').strip()
    try:
        quantity = _round_to_1_decimal(request.form.get('requested_quantity') or 0)
    except ValueError:
        quantity = 0

    req_user = session.get('user', {}).get('name') or session.get('user', {}).get('username')
    req_username = session.get('user', {}).get('username')
    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            req_id, pool_code, lot_text = _register_export_request_row(
                cursor, workplace, req_user, req_username, material_id, lot_id, quantity, reason, reason_detail, note
            )

            audit_log(
                conn,
                'create',
                'logistics_return_request',
                req_id,
                {
                    'material_id': material_id,
                    'material_code': pool_code,
                    'material_lot_id': lot_id,
                    'lot': lot_text,
                    'workplace': workplace,
                    'quantity': quantity,
                    'reason': reason,
                    'reason_detail': reason_detail,
                    'note': note,
                },
            )
    except ValueError as e:
        return _alert_back(str(e))
    except Exception:
        return _alert_back('반출 요청 등록 중 오류가 발생했습니다.')

    return redirect(url_for('materials.materials', req_tab='export'))


@bp.route('/export-requests/bulk-add', methods=['POST'])
@login_required
def bulk_add_export_request():
    workplace = get_workplace()
    if workplace == LOGISTICS_WORKPLACE:
        return _alert_back('물류 작업장에서는 반출 요청을 등록할 수 없습니다.')

    material_ids = request.form.getlist('material_id[]')
    lot_ids = request.form.getlist('material_lot_id[]')
    quantities = request.form.getlist('requested_quantity[]')
    reasons = request.form.getlist('reason[]')
    reason_details = request.form.getlist('reason_detail[]')
    notes = request.form.getlist('note[]')

    rows = []
    for i, raw_mid in enumerate(material_ids):
        try:
            mid = int(raw_mid or 0)
        except ValueError:
            mid = 0
        try:
            lid = int((lot_ids[i] if i < len(lot_ids) else 0) or 0)
        except ValueError:
            lid = 0
        try:
            qty = _round_to_1_decimal((quantities[i] if i < len(quantities) else 0) or 0)
        except ValueError:
            qty = 0
        reason = (reasons[i] if i < len(reasons) else '').strip()
        reason_detail = (reason_details[i] if i < len(reason_details) else '').strip()
        note = (notes[i] if i < len(notes) else '').strip()
        if mid <= 0 or lid <= 0 or qty <= 0:
            continue
        rows.append((mid, lid, qty, reason, reason_detail, note))

    if not rows:
        return _alert_back('반출 요청할 항목을 추가해주세요.')

    req_user = session.get('user', {}).get('name') or session.get('user', {}).get('username')
    req_username = session.get('user', {}).get('username')
    success_count = 0
    failed_items = []
    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            for idx, (mid, lid, qty, reason, reason_detail, note) in enumerate(rows, start=1):
                try:
                    req_id, pool_code, lot_text = _register_export_request_row(
                        cursor, workplace, req_user, req_username, mid, lid, qty, reason, reason_detail, note
                    )
                    audit_log(
                        conn,
                        'create',
                        'logistics_return_request',
                        req_id,
                        {
                            'material_id': mid,
                            'material_code': pool_code,
                            'material_lot_id': lid,
                            'lot': lot_text,
                            'workplace': workplace,
                            'quantity': qty,
                            'reason': reason,
                            'reason_detail': reason_detail,
                            'note': note,
                            'bulk': True,
                            'row_index': idx,
                        },
                    )
                    success_count += 1
                except ValueError as e:
                    failed_items.append(f'{idx}. {_describe_export_request_item(cursor, mid, lid)} - {str(e)}')
        if failed_items:
            summary_lines = [f'반출 요청 등록 완료 {success_count}건, 실패 {len(failed_items)}건']
            summary_lines.extend(failed_items)
            return _alert_redirect('\n'.join(summary_lines), url_for('materials.materials', req_tab='export'))
    except Exception:
        return _alert_back('반출 요청 등록 중 오류가 발생했습니다.')

    return redirect(url_for('materials.materials', req_tab='export'))


@bp.route('/issue-requests/<int:req_id>/update', methods=['POST'])
@login_required
def update_issue_request(req_id):
    workplace = get_workplace()
    if workplace == LOGISTICS_WORKPLACE:
        return "<script>alert('물류 작업장에서는 불출 요청 수정을 할 수 없습니다.'); history.back();</script>"

    try:
        requested_qty = float(request.form.get('requested_quantity') or 0)
    except ValueError:
        requested_qty = 0
    if requested_qty <= 0:
        return "<script>alert('요청 수량을 확인해주세요.'); history.back();</script>"

    note = (request.form.get('note') or '').strip()

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM logistics_issue_requests WHERE id = ?', (req_id,))
        req_row = cursor.fetchone()
        if not req_row:
            return "<script>alert('불출 요청을 찾을 수 없습니다.'); history.back();</script>"
        if (req_row['request_type'] or 'ISSUE') != 'ISSUE':
            return "<script>alert('불출 요청 건만 수정할 수 있습니다.'); history.back();</script>"
        if req_row['requester_workplace'] != workplace:
            return "<script>alert('현재 작업장의 요청만 수정할 수 있습니다.'); history.back();</script>"
        if (req_row['status'] or '') != ISSUE_STATUS_REQUESTED or req_row['processed_at']:
            return "<script>alert('요청 중 상태의 불출 요청만 수정할 수 있습니다.'); history.back();</script>"

        cursor.execute(
            '''
            UPDATE logistics_issue_requests
            SET requested_quantity = ?, note = ?
            WHERE id = ?
            ''',
            (requested_qty, note, req_id),
        )
        audit_log(
            conn,
            'update',
            'logistics_issue_request',
            req_id,
            {
                'requested_quantity': requested_qty,
                'note': note,
                'requester_workplace': workplace,
                'updated_by': session.get('user', {}).get('name') or session.get('user', {}).get('username'),
            },
        )

    return redirect(url_for('materials.materials', req_tab='issue', issue_status_tab='pending'))


@bp.route('/issue-requests/<int:req_id>/delete', methods=['POST'])
@login_required
def delete_issue_request(req_id):
    workplace = get_workplace()
    if workplace == LOGISTICS_WORKPLACE:
        return "<script>alert('물류 작업장에서는 불출 요청 삭제를 할 수 없습니다.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM logistics_issue_requests WHERE id = ?', (req_id,))
        req_row = cursor.fetchone()
        if not req_row:
            return "<script>alert('불출 요청을 찾을 수 없습니다.'); history.back();</script>"
        if (req_row['request_type'] or 'ISSUE') != 'ISSUE':
            return "<script>alert('불출 요청 건만 삭제할 수 있습니다.'); history.back();</script>"
        if req_row['requester_workplace'] != workplace:
            return "<script>alert('현재 작업장의 요청만 삭제할 수 있습니다.'); history.back();</script>"
        if (req_row['status'] or '') != ISSUE_STATUS_REQUESTED or req_row['processed_at']:
            return "<script>alert('요청 중 상태의 불출 요청만 삭제할 수 있습니다.'); history.back();</script>"

        cursor.execute('DELETE FROM logistics_issue_requests WHERE id = ?', (req_id,))
        audit_log(
            conn,
            'delete',
            'logistics_issue_request',
            req_id,
            {
                'material_id': req_row['material_id'],
                'material_code': req_row['material_code'],
                'material_name': req_row['material_name'],
                'requester_workplace': workplace,
                'requested_quantity': req_row['requested_quantity'],
            },
        )

    return redirect(url_for('materials.materials', req_tab='issue', issue_status_tab='pending'))


@bp.route('/issue-requests/delete-all-pending', methods=['POST'])
@login_required
def delete_all_pending_issue_requests():
    workplace = get_workplace()
    if workplace == LOGISTICS_WORKPLACE:
        return "<script>alert('물류 작업장에서는 불출 요청 일괄 삭제를 할 수 없습니다.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT id, material_id, material_code, material_name, requested_quantity
            FROM logistics_issue_requests
            WHERE requester_workplace = ?
              AND COALESCE(request_type, 'ISSUE') = 'ISSUE'
              AND status = ?
              AND processed_at IS NULL
            ORDER BY id
            ''',
            (workplace, ISSUE_STATUS_REQUESTED),
        )
        rows = cursor.fetchall()
        if not rows:
            return "<script>alert('삭제할 불출 요청이 없습니다.'); history.back();</script>"

        ids = [int(row['id']) for row in rows]
        placeholders = ','.join(['?'] * len(ids))
        cursor.execute(f'DELETE FROM logistics_issue_requests WHERE id IN ({placeholders})', ids)

        audit_log(
            conn,
            'delete',
            'logistics_issue_request',
            None,
            {
                'requester_workplace': workplace,
                'deleted_count': len(ids),
                'deleted_ids': ids,
                'deleted_by': session.get('user', {}).get('name') or session.get('user', {}).get('username'),
            },
        )

    return redirect(url_for('materials.materials', req_tab='issue', issue_status_tab='pending'))


@bp.route('/export-requests/delete-all-pending', methods=['POST'])
@login_required
def delete_all_pending_export_requests():
    workplace = get_workplace()
    if workplace == LOGISTICS_WORKPLACE:
        return "<script>alert('물류 작업장에서는 반출 요청 일괄 삭제를 할 수 없습니다.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT id
            FROM logistics_issue_requests
            WHERE requester_workplace = ?
              AND COALESCE(request_type, 'ISSUE') = 'RETURN'
              AND status = ?
              AND processed_at IS NULL
            ORDER BY id
            ''',
            (workplace, ISSUE_STATUS_REQUESTED),
        )
        rows = cursor.fetchall()
        if not rows:
            return "<script>alert('삭제할 반출 요청이 없습니다.'); history.back();</script>"

        ids = [int(row['id']) for row in rows]
        placeholders = ','.join(['?'] * len(ids))
        cursor.execute(f'DELETE FROM logistics_issue_requests WHERE id IN ({placeholders})', ids)
        audit_log(
            conn,
            'delete',
            'logistics_return_request',
            None,
            {
                'requester_workplace': workplace,
                'deleted_count': len(ids),
                'deleted_ids': ids,
                'deleted_by': session.get('user', {}).get('name') or session.get('user', {}).get('username'),
            },
        )

    return redirect(url_for('materials.materials', req_tab='export', export_status_tab='pending'))


@bp.route('/export-requests/<int:req_id>/delete', methods=['POST'])
@login_required
def delete_export_request(req_id):
    workplace = get_workplace()
    if workplace == LOGISTICS_WORKPLACE:
        return "<script>alert('물류 작업장에서는 반출 요청 삭제를 할 수 없습니다.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM logistics_issue_requests WHERE id = ?', (req_id,))
        req_row = cursor.fetchone()
        if not req_row:
            return "<script>alert('반출 요청을 찾을 수 없습니다.'); history.back();</script>"
        if (req_row['request_type'] or 'ISSUE') != 'RETURN':
            return "<script>alert('반출 요청 건만 삭제할 수 있습니다.'); history.back();</script>"
        if req_row['requester_workplace'] != workplace:
            return "<script>alert('현재 작업장의 요청만 삭제할 수 있습니다.'); history.back();</script>"
        if (req_row['status'] or '') != ISSUE_STATUS_REQUESTED or req_row['processed_at']:
            return "<script>alert('요청 중 상태의 반출 요청만 삭제할 수 있습니다.'); history.back();</script>"

        cursor.execute('DELETE FROM logistics_issue_requests WHERE id = ?', (req_id,))
        audit_log(
            conn,
            'delete',
            'logistics_return_request',
            req_id,
            {
                'material_id': req_row['material_id'],
                'material_code': req_row['material_code'],
                'material_name': req_row['material_name'],
                'requester_workplace': workplace,
                'requested_quantity': req_row['requested_quantity'],
            },
        )

    return redirect(url_for('materials.materials', req_tab='export', export_status_tab='pending'))


@bp.route('/issue-requests/<int:req_id>/complete', methods=['POST'])
@login_required
def complete_issue_request(req_id):
    approved_qty = float(request.form.get('actual_quantity') or request.form.get('approved_quantity') or 0)
    process_note = (request.form.get('process_note') or '').strip()
    try:
        processed_at = _parse_manual_processed_from_form(request.form)
    except ValueError as e:
        return _alert_back(str(e))
    if approved_qty <= 0:
        return "<script>alert('?? ?? ??? ??? ???.'); history.back();</script>"
    split_rows = _build_receipt_split_rows(request.form, approved_qty)
    if not split_rows:
        return "<script>alert('?? lot ??? ??? ???.'); history.back();</script>"
    total_split_qty = round(sum(float(row.get('quantity') or 0) for row in split_rows), 4)
    if abs(total_split_qty - approved_qty) > 0.01:
        return "<script>alert('?? ?? ?? ??? ?????? ????.'); history.back();</script>"
    for row in split_rows:
        if float(row.get('quantity') or 0) <= 0:
            return "<script>alert('? lot? ????? ??? ???.'); history.back();</script>"
        if not (row.get('receiving_date') or '').strip():
            return "<script>alert('? lot? ???? ?????.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM logistics_issue_requests WHERE id = ?', (req_id,))
        req_row = cursor.fetchone()
        if not req_row:
            return "<script>alert('\ubd88\ucd9c \uc694\uccad\uc744 \ucc3e\uc744 \uc218 \uc5c6\uc2b5\ub2c8\ub2e4.'); history.back();</script>"
        if (req_row['request_type'] or 'ISSUE') != 'ISSUE':
            return "<script>alert('\ubd88\ucd9c \uc694\uccad \uac74\ub9cc \ucc98\ub9ac\ud560 \uc218 \uc788\uc2b5\ub2c8\ub2e4.'); history.back();</script>"
        if req_row['status'] != ISSUE_STATUS_REQUESTED:
            return "<script>alert('\uc774\ubbf8 \ucc98\ub9ac\ub41c \ubd88\ucd9c \uc694\uccad\uc785\ub2c8\ub2e4.'); history.back();</script>"

        current_username = (session.get('user', {}) or {}).get('username')
        if (req_row['requester_username'] or '') != current_username:
            return "<script>alert('요청을 등록한 사용자만 실입고 완료 처리할 수 있습니다.'); history.back();</script>"

        created_receipt_rows = []
        for row in split_rows:
            lot_id, _lot_name = _create_request_receipt_lot(
                cursor,
                int(req_row['material_id']),
                req_row['material_code'],
                float(row.get('quantity') or 0),
                req_row['requester_workplace'],
                row.get('receiving_date'),
                row.get('manufacture_date'),
                row.get('expiry_date'),
                int(row.get('manufacture_date_unknown') or 0),
                int(row.get('expiry_date_unknown') or 0),
                created_at=processed_at,
            )
            created_receipt_rows.append((int(lot_id), float(row.get('quantity') or 0)))
        for lot_id, quantity in created_receipt_rows:
            _record_issue_receipt_lot(cursor, req_id, lot_id, quantity)
        _sync_material_stock_with_lots(conn, int(req_row['material_id']))
        cursor.execute(
            '''
            UPDATE logistics_issue_requests
            SET status = ?, approved_quantity = ?, processed_by = ?, processed_at = ?, process_note = ?,
                receipt_updated_at = NULL,
                original_approved_quantity = ?
            WHERE id = ?
            ''',
            (
                ISSUE_STATUS_COMPLETED,
                approved_qty,
                session.get('user', {}).get('name'),
                processed_at,
                process_note,
                approved_qty,
                req_id,
            ),
        )
        audit_log(
            conn,
            'update',
            'logistics_issue_request',
            req_id,
            {
                'status': ISSUE_STATUS_COMPLETED,
                'approved_quantity': approved_qty,
                'processed_by': session.get('user', {}).get('name'),
                'material_code': req_row['material_code'],
                'material_id': req_row['material_id'],
                'requester_workplace': req_row['requester_workplace'],
            },
        )
        cursor.execute(
            '''
            SELECT COUNT(*) AS cnt
            FROM logistics_issue_requests
            WHERE request_type = 'ISSUE'
              AND status = ?
              AND requester_workplace = ?
            ''',
            (ISSUE_STATUS_REQUESTED, req_row['requester_workplace']),
        )
        pending_row = cursor.fetchone()
        remaining_pending_count = int(pending_row['cnt'] or 0) if pending_row else 0

    next_issue_status = 'pending' if remaining_pending_count > 0 else 'completed'
    return redirect(url_for('materials.materials', req_tab='issue', issue_status=next_issue_status))


@bp.route('/issue-requests/<int:req_id>/cancel-complete', methods=['POST'])
@login_required
def cancel_completed_issue_request(req_id):
    current_username = (session.get('user', {}) or {}).get('username')
    current_name = (session.get('user', {}) or {}).get('name')
    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM logistics_issue_requests WHERE id = ?', (req_id,))
            req_row = cursor.fetchone()
            if not req_row:
                return _alert_back('불출 요청을 찾을 수 없습니다.')
            if (req_row['request_type'] or 'ISSUE') != 'ISSUE':
                return _alert_back('불출 요청 건만 취소할 수 있습니다.')
            if req_row['status'] != ISSUE_STATUS_COMPLETED:
                return _alert_back('완료된 불출 요청만 취소할 수 있습니다.')
            requester_username = (req_row['requester_username'] or '').strip()
            requester_name = (req_row['requested_by'] or '').strip()
            if current_username not in {requester_username, requester_name} and (current_name or '') not in {requester_username, requester_name}:
                return _alert_back('요청을 등록한 사용자만 완료 취소할 수 있습니다.')

            receipt_rows = _load_issue_receipt_lots(cursor, req_id)
            if not receipt_rows:
                receipt_rows = _infer_issue_receipt_lots(cursor, req_row)
            if not receipt_rows:
                return _alert_back('완료 취소에 필요한 lot 정보를 찾을 수 없습니다.')

            detail_rows = _load_issue_receipt_lot_details(cursor, req_id)
            if _receipt_rows_include_disposed(detail_rows):
                return _alert_back('폐기된 로트가 포함된 완료건은 취소할 수 없습니다.')

            _rebuild_material_fifo_for_workplace(
                conn,
                cursor,
                int(req_row['material_id']),
                req_row['requester_workplace'],
                (req_row['processed_at'] or '')[:10],
                reconsume=False,
            )

            for row in receipt_rows:
                lot_qty = _get_workplace_lot_quantity(cursor, req_row['requester_workplace'], int(row['material_id']), int(row['material_lot_id']))
                if not _has_enough_quantity(lot_qty, float(row['quantity'] or 0)):
                    return _alert_back('이미 사용되었거나 이동된 재고가 있어 완료 취소할 수 없습니다.')

            for row in receipt_rows:
                rollback_qty = float(row['quantity'] or 0)
                consumed_qty = _consume_workplace_lot_quantity(cursor, req_row['requester_workplace'], int(row['material_lot_id']), rollback_qty)
                if not _has_enough_quantity(consumed_qty, rollback_qty):
                    raise ValueError('재고 롤백 중 수량이 변경되었습니다. 다시 시도해 주세요.')
                cursor.execute(
                    '''
                    UPDATE material_lots
                    SET received_quantity = MAX(COALESCE(received_quantity, 0) - ?, 0),
                        current_quantity = MAX(COALESCE(current_quantity, 0) - ?, 0),
                        quantity = MAX(COALESCE(quantity, 0) - ?, 0)
                    WHERE id = ?
                    ''',
                    (rollback_qty, rollback_qty, rollback_qty, int(row['material_lot_id'])),
                )
                cursor.execute(
                    '''
                    INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
                    VALUES (?, ?, 'issue_request_cancel', ?, ?)
                    ''',
                    (
                        int(row['material_lot_id']),
                        int(row['material_id']),
                        rollback_qty,
                        f"{req_row['requester_workplace']} 실입고 완료 취소",
                    ),
                )

            cursor.execute('DELETE FROM logistics_issue_receipt_lots WHERE request_id = ?', (req_id,))
            cursor.execute(
                '''
                UPDATE logistics_issue_requests
                SET status = ?, approved_quantity = 0, processed_by = NULL, processed_at = NULL, process_note = NULL,
                    receipt_updated_at = NULL, original_approved_quantity = NULL
                WHERE id = ?
                ''',
                (ISSUE_STATUS_REQUESTED, req_id),
            )
            _sync_material_stock_with_lots(conn, int(req_row['material_id']))
            audit_log(
                conn,
                'update',
                'logistics_issue_request',
                req_id,
                {
                    'status': ISSUE_STATUS_REQUESTED,
                    'cancel_completed': True,
                    'material_code': req_row['material_code'],
                    'material_id': req_row['material_id'],
                    'requester_workplace': req_row['requester_workplace'],
                },
            )
            _rebuild_material_fifo_for_workplace(
                conn,
                cursor,
                int(req_row['material_id']),
                req_row['requester_workplace'],
                (req_row['processed_at'] or '')[:10],
            )
    except ValueError as e:
        return _alert_back(str(e))

    return redirect(url_for('materials.materials', req_tab='issue', issue_status='completed'))


@bp.route('/issue-requests/<int:req_id>/update-complete', methods=['POST'])
@login_required
def update_completed_issue_request(req_id):
    approved_qty = float(request.form.get('actual_quantity') or request.form.get('approved_quantity') or 0)
    process_note = (request.form.get('process_note') or '').strip()
    receipt_updated_at = _local_timestamp_str()
    try:
        processed_at = _parse_manual_processed_from_form(request.form)
    except ValueError as e:
        return _alert_back(str(e))
    if approved_qty <= 0:
        return _alert_back('실입고 수량을 입력해주세요.')
    split_rows = _build_receipt_split_rows(request.form, approved_qty)
    if not split_rows:
        return _alert_back('수정할 lot 정보를 입력해주세요.')
    total_split_qty = round(sum(float(row.get('quantity') or 0) for row in split_rows), 4)
    if abs(total_split_qty - approved_qty) > 0.01:
        return _alert_back('실입고 수량과 lot 분할합이 일치해야 합니다.')
    for row in split_rows:
        if float(row.get('quantity') or 0) <= 0:
            return _alert_back('각 lot 수량은 0보다 커야 합니다.')
        if not (row.get('receiving_date') or '').strip():
            return _alert_back('각 lot의 입고일을 입력해주세요.')

    current_username = (session.get('user', {}) or {}).get('username')
    current_name = (session.get('user', {}) or {}).get('name')
    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM logistics_issue_requests WHERE id = ?', (req_id,))
            req_row = cursor.fetchone()
            if not req_row:
                return _alert_back('불출 완료건을 찾을 수 없습니다.')
            if (req_row['request_type'] or 'ISSUE') != 'ISSUE':
                return _alert_back('불출 완료건만 수정할 수 있습니다.')
            if req_row['status'] != ISSUE_STATUS_COMPLETED:
                return _alert_back('완료된 불출건만 수정할 수 있습니다.')
            requester_username = (req_row['requester_username'] or '').strip()
            requester_name = (req_row['requested_by'] or '').strip()
            if current_username not in {requester_username, requester_name} and (current_name or '') not in {requester_username, requester_name}:
                return _alert_back('요청을 등록한 사용자만 완료건을 수정할 수 있습니다.')

            receipt_rows = _load_issue_receipt_lots(cursor, req_id)
            if not receipt_rows:
                receipt_rows = _infer_issue_receipt_lots(cursor, req_row)
            if not receipt_rows:
                return _alert_back('기존 실입고 lot 정보를 찾을 수 없어 수정할 수 없습니다.')

            detail_rows = _load_issue_receipt_lot_details(cursor, req_id)
            if _receipt_rows_include_disposed(detail_rows):
                return _alert_back('폐기된 로트가 포함된 완료건은 수정할 수 없습니다.')
            previous_approved_qty = _round_to_1_decimal(req_row['approved_quantity'] or 0)
            previous_processed_date = (req_row['processed_at'] or '')[:10]
            _rebuild_material_fifo_for_workplace(
                conn,
                cursor,
                int(req_row['material_id']),
                req_row['requester_workplace'],
                previous_processed_date,
                reconsume=False,
            )
            _rollback_issue_receipt_rows(cursor, req_row, receipt_rows, '실입고 완료 수정 롤백')
            cursor.execute('DELETE FROM logistics_issue_receipt_lots WHERE request_id = ?', (req_id,))

            created_receipt_rows = []
            for index, row in enumerate(split_rows):
                before_row = detail_rows[index] if index < len(detail_rows) else None
                lot_id, _lot_name = _create_request_receipt_lot(
                    cursor,
                    int(req_row['material_id']),
                    req_row['material_code'],
                    float(row.get('quantity') or 0),
                    req_row['requester_workplace'],
                    row.get('receiving_date'),
                    row.get('manufacture_date'),
                    row.get('expiry_date'),
                    int(row.get('manufacture_date_unknown') or 0),
                    int(row.get('expiry_date_unknown') or 0),
                    log_action='issue_request_update',
                    log_note=_build_issue_receipt_update_note(
                        req_row['requester_workplace'],
                        previous_approved_qty,
                        approved_qty,
                        before_row,
                        row,
                    ),
                    created_at=processed_at,
                )
                created_receipt_rows.append((int(lot_id), float(row.get('quantity') or 0)))
            for lot_id, quantity in created_receipt_rows:
                _record_issue_receipt_lot(cursor, req_id, lot_id, quantity)

            cursor.execute(
                '''
                UPDATE logistics_issue_requests
                SET approved_quantity = ?, processed_by = ?, processed_at = ?, process_note = ?,
                    receipt_updated_at = ?,
                    original_approved_quantity = COALESCE(original_approved_quantity, ?)
                WHERE id = ?
                ''',
                (
                    approved_qty,
                    session.get('user', {}).get('name'),
                    processed_at,
                    process_note,
                    receipt_updated_at,
                    previous_approved_qty,
                    req_id,
                ),
            )
            _sync_material_stock_with_lots(conn, int(req_row['material_id']))
            audit_log(
                conn,
                'update',
                'logistics_issue_request',
                req_id,
                {
                    'status': ISSUE_STATUS_COMPLETED,
                    'updated_completed_receipt': True,
                    'before_approved_quantity': previous_approved_qty,
                    'approved_quantity': approved_qty,
                    'processed_by': session.get('user', {}).get('name'),
                    'material_code': req_row['material_code'],
                    'material_id': req_row['material_id'],
                    'requester_workplace': req_row['requester_workplace'],
                },
            )
            _rebuild_material_fifo_for_workplace(
                conn,
                cursor,
                int(req_row['material_id']),
                req_row['requester_workplace'],
                processed_at[:10],
            )
    except ValueError as e:
        return _alert_back(str(e))

    return redirect(url_for('materials.materials', req_tab='issue', issue_status='completed'))


@bp.route('/issue-requests/<int:req_id>/reject', methods=['POST'])
@role_required('logistics')
def reject_issue_request(req_id):
    rejected_reason = (request.form.get('rejected_reason') or '').strip()
    if not rejected_reason:
        return "<script>alert('\ubc18\ub824 \uc0ac\uc720\ub97c \uc785\ub825\ud574 \uc8fc\uc138\uc694.'); history.back();</script>"

    manager_name = session.get('user', {}).get('name') or session.get('user', {}).get('username')
    processed_at = _local_timestamp_str()
    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM logistics_issue_requests WHERE id = ?', (req_id,))
        req_row = cursor.fetchone()
        if not req_row:
            return "<script>alert('\ubd88\ucd9c \uc694\uccad\uc744 \ucc3e\uc744 \uc218 \uc5c6\uc2b5\ub2c8\ub2e4.'); history.back();</script>"
        if (req_row['request_type'] or 'ISSUE') != 'ISSUE':
            return "<script>alert('\ubd88\ucd9c \uc694\uccad \uac74\ub9cc \ubc18\ub824\ud560 \uc218 \uc788\uc2b5\ub2c8\ub2e4.'); history.back();</script>"
        if req_row['processed_at']:
            return "<script>alert('\uc774\ubbf8 \ucc98\ub9ac\ub41c \ubd88\ucd9c \uc694\uccad\uc785\ub2c8\ub2e4.'); history.back();</script>"

        cursor.execute(
            '''
            UPDATE logistics_issue_requests
            SET status = ?,
                rejected_reason = ?,
                rejected_by = ?,
                rejected_at = ?,
                processed_by = ?,
                processed_at = ?,
                process_note = ?
            WHERE id = ?
            ''',
            (ISSUE_STATUS_REJECTED, rejected_reason, manager_name, processed_at, manager_name, processed_at, rejected_reason, req_id),
        )
        audit_log(
            conn,
            'update',
            'logistics_issue_request',
            req_id,
            {
                'status': ISSUE_STATUS_REJECTED,
                'rejected_reason': rejected_reason,
                'rejected_by': manager_name,
                'material_code': req_row['material_code'],
                'material_id': req_row['material_id'],
                'requester_workplace': req_row['requester_workplace'],
            },
        )
        add_user_notification(
            conn,
            req_row['requester_username'],
            f"\ubd88\ucd9c \uc694\uccad\uc774 \ubc18\ub824\ub418\uc5c8\uc2b5\ub2c8\ub2e4: {req_row['material_name']}",
            f"\uc0ac\uc720: {rejected_reason}",
            '/materials?req_tab=issue',
        )

    return redirect(url_for('materials.materials', req_tab='issue', issue_status='pending'))


@bp.route('/export-requests/<int:req_id>/complete', methods=['POST'])
@login_required
def complete_export_request(req_id):
    approved_qty = _round_to_1_decimal(request.form.get('actual_quantity') or request.form.get('approved_quantity') or 0)
    process_note = (request.form.get('process_note') or '').strip()
    try:
        processed_at = _parse_manual_processed_from_form(request.form)
    except ValueError as e:
        return _alert_back(str(e))
    if approved_qty <= 0:
        return _alert_back('실제 반출 수량을 확인해 주세요.')

    manager_name = session.get('user', {}).get('name') or session.get('user', {}).get('username')
    current_username = (session.get('user', {}) or {}).get('username')
    current_name = (session.get('user', {}) or {}).get('name')
    remaining_pending_count = 0
    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            req_row = _complete_export_request_row(
                conn,
                cursor,
                req_id,
                approved_qty,
                process_note,
                processed_at,
                manager_name,
                current_username,
                current_name,
            )
            cursor.execute(
                '''
                SELECT COUNT(*) AS cnt
                FROM logistics_issue_requests
                WHERE requester_workplace = ?
                  AND COALESCE(request_type, 'ISSUE') = 'RETURN'
                  AND status = ?
                ''',
                (req_row['requester_workplace'], ISSUE_STATUS_REQUESTED),
            )
            pending_row = cursor.fetchone()
            remaining_pending_count = int((pending_row['cnt'] if pending_row else 0) or 0)
    except ValueError as e:
        return _alert_back(str(e))
    except Exception:
        return _alert_back('반출 완료 처리 중 오류가 발생했습니다.')

    next_status = 'pending' if remaining_pending_count > 0 else 'completed'
    return redirect(url_for('materials.materials', req_tab='export', export_status=next_status))


@bp.route('/export-requests/<int:req_id>/cancel-complete', methods=['POST'])
@login_required
def cancel_completed_export_request(req_id):
    current_username = (session.get('user', {}) or {}).get('username')
    current_name = (session.get('user', {}) or {}).get('name')
    manager_name = session.get('user', {}).get('name') or session.get('user', {}).get('username')
    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM logistics_issue_requests WHERE id = ?', (req_id,))
        req_row = cursor.fetchone()
        if not req_row:
            return _alert_back('반출 요청을 찾을 수 없습니다.')
        if (req_row['request_type'] or 'ISSUE') != 'RETURN':
            return _alert_back('반출 요청 건만 취소할 수 있습니다.')
        if req_row['status'] != ISSUE_STATUS_COMPLETED:
            return _alert_back('완료된 반출 요청만 취소할 수 있습니다.')
        requester_username = (req_row['requester_username'] or '').strip()
        requester_name = (req_row['requested_by'] or '').strip()
        if current_username not in {requester_username, requester_name} and (current_name or '') not in {requester_username, requester_name}:
            return _alert_back('요청을 등록한 사용자만 완료 취소할 수 있습니다.')

        location_id = _get_inventory_location_id(cursor, req_row['requester_workplace'])
        if not location_id:
            return _alert_back('작업장 재고 위치를 찾을 수 없습니다.')
        rollback_qty = _round_to_1_decimal(req_row['approved_quantity'] or req_row['requested_quantity'] or 0)
        if rollback_qty <= 0:
            return _alert_back('롤백할 반출 수량이 없습니다.')

        _increase_material_lot_balance(cursor, location_id, int(req_row['material_lot_id']), rollback_qty)
        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'export_request_cancel', ?, ?)
            ''',
            (
                int(req_row['material_lot_id']),
                int(req_row['material_id']),
                rollback_qty,
                f"{req_row['requester_workplace']} 반출 완료 취소",
            ),
        )
        cursor.execute(
            '''
            UPDATE logistics_issue_requests
            SET status = ?, approved_quantity = 0, processed_by = NULL, processed_at = NULL, process_note = NULL
            WHERE id = ?
            ''',
            (ISSUE_STATUS_REQUESTED, req_id),
        )
        _sync_material_stock_with_lots(conn, int(req_row['material_id']))
        audit_log(
            conn,
            'update',
            'logistics_return_request',
            req_id,
            {
                'status': ISSUE_STATUS_REQUESTED,
                'cancel_completed': True,
                'approved_quantity': rollback_qty,
                'processed_by': manager_name,
                'material_code': req_row['material_code'],
                'material_id': req_row['material_id'],
                'requester_workplace': req_row['requester_workplace'],
            },
        )

    return redirect(url_for('materials.materials', req_tab='export', export_status='completed'))


def _complete_export_request_row(conn, cursor, req_id, approved_qty, process_note, processed_at, manager_name, current_username, current_name):
    cursor.execute('SELECT * FROM logistics_issue_requests WHERE id = ?', (req_id,))
    req_row = cursor.fetchone()
    if not req_row:
        raise ValueError('반출 요청을 찾을 수 없습니다.')
    if (req_row['request_type'] or 'ISSUE') != 'RETURN':
        raise ValueError('반출 요청 건만 처리할 수 있습니다.')
    if req_row['status'] != ISSUE_STATUS_REQUESTED:
        raise ValueError('이미 처리된 반출 요청입니다.')
    requested_qty = _round_to_1_decimal(req_row['requested_quantity'] or 0)
    if approved_qty > requested_qty + 1e-9:
        raise ValueError('실제 반출 수량은 요청 수량을 초과할 수 없습니다.')
    requester_username = (req_row['requester_username'] or '').strip()
    requester_name = (req_row['requested_by'] or '').strip()
    if current_username not in {requester_username, requester_name} and (current_name or '') not in {requester_username, requester_name}:
        raise ValueError('요청을 등록한 사용자만 반출 완료 처리할 수 있습니다.')

    workplace_location_ids = _get_inventory_location_ids(cursor, req_row['requester_workplace'])
    if not workplace_location_ids:
        raise ValueError('작업장 재고 위치를 찾을 수 없습니다.')

    workplace_lot_qty = _round_to_1_decimal(_get_workplace_lot_quantity(
        cursor,
        req_row['requester_workplace'],
        req_row['material_id'],
        req_row['material_lot_id'],
    ))
    if not _has_enough_quantity(workplace_lot_qty, approved_qty):
        raise ValueError('현재 로트 재고가 실제 반출 수량보다 부족합니다.')

    consumed_qty = _consume_workplace_lot_quantity(
        cursor,
        req_row['requester_workplace'],
        req_row['material_lot_id'],
        approved_qty,
    )
    if not _has_enough_quantity(consumed_qty, approved_qty):
        raise ValueError('반출 처리 중 로트 재고가 변경되었습니다. 다시 시도해 주세요.')
    _sync_material_stock_with_lots(conn, int(req_row['material_id']))

    cursor.execute(
        '''
        UPDATE logistics_issue_requests
        SET status = ?, approved_quantity = ?, processed_by = ?, processed_at = ?, process_note = ?
        WHERE id = ?
        ''',
        (ISSUE_STATUS_COMPLETED, approved_qty, manager_name, processed_at, process_note, req_id),
    )
    cursor.execute(
        '''
        INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note, created_at)
        VALUES (?, ?, 'export_request_complete', ?, ?, ?)
        ''',
        (
            req_row['material_lot_id'],
            req_row['material_id'],
            approved_qty,
            f"{req_row['requester_workplace']} 반출 완료" + (f" / {process_note}" if process_note else ''),
            processed_at,
        ),
    )
    audit_log(
        conn,
        'update',
        'logistics_return_request',
        req_id,
        {
            'status': ISSUE_STATUS_COMPLETED,
            'approved_quantity': approved_qty,
            'processed_by': manager_name,
            'material_code': req_row['material_code'],
            'material_id': req_row['material_id'],
            'requester_workplace': req_row['requester_workplace'],
        },
    )
    return req_row


@bp.route('/export-requests/complete-selected', methods=['POST'])
@login_required
def complete_selected_export_requests():
    req_ids = []
    for raw_id in request.form.getlist('request_ids[]'):
        try:
            req_id = int(raw_id or 0)
        except ValueError:
            req_id = 0
        if req_id > 0:
            req_ids.append(req_id)
    if not req_ids:
        return _alert_back('일괄 반출 완료 처리할 항목을 선택해 주세요.')
    try:
        processed_at = _parse_manual_processed_from_form(request.form)
    except ValueError as e:
        return _alert_back(str(e))

    manager_name = session.get('user', {}).get('name') or session.get('user', {}).get('username')
    current_username = (session.get('user', {}) or {}).get('username')
    current_name = (session.get('user', {}) or {}).get('name')
    completed_count = 0
    failed_items = []
    last_workplace = None
    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            for req_id in req_ids:
                savepoint_name = f'export_bulk_complete_{req_id}'
                cursor.execute(f'SAVEPOINT {savepoint_name}')
                try:
                    cursor.execute('SELECT requester_workplace, requested_quantity FROM logistics_issue_requests WHERE id = ?', (req_id,))
                    row = cursor.fetchone()
                    if not row:
                        raise ValueError('반출 요청을 찾을 수 없습니다.')
                    last_workplace = row['requester_workplace']
                    approved_qty = _round_to_1_decimal(row['requested_quantity'] or 0)
                    _complete_export_request_row(
                        conn,
                        cursor,
                        req_id,
                        approved_qty,
                        '',
                        processed_at,
                        manager_name,
                        current_username,
                        current_name,
                    )
                    cursor.execute(f'RELEASE SAVEPOINT {savepoint_name}')
                    completed_count += 1
                except Exception as e:
                    cursor.execute(f'ROLLBACK TO SAVEPOINT {savepoint_name}')
                    cursor.execute(f'RELEASE SAVEPOINT {savepoint_name}')
                    failed_items.append(f'{_describe_export_request_by_id(cursor, req_id)} - {str(e)}')
    except Exception:
        return _alert_back('일괄 반출 완료 처리 중 오류가 발생했습니다.')

    target_url = url_for('materials.materials', req_tab='export', export_status='pending')
    if completed_count > 0 and not failed_items:
        return _alert_redirect(f'선택한 {completed_count}건을 일괄 반출 완료 처리했습니다.', target_url)
    if failed_items:
        summary_lines = [f'일괄 반출 완료 {completed_count}건, 실패 {len(failed_items)}건']
        summary_lines.extend(failed_items)
        return _alert_redirect('\n'.join(summary_lines), target_url)
    return _alert_back('일괄 반출 완료 처리할 항목이 없습니다.')


@bp.route('/export-requests/<int:req_id>/reject', methods=['POST'])
@role_required('logistics')
def reject_export_request(req_id):
    rejected_reason = (request.form.get('rejected_reason') or '').strip()
    if not rejected_reason:
        return "<script>alert('반려 사유를 입력해 주세요.'); history.back();</script>"

    manager_name = session.get('user', {}).get('name') or session.get('user', {}).get('username')
    processed_at = _local_timestamp_str()
    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM logistics_issue_requests WHERE id = ?', (req_id,))
        req_row = cursor.fetchone()
        if not req_row:
            return "<script>alert('반출 요청을 찾을 수 없습니다.'); history.back();</script>"
        if (req_row['request_type'] or 'ISSUE') != 'RETURN':
            return "<script>alert('반출 요청 건만 반려할 수 있습니다.'); history.back();</script>"
        if req_row['processed_at']:
            return "<script>alert('이미 처리된 반출 요청입니다.'); history.back();</script>"

        cursor.execute(
            '''
            UPDATE logistics_issue_requests
            SET status = ?,
                rejected_reason = ?,
                rejected_by = ?,
                rejected_at = ?,
                processed_by = ?,
                processed_at = ?,
                process_note = ?
            WHERE id = ?
            ''',
            (ISSUE_STATUS_REJECTED, rejected_reason, manager_name, processed_at, manager_name, processed_at, rejected_reason, req_id),
        )
        audit_log(
            conn,
            'update',
            'logistics_return_request',
            req_id,
            {
                'status': ISSUE_STATUS_REJECTED,
                'rejected_reason': rejected_reason,
                'rejected_by': manager_name,
                'material_code': req_row['material_code'],
                'material_id': req_row['material_id'],
                'requester_workplace': req_row['requester_workplace'],
            },
        )
        add_user_notification(
            conn,
            req_row['requester_username'],
            f"반출 요청이 반려되었습니다: {req_row['material_name']}",
            f"사유: {rejected_reason}",
            '/materials?req_tab=export',
        )

    return redirect(url_for('materials.purchase_orders'))

@bp.route('/purchase-requests/<int:req_id>/reschedule', methods=['POST'])
@role_required('purchase')
def reschedule_purchase_request(req_id):
    """\uc785\uace0 \uc608\uc815\uc77c \ubcc0\uacbd \ucc98\ub9ac"""
    new_expected_date = (request.form.get('expected_delivery_date') or '').strip()
    note = (request.form.get('note') or '').strip()
    next_page = (request.form.get('next_page') or '').strip()
    if not new_expected_date:
        return "<script>alert('\ubcc0\uacbd\ud560 \uc785\uace0 \uc608\uc815\uc77c\uc744 \uc785\ub825\ud574 \uc8fc\uc138\uc694.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM purchase_requests WHERE id = ?', (req_id,))
        before = cursor.fetchone()
        if not before:
            return "<script>alert('\ubc1c\uc8fc \uc694\uccad\uc744 \ucc3e\uc744 \uc218 \uc5c6\uc2b5\ub2c8\ub2e4.'); history.back();</script>"
        cursor.execute('SELECT name FROM materials WHERE id = ?', (before['material_id'],))
        mat = cursor.fetchone()
        material_name = (mat['name'] or '\ubd80\uc790\uc7ac') if mat else '\ubd80\uc790\uc7ac'
        memo = f"[\uc785\uace0\uc77c \ubcc0\uacbd] {note}" if note else '[\uc785\uace0\uc77c \ubcc0\uacbd]'
        cursor.execute(
            '''
            UPDATE purchase_requests
            SET expected_delivery_date = ?,
                logistics_closed = 0,
                logistics_closed_at = NULL,
                logistics_close_note = NULL,
                logistics_close_type = NULL,
                note = COALESCE(note, '') || CASE WHEN COALESCE(note, '') = '' THEN ? ELSE '\n' || ? END
            WHERE id = ?
            ''',
            (new_expected_date, memo, memo, req_id),
        )
        audit_log(
            conn,
            'update',
            'purchase_request_reschedule',
            req_id,
            {
                'before_expected_delivery_date': before['expected_delivery_date'],
                'after_expected_delivery_date': new_expected_date,
                'note': note,
            },
        )
        add_user_notification(
            conn,
            before['requester_username'],
            f"\uc785\uace0 \uc608\uc815\uc77c\uc774 \ubcc0\uacbd\ub418\uc5c8\uc2b5\ub2c8\ub2e4: {material_name}",
            f"\ubcc0\uacbd\uc77c {new_expected_date}" + (f" / \uc0ac\uc720: {note}" if note else ''),
            '/purchase-orders',
        )
    if next_page == 'logistics':
        return redirect(url_for('materials.purchase_orders'))
    return redirect(url_for('materials.purchase_orders'))

@bp.route('/purchase-requests/<int:req_id>/reject-close', methods=['POST'])
@role_required('logistics')
def reject_close_purchase_request(req_id):
    reason = (request.form.get('reason') or '').strip()
    next_page = (request.form.get('next_page') or '').strip()
    if not reason:
        return "<script>alert('?? ?? ??? ??? ???.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM purchase_requests WHERE id = ?', (req_id,))
        row = cursor.fetchone()
        if not row:
            return "<script>alert('?? ??? ?? ? ????.'); history.back();</script>"
        if (row['status'] or '') != PURCHASE_STATUS_RECEIVED:
            return "<script>alert('?? ?? ?? ?? ?? ??? ? ????.'); history.back();</script>"
        if int(row['logistics_closed'] or 0) == 1:
            return "<script>alert('?? ?? ?? ?? ??? ????.'); history.back();</script>"

        cursor.execute(
            '''
            UPDATE purchase_requests
            SET logistics_closed = 1,
                logistics_closed_at = CURRENT_TIMESTAMP,
                logistics_close_note = ?,
                logistics_close_type = 'rejected'
            WHERE id = ?
            ''',
            (reason, req_id),
        )
        audit_log(
            conn,
            'update',
            'purchase_request_logistics_reject_close',
            req_id,
            {'reason': reason},
        )

    if next_page == 'logistics':
        return redirect(url_for('materials.purchase_orders'))
    return redirect(url_for('materials.purchase_orders'))


@bp.route('/purchase-requests/logistics-finalize-date', methods=['POST'])
@role_required('purchase')
def finalize_purchase_requests_by_date():
    finalize_date = (request.form.get('finalize_date') or '').strip()
    next_page = (request.form.get('next_page') or '').strip()
    if not finalize_date:
        return "<script>alert('?? ?? ??? ??? ??? ???.'); history.back();</script>"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT COUNT(*) as cnt
            FROM purchase_requests
            WHERE expected_delivery_date = ?
              AND status = ?
              AND COALESCE(logistics_closed, 0) = 0
            ''',
            (finalize_date, PURCHASE_STATUS_ORDERED),
        )
        pending = cursor.fetchone()['cnt']
        if pending > 0:
            return "<script>alert('?? ??? ?? ?? ??? ?? ?? ?? ?? ?? ??? ? ????.'); history.back();</script>"

        cursor.execute(
            '''
            UPDATE purchase_requests
            SET logistics_closed = 1,
                logistics_closed_at = CURRENT_TIMESTAMP,
                logistics_close_type = 'completed'
            WHERE expected_delivery_date = ?
              AND status = ?
              AND COALESCE(logistics_closed, 0) = 0
            ''',
            (finalize_date, PURCHASE_STATUS_RECEIVED),
        )
        closed_count = cursor.rowcount
        audit_log(
            conn,
            'update',
            'purchase_request_logistics_finalize',
            None,
            {'finalize_date': finalize_date, 'closed_count': closed_count},
        )
    if next_page == 'logistics':
        return redirect(url_for('materials.purchase_orders'))
    return redirect(url_for('materials.purchase_orders'))


@bp.route('/purchase-requests/<int:req_id>/receive', methods=['POST'])
@role_required('logistics')
def receive_purchase_request(req_id):
    """\uc785\uace0 \uc644\ub8cc \ucc98\ub9ac \ubc0f \ub85c\ud2b8 \ub4f1\ub85d"""
    if not _is_logistics_manager():
        return "<script>alert('\ubb3c\ub958\uad00\ub9ac \uad8c\ud55c\uc774 \ud544\uc694\ud569\ub2c8\ub2e4.'); history.back();</script>"

    received_qty = float(request.form.get('received_quantity', 0))
    receiving_date = request.form.get('receiving_date')
    manufacture_date = request.form.get('manufacture_date')
    expiry_date = request.form.get('expiry_date')
    unit_price = float(request.form.get('unit_price') or 0)
    next_page = (request.form.get('next_page') or '').strip()
    received_at = _local_timestamp_str()
    if (receiving_date or '').strip():
        received_at = f"{receiving_date.strip()} {now_local().strftime('%H:%M:%S')}"

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT pr.material_id, pr.requester_username, pr.workplace,
                   m.code as material_code, m.name as material_name, m.unit
            FROM purchase_requests pr
            JOIN materials m ON m.id = pr.material_id
            WHERE pr.id = ?
            ''',
            (req_id,),
        )
        row = cursor.fetchone()
        username = session['user'].get('name') if session.get('user') else None

        if row and received_qty > 0:
            cursor.execute(
                """
                UPDATE purchase_requests
                SET status=?, received_quantity=?, received_at=?, received_by=?
                WHERE id=?
                """,
                (PURCHASE_STATUS_RECEIVED, received_qty, received_at, username, req_id),
            )

            cursor.execute(
                """
                UPDATE materials SET unit_price = ? WHERE id=?
                """,
                (unit_price, row['material_id']),
            )
            cursor.execute('SELECT id, code, name, unit FROM materials WHERE id = ?', (row['material_id'],))
            mat_row = cursor.fetchone()
            if mat_row:
                pool_code = _pool_code_from_row(mat_row)
                _increase_logistics_stock(
                    cursor,
                    pool_code,
                    mat_row['name'],
                    mat_row['unit'],
                    received_qty,
                    username,
                )

            supplier_lot = (request.form.get('supplier_lot') or '').strip()
            matched_lot = _find_matching_material_lot(
                cursor,
                int(row['material_id']),
                receiving_date,
                manufacture_date=manufacture_date,
                expiry_date=expiry_date,
                supplier_lot=supplier_lot,
            )
            if matched_lot:
                lot_id = int(matched_lot['id'])
                lot = matched_lot['lot']
                cursor.execute(
                    '''
                    UPDATE material_lots
                    SET unit_price = ?,
                        received_quantity = COALESCE(received_quantity, 0) + ?,
                        current_quantity = COALESCE(current_quantity, 0) + ?,
                        quantity = COALESCE(quantity, 0) + ?,
                        manufacture_date = COALESCE(manufacture_date, ?),
                        expiry_date = COALESCE(expiry_date, ?)
                    WHERE id = ?
                    ''',
                    (unit_price, received_qty, received_qty, received_qty, manufacture_date, expiry_date, lot_id),
                )
                lot_action = 'update'
            else:
                lot, lot_seq = _next_unique_material_lot(cursor, int(row['material_id']), row['material_code'], receiving_date)
                cursor.execute(
                    '''
                    INSERT INTO material_lots
                    (material_id, lot, lot_seq, receiving_date, manufacture_date, expiry_date, unit_price, received_quantity, current_quantity, supplier_lot, quantity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (row['material_id'], lot, lot_seq, receiving_date, manufacture_date, expiry_date, unit_price, received_qty, received_qty, supplier_lot, received_qty),
                )
                lot_id = cursor.lastrowid
                lot_action = 'create'

            cursor.execute(
                '''
                INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (lot_id, row['material_id'], lot_action, received_qty, f'purchase_request:{req_id}', received_at),
            )

            logistics_location_id = _get_inventory_location_id(cursor, '\ubb3c\ub958\ucc3d\uace0')
            if logistics_location_id:
                _increase_material_lot_balance(cursor, logistics_location_id, lot_id, received_qty)

            audit_log(
                conn,
                'receive',
                'purchase_request',
                req_id,
                {
                    'received_quantity': received_qty,
                    'received_by': username,
                    'material_id': row['material_id'],
                    'receiving_date': receiving_date,
                    'manufacture_date': manufacture_date,
                    'expiry_date': expiry_date,
                    'unit_price': unit_price,
                    'lot': lot,
                    'supplier_lot': supplier_lot,
                },
            )
            add_user_notification(
                conn,
                row['requester_username'],
                f"\ubc1c\uc8fc \uc790\uc7ac\uac00 \uc785\uace0\ub418\uc5c8\uc2b5\ub2c8\ub2e4: {row['material_name']}",
                f"{row['workplace']} / {received_qty:g}{row['unit'] or ''} \uc785\uace0 \uc644\ub8cc",
                '/purchase-orders',
            )

    if next_page == 'logistics':
        return redirect(url_for('materials.purchase_orders'))
    return redirect(url_for('materials.purchase_orders'))

@bp.route('/purchase-requests/<int:req_id>/delete', methods=['POST'])
@role_required('purchase')
def delete_purchase_request(req_id):
    """발주 요청 삭제"""
    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM purchase_requests WHERE id=?', (req_id,))
        before = cursor.fetchone()
        conn.execute('DELETE FROM purchase_requests WHERE id=?', (req_id,))
        audit_log(conn, 'delete', 'purchase_request', req_id, {'before': dict(before) if before else None})
    return redirect(url_for('materials.purchase_orders'))


@bp.route('/purchase-requests/auto-scan', methods=['POST'])
@role_required('purchase')
def auto_scan_low_stock():
    """최소재고 미달 부자재 발주필요 자동 등록"""
    workplace = get_workplace()
    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, workplace FROM materials
            WHERE current_stock < min_stock AND min_stock > 0
              AND (workplace = ? OR workplace = ? OR workplace IS NULL)
        """,
            (workplace, SHARED_WORKPLACE),
        )
        rows = cursor.fetchall()
        for row in rows:
            target_workplace = SHARED_WORKPLACE if row['workplace'] in (None, SHARED_WORKPLACE) else workplace
            cursor.execute(
                """
                SELECT id FROM purchase_requests
                WHERE material_id = ? AND status != '입고완료' AND workplace = ?
            """,
                (row['id'], target_workplace),
            )
            if cursor.fetchone():
                continue
            cursor.execute(
                """
                INSERT INTO purchase_requests (material_id, status, requested_quantity, workplace)
                VALUES (?, '발주필요', 0, ?)
            """,
                (row['id'], target_workplace),
            )
            audit_log(
                conn,
                'create',
                'purchase_request',
                cursor.lastrowid,
                {'material_id': row['id'], 'status': '발주필요', 'workplace': target_workplace},
            )
    return redirect(url_for('materials.purchase_orders'))


@bp.route('/suppliers/update', methods=['POST'])
@role_required('purchase')
def update_supplier():
    """업체 정보 수정"""
    sid = request.form.get('supplier_id')
    name = request.form.get('name')
    contact = request.form.get('contact')
    address = request.form.get('address')
    note = request.form.get('note')
    with db_transaction() as conn:
        conn.execute('UPDATE suppliers SET name=?,contact=?,address=?,note=? WHERE id=?', (name, contact, address, note, sid))
    return redirect(url_for('materials.purchase_orders'))


@bp.route('/suppliers/delete/<int:supplier_id>', methods=['POST'])
@role_required('purchase')
def delete_supplier(supplier_id):
    """업체 삭제"""
    with db_transaction() as conn:
        conn.execute('DELETE FROM suppliers WHERE id=?', (supplier_id,))
    return redirect(url_for('materials.purchase_orders'))


@bp.route('/suppliers/api', methods=['GET'])
@login_required
def suppliers_api():
    """업체 목록 API (모달 검색용)"""
    keyword = request.args.get('q', '')
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, name, contact, address, note FROM suppliers
            WHERE name LIKE ? OR contact LIKE ?
            ORDER BY name
        """,
            (f'%{keyword}%', f'%{keyword}%'),
        )
        rows = cursor.fetchall()
    return jsonify([dict(r) for r in rows])


# API
@bp.route('/api/materials/by-supplier/<int:supplier_id>')
@login_required
def api_materials_by_supplier(supplier_id):
    """업체별 부자재 조회 API"""
    workplace = get_workplace()
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT id, code, name, unit, unit_price, current_stock
            FROM materials
            WHERE supplier_id = ?
              AND (workplace = ? OR workplace = ? OR workplace IS NULL)
            ORDER BY name
        ''',
            (supplier_id, workplace, SHARED_WORKPLACE),
        )

        materials = []
        for row in cursor.fetchall():
            materials.append(
                {
                    'id': row[0],
                    'code': row[1],
                    'name': row[2],
                    'unit': row[3],
                    'unit_price': row[4],
                    'current_stock': row[5],
                }
            )

    return jsonify(materials)
