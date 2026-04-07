from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
from datetime import datetime, date, timedelta
import calendar
import json
import math

from core import (
    get_db,
    get_workplace,
    rows_to_dict,
    login_required,
    role_required,
    audit_log,
    SHARED_WORKPLACE,
    SHARED_MATERIAL_CATEGORIES,
)

bp = Blueprint('production', __name__)


def _normalize_production_status(status_value):
    s = (status_value or '').strip()
    done = '\uC644\uB8CC'
    planned = '\uC608\uC815'
    in_progress = '\uC9C4\uD589\uC911'
    if not s:
        return planned
    if s == done or s == '?\uafa8\uc9ba' or '\uafa8\uc9ba' in s:
        return done
    if s == in_progress:
        return in_progress
    if s == planned or s == '\uACC4\uD68D' or s == '?\ub35c\uc82d' or '\ub35c\uc82d' in s:
        return planned
    return s

def _sync_material_stock_with_lots(conn, material_id=None):
    cursor = conn.cursor()
    if material_id is not None:
        cursor.execute(
            '''
            UPDATE materials
            SET current_stock = (
                SELECT COALESCE(SUM(COALESCE(ml.current_quantity, ml.quantity, 0)), 0)
                FROM material_lots ml
                WHERE ml.material_id = materials.id
                  AND COALESCE(ml.is_disposed, 0) = 0
            )
            WHERE id = ?
              AND EXISTS (SELECT 1 FROM material_lots x WHERE x.material_id = materials.id)
            ''',
            (material_id,),
        )
        return
    cursor.execute(
        '''
        UPDATE materials
        SET current_stock = (
            SELECT COALESCE(SUM(COALESCE(ml.current_quantity, ml.quantity, 0)), 0)
            FROM material_lots ml
            WHERE ml.material_id = materials.id
              AND COALESCE(ml.is_disposed, 0) = 0
        )
        '''
    )


def _get_inventory_location_id(cursor, name):
    target_name = (name or '').strip()
    if not target_name:
        return None
    row = cursor.execute(
        '''
        SELECT id
        FROM inv_locations
        WHERE name = ?
           OR COALESCE(workplace_code, '') = ?
           OR REPLACE(COALESCE(name, ''), ' ', '') = REPLACE(?, ' ', '')
           OR REPLACE(COALESCE(workplace_code, ''), ' ', '') = REPLACE(?, ' ', '')
        ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, id
        LIMIT 1
        ''',
        (target_name, target_name, target_name, target_name, target_name),
    ).fetchone()
    return int(row['id']) if row else None


def _upsert_material_lot_balance(cursor, location_id, material_lot_id, qty):
    qty_value = float(qty or 0)
    existing = cursor.execute(
        '''
        SELECT id
        FROM inv_material_lot_balances
        WHERE location_id = ? AND material_lot_id = ?
        LIMIT 1
        ''',
        (location_id, material_lot_id),
    ).fetchone()
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


def _recalculate_material_lot_quantity_from_balances(cursor, material_lot_id):
    total_row = cursor.execute(
        '''
        SELECT COALESCE(SUM(qty), 0) AS total_qty
        FROM inv_material_lot_balances
        WHERE material_lot_id = ?
        ''',
        (material_lot_id,),
    ).fetchone()
    total_qty = float(total_row['total_qty'] or 0) if total_row else 0.0
    cursor.execute(
        '''
        UPDATE material_lots
        SET current_quantity = ?,
            quantity = ?
        WHERE id = ?
        ''',
        (total_qty, total_qty, material_lot_id),
    )
    return total_qty


def _rollback_material_lot_usage_for_production(cursor, production_id, note_prefix='production rollback'):
    cursor.execute(
        '''
        SELECT id, material_lot_id, material_id, quantity, location_id
        FROM production_material_lot_usage
        WHERE production_id = ?
        ORDER BY id DESC
        ''',
        (production_id,),
    )
    rows = cursor.fetchall()
    touched = set()
    for row in rows:
        qty = float(row['quantity'] or 0)
        if qty <= 0:
            continue
        location_id = row['location_id']
        if location_id:
            balance_row = cursor.execute(
                '''
                SELECT qty
                FROM inv_material_lot_balances
                WHERE location_id = ? AND material_lot_id = ?
                LIMIT 1
                ''',
                (location_id, row['material_lot_id']),
            ).fetchone()
            current_loc_qty = float(balance_row['qty'] or 0) if balance_row else 0.0
            _upsert_material_lot_balance(cursor, int(location_id), row['material_lot_id'], current_loc_qty + qty)
            _recalculate_material_lot_quantity_from_balances(cursor, row['material_lot_id'])
        else:
            cursor.execute(
                '''
                UPDATE material_lots
                SET current_quantity = COALESCE(current_quantity, quantity, 0) + ?,
                    quantity = COALESCE(current_quantity, quantity, 0) + ?
                WHERE id = ?
                ''',
                (qty, qty, row['material_lot_id']),
            )
        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'rollback', ?, ?)
            ''',
            (row['material_lot_id'], row['material_id'], qty, f'{note_prefix}:{production_id}'),
        )
        touched.add(row['material_id'])
    cursor.execute('DELETE FROM production_material_lot_usage WHERE production_id = ?', (production_id,))
    return touched


def _consume_material_fifo(cursor, production_id, usage_id, material_id, required_qty, workplace=None):
    required = float(required_qty or 0)
    if required <= 0:
        return []

    workplace_location_id = _get_inventory_location_id(cursor, workplace)
    if workplace_location_id:
        cursor.execute(
            '''
            SELECT
                ml.id,
                ml.lot,
                COALESCE(b.qty, 0) as available_qty
            FROM inv_material_lot_balances b
            JOIN material_lots ml ON ml.id = b.material_lot_id
            WHERE b.location_id = ?
              AND ml.material_id = ?
              AND COALESCE(ml.is_disposed, 0) = 0
              AND COALESCE(b.qty, 0) > 0
            ORDER BY ml.receiving_date ASC, ml.lot_seq ASC, ml.id ASC
            ''',
            (workplace_location_id, material_id),
        )
        lots = cursor.fetchall()
    else:
        cursor.execute(
            '''
            SELECT id, lot, COALESCE(current_quantity, quantity, 0) as available_qty
            FROM material_lots
            WHERE material_id = ?
              AND COALESCE(is_disposed, 0) = 0
              AND COALESCE(current_quantity, quantity, 0) > 0
            ORDER BY receiving_date ASC, lot_seq ASC, id ASC
            ''',
            (material_id,),
        )
        lots = cursor.fetchall()

    total_available = sum(float(l['available_qty'] or 0) for l in lots)
    if total_available + 1e-9 < required:
        raise ValueError(f'??? ??? ?????. material_id={material_id}, ??={required:.2f}, ??={total_available:.2f}')

    remain = required
    consumed_lots = []
    for lot in lots:
        if remain <= 1e-9:
            break
        available = float(lot['available_qty'] or 0)
        if available <= 0:
            continue
        used = available if available < remain else remain
        if workplace_location_id:
            _upsert_material_lot_balance(cursor, workplace_location_id, lot['id'], max(available - used, 0.0))
            _recalculate_material_lot_quantity_from_balances(cursor, lot['id'])
        else:
            cursor.execute(
                '''
                UPDATE material_lots
                SET current_quantity = COALESCE(current_quantity, quantity, 0) - ?,
                    quantity = COALESCE(current_quantity, quantity, 0) - ?
                WHERE id = ?
                ''',
                (used, used, lot['id']),
            )
        cursor.execute(
            '''
            INSERT INTO production_material_lot_usage
            (production_id, production_usage_id, material_id, material_lot_id, quantity, location_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (production_id, usage_id, material_id, lot['id'], used, workplace_location_id),
        )
        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'consume', ?, ?)
            ''',
            (lot['id'], material_id, -used, f'production:{production_id}'),
        )
        remain -= used
        consumed_lots.append({'lot_id': int(lot['id']), 'quantity': float(used or 0), 'lot': lot['lot']})
    return consumed_lots


def _material_required_info_fields(category):
    cat = (category or '').strip()
    if cat in ('기름', '소금'):
        return ['manufacture_date', 'expiry_date']
    return ['manufacture_date']


def _has_material_info(value, unknown_flag=0):
    text = (value or '').strip()
    return bool(text) or text == '없음' or bool(int(unknown_flag or 0))


def _material_missing_info_fields(category, manufacture_date, expiry_date, manufacture_unknown=0, expiry_unknown=0):
    has_manufacture = _has_material_info(manufacture_date, manufacture_unknown)
    has_expiry = _has_material_info(expiry_date, expiry_unknown)
    if has_manufacture or has_expiry:
        return []
    return list(_material_required_info_fields(category))


def _get_material_info_gap(cursor, material_id, category, workplace=None):
    workplace_location_id = _get_inventory_location_id(cursor, workplace) if workplace else None
    if workplace_location_id:
        cursor.execute(
            '''
            SELECT
                ml.id,
                ml.lot,
                ml.receiving_date,
                ml.manufacture_date,
                ml.manufacture_date_unknown,
                ml.expiry_date,
                ml.expiry_date_unknown,
                COALESCE(b.qty, 0) AS available_qty
            FROM inv_material_lot_balances b
            JOIN material_lots ml ON ml.id = b.material_lot_id
            WHERE b.location_id = ?
              AND ml.material_id = ?
              AND COALESCE(ml.is_disposed, 0) = 0
              AND COALESCE(b.qty, 0) > 0
            ORDER BY ml.receiving_date ASC, ml.lot_seq ASC, ml.id ASC
            ''',
            (workplace_location_id, material_id),
        )
    else:
        cursor.execute(
            '''
            SELECT id, lot, receiving_date, manufacture_date, manufacture_date_unknown, expiry_date, expiry_date_unknown,
                   COALESCE(current_quantity, quantity, 0) AS available_qty
            FROM material_lots
            WHERE material_id = ?
              AND COALESCE(is_disposed, 0) = 0
              AND COALESCE(current_quantity, quantity, 0) > 0
            ORDER BY receiving_date ASC, lot_seq ASC, id ASC
            ''',
            (material_id,),
        )
    for row in cursor.fetchall():
        missing = _material_missing_info_fields(
            category,
            row['manufacture_date'],
            row['expiry_date'],
            row['manufacture_date_unknown'],
            row['expiry_date_unknown'],
        )
        if missing:
            return {
                'lot_id': int(row['id']),
                'lot': row['lot'],
                'receiving_date': row['receiving_date'],
                'available_qty': float(row['available_qty'] or 0),
                'missing_fields': missing,
            }
    return None


def _apply_missing_material_info_to_last_lot(
    cursor,
    consumed_lots,
    category,
    receiving_date,
    manufacture_date,
    expiry_date,
    manufacture_none=False,
    expiry_none=False,
):
    if not consumed_lots:
        return
    last_lot_id = int(consumed_lots[-1]['lot_id'])
    _apply_missing_material_info_to_lot(
        cursor,
        last_lot_id,
        category,
        receiving_date,
        manufacture_date,
        expiry_date,
        manufacture_none,
        expiry_none,
    )


def _apply_missing_material_info_to_lot(
    cursor,
    lot_id,
    category,
    receiving_date,
    manufacture_date,
    expiry_date,
    manufacture_none=False,
    expiry_none=False,
):
    lot = cursor.execute(
        '''
        SELECT id, lot, receiving_date, manufacture_date, manufacture_date_unknown, expiry_date, expiry_date_unknown, material_id
        FROM material_lots
        WHERE id = ?
        ''',
        (lot_id,),
    ).fetchone()
    if not lot:
        return

    current_receiving = (lot['receiving_date'] or '').strip()
    current_manufacture = (lot['manufacture_date'] or '').strip()
    current_manufacture_unknown = int(lot['manufacture_date_unknown'] or 0)
    current_expiry = (lot['expiry_date'] or '').strip()
    current_expiry_unknown = int(lot['expiry_date_unknown'] or 0)
    missing = _material_missing_info_fields(
        category,
        current_manufacture,
        current_expiry,
        current_manufacture_unknown,
        current_expiry_unknown,
    )
    if not missing:
        receiving_input = (receiving_date or '').strip()
        if receiving_input and receiving_input != current_receiving:
            cursor.execute(
                '''
                UPDATE material_lots
                SET receiving_date = ?
                WHERE id = ?
                ''',
                (receiving_input, lot_id),
            )
            cursor.execute(
                '''
                INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
                VALUES (?, ?, 'update', 0, ?)
                ''',
                (
                    lot_id,
                    lot['material_id'],
                    f'production-info-update:{receiving_input}:{current_manufacture or "-"}:{current_expiry or "-"}',
                ),
            )
        return

    receiving_input = (receiving_date or '').strip()
    manufacture_input = (manufacture_date or '').strip()
    expiry_input = (expiry_date or '').strip()
    manufacture_none = bool(manufacture_none)
    expiry_none = bool(expiry_none)
    manufacture_supplied = bool(manufacture_input or manufacture_none)
    expiry_supplied = bool(expiry_input or expiry_none)
    if missing == ['manufacture_date'] and not manufacture_supplied:
        raise ValueError('???? ???? ???? ????? ???? ???? ???.')
    if missing == ['expiry_date'] and not expiry_supplied:
        raise ValueError('???? ???? ????? ????? ???? ???? ???.')
    if 'manufacture_date' in missing and 'expiry_date' in missing and not (manufacture_supplied or expiry_supplied):
        raise ValueError('??? ?? ???? ? ??? ????? ???? ???? ???.')

    final_receiving = receiving_input or current_receiving
    final_manufacture = current_manufacture or manufacture_input
    final_expiry = current_expiry or expiry_input
    final_manufacture_unknown = 0 if final_manufacture else (1 if (current_manufacture_unknown or manufacture_none) else 0)
    final_expiry_unknown = 0 if final_expiry else (1 if (current_expiry_unknown or expiry_none) else 0)
    cursor.execute(
        '''
        UPDATE material_lots
        SET receiving_date = ?, manufacture_date = ?, manufacture_date_unknown = ?, expiry_date = ?, expiry_date_unknown = ?
        WHERE id = ?
        ''',
        (
            final_receiving or None,
            final_manufacture or None,
            final_manufacture_unknown,
            final_expiry or None,
            final_expiry_unknown,
            lot_id,
        ),
    )
    cursor.execute(
        '''
        INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
        VALUES (?, ?, 'update', 0, ?)
        ''',
        (
            lot_id,
            lot['material_id'],
            f'production-info-update:{final_receiving or "-"}:{final_manufacture or "-"}:{final_expiry or "-"}',
        ),
    )


def _consume_raw_by_code_fifo(cursor, source_raw_material_id, required_qty, production_id, username):
    required = round(float(required_qty or 0), 4)
    if required <= 0:
        return []

    cursor.execute(
        '''
        SELECT
            id,
            name,
            COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id)) as code,
            COALESCE(current_stock, 0) as current_stock
        FROM raw_materials
        WHERE id = ?
        ''',
        (source_raw_material_id,),
    )
    lot = cursor.fetchone()
    if not lot:
        raise ValueError(f'Raw lot not found: id={source_raw_material_id}')

    available = float(lot['current_stock'] or 0)
    if available + 1e-9 < required:
        raise ValueError(
            f"Selected raw lot stock shortage: code={(lot['code'] or '').strip()}, need={required:.2f}, available={available:.2f}"
        )

    cursor.execute(
        '''
        UPDATE raw_materials
        SET current_stock = current_stock - ?,
            used_quantity = used_quantity + ?
        WHERE id = ?
        ''',
        (required, required, lot['id']),
    )
    cursor.execute(
        '''
        INSERT INTO raw_material_logs (raw_material_id, type, quantity, note, production_id, created_by)
        VALUES (?, 'production', ?, '?? ??(?? ??)', ?, ?)
        ''',
        (lot['id'], -required, production_id, username),
    )
    return [{'raw_material_id': int(lot['id']), 'raw_material_name': lot['name'], 'quantity': float(required)}]


def _rollback_raw_usage_for_production(cursor, production_id, created_by=None, note_prefix='production_resave'):
    cursor.execute(
        '''
        SELECT raw_material_id, SUM(COALESCE(actual_quantity, 0)) as qty
        FROM production_material_usage
        WHERE production_id = ?
          AND raw_material_id IS NOT NULL
          AND COALESCE(actual_quantity, 0) > 0
        GROUP BY raw_material_id
        ''',
        (production_id,),
    )
    rows = cursor.fetchall()
    rolled_back = 0
    for row in rows:
        rm_id = int(row['raw_material_id'])
        qty = round(float(row['qty'] or 0), 4)
        if qty <= 0:
            continue
        cursor.execute(
            '''
            UPDATE raw_materials
            SET current_stock = COALESCE(current_stock, 0) + ?,
                used_quantity = MAX(0, COALESCE(used_quantity, 0) - ?)
            WHERE id = ?
            ''',
            (qty, qty, rm_id),
        )
        cursor.execute(
            '''
            INSERT INTO raw_material_logs (raw_material_id, type, quantity, note, production_id, created_by)
            VALUES (?, 'RETURN', ?, ?, ?, ?)
            ''',
            (rm_id, qty, f'{note_prefix}:fifo rollback', production_id, created_by),
        )
        rolled_back += 1
    return rolled_back

@bp.route('/schedules')
@login_required
def schedules():
    """Auto-generated docstring."""
    payload = request.get_json(silent=True) or {}
    workplace = (payload.get('workplace') or request.form.get('workplace') or get_workplace() or '').strip()
    conn = get_db()
    cursor = conn.cursor()

    # ?곹뭹 紐⑸줉 媛?몄삤湲?(?꾩옱 ?묒뾽?λ쭔)
    cursor.execute('SELECT id, name FROM products WHERE workplace = ? ORDER BY name ASC', (workplace,))
    products = cursor.fetchall()

    # ?????뚮씪誘명꽣 (URL?먯꽌 諛쏄린)
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)

    # 湲곕낯媛? ?대쾲 ??
    today = date.today()
    if not year or not month:
        year = today.year
        month = today.month

    # ?대떦 ?붿쓽 ?쒖옉?쇨낵 醫낅즺??
    month_start = date(year, month, 1)
    if month == 12:
        month_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(year, month + 1, 1) - timedelta(days=1)

    cursor.execute(
        '''
        SELECT
            ps.*,
            p.name as product_name,
            COALESCE(ps.production_id, pr.id) as linked_production_id,
            pr.actual_boxes as prod_actual_boxes,
            pr.status as prod_status
        FROM production_schedules ps
        LEFT JOIN products p ON ps.product_id = p.id
        LEFT JOIN productions pr ON pr.schedule_id = ps.id
        WHERE ps.scheduled_date BETWEEN ? AND ?
        AND ps.workplace = ?
        ORDER BY ps.scheduled_date
    ''',
        (month_start.isoformat(), month_end.isoformat(), workplace),
    )
    schedules = cursor.fetchall()

    # ?대떦 ?붿쓽 洹쇰Т???뺣낫 媛?몄삤湲?
    cursor.execute(
        '''
        SELECT date, type, overtime_hours
        FROM work_days
        WHERE date BETWEEN ? AND ?
    ''',
        (month_start.isoformat(), month_end.isoformat()),
    )
    work_days_data = {
        row['date']: {'type': row['type'], 'overtime_hours': row['overtime_hours']} for row in cursor.fetchall()
    }

    conn.close()

    # ?ㅼ?以??곗씠?곕? JSON?쇰줈 蹂??(JavaScript?먯꽌 ?ъ슜)
    schedules_view = []
    schedules_list = []
    weekday_labels = ['월', '화', '수', '목', '금', '토', '일']
    for s in schedules:
        status_value = s['prod_status'] if s['prod_status'] else s['status']
        scheduled_date = s['scheduled_date']
        try:
            weekday_text = weekday_labels[datetime.strptime(scheduled_date, '%Y-%m-%d').weekday()]
        except Exception:
            weekday_text = '-'

        work_info = work_days_data.get(scheduled_date) or {}
        raw_work_type = (work_info.get('type') or '').strip()
        if raw_work_type == 'overtime':
            work_type_text = '잔업'
        elif raw_work_type == 'extra':
            work_type_text = '특근'
        elif raw_work_type == 'holiday':
            work_type_text = '휴무'
        else:
            work_type_text = '일반'
        overtime_hours = work_info.get('overtime_hours')
        overtime_hours = float(overtime_hours or 0) if overtime_hours not in (None, '') else 0.0

        view_row = dict(s)
        view_row['display_weekday'] = weekday_text
        view_row['display_work_type'] = work_type_text
        view_row['display_overtime_hours'] = overtime_hours
        schedules_view.append(view_row)

        schedules_list.append(
            {
                'id': s['id'],
                'product_name': s['product_name'],
                'scheduled_date': s['scheduled_date'],
                'planned_boxes': s['planned_boxes'],
                'status': status_value,
                'note': s['note'],
                'line': s['line'] if s['line'] else '',
                'production_id': s['linked_production_id'],
                'actual_boxes': s['prod_actual_boxes'],
                'is_completed': status_value == '완료',
                'weekday': weekday_text,
                'work_type': work_type_text,
                'overtime_hours': overtime_hours,
            }
        )
    schedules_json = json.dumps(schedules_list, ensure_ascii=False)

    # ?곹뭹 ?곗씠?곕룄 JSON?쇰줈 蹂??(寃??湲곕뒫??
    products_list = []
    for p in products:
        products_list.append({'id': p['id'], 'name': p['name']})
    products_json = json.dumps(products_list, ensure_ascii=False)

    # 洹쇰Т???곗씠?곕룄 JSON?쇰줈 蹂??
    work_days_json = json.dumps(work_days_data, ensure_ascii=False)

    return render_template(
        'schedules.html',
        user=session['user'],
        current_workplace=workplace,
        schedules=schedules_view,
        schedules_json=schedules_json,
        products=products,
        products_json=products_json,
        work_days_json=work_days_json,
        month_start=month_start,
        month_end=month_end,
    )


@bp.route('/schedules/requirements-data')
@login_required
def schedule_requirements_data():
    workplace = get_workplace()

    conn = get_db()
    cursor = conn.cursor()
    try:
        def _normalize_sub_category(raw_category):
            text = (raw_category or '').strip()
            if text == '박스':
                return 'box'
            if text == '내포':
                return 'inner'
            if text == '외포':
                return 'outer'
            if text == '실리카':
                return 'silica'
            if text == '트레이':
                return 'tray'
            return 'etc'

        cursor.execute(
            '''
            SELECT ps.id, ps.product_id, ps.planned_boxes, ps.status, p.name as product_name
            FROM production_schedules ps
            LEFT JOIN products p ON p.id = ps.product_id
            WHERE ps.workplace = ?
            ORDER BY ps.scheduled_date, ps.id
            ''',
            (workplace,),
        )
        schedule_rows = [dict(r) for r in cursor.fetchall()]
        planned_rows = [r for r in schedule_rows if _normalize_production_status(r.get('status')) == '예정']

        product_box_map = {}
        product_name_map = {}
        for row in planned_rows:
            pid = int(row.get('product_id') or 0)
            if pid <= 0:
                continue
            planned_boxes = float(row.get('planned_boxes') or 0)
            if planned_boxes <= 0:
                continue
            product_box_map[pid] = product_box_map.get(pid, 0.0) + planned_boxes
            if row.get('product_name'):
                product_name_map[pid] = row.get('product_name')

        if not product_box_map:
            return jsonify(
                {
                    'ok': True,
                    'scope': 'all_planned',
                    'summary': {'raw': [], 'base': [], 'sub': []},
                    'products': [],
                }
            )

        product_ids = list(product_box_map.keys())
        placeholders = ','.join(['?'] * len(product_ids))
        cursor.execute(
            f'''
            SELECT
                b.product_id,
                b.raw_material_id,
                b.material_id,
                COALESCE(p.sok_per_box, b.quantity_per_box, 0) as raw_qty_per_box,
                COALESCE(b.quantity_per_box, 0) as quantity_per_box,
                rm.name as raw_name,
                COALESCE(NULLIF(TRIM(rm.code), ''), printf('RM%05d', rm.id)) as raw_code,
                m.name as material_name,
                COALESCE(NULLIF(TRIM(m.code), ''), printf('M%05d', m.id)) as material_code,
                COALESCE(m.category, '') as material_category,
                COALESCE(NULLIF(TRIM(m.unit), ''), '개') as material_unit
            FROM bom b
            LEFT JOIN products p ON p.id = b.product_id
            LEFT JOIN raw_materials rm ON rm.id = b.raw_material_id
            LEFT JOIN materials m ON m.id = b.material_id
            WHERE b.product_id IN ({placeholders})
            ''',
            product_ids,
        )
        bom_rows = [dict(r) for r in cursor.fetchall()]

        cursor.execute(
            '''
            SELECT
                COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id)) as code,
                MIN(name) as name,
                COALESCE(SUM(COALESCE(current_stock, 0)), 0) as stock
            FROM raw_materials
            WHERE workplace = ?
            GROUP BY COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id))
            ''',
            (workplace,),
        )
        raw_stock_map = {str(r['code']): float(r['stock'] or 0) for r in cursor.fetchall()}

        stock_workplaces = [workplace]
        if workplace != '공통':
            stock_workplaces.append('공통')
        wp_placeholders = ','.join(['?'] * len(stock_workplaces))
        cursor.execute(
            f'''
            SELECT id, COALESCE(current_stock, 0) as stock
            FROM materials
            WHERE workplace IN ({wp_placeholders})
            ''',
            stock_workplaces,
        )
        material_stock_map = {int(r['id']): float(r['stock'] or 0) for r in cursor.fetchall()}

        summary_raw = {}
        summary_base = {}
        summary_sub = {}
        summary_sub_groups = {'box': {}, 'inner': {}, 'outer': {}, 'silica': {}, 'tray': {}, 'etc': {}}
        product_detail = {}

        def _upsert_item(target, key, code, name, unit, stock, required):
            if key not in target:
                target[key] = {
                    'code': (code or '-'),
                    'name': name or '-',
                    'unit': (unit or '개'),
                    'stock': float(stock or 0),
                    'required': 0.0,
                }
            target[key]['required'] += float(required or 0)

        seen_product_raw_keys = set()
        for row in bom_rows:
            pid = int(row.get('product_id') or 0)
            if pid <= 0 or pid not in product_box_map:
                continue

            if pid not in product_detail:
                product_detail[pid] = {
                    'product_id': pid,
                    'product_name': product_name_map.get(pid) or f'상품 {pid}',
                    'planned_boxes': float(product_box_map.get(pid) or 0),
                    'raw_map': {},
                    'base_map': {},
                    'sub_map': {},
                }

            if row.get('raw_material_id'):
                code = str(row.get('raw_code') or '')
                if not code:
                    continue
                dedupe_key = (pid, code)
                if dedupe_key in seen_product_raw_keys:
                    continue
                seen_product_raw_keys.add(dedupe_key)
                qty_per_box = float(row.get('raw_qty_per_box') or row.get('quantity_per_box') or 0)
                if qty_per_box <= 0:
                    continue
                need_qty = qty_per_box * float(product_box_map.get(pid) or 0)
                if need_qty <= 0:
                    continue
                name = row.get('raw_name') or code or '원초'
                stock = raw_stock_map.get(code, 0.0)
                _upsert_item(summary_raw, code or name, code, name, '속', stock, need_qty)
                _upsert_item(product_detail[pid]['raw_map'], code or name, code, name, '속', stock, need_qty)
            elif row.get('material_id'):
                qty_per_box = float(row.get('quantity_per_box') or 0)
                if qty_per_box <= 0:
                    continue
                need_qty = qty_per_box * float(product_box_map.get(pid) or 0)
                if need_qty <= 0:
                    continue
                mid = int(row.get('material_id') or 0)
                code = str(row.get('material_code') or f'M{mid:05d}')
                name = row.get('material_name') or code
                stock = material_stock_map.get(mid, 0.0)
                category = (row.get('material_category') or '').strip()
                is_base = category in ('기름', '소금')
                target_summary = summary_base if is_base else summary_sub
                target_product = product_detail[pid]['base_map'] if is_base else product_detail[pid]['sub_map']
                _upsert_item(target_summary, code or name, code, name, row.get('material_unit') or '개', stock, need_qty)
                _upsert_item(target_product, code or name, code, name, row.get('material_unit') or '개', stock, need_qty)
                if not is_base:
                    sub_key = _normalize_sub_category(category)
                    _upsert_item(summary_sub_groups[sub_key], code or name, code, name, row.get('material_unit') or '개', stock, need_qty)

        def _to_sorted_list(data_map):
            rows = []
            for _, item in data_map.items():
                stock = float(item.get('stock') or 0)
                required = float(item.get('required') or 0)
                shortage = required - stock
                if shortage < 0:
                    shortage = 0.0
                rows.append(
                    {
                        'code': item.get('code') or '-',
                        'name': item.get('name') or '-',
                        'unit': item.get('unit') or '개',
                        'stock': round(stock, 2),
                        'required': round(required, 2),
                        'shortage': round(shortage, 2),
                    }
                )
            rows.sort(key=lambda x: (x['shortage'] > 0, x['shortage'], x['required']), reverse=True)
            return rows

        products_payload = []
        for pid, item in product_detail.items():
            products_payload.append(
                {
                    'product_id': pid,
                    'product_name': item['product_name'],
                    'planned_boxes': round(float(item.get('planned_boxes') or 0), 2),
                    'raw_items': _to_sorted_list(item['raw_map']),
                    'base_items': _to_sorted_list(item['base_map']),
                    'sub_items': _to_sorted_list(item['sub_map']),
                }
            )
        products_payload.sort(key=lambda x: x['product_name'])

        return jsonify(
            {
                'ok': True,
                'scope': 'all_planned',
                'summary': {
                    'raw': _to_sorted_list(summary_raw),
                    'base': _to_sorted_list(summary_base),
                    'sub': _to_sorted_list(summary_sub),
                    'sub_groups': {
                        'box': _to_sorted_list(summary_sub_groups['box']),
                        'inner': _to_sorted_list(summary_sub_groups['inner']),
                        'outer': _to_sorted_list(summary_sub_groups['outer']),
                        'silica': _to_sorted_list(summary_sub_groups['silica']),
                        'tray': _to_sorted_list(summary_sub_groups['tray']),
                        'etc': _to_sorted_list(summary_sub_groups['etc']),
                    },
                },
                'products': products_payload,
            }
        )
    finally:
        conn.close()


@bp.route('/schedules/requirements-auto-purchase', methods=['POST'])
@role_required('production', 'purchase')
def schedule_requirements_auto_purchase():
    """??? ?? ?? ???? ?? ?? ???? ????."""
    payload = request.get_json(silent=True) or {}
    workplace = (payload.get('workplace') or request.form.get('workplace') or get_workplace() or '').strip()
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT ps.product_id, ps.planned_boxes, ps.status
            FROM production_schedules ps
            WHERE ps.workplace = ?
            ''',
            (workplace,),
        )
        planned_rows = []
        for raw_row in cursor.fetchall():
            row = dict(raw_row)
            if _normalize_production_status(row.get('status')) == '??':
                planned_rows.append(row)

        product_box_map = {}
        for row in planned_rows:
            pid = int(row.get('product_id') or 0)
            if pid <= 0:
                continue
            planned_boxes = float(row.get('planned_boxes') or 0)
            if planned_boxes <= 0:
                continue
            product_box_map[pid] = product_box_map.get(pid, 0.0) + planned_boxes

        if not product_box_map:
            return jsonify({
                'ok': True,
                'created_count': 0,
                'issue_created_count': 0,
                'purchase_created_count': 0,
                'skipped_count': 0,
                'message': '?? ???? ????.',
            })

        product_ids = list(product_box_map.keys())
        placeholders = ','.join(['?'] * len(product_ids))
        cursor.execute(
            f'''
            SELECT
                b.product_id,
                b.material_id,
                COALESCE(b.quantity_per_box, 0) as quantity_per_box,
                m.code as material_code,
                m.name as material_name,
                COALESCE(m.unit, '') as unit
            FROM bom b
            JOIN materials m ON m.id = b.material_id
            WHERE b.product_id IN ({placeholders})
              AND b.material_id IS NOT NULL
            ''',
            product_ids,
        )
        bom_rows = [dict(r) for r in cursor.fetchall()]

        material_ids = sorted({int(row.get('material_id') or 0) for row in bom_rows if int(row.get('material_id') or 0) > 0})
        workplace_stock_map = {}
        if material_ids:
            workplace_location = cursor.execute(
                '''
                SELECT id
                FROM inv_locations
                WHERE name = ? OR workplace_code = ?
                ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, id
                LIMIT 1
                ''',
                (workplace, workplace, workplace),
            ).fetchone()
            if workplace_location:
                material_placeholders = ','.join(['?'] * len(material_ids))
                cursor.execute(
                    f'''
                    SELECT ml.material_id, COALESCE(SUM(b.qty), 0) as qty
                    FROM inv_material_lot_balances b
                    JOIN material_lots ml ON ml.id = b.material_lot_id
                    WHERE b.location_id = ?
                      AND ml.material_id IN ({material_placeholders})
                      AND COALESCE(ml.is_disposed, 0) = 0
                    GROUP BY ml.material_id
                    ''',
                    [int(workplace_location['id']), *material_ids],
                )
                workplace_stock_map = {int(r['material_id']): float(r['qty'] or 0) for r in cursor.fetchall()}

        req_map = {}
        for row in bom_rows:
            pid = int(row.get('product_id') or 0)
            mid = int(row.get('material_id') or 0)
            if pid <= 0 or mid <= 0:
                continue
            boxes = float(product_box_map.get(pid) or 0)
            qty_per_box = float(row.get('quantity_per_box') or 0)
            if boxes <= 0 or qty_per_box <= 0:
                continue
            need_qty = boxes * qty_per_box
            if mid not in req_map:
                req_map[mid] = {
                    'material_id': mid,
                    'code': (row.get('material_code') or f'M{mid:05d}'),
                    'name': row.get('material_name') or f'??? {mid}',
                    'unit': row.get('unit') or '',
                    'required': 0.0,
                }
            req_map[mid]['required'] += need_qty

        req_user = session.get('user', {}).get('name') or session.get('user', {}).get('username') or 'system'
        req_username = session.get('user', {}).get('username')
        issue_created = []
        skipped = []

        for item in req_map.values():
            mid = int(item['material_id'])
            code = item['code']
            unit = item.get('unit') or ''
            required = float(item.get('required') or 0)
            workplace_stock = float(workplace_stock_map.get(mid, 0.0))
            shortage = required - workplace_stock
            if shortage <= 0:
                continue

            cursor.execute(
                '''
                SELECT id
                FROM logistics_issue_requests
                WHERE material_id = ?
                  AND requester_workplace = ?
                  AND COALESCE(request_type, 'ISSUE') = 'ISSUE'
                  AND status = '??'
                LIMIT 1
                ''',
                (mid, workplace),
            )
            existing_issue = cursor.fetchone()
            if existing_issue:
                skipped.append({'type': 'issue', 'material_id': mid, 'code': code, 'name': item['name']})
                continue

            issue_qty = round(shortage, 2)
            issue_note = f"[????] ?? ???? ?? ?? ?? (?? {issue_qty}{unit})"
            cursor.execute(
                '''
                INSERT INTO logistics_issue_requests
                (material_id, material_code, material_name, unit, requester_workplace, requested_quantity, request_type, note, requested_by, requester_username)
                VALUES (?, ?, ?, ?, ?, ?, 'ISSUE', ?, ?, ?)
                ''',
                (mid, code, item['name'], unit, workplace, issue_qty, issue_note, req_user, req_username),
            )
            issue_id = cursor.lastrowid
            audit_log(
                conn,
                'create',
                'logistics_issue_request',
                issue_id,
                {
                    'material_id': mid,
                    'material_code': code,
                    'material_name': item['name'],
                    'requester_workplace': workplace,
                    'requested_quantity': issue_qty,
                    'request_type': 'ISSUE',
                    'note': issue_note,
                    'requested_by': req_user,
                    'requester_username': req_username,
                    'source': 'schedule_requirements_auto_purchase',
                },
            )
            issue_created.append({
                'request_id': issue_id,
                'material_id': mid,
                'code': code,
                'name': item['name'],
                'requested_qty': issue_qty,
                'unit': unit,
            })

        conn.commit()
        return jsonify(
            {
                'ok': True,
                'created_count': len(issue_created),
                'issue_created_count': len(issue_created),
                'purchase_created_count': 0,
                'skipped_count': len(skipped),
                'issue_created': issue_created,
                'purchase_created': [],
                'skipped': skipped,
            }
        )
    except Exception as e:
        conn.rollback()
        return jsonify({'ok': False, 'message': str(e)}), 500
    finally:
        conn.close()


@bp.route('/schedules/add', methods=['POST'])
@role_required('production')
def add_schedule():
    """Auto-generated docstring."""
    workplace = get_workplace()
    product_id = (request.form.get('product_id') or '').strip()
    scheduled_dates = request.form.getlist('scheduled_dates')
    scheduled_dates = list(dict.fromkeys(d for d in scheduled_dates if d))
    planned_boxes = request.form.get('planned_boxes')
    production_lines = request.form.getlist('production_lines')
    production_lines_str = ','.join(production_lines) if production_lines else ''
    note = request.form.get('note', '')

    conn = get_db()
    cursor = conn.cursor()

    if not product_id:
        conn.close()
        return "<script>alert('상품을 선택해 주세요.');history.back();</script>", 400

    cursor.execute('SELECT id FROM products WHERE id = ? AND workplace = ?', (product_id, workplace))
    if not cursor.fetchone():
        conn.close()
        return "<script>alert('선택한 상품을 찾을 수 없습니다. 다시 선택해 주세요.');history.back();</script>", 400

    # 媛??좎쭨??????ㅼ?以?異붽?
    for scheduled_date in scheduled_dates:
        if scheduled_date:
            # ?ㅼ?以??앹꽦
            cursor.execute(
                '''
                INSERT INTO production_schedules (product_id, scheduled_date, planned_boxes, note, status, line, workplace)
                VALUES (?, ?, ?, ?, '예정', ?, ?)
            ''',
                (product_id, scheduled_date, planned_boxes, note, production_lines_str, workplace),
            )
            schedule_id = cursor.lastrowid

            # ???묐갑???곕룞: ?앹궛 愿由ъ뿉???먮룞 ?깅줉
            cursor.execute(
                '''
                INSERT INTO productions (product_id, production_date, planned_boxes, status, note, schedule_id, workplace)
                VALUES (?, ?, ?, '예정', ?, ?, ?)
            ''',
                (product_id, scheduled_date, planned_boxes, note, schedule_id, workplace),
            )
            production_id = cursor.lastrowid

            # ?ㅼ?以꾩뿉 production_id ???
            cursor.execute('UPDATE production_schedules SET production_id = ? WHERE id = ?', (production_id, schedule_id))

            audit_log(
                conn,
                'create',
                'production_schedule',
                schedule_id,
                {
                    'product_id': product_id,
                    'scheduled_date': scheduled_date,
                    'planned_boxes': planned_boxes,
                    'note': note,
                    'line': production_lines_str,
                    'workplace': workplace,
                    'production_id': production_id,
                },
            )

    conn.commit()
    conn.close()

    return redirect(url_for('production.schedules'))


@bp.route('/schedules/copy', methods=['POST'])
@role_required('production')
def copy_schedule():
    """Auto-generated docstring."""
    workplace = get_workplace()
    schedule_id = request.form.get('schedule_id')
    target_dates = request.form.getlist('target_dates')

    conn = get_db()
    cursor = conn.cursor()

    # ?먮낯 ?ㅼ?以??뺣낫 媛?몄삤湲?
    cursor.execute(
        '''
        SELECT product_id, planned_boxes, note, line
        FROM production_schedules
        WHERE id = ?
    ''',
        (schedule_id,),
    )
    original = cursor.fetchone()

    if original:
        # 媛?紐⑺몴 ?좎쭨??蹂듭궗
        for target_date in target_dates:
            if target_date:
                # ?ㅼ?以?蹂듭궗
                cursor.execute(
                    '''
                    INSERT INTO production_schedules (product_id, scheduled_date, planned_boxes, note, status, line, workplace)
                    VALUES (?, ?, ?, ?, '예정', ?, ?)
                ''',
                    (
                        original['product_id'],
                        target_date,
                        original['planned_boxes'],
                        original['note'],
                        original['line'],
                        workplace,
                    ),
                )
                new_schedule_id = cursor.lastrowid

                # ???묐갑???곕룞: ?앹궛 愿由ъ뿉???먮룞 ?앹꽦
                cursor.execute(
                    '''
                    INSERT INTO productions (product_id, production_date, planned_boxes, status, note, schedule_id, workplace)
                    VALUES (?, ?, ?, '예정', ?, ?, ?)
                ''',
                    (
                        original['product_id'],
                        target_date,
                        original['planned_boxes'],
                        original['note'],
                        new_schedule_id,
                        workplace,
                    ),
                )
                production_id = cursor.lastrowid

                # ?ㅼ?以꾩뿉 production_id ???
                cursor.execute('UPDATE production_schedules SET production_id = ? WHERE id = ?', (production_id, new_schedule_id))

                audit_log(
                    conn,
                    'copy',
                    'production_schedule',
                    new_schedule_id,
                    {
                        'source_schedule_id': schedule_id,
                        'product_id': original['product_id'],
                        'scheduled_date': target_date,
                        'planned_boxes': original['planned_boxes'],
                        'note': original['note'],
                        'line': original['line'],
                        'workplace': workplace,
                        'production_id': production_id,
                    },
                )

    conn.commit()
    conn.close()

    return redirect(url_for('production.schedules'))


@bp.route('/schedules/delete/<int:schedule_id>', methods=['POST'])
@role_required('production')
def delete_schedule(schedule_id):
    """Auto-generated docstring."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM production_schedules WHERE id = ?', (schedule_id,))
    schedule_before = cursor.fetchone()
    if not schedule_before:
        conn.close()
        return redirect(request.referrer or url_for('production.schedules'))

    if _normalize_production_status(schedule_before['status']) == '완료':
        conn.close()
        return redirect(request.referrer or url_for('production.schedules'))

    row = schedule_before

    if row and row['production_id']:
        production_id = row['production_id']
        cursor.execute('SELECT status FROM productions WHERE id = ?', (production_id,))
        prod = cursor.fetchone()

        if prod and _normalize_production_status(prod['status']) == '완료':
            cursor.execute(
                '''
                SELECT pmu.actual_quantity, pmu.material_id,
                       pmu.raw_material_id, pmu.raw_material_name
                FROM production_material_usage pmu
                WHERE pmu.production_id = ? AND pmu.actual_quantity > 0
            ''',
                (production_id,),
            )
            usages = cursor.fetchall()
            legacy_material_rollbacks = []

            for usage in usages:
                actual_qty = usage['actual_quantity']
                material_id = usage['material_id']
                raw_material_id = usage['raw_material_id']
                raw_material_name = usage['raw_material_name']

                if raw_material_name and not material_id:
                    rm_id = raw_material_id
                    if not rm_id:
                        cursor.execute('SELECT id FROM raw_materials WHERE name = ?', (raw_material_name,))
                        r = cursor.fetchone()
                        rm_id = r['id'] if r else None
                    if rm_id:
                        cursor.execute(
                            '''
                            UPDATE raw_materials
                            SET current_stock = current_stock + ?,
                                used_quantity = MAX(0, used_quantity - ?)
                            WHERE id = ?
                        ''',
                            (actual_qty, actual_qty, rm_id),
                        )
                elif material_id:
                    legacy_material_rollbacks.append((material_id, actual_qty))

            touched = _rollback_material_lot_usage_for_production(cursor, production_id, 'schedule_delete')
            for mid in touched:
                _sync_material_stock_with_lots(conn, mid)
            if not touched:
                for mat_id, qty in legacy_material_rollbacks:
                    cursor.execute('UPDATE materials SET current_stock = current_stock + ? WHERE id = ?', (qty, mat_id))

        cursor.execute('DELETE FROM production_material_usage WHERE production_id = ?', (production_id,))
        cursor.execute('DELETE FROM productions WHERE id = ?', (production_id,))

    cursor.execute('DELETE FROM production_schedules WHERE id = ?', (schedule_id,))
    audit_log(
        conn,
        'delete',
        'production_schedule',
        schedule_id,
        {'before': dict(schedule_before) if schedule_before else None},
    )

    conn.commit()
    conn.close()

    return redirect(request.referrer or url_for('production.schedules'))


@bp.route('/schedules/<date>')
@login_required
def schedule_detail(date):
    """Auto-generated docstring."""
    workplace = get_workplace()
    conn = get_db()
    cursor = conn.cursor()

    # ?대떦 ?좎쭨???ㅼ?以?媛?몄삤湲?
    cursor.execute(
        '''
        SELECT ps.*, p.name as product_name
        FROM production_schedules ps
        LEFT JOIN products p ON ps.product_id = p.id
        WHERE ps.scheduled_date = ? AND ps.workplace = ?
        ORDER BY ps.created_at DESC
    ''',
        (date, workplace),
    )
    schedules = cursor.fetchall()

    # ?곹뭹 紐⑸줉 (JSON 吏곷젹??媛?ν븯?꾨줉 dict濡?蹂??
    cursor.execute('SELECT id, name FROM products WHERE workplace = ? ORDER BY name ASC', (workplace,))
    products = rows_to_dict(cursor.fetchall())

    conn.close()

    return render_template('schedule_detail.html', user=session['user'], date=date, schedules=schedules, products=products)


@bp.route('/schedules/<date>/add', methods=['POST'])
@role_required('production')
def add_schedule_to_date(date):
    """Auto-generated docstring."""
    workplace = get_workplace()
    product_id = (request.form.get('product_id') or '').strip()
    planned_boxes = request.form.get('planned_boxes')
    production_lines = request.form.getlist('production_lines')
    production_lines_str = ','.join(production_lines) if production_lines else ''

    conn = get_db()
    cursor = conn.cursor()

    if not product_id:
        conn.close()
        return "<script>alert('상품을 선택해 주세요.');history.back();</script>", 400

    cursor.execute('SELECT id FROM products WHERE id = ? AND workplace = ?', (product_id, workplace))
    if not cursor.fetchone():
        conn.close()
        return "<script>alert('선택한 상품을 찾을 수 없습니다. 다시 선택해 주세요.');history.back();</script>", 400

    # ?ㅼ?以??앹꽦 (workplace 異붽?)
    cursor.execute(
        '''
        INSERT INTO production_schedules (product_id, scheduled_date, planned_boxes, status, line, workplace)
        VALUES (?, ?, ?, '예정', ?, ?)
    ''',
        (product_id, date, planned_boxes, production_lines_str, workplace),
    )
    schedule_id = cursor.lastrowid

    # ???묐갑???곕룞: ?앹궛 愿由ъ뿉???먮룞 ?깅줉 (workplace 異붽?)
    cursor.execute(
        '''
        INSERT INTO productions (product_id, production_date, planned_boxes, status, schedule_id, workplace)
        VALUES (?, ?, ?, '예정', ?, ?)
    ''',
        (product_id, date, planned_boxes, schedule_id, workplace),
    )
    production_id = cursor.lastrowid

    # ?ㅼ?以꾩뿉 production_id ???
    cursor.execute('UPDATE production_schedules SET production_id = ? WHERE id = ?', (production_id, schedule_id))

    audit_log(
        conn,
        'create',
        'production_schedule',
        schedule_id,
        {
            'product_id': product_id,
            'scheduled_date': date,
            'planned_boxes': planned_boxes,
            'line': production_lines_str,
            'workplace': workplace,
            'production_id': production_id,
        },
    )

    conn.commit()
    conn.close()

    return redirect(url_for('production.schedule_detail', date=date))


@bp.route('/work-days')
@login_required
def work_days():
    """Auto-generated docstring."""
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)

    # 湲곕낯媛? ?대쾲 ??
    today = date.today()
    if not year or not month:
        year = today.year
        month = today.month

    conn = get_db()
    cursor = conn.cursor()

    # ?대떦 ?붿쓽 洹쇰Т???뺣낫 媛?몄삤湲?
    month_start = date(year, month, 1)
    if month == 12:
        month_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(year, month + 1, 1) - timedelta(days=1)

    cursor.execute(
        '''
        SELECT date, type, overtime_hours, note
        FROM work_days
        WHERE date BETWEEN ? AND ?
    ''',
        (month_start.isoformat(), month_end.isoformat()),
    )
    work_days_data = {row['date']: row for row in cursor.fetchall()}

    # 罹섎┛???앹꽦
    from calendar import monthrange

    first_weekday = month_start.weekday()
    if first_weekday == 6:  # ?쇱슂??
        first_weekday = 0
    else:
        first_weekday += 1

    days_in_month = monthrange(year, month)[1]

    calendar_days = []

    # ?댁쟾 ???좎쭨 梨꾩슦湲?
    if first_weekday > 0:
        prev_month = month - 1 if month > 1 else 12
        prev_year = year if month > 1 else year - 1
        prev_days_in_month = monthrange(prev_year, prev_month)[1]
        for i in range(first_weekday):
            day_num = prev_days_in_month - first_weekday + i + 1
            calendar_days.append(
                {
                    'day': day_num,
                    'date': date(prev_year, prev_month, day_num).isoformat(),
                    'current_month': False,
                    'type': None,
                    'overtime_hours': 0,
                    'note': '',
                }
            )

    # ?꾩옱 ???좎쭨
    for day in range(1, days_in_month + 1):
        day_date = date(year, month, day).isoformat()
        work_day = work_days_data.get(day_date)
        calendar_days.append(
            {
                'day': day,
                'date': day_date,
                'current_month': True,
                'type': work_day['type'] if work_day else None,
                'overtime_hours': work_day['overtime_hours'] if work_day else 0,
                'note': work_day['note'] if work_day else '',
            }
        )

    # ?ㅼ쓬 ???좎쭨 梨꾩슦湲?
    remaining = 42 - len(calendar_days)  # 6二?= 42移?
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    for day in range(1, remaining + 1):
        calendar_days.append(
            {
                'day': day,
                'date': date(next_year, next_month, day).isoformat(),
                'current_month': False,
                'type': None,
                'overtime_hours': 0,
                'note': '',
            }
        )

    # ?듦퀎 怨꾩궛
    stats = {
        'work': sum(1 for d in work_days_data.values() if d['type'] == 'work'),
        'holiday': sum(1 for d in work_days_data.values() if d['type'] == 'holiday'),
        'overtime': sum(1 for d in work_days_data.values() if d['type'] == 'overtime'),
        'extra': sum(1 for d in work_days_data.values() if d['type'] == 'extra'),
    }

    conn.close()

    return render_template(
        'work_days.html',
        user=session['user'],
        year=year,
        month=month,
        calendar_days=calendar_days,
        stats=stats,
    )


@bp.route('/work-days/manage', methods=['POST'])
@role_required('production')
def manage_work_day():
    """Auto-generated docstring."""
    work_date = request.form.get('date')
    work_type = request.form.get('type')
    overtime_hours = request.form.get('overtime_hours') or 0
    note = request.form.get('note', '')

    conn = get_db()
    cursor = conn.cursor()

    # 湲곗〈 ?곗씠?곌? ?덈뒗吏 ?뺤씤
    cursor.execute('SELECT id FROM work_days WHERE date = ?', (work_date,))
    existing = cursor.fetchone()

    if existing:
        # ?낅뜲?댄듃
        cursor.execute(
            '''
            UPDATE work_days
            SET type = ?, overtime_hours = ?, note = ?
            WHERE date = ?
        ''',
            (work_type, overtime_hours, note, work_date),
        )
        audit_log(
            conn,
            'update',
            'work_day',
            None,
            {'date': work_date, 'type': work_type, 'overtime_hours': overtime_hours, 'note': note},
        )
    else:
        # ?쎌엯
        cursor.execute(
            '''
            INSERT INTO work_days (date, type, overtime_hours, note)
            VALUES (?, ?, ?, ?)
        ''',
            (work_date, work_type, overtime_hours, note),
        )
        audit_log(
            conn,
            'create',
            'work_day',
            None,
            {'date': work_date, 'type': work_type, 'overtime_hours': overtime_hours, 'note': note},
        )

    conn.commit()
    conn.close()

    # ?대떦 ?좎쭨???곗썡濡?由щ떎?대젆??
    work_date_obj = datetime.strptime(work_date, '%Y-%m-%d').date()
    return redirect(url_for('production.work_days', year=work_date_obj.year, month=work_date_obj.month))


@bp.route('/work-days/delete', methods=['POST'])
@role_required('production')
def delete_work_day():
    """Auto-generated docstring."""
    work_date = request.form.get('date')

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('DELETE FROM work_days WHERE date = ?', (work_date,))
    audit_log(conn, 'delete', 'work_day', None, {'date': work_date})
    conn.commit()
    conn.close()

    # ?대떦 ?좎쭨???곗썡濡?由щ떎?대젆??
    work_date_obj = datetime.strptime(work_date, '%Y-%m-%d').date()
    return redirect(url_for('production.work_days', year=work_date_obj.year, month=work_date_obj.month))


@bp.route('/production')
@login_required
def production_list():
    """Auto-generated docstring."""
    from datetime import datetime as dt

    workplace = get_workplace()

    # 荑쇰━ ?뚮씪誘명꽣
    month_param = request.args.get('month', '')
    tab_param = request.args.get('tab', 'active')  # active | done

    # ?꾩옱 ??諛?理쒓렐 6媛쒖썡 怨꾩궛
    today = dt.today()
    if month_param:
        try:
            current_dt = dt.strptime(month_param + '-01', '%Y-%m-%d')
        except Exception:
            current_dt = dt(today.year, today.month, 1)
    else:
        current_dt = dt(today.year, today.month, 1)

    current_month = current_dt.strftime('%Y-%m')
    current_year = current_dt.strftime('%Y')
    current_month_num = current_dt.strftime('%m')

    # ?댁쟾/?ㅼ쓬 ??
    if current_dt.month == 1:
        prev_dt = current_dt.replace(year=current_dt.year - 1, month=12)
    else:
        prev_dt = current_dt.replace(month=current_dt.month - 1)

    if current_dt.month == 12:
        next_dt = current_dt.replace(year=current_dt.year + 1, month=1)
    else:
        next_dt = current_dt.replace(month=current_dt.month + 1)
    prev_month = prev_dt.strftime('%Y-%m')
    next_month = next_dt.strftime('%Y-%m')

    conn = get_db()
    cursor = conn.cursor()

    # 월별 데이터 조회 후 상태 정규화로 탭 분리
    cursor.execute(
        '''
        SELECT
            pr.*,
            p.name as product_name,
            COALESCE(NULLIF(pr.supply_line, ''), ps.line, '') as display_line
        FROM productions pr
        LEFT JOIN products p ON pr.product_id = p.id
        LEFT JOIN production_schedules ps ON pr.schedule_id = ps.id
        WHERE strftime('%Y-%m', pr.production_date) = ?
          AND pr.workplace = ?
        ORDER BY pr.production_date DESC
        ''',
        (current_month, workplace),
    )
    all_rows = [dict(r) for r in cursor.fetchall()]
    for row in all_rows:
        row['status'] = _normalize_production_status(row.get('status'))

    done_rows = [r for r in all_rows if r.get('status') == '완료']
    active_rows = [r for r in all_rows if r.get('status') != '완료']
    done_count = len(done_rows)
    active_count = len(active_rows)
    if tab_param == 'active' and not active_rows and done_rows:
        tab_param = 'done'
    productions = done_rows if tab_param == 'done' else active_rows

    cursor.execute(
        '''
        SELECT production_date, status
        FROM productions
        WHERE workplace = ?
          AND production_date IS NOT NULL
          AND production_date != ''
        ORDER BY production_date DESC
        ''',
        (workplace,),
    )
    all_nav_rows = [dict(r) for r in cursor.fetchall()]
    available_calendar_dates = []
    seen_calendar_dates = set()
    for row in all_nav_rows:
        normalized_status = _normalize_production_status(row.get('status'))
        include_row = normalized_status == '완료' if tab_param == 'done' else normalized_status != '완료'
        if not include_row:
            continue
        production_date = (row.get('production_date') or '').strip()
        if not production_date or production_date in seen_calendar_dates:
            continue
        seen_calendar_dates.add(production_date)
        available_calendar_dates.append(production_date)
    conn.close()

    return render_template(
        'production.html',
        user=session['user'],
        productions=productions,
        current_month=current_month,
        current_year=current_year,
        current_month_num=current_month_num,
        prev_month=prev_month,
        next_month=next_month,
        current_tab=tab_param,
        active_count=active_count,
        done_count=done_count,
        available_calendar_dates_json=json.dumps(available_calendar_dates, ensure_ascii=False),
        search_start=(dt.today() - timedelta(days=90)).strftime('%Y-%m-%d'),
        search_end=dt.today().strftime('%Y-%m-%d'),
    )


@bp.route('/production/add', methods=['GET', 'POST'])
@login_required
def add_production():
    """Auto-generated docstring."""
    workplace = get_workplace()

    if request.method == 'GET':
        conn = get_db()
        cursor = conn.cursor()

        # ?곹뭹 紐⑸줉 (?꾩옱 ?묒뾽?λ쭔)
        cursor.execute('SELECT id, name, box_quantity FROM products WHERE workplace = ? ORDER BY name', (workplace,))
        products = cursor.fetchall()

        # ?곹뭹蹂?BOM ?먯큹 ?뺣낫 (?먯큹 ?ш퀬/?먰샇/?낃퀬???ы븿) - ?묒뾽???꾪꽣
        cursor.execute(
            '''
            SELECT b.product_id, b.quantity_per_box,
                   rm.id as rm_id, rm.name as rm_name,
                   rm.car_number, rm.receiving_date, rm.current_stock
            FROM bom b
            JOIN raw_materials rm ON b.raw_material_id = rm.id
            JOIN products p ON b.product_id = p.id
            WHERE p.workplace = ?
        ''',
            (workplace,),
        )
        bom_raw = cursor.fetchall()

        # ?곹뭹蹂?BOM 遺?먯옱 ?뺣낫 - ?묒뾽???꾪꽣
        cursor.execute(
            '''
            SELECT b.product_id, b.quantity_per_box,
                   m.id as m_id, m.name as m_name, m.unit
            FROM bom b
            JOIN materials m ON b.material_id = m.id
            JOIN products p ON b.product_id = p.id
            WHERE p.workplace = ?
        ''',
            (workplace,),
        )
        bom_mat = cursor.fetchall()

        conn.close()

        # JSON?쇰줈 蹂??
        products_list = [{'id': p['id'], 'name': p['name'], 'box_quantity': p['box_quantity']} for p in products]
        products_json = json.dumps(products_list, ensure_ascii=False)

        bom_raw_data = {}
        for row in bom_raw:
            pid = row['product_id']
            if pid not in bom_raw_data:
                bom_raw_data[pid] = []
            bom_raw_data[pid].append(
                {
                    'rm_id': row['rm_id'],
                    'rm_name': row['rm_name'],
                    'car_number': row['car_number'] or '-',
                    'receiving_date': row['receiving_date'] or '-',
                    'current_stock': row['current_stock'],
                    'quantity_per_box': row['quantity_per_box'],
                }
            )

        bom_mat_data = {}
        for row in bom_mat:
            pid = row['product_id']
            if pid not in bom_mat_data:
                bom_mat_data[pid] = []
            bom_mat_data[pid].append(
                {
                    'm_id': row['m_id'],
                    'm_name': row['m_name'],
                    'unit': row['unit'],
                    'quantity_per_box': row['quantity_per_box'],
                }
            )

        return render_template(
            'production_add.html',
            user=session['user'],
            products=products,
            products_json=products_json,
            today=date.today(),
            bom_raw_data=bom_raw_data,
            bom_mat_data=bom_mat_data,
        )

    # POST ?붿껌 泥섎━
    product_id = request.form.get('product_id')
    production_date = request.form.get('production_date')
    planned_boxes = request.form.get('planned_boxes')
    production_lines = request.form.getlist('production_lines')
    production_lines_str = ','.join(production_lines) if production_lines else ''
    note = request.form.get('note')

    if not production_lines:
        return "<script>alert('?앹궛 ?쇱씤???좏깮?댁＜?몄슂.'); window.history.back();</script>"

    conn = get_db()
    cursor = conn.cursor()

    # ?앹궛 湲곕줉 ?앹꽦 (workplace 異붽?)
    cursor.execute(
        '''
        INSERT INTO productions (product_id, production_date, planned_boxes, status, note, workplace)
        VALUES (?, ?, ?, '예정', ?, ?)
    ''',
        (product_id, production_date, planned_boxes, note, workplace),
    )

    production_id = cursor.lastrowid

    # ???묐갑???곕룞: ?ㅼ?以꾩뿉???먮룞 ?깅줉 (workplace 異붽?)
    cursor.execute(
        '''
        INSERT INTO production_schedules (product_id, scheduled_date, planned_boxes, status, note, production_id, line, workplace)
        VALUES (?, ?, ?, '예정', ?, ?, ?, ?)
    ''',
        (product_id, production_date, planned_boxes, note, production_id, production_lines_str, workplace),
    )
    schedule_id = cursor.lastrowid

    # ?앹궛??schedule_id ???
    cursor.execute('UPDATE productions SET schedule_id = ? WHERE id = ?', (schedule_id, production_id))

    audit_log(
        conn,
        'create',
        'production',
        production_id,
        {
            'product_id': product_id,
            'production_date': production_date,
            'planned_boxes': planned_boxes,
            'line': production_lines_str,
            'note': note,
            'workplace': workplace,
            'schedule_id': schedule_id,
        },
    )

    conn.commit()
    conn.close()

    return redirect(url_for('production.production_detail', production_id=production_id))


@bp.route('/production/<int:production_id>')
@login_required
def production_detail(production_id):
    """Auto-generated docstring."""
    import math

    material_shortage_popup = session.pop('material_shortage_popup', None)
    if material_shortage_popup and int(material_shortage_popup.get('production_id', 0) or 0) != int(production_id):
        material_shortage_popup = None

    conn = get_db()
    cursor = conn.cursor()

    # ?앹궛 ?뺣낫
    cursor.execute(
        '''
        SELECT pr.*, p.name as product_name, p.box_quantity, p.sok_per_box, p.expiry_months
        FROM productions pr
        LEFT JOIN products p ON pr.product_id = p.id
        WHERE pr.id = ?
    ''',
        (production_id,),
    )
    production = cursor.fetchone()

    if not production:
        conn.close()
        return redirect(url_for('production.production_list'))

    production = dict(production)
    production['status'] = _normalize_production_status(production.get('status'))
    edit_completed = request.args.get('edit') == '1'


    calculated_expiry_date = ''
    try:
        prod_dt = datetime.strptime(production['production_date'], '%Y-%m-%d').date()
        expiry_months = int(production['expiry_months'] or 12)
        month_index = (prod_dt.month - 1) + expiry_months
        expiry_year = prod_dt.year + (month_index // 12)
        expiry_month = (month_index % 12) + 1
        expiry_day = min(prod_dt.day, calendar.monthrange(expiry_year, expiry_month)[1])
        calculated_expiry_date = (date(expiry_year, expiry_month, expiry_day) - timedelta(days=1)).isoformat()
    except Exception:
        calculated_expiry_date = production['production_date'] or ''

    # ???곹뭹??BOM ?먯큹 紐⑸줉
    # - 완료???앹궛: ?ㅼ젣 ?ъ슜???먯큹留??쒖떆 (production_material_usage 湲곗?)
    # - 吏꾪뻾 以??앹궛: ?ш퀬 ?덈뒗 ?먯큹 ?쒖떆 (?좏깮 媛??
    product_sok = float(production['sok_per_box'] or 0)

    if production['status'] == '완료' and not edit_completed:
        # 완료嫄? ?ㅼ젣 ?ъ슜???먯큹 湲곕줉 ?쒖떆 (?뚯쭊???먯큹???대쫫 ?쒖떆)
        cursor.execute(
            '''
            SELECT pmu.raw_material_id as rm_id, 
                   pmu.actual_quantity as quantity_per_box,
                   pmu.actual_quantity as actual_quantity,
                   pmu.expected_quantity as expected_quantity,
                   pmu.yield_rate as yield_rate,
                   COALESCE(rm.name, pmu.raw_material_name, '(??젣???먯큹)') as rm_name, 
                   rm.car_number, 
                   rm.receiving_date,
                   COALESCE(rm.current_stock, 0) as current_stock
            FROM production_material_usage pmu
            LEFT JOIN raw_materials rm ON pmu.raw_material_id = rm.id
            WHERE pmu.production_id = ? 
            AND pmu.raw_material_id IS NOT NULL
            AND pmu.actual_quantity > 0
            ORDER BY rm.receiving_date ASC
        ''',
            (production_id,),
        )
    else:
        # ???: BOM ?? ??? ???? ??? ?? ??? ??
        if edit_completed:
            cursor.execute(
                '''
                WITH old_usage AS (
                    SELECT raw_material_id, SUM(COALESCE(actual_quantity, 0)) as rolled_back_qty
                    FROM production_material_usage
                    WHERE production_id = ?
                      AND raw_material_id IS NOT NULL
                    GROUP BY raw_material_id
                ),
                bom_codes AS (
                    SELECT DISTINCT
                        COALESCE(NULLIF(TRIM(rm.code), ''), printf('RM%05d', rm.id)) as raw_code
                    FROM bom b
                    JOIN raw_materials rm ON b.raw_material_id = rm.id
                    WHERE b.product_id = ?
                      AND b.raw_material_id IS NOT NULL
                )
                SELECT
                    src.id as raw_material_id,
                    ? as quantity_per_box,
                    src.name as rm_name,
                    src.car_number,
                    src.receiving_date,
                    (COALESCE(src.current_stock, 0) + COALESCE(ou.rolled_back_qty, 0)) as current_stock,
                    src.id as rm_id
                FROM raw_materials src
                JOIN bom_codes bc
                  ON bc.raw_code = COALESCE(NULLIF(TRIM(src.code), ''), printf('RM%05d', src.id))
                LEFT JOIN old_usage ou ON ou.raw_material_id = src.id
                WHERE src.workplace = ?
                  AND (COALESCE(src.current_stock, 0) > 0 OR COALESCE(ou.rolled_back_qty, 0) > 0)
                ORDER BY
                    CASE WHEN src.receiving_date IS NULL OR TRIM(src.receiving_date) = '' THEN 1 ELSE 0 END ASC,
                    src.receiving_date ASC,
                    src.id ASC
            ''',
                (production_id, production['product_id'], product_sok, production['workplace']),
            )
        else:
            cursor.execute(
                '''
                WITH bom_codes AS (
                    SELECT DISTINCT
                        COALESCE(NULLIF(TRIM(rm.code), ''), printf('RM%05d', rm.id)) as raw_code
                    FROM bom b
                    JOIN raw_materials rm ON b.raw_material_id = rm.id
                    WHERE b.product_id = ?
                      AND b.raw_material_id IS NOT NULL
                )
                SELECT
                    src.id as raw_material_id,
                    ? as quantity_per_box,
                    src.name as rm_name,
                    src.car_number,
                    src.receiving_date,
                    src.current_stock,
                    src.id as rm_id
                FROM raw_materials src
                JOIN bom_codes bc
                  ON bc.raw_code = COALESCE(NULLIF(TRIM(src.code), ''), printf('RM%05d', src.id))
                WHERE src.workplace = ?
                  AND COALESCE(src.current_stock, 0) > 0
                ORDER BY
                    CASE WHEN src.receiving_date IS NULL OR TRIM(src.receiving_date) = '' THEN 1 ELSE 0 END ASC,
                    src.receiving_date ASC,
                    src.id ASC
            ''',
                (production['product_id'], product_sok, production['workplace']),
            )


    bom_raw_items = cursor.fetchall()

    # 遺?먯옱 ?ъ슜 ?댁뿭 ?뺤씤
    cursor.execute('SELECT COUNT(*) as count FROM production_material_usage WHERE production_id = ?', (production_id,))
    usage_count = cursor.fetchone()['count']

    # 遺?먯옱 ?ъ슜?됱씠 ?놁쑝硫?BOM 湲곕컲?쇰줈 ?먮룞 ?앹꽦 (遺?먯옱留? ?먯큹???ъ슜?먭? ?좏깮)
    if usage_count == 0 and production:
        planned = float(production['planned_boxes'])

        cursor.execute(
            '''
            SELECT b.*, m.name as material_name, m.unit, m.category
            FROM bom b
            JOIN materials m ON b.material_id = m.id
            WHERE b.product_id = ?
        ''',
            (production['product_id'],),
        )
        bom_mats = cursor.fetchall()

        for bom in bom_mats:
            exact_qty = float(bom['quantity_per_box']) * planned
            expected_qty = math.ceil(exact_qty * 100) / 100
            cursor.execute(
                '''
                INSERT INTO production_material_usage 
                (production_id, material_id, raw_material_name, expected_quantity)
                VALUES (?, ?, NULL, ?)
            ''',
                (production_id, bom['material_id'], expected_qty),
            )

        conn.commit()

    # 遺?먯옱 ?ъ슜 ?댁뿭 議고쉶
    cursor.execute(
        '''
        SELECT pmu.*, 
               COALESCE(pmu.raw_material_name, m.name) as material_name,
               COALESCE(m.unit, '-') as unit,
               COALESCE(m.category, '원초') as category
        FROM production_material_usage pmu
        LEFT JOIN materials m ON pmu.material_id = m.id
        WHERE pmu.production_id = ?
        ORDER BY category, material_name
    ''',
        (production_id,),
    )
    material_usage = [dict(row) for row in cursor.fetchall()]
    current_workplace = (production.get('workplace') or get_workplace() or session.get('workplace') or '').strip()
    for row in material_usage:
        material_id = row.get('material_id')
        if material_id:
            gap = _get_material_info_gap(cursor, int(material_id), row.get('category'), current_workplace)
            row['lot_info_gap'] = gap
            row['has_lot_info_gap'] = bool(gap)
        else:
            row['lot_info_gap'] = None
            row['has_lot_info_gap'] = False
    raw_saved_map = {}
    for row in material_usage:
        if row['raw_material_id'] and row['actual_quantity'] is not None:
            raw_saved_map[row['raw_material_id']] = raw_saved_map.get(row['raw_material_id'], 0) + row['actual_quantity']

    conn.close()

    return render_template(
        'production_detail.html',
        user=session['user'],
        production=production,
        material_usage=material_usage,
        bom_raw_items=bom_raw_items,
        calculated_expiry_date=calculated_expiry_date,
        raw_saved_map=raw_saved_map,
        material_shortage_popup=material_shortage_popup,
    )


@bp.route('/production/<int:production_id>/update-usage', methods=['POST'])
@role_required('production')
def update_production_usage(production_id):
    """Auto-generated docstring."""
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('BEGIN IMMEDIATE')

        def _detail_redirect():
            kwargs = {'production_id': production_id}
            if (request.form.get('return_edit') or '').strip() == '1':
                kwargs['edit'] = 1
            return redirect(url_for('production.production_detail', **kwargs))

        save_action = (request.form.get('save_action') or 'complete').strip().lower()
        apply_usage_id = (request.form.get('apply_usage_id') or '').strip()
        actual_boxes = float(request.form.get('actual_boxes', 0) or 0)
        cursor.execute(
            '''
            SELECT pr.planned_boxes, pr.product_id, pr.production_date, pr.status, p.expiry_months
            FROM productions pr
            LEFT JOIN products p ON p.id = pr.product_id
            WHERE pr.id = ?
            ''',
            (production_id,),
        )
        prod_row = cursor.fetchone()
        planned_boxes = float(prod_row['planned_boxes']) if prod_row and prod_row['planned_boxes'] else 0
        product_id = prod_row['product_id'] if prod_row else None
        touched_material_ids = set()
        current_workplace = (get_workplace() or session.get('workplace') or '').strip()
        workplace_location_id = _get_inventory_location_id(cursor, current_workplace) if current_workplace else None
        production_status = _normalize_production_status(prod_row['status'] if prod_row and prod_row['status'] else '')
        is_completed_status = production_status == _normalize_production_status('\uC644\uB8CC')

        if apply_usage_id:
            cursor.execute(
                '''
                SELECT pmu.id, pmu.material_id, COALESCE(m.category, '') AS category
                FROM production_material_usage pmu
                LEFT JOIN materials m ON pmu.material_id = m.id
                WHERE pmu.id = ? AND pmu.production_id = ? AND pmu.material_id IS NOT NULL
                ''',
                (apply_usage_id, production_id),
            )
            usage_row = cursor.fetchone()
            if not usage_row:
                conn.execute('ROLLBACK')
                return "<script>alert('정보를 적용할 부자재 항목을 찾을 수 없습니다.'); window.history.back();</script>"

            gap = _get_material_info_gap(cursor, int(usage_row['material_id']), usage_row['category'], current_workplace)
            if not gap:
                cursor.execute('COMMIT')
                return _detail_redirect()

            _apply_missing_material_info_to_lot(
                cursor,
                int(gap['lot_id']),
                usage_row['category'],
                request.form.get(f'mat_info_receiving_{apply_usage_id}') or '',
                request.form.get(f'mat_info_manufacture_{apply_usage_id}') or '',
                request.form.get(f'mat_info_expiry_{apply_usage_id}') or '',
                (request.form.get(f'mat_info_manufacture_none_{apply_usage_id}') or '').strip() == '1',
                (request.form.get(f'mat_info_expiry_none_{apply_usage_id}') or '').strip() == '1',
            )
            cursor.execute('COMMIT')
            return _detail_redirect()

        def _to_int(name):
            raw = (request.form.get(name) or '').strip()
            return int(raw) if raw else None

        supply_people = _to_int('supply_people')
        packing_people = _to_int('packing_people')
        outer_packing_people = _to_int('outer_packing_people')
        work_time = (request.form.get('work_time') or '').strip()
        personnel_note = (request.form.get('personnel_note') or '').strip()
        expiry_date_input = (request.form.get('expiry_date') or '').strip()

        production_date_str = prod_row['production_date'] if prod_row and prod_row['production_date'] else ''
        expiry_months = int(prod_row['expiry_months'] or 12) if prod_row else 12
        default_expiry_date = production_date_str
        try:
            prod_dt = datetime.strptime(production_date_str, '%Y-%m-%d').date()
            month_index = (prod_dt.month - 1) + expiry_months
            expiry_year = prod_dt.year + (month_index // 12)
            expiry_month = (month_index % 12) + 1
            expiry_day = min(prod_dt.day, calendar.monthrange(expiry_year, expiry_month)[1])
            default_expiry_date = (date(expiry_year, expiry_month, expiry_day) - timedelta(days=1)).isoformat()
        except Exception:
            pass

        expiry_date = expiry_date_input or default_expiry_date
        import re
        if expiry_date and not re.match(r'^\d{4}-\d{2}-\d{2}[A-Za-z]*$', expiry_date):
            conn.execute('ROLLBACK')
            return "<script>alert('?뚮퉬湲고븳 ?뺤떇? YYYY-MM-DD ?먮뒗 YYYY-MM-DDA ?뺥깭濡??낅젰?댁＜?몄슂.'); window.history.back();</script>"

        missing = []
        if supply_people is None:
            missing.append('怨듦툒 ?몄썝')
        if packing_people is None:
            missing.append('?ъ옣 ?몄썝')
        if outer_packing_people is None:
            missing.append('?명룷???몄썝')
        if not work_time:
            missing.append('?묒뾽?쒓컙')
        if missing:
            conn.execute('ROLLBACK')
            return f"<script>alert('?몄썝愿由??꾩닔 ?낅젰: {', '.join(missing)}'); window.history.back();</script>"

        cursor.execute(
            '''
            SELECT COALESCE(
                (SELECT line FROM production_schedules WHERE production_id = ? LIMIT 1),
                (SELECT ps.line
                 FROM productions pr
                 LEFT JOIN production_schedules ps ON pr.schedule_id = ps.id
                 WHERE pr.id = ? LIMIT 1),
                ''
            ) as line
            ''',
            (production_id, production_id),
        )
        schedule_row = cursor.fetchone()
        planned_line = (schedule_row['line'] if schedule_row and schedule_row['line'] else '')

        cursor.execute(
            '''
            UPDATE productions
            SET supply_line = ?, supply_people = ?,
                packing_line = ?, packing_people = ?,
                outer_packing_line = ?, outer_packing_people = ?,
                work_time = ?, personnel_note = ?, expiry_date = ?
            WHERE id = ?
            ''',
            (
                planned_line,
                supply_people,
                planned_line,
                packing_people,
                planned_line,
                outer_packing_people,
                work_time,
                personnel_note,
                expiry_date,
                production_id,
            ),
        )

        # 1. ?ㅼ젣 諛뺤뒪 ?섎줈 ?덉긽???ш퀎????湲곗〈 usage ?낅뜲?댄듃
        if actual_boxes > 0:
            import math

            cursor.execute(
                '''
                SELECT b.material_id, b.quantity_per_box
                FROM bom b
                WHERE b.product_id = (SELECT product_id FROM productions WHERE id = ?)
                AND b.material_id IS NOT NULL
            ''',
                (production_id,),
            )
            bom_mats = cursor.fetchall()

            for bom in bom_mats:
                exact = float(bom['quantity_per_box']) * actual_boxes
                new_expected = math.ceil(exact * 100) / 100
                cursor.execute(
                    '''
                    UPDATE production_material_usage
                    SET expected_quantity = ?
                    WHERE production_id = ? AND material_id = ?
                ''',
                    (new_expected, production_id, bom['material_id']),
                )

        # 2. ?먯큹 ?ㅼ쨷 ?좏깮 泥섎━ (raw_rm_id_N ?뺤떇)
        raw_entries = {}
        for key in request.form:
            if key.startswith('raw_rm_id_'):
                idx = key.replace('raw_rm_id_', '')
                rm_id = request.form.get(key)
                qty_key = f'raw_actual_{idx}'
                qty_raw = (request.form.get(qty_key, '') or '').strip()
                if not rm_id or not qty_raw:
                    continue
                qty = round(float(qty_raw), 4)
                if qty <= 0:
                    continue
                raw_entries[idx] = {'rm_id': rm_id, 'qty': qty}

        if save_action == 'temp':
            cursor.execute(
                '''
                UPDATE production_material_usage
                SET actual_quantity = NULL, loss_quantity = NULL, yield_rate = NULL
                WHERE production_id = ?
                ''',
                (production_id,),
            )
        else:
            touched_material_ids |= _rollback_material_lot_usage_for_production(cursor, production_id, 'resave')
            if is_completed_status:
                _rollback_raw_usage_for_production(
                    cursor,
                    production_id,
                    session.get('user', {}).get('username'),
                    note_prefix='production_edit',
                )
            if is_completed_status and not touched_material_ids:
                cursor.execute(
                    '''
                    SELECT material_id, COALESCE(actual_quantity, 0) as qty
                    FROM production_material_usage
                    WHERE production_id = ?
                      AND material_id IS NOT NULL
                      AND COALESCE(actual_quantity, 0) > 0
                    ''',
                    (production_id,),
                )
                legacy_rows = cursor.fetchall()
                for legacy in legacy_rows:
                    cursor.execute(
                        'UPDATE materials SET current_stock = current_stock + ? WHERE id = ?',
                        (legacy['qty'], legacy['material_id']),
                    )
                    touched_material_ids.add(legacy['material_id'])

        # ???먯큹 ?ш퀬 遺議?寃利?(李④컧 ?꾩뿉 癒쇱? 泥댄겕)
        raw_requests = []
        for idx in sorted(raw_entries.keys(), key=lambda x: int(x)):
            entry = raw_entries[idx]
            cursor.execute('SELECT id, code, lot, name, workplace FROM raw_materials WHERE id = ?', (entry['rm_id'],))
            rm = cursor.fetchone()
            if not rm:
                continue
            raw_requests.append(
                {
                    'source_rm_id': int(rm['id']),
                    'code': (rm['code'] or '').strip(),
                    'lot': (rm['lot'] or '').strip(),
                    'name': rm['name'],
                    'workplace': rm['workplace'],
                    'actual_qty': float(entry['qty'] or 0),
                }
            )

        cursor.execute(
            '''
            SELECT COUNT(*) as cnt
            FROM bom
            WHERE product_id = ?
              AND raw_material_id IS NOT NULL
            ''',
            (product_id,),
        )
        has_raw_bom = (cursor.fetchone()['cnt'] or 0) > 0
        if save_action != 'temp' and has_raw_bom and not raw_requests:
            conn.execute('ROLLBACK')
            return "<script>alert('Please enter raw material usage.'); window.history.back();</script>"

        insufficient_raw = []
        if save_action != 'temp':
            for req in raw_requests:
                if req['actual_qty'] <= 0:
                    continue
                cursor.execute(
                    '''
                    SELECT
                        id,
                        name,
                        code,
                        lot,
                        COALESCE(current_stock, 0) as current_stock
                    FROM raw_materials
                    WHERE id = ?
                    ''',
                    (req['source_rm_id'],),
                )
                row = cursor.fetchone()
                need_qty = float(req['actual_qty'] or 0)
                if not row:
                    insufficient_raw.append(
                        {
                            'name': f"id:{req['source_rm_id']}",
                            'code': '-',
                            'lot': 'ALL',
                            'need': need_qty,
                            'have': 0.0,
                            'short': need_qty,
                        }
                    )
                    continue
                available = float(row['current_stock'] or 0)
                if available + 1e-9 < need_qty:
                    insufficient_raw.append(
                        {
                            'name': row['name'] or '-',
                            'code': row['code'] or '-',
                            'lot': row['lot'] or '-',
                            'need': need_qty,
                            'have': available,
                            'short': need_qty - available,
                        }
                    )

        if save_action != 'temp' and insufficient_raw:
            if conn:
                try:
                    conn.execute('ROLLBACK')
                except Exception:
                    pass
                conn.close()

            msg = 'Selected raw lots do not have enough stock:\n'
            for item in insufficient_raw:
                msg += (
                    f"\n- {item['name']} [{item.get('code', '-')} / {item.get('lot', '-')}] "
                    f"?? {item['short']:.1f}? (?? {item['need']:.1f}?, ?? {item['have']:.1f}?)"
                )

            return f'''
                <script>
                    if (confirm("{msg}\n\nGo to Raw Materials page?")) {{
                        window.location.href = "/raw-materials";
                    }} else {{
                        window.history.back();
                    }}
                </script>
            '''

        cursor.execute('SELECT sok_per_box FROM products WHERE id = ?', (product_id,))
        p = cursor.fetchone()
        per_box = float(p['sok_per_box']) if p and p['sok_per_box'] else 0
        boxes_for_need = actual_boxes if actual_boxes > 0 else planned_boxes
        total_need = per_box * boxes_for_need

        if save_action != 'temp':
            cursor.execute(
                '''
                SELECT id, material_id
                FROM production_material_usage
                WHERE production_id = ? AND material_id IS NOT NULL
                ''',
                (production_id,),
            )
            usage_map = {str(r['id']): r['material_id'] for r in cursor.fetchall()}
            needed_by_material = {}
            for key in request.form:
                if not key.startswith('actual_mat_'):
                    continue
                usage_id = key.replace('actual_mat_', '')
                actual_str = (request.form.get(key) or '').strip()
                if not actual_str:
                    continue
                qty = round(float(actual_str), 4)
                if qty <= 0:
                    continue
                material_id = usage_map.get(usage_id)
                if not material_id:
                    continue
                needed_by_material[material_id] = needed_by_material.get(material_id, 0.0) + qty

            material_shortages = []
            for material_id, need_qty in needed_by_material.items():
                if workplace_location_id:
                    cursor.execute(
                        '''
                        SELECT
                            m.name,
                            m.unit,
                            COALESCE(SUM(b.qty), 0) as workplace_available
                        FROM materials m
                        LEFT JOIN material_lots ml
                          ON ml.material_id = m.id
                         AND COALESCE(ml.is_disposed, 0) = 0
                        LEFT JOIN inv_material_lot_balances b
                          ON b.material_lot_id = ml.id
                         AND b.location_id = ?
                        WHERE m.id = ?
                        GROUP BY m.id
                        ''',
                        (workplace_location_id, material_id),
                    )
                else:
                    cursor.execute(
                        '''
                        SELECT
                            m.name,
                            m.unit,
                            m.current_stock,
                            COUNT(ml.id) as lot_count,
                            COALESCE(SUM(COALESCE(ml.current_quantity, ml.quantity, 0)), 0) as lot_available
                        FROM materials m
                        LEFT JOIN material_lots ml
                          ON ml.material_id = m.id
                         AND COALESCE(ml.is_disposed, 0) = 0
                        WHERE m.id = ?
                        GROUP BY m.id
                        ''',
                        (material_id,),
                    )
                row = cursor.fetchone()
                if not row:
                    continue
                if workplace_location_id:
                    available = float(row['workplace_available'] or 0)
                else:
                    lot_available = float(row['lot_available'] or 0)
                    available = lot_available if int(row['lot_count'] or 0) > 0 else float(row['current_stock'] or 0)
                if available + 1e-9 < need_qty:
                    material_shortages.append(
                        {
                            'material_id': material_id,
                            'name': row['name'],
                            'unit': row['unit'],
                            'have': available,
                            'need': need_qty,
                        }
                    )

            if material_shortages:
                if conn:
                    try:
                        conn.execute('ROLLBACK')
                    except Exception:
                        pass
                    conn.close()
                form_payload = []
                for k in request.form.keys():
                    if k in ('save_action', 'move_to_purchase'):
                        continue
                    for v in request.form.getlist(k):
                        form_payload.append({'name': k, 'value': v})
                session['material_shortage_popup'] = {
                    'production_id': int(production_id),
                    'shortages': material_shortages,
                    'form_payload': form_payload,
                }
                return redirect(url_for('production.production_detail', production_id=production_id))

        cursor.execute(
            '''
            DELETE FROM production_material_usage
            WHERE production_id = ?
              AND material_id IS NULL
            ''',
            (production_id,),
        )

        used_expected = 0.0
        for req in raw_requests:
            actual_qty = float(req['actual_qty'] or 0)
            if actual_qty <= 0:
                continue

            remaining_need = max(total_need - used_expected, 0)
            expected_qty = min(remaining_need, actual_qty)
            used_expected += expected_qty

            if save_action == 'temp':
                loss = actual_qty - expected_qty
                yield_rate = round(expected_qty / actual_qty * 100, 2) if actual_qty > 0 and expected_qty > 0 else None
                cursor.execute(
                    '''
                    INSERT INTO production_material_usage
                    (production_id, material_id, raw_material_id, raw_material_name, expected_quantity, actual_quantity, loss_quantity, yield_rate)
                    VALUES (?, NULL, ?, ?, ?, ?, ?, ?)
                    ''',
                    (production_id, req['source_rm_id'], req['name'], expected_qty, actual_qty, loss, yield_rate),
                )
                continue

            consumed = _consume_raw_by_code_fifo(
                cursor,
                req['source_rm_id'],
                actual_qty,
                production_id,
                session['user']['username'],
            )
            if not consumed:
                continue
            expected_remain = expected_qty
            consumed_total = sum(float(seg['quantity'] or 0) for seg in consumed)
            for i, seg in enumerate(consumed):
                seg_qty = float(seg['quantity'] or 0)
                if seg_qty <= 0:
                    continue
                if i == len(consumed) - 1:
                    seg_expected = max(expected_remain, 0.0)
                else:
                    ratio = seg_qty / consumed_total if consumed_total > 0 else 0
                    seg_expected = round(expected_qty * ratio, 4)
                    expected_remain -= seg_expected
                seg_loss = seg_qty - seg_expected
                seg_yield = round(seg_expected / seg_qty * 100, 2) if seg_qty > 0 and seg_expected > 0 else None
                cursor.execute(
                    '''
                    INSERT INTO production_material_usage
                    (production_id, material_id, raw_material_id, raw_material_name, expected_quantity, actual_quantity, loss_quantity, yield_rate)
                    VALUES (?, NULL, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        production_id,
                        seg['raw_material_id'],
                        seg['raw_material_name'],
                        seg_expected,
                        seg_qty,
                        seg_loss,
                        seg_yield,
                    ),
                )

        # 3. ?쇰컲 遺?먯옱 ?ㅼ궗?⑸웾 泥섎━
        for key in request.form:
            if key.startswith('actual_mat_'):
                usage_id = key.replace('actual_mat_', '')
                actual_str = request.form.get(key, '').strip()
                if not actual_str:
                    continue
                actual = round(float(actual_str), 4)

                cursor.execute(
                    '''
                    SELECT pmu.expected_quantity, pmu.material_id, COALESCE(m.category, '') AS category
                    FROM production_material_usage pmu
                    LEFT JOIN materials m ON pmu.material_id = m.id
                    WHERE pmu.id = ?
                ''',
                    (usage_id,),
                )
                row = cursor.fetchone()
                if not row:
                    continue

                expected = float(row['expected_quantity']) if row['expected_quantity'] else 0
                loss = round(actual - expected, 4)
                yield_rate = round(expected / actual * 100, 2) if actual > 0 and expected > 0 else None
                cursor.execute(
                    '''
                    UPDATE production_material_usage
                    SET actual_quantity = ?, loss_quantity = ?, yield_rate = ?
                    WHERE id = ?
                ''',
                    (actual, loss, yield_rate, usage_id),
                )

                # 遺?먯옱 ?ш퀬 李④컧
                if save_action != 'temp' and row['material_id']:
                    consumed_lots = _consume_material_fifo(
                        cursor,
                        production_id,
                        usage_id,
                        row['material_id'],
                        actual,
                        current_workplace,
                    )
                    _apply_missing_material_info_to_last_lot(
                        cursor,
                        consumed_lots,
                        row['category'],
                        request.form.get(f'mat_info_receiving_{usage_id}') or '',
                        request.form.get(f'mat_info_manufacture_{usage_id}') or '',
                        request.form.get(f'mat_info_expiry_{usage_id}') or '',
                        (request.form.get(f'mat_info_manufacture_none_{usage_id}') or '').strip() == '1',
                        (request.form.get(f'mat_info_expiry_none_{usage_id}') or '').strip() == '1',
                    )
                    touched_material_ids.add(row['material_id'])

        if save_action == 'temp':
            if actual_boxes > 0:
                cursor.execute(
                    '''
                    UPDATE productions
                    SET actual_boxes = ?,
                        status = CASE WHEN status='완료' OR status LIKE '%꾨즺%' THEN '완료' ELSE '예정' END
                    WHERE id = ?
                    ''',
                    (actual_boxes, production_id),
                )
            audit_log(
                conn,
                'update',
                'production',
                production_id,
                {
                    'save_action': 'temp',
                    'actual_boxes': actual_boxes,
                    'expiry_date': expiry_date,
                    'raw_entries': raw_entries,
                },
            )
            cursor.execute('COMMIT')
            if request.form.get('move_to_purchase') == '1':
                return redirect(url_for('materials.purchase_orders'))
            return redirect(url_for('production.production_detail', production_id=production_id))

        # 4. ?앹궛 완료 泥섎━
        if actual_boxes > 0:
            cursor.execute(
                '''
                UPDATE productions SET actual_boxes = ?, status = '완료' WHERE id = ?
                ''',
                (actual_boxes, production_id),
            )
            cursor.execute(
                '''
                UPDATE production_schedules SET status = '완료' WHERE production_id = ?
                ''',
                (production_id,),
            )

        if save_action != 'temp' and touched_material_ids:
            for material_id in touched_material_ids:
                _sync_material_stock_with_lots(conn, material_id)

        audit_log(
            conn,
            'update',
            'production',
            production_id,
            {
                'actual_boxes': actual_boxes,
                'planned_boxes': planned_boxes,
                'expiry_date': expiry_date,
                'raw_entries': raw_entries,
            },
        )

        cursor.execute('COMMIT')

    except ValueError as e:
        if conn:
            try:
                conn.execute('ROLLBACK')
            except Exception:
                pass
        msg = str(e).replace("'", "\\'")
        return f"<script>alert('{msg}'); window.history.back();</script>"
    except Exception as e:
        if conn:
            try:
                conn.execute('ROLLBACK')
            except Exception:
                pass
        import traceback

        traceback.print_exc()
        raise
    finally:
        if conn:
            conn.close()

    return redirect(url_for('production.production_detail', production_id=production_id))


@bp.route('/production/<int:production_id>/delete', methods=['POST'])
@role_required('production')
def delete_production(production_id):
    success_redirect = (request.form.get('next') or request.args.get('next') or '').strip()
    if not success_redirect:
        success_redirect = url_for('production.production_list')
    return _delete_production_record_response(production_id, success_redirect)


def _delete_production_record(conn, production_id, actor_user_id=None):
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM productions WHERE id = ?', (production_id,))
    prod = cursor.fetchone()
    if not prod:
        return False

    status = _normalize_production_status(prod['status'])
    product_id, schedule_id = prod['product_id'], prod['schedule_id']

    if status == '완료':
        cursor.execute(
            '''
            SELECT raw_material_id, material_id, actual_quantity
            FROM production_material_usage
            WHERE production_id = ?
            ''',
            (production_id,),
        )
        usage_records = cursor.fetchall()
        legacy_material_rollbacks = []

        for record in usage_records:
            rm_id = record['raw_material_id']
            mat_id = record['material_id']
            qty = record['actual_quantity'] or 0

            if qty <= 0:
                continue

            if rm_id:
                cursor.execute(
                    '''
                    UPDATE raw_materials
                    SET current_stock = current_stock + ?,
                        used_quantity = MAX(0, used_quantity - ?)
                    WHERE id = ?
                    ''',
                    (qty, qty, rm_id),
                )
                cursor.execute(
                    '''
                    INSERT INTO raw_material_logs (raw_material_id, type, quantity, note, production_id, created_by)
                    VALUES (?, 'RETURN', ?, '생산 삭제: 선입선출 분할 롤백', ?, ?)
                    ''',
                    (rm_id, qty, production_id, actor_user_id),
                )

                cursor.execute('SELECT COUNT(*) as cnt FROM bom WHERE product_id = ? AND raw_material_id = ?', (product_id, rm_id))
                if cursor.fetchone()['cnt'] == 0:
                    cursor.execute('SELECT sok_per_box FROM products WHERE id = ?', (product_id,))
                    p_info = cursor.fetchone()
                    s_box = p_info['sok_per_box'] if p_info else 0
                    cursor.execute(
                        '''
                        INSERT INTO bom (product_id, raw_material_id, sok_per_box, quantity_per_box)
                        VALUES (?, ?, ?, ?)
                        ''',
                        (product_id, rm_id, s_box, s_box),
                    )
            elif mat_id:
                legacy_material_rollbacks.append((mat_id, qty))

        touched = _rollback_material_lot_usage_for_production(cursor, production_id, 'production_delete')
        for mid in touched:
            _sync_material_stock_with_lots(conn, mid)
        if not touched:
            for mat_id, qty in legacy_material_rollbacks:
                cursor.execute('UPDATE materials SET current_stock = current_stock + ? WHERE id = ?', (qty, mat_id))

    cursor.execute('DELETE FROM production_material_usage WHERE production_id = ?', (production_id,))
    cursor.execute('DELETE FROM productions WHERE id = ?', (production_id,))
    if schedule_id:
        cursor.execute('DELETE FROM production_schedules WHERE id = ?', (schedule_id,))

    audit_log(conn, 'delete', 'production', production_id, {'before': dict(prod)})
    return True


def _delete_production_record_response(production_id, success_redirect):
    conn = get_db()
    try:
        deleted = _delete_production_record(conn, production_id, session.get('user_id'))
        conn.commit()
        if not deleted:
            return redirect(success_redirect)
    except Exception as e:
        conn.rollback()
        print(f"CRITICAL ERROR: {e}")
        return f"삭제 실패: {str(e)}", 500
    finally:
        conn.close()

    return redirect(success_redirect)


@bp.route('/production/search')
@login_required
def production_search():
    """Auto-generated docstring."""
    start = request.args.get('start', '')
    end = request.args.get('end', '')
    keyword = (request.args.get('keyword', '') or '').strip()
    workplace = get_workplace()

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT pr.*, p.name as product_name
        FROM productions pr
        LEFT JOIN products p ON pr.product_id = p.id
        WHERE (pr.status = '완료' OR pr.status LIKE '%꾨즺%')
        AND pr.workplace = ?
    """
    params = [workplace]

    if keyword.isdigit():
        query += ' AND pr.id = ?'
        params.append(int(keyword))
    else:
        if start:
            query += ' AND pr.production_date >= ?'
            params.append(start)
        if end:
            query += ' AND pr.production_date <= ?'
            params.append(end)
    if keyword and not keyword.isdigit():
        query += ' AND p.name LIKE ?'
        params.append(f'%{keyword}%')

    query += ' ORDER BY pr.production_date DESC LIMIT 100'

    cursor.execute(query, params)
    results = cursor.fetchall()
    conn.close()

    # JSON 蹂??
    data = [
        {
            'id': r['id'],
            'production_date': r['production_date'],
            'product_name': r['product_name'],
            'planned_boxes': r['planned_boxes'],
            'actual_boxes': r['actual_boxes'],
        }
        for r in results
    ]

    return json.dumps({'results': data}, ensure_ascii=False), 200, {'Content-Type': 'application/json'}
