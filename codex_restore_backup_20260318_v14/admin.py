from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
import csv
import io
from urllib.parse import quote

from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify, send_file, Response

from core import (
    get_db,
    login_required,
    admin_required,
    WORKPLACES,
    SHARED_WORKPLACE,
    SHARED_MATERIAL_CATEGORIES,
    audit_log,
)

bp = Blueprint('admin', __name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / 'yemat.db'
BACKUP_DIR = PROJECT_ROOT / 'backups'
BACKUP_KEEP_DEFAULT = 10


def _normalize_date_token(value):
    raw = (value or '').strip()
    if not raw:
        return '00000000'
    return raw.replace('-', '')


def _round_to_1_decimal(value):
    return round(float(value or 0) + 1e-9, 1)


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
        WHERE EXISTS (SELECT 1 FROM material_lots x WHERE x.material_id = materials.id)
        '''
    )


def _parse_keep_count(raw):
    try:
        value = int(raw or BACKUP_KEEP_DEFAULT)
    except (TypeError, ValueError):
        value = BACKUP_KEEP_DEFAULT
    if value < 1:
        return 1
    if value > 200:
        return 200
    return value


def _list_db_backups():
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for path in sorted(BACKUP_DIR.glob('yemat_*.db'), key=lambda p: p.stat().st_mtime, reverse=True):
        stat = path.stat()
        rows.append(
            {
                'filename': path.name,
                'size_bytes': int(stat.st_size),
                'size_mb': round(stat.st_size / (1024 * 1024), 2),
                'created_at': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
            }
        )
    return rows


def _query_inventory_audit_rows(cursor, selected_inventory_wps):
    filter_clause = ''
    filter_params = []
    if selected_inventory_wps:
        placeholders = ','.join(['?'] * len(selected_inventory_wps))
        filter_clause = f'WHERE COALESCE(t.workplace, \'\') IN ({placeholders})'
        filter_params.extend(selected_inventory_wps)

    cursor.execute(
        f'''
        SELECT *
        FROM (
            SELECT
                'raw_material' as inv_type,
                rm.id as inv_id,
                rm.workplace as workplace,
                '원초' as inv_category,
                COALESCE(NULLIF(TRIM(rm.code), ''), printf('RM%05d', rm.id)) as code,
                rm.name as item_name,
                rm.car_number as car_number,
                rm.receiving_date as receiving_date,
                COALESCE(rm.current_stock, 0) as current_stock,
                1 as cat_order
            FROM raw_materials rm
            WHERE COALESCE(rm.current_stock, 0) > 0

            UNION ALL

            SELECT
                'material' as inv_type,
                m.id as inv_id,
                m.workplace as workplace,
                '원자재' as inv_category,
                COALESCE(NULLIF(TRIM(m.code), ''), printf('M%05d', m.id)) as code,
                m.name as item_name,
                '' as car_number,
                '' as receiving_date,
                COALESCE(m.current_stock, 0) as current_stock,
                2 as cat_order
            FROM materials m
            WHERE COALESCE(m.current_stock, 0) > 0
              AND COALESCE(m.category, '') IN ('기름', '소금')

            UNION ALL

            SELECT
                'material' as inv_type,
                m.id as inv_id,
                m.workplace as workplace,
                '부재료' as inv_category,
                COALESCE(NULLIF(TRIM(m.code), ''), printf('M%05d', m.id)) as code,
                m.name as item_name,
                '' as car_number,
                '' as receiving_date,
                COALESCE(m.current_stock, 0) as current_stock,
                3 as cat_order
            FROM materials m
            WHERE COALESCE(m.current_stock, 0) > 0
              AND COALESCE(m.category, '') NOT IN ('기름', '소금')
        ) t
        {filter_clause}
        ORDER BY workplace, cat_order, item_name
        ''',
        filter_params,
    )
    return cursor.fetchall()


def _create_db_backup(keep_count):
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    backup_name = f'yemat_{timestamp}.db'
    backup_path = BACKUP_DIR / backup_name

    src_conn = sqlite3.connect(str(DB_PATH))
    dst_conn = sqlite3.connect(str(backup_path))
    try:
        src_conn.backup(dst_conn)
    finally:
        try:
            dst_conn.close()
        finally:
            src_conn.close()

    all_backups = sorted(BACKUP_DIR.glob('yemat_*.db'), key=lambda p: p.stat().st_mtime, reverse=True)
    deleted_count = 0
    for old_path in all_backups[keep_count:]:
        try:
            old_path.unlink(missing_ok=True)
            deleted_count += 1
        except Exception:
            pass
    return backup_name, deleted_count


def _get_stats_period_bounds(period, anchor_raw):
    try:
        anchor = datetime.strptime((anchor_raw or '').strip(), '%Y-%m-%d').date()
    except ValueError:
        anchor = datetime.now().date()

    if period == 'week':
        start_date = anchor - timedelta(days=anchor.weekday())
        end_date = start_date + timedelta(days=6)
        labels = [(start_date + timedelta(days=i)).strftime('%m-%d') for i in range(7)]
    else:
        start_date = anchor.replace(day=1)
        if start_date.month == 12:
            next_month = start_date.replace(year=start_date.year + 1, month=1, day=1)
        else:
            next_month = start_date.replace(month=start_date.month + 1, day=1)
        end_date = next_month - timedelta(days=1)
        labels = [(start_date + timedelta(days=i)).strftime('%m-%d') for i in range((end_date - start_date).days + 1)]

    return anchor, start_date, end_date, labels


def _build_stats_series(labels, totals_by_label):
    values = [round(float(totals_by_label.get(label, 0) or 0), 1) for label in labels]
    max_value = max(values) if values else 0
    points = []
    for label, value in zip(labels, values):
        points.append({
            'label': label,
            'value': value,
            'width_pct': (value / max_value * 100) if max_value > 0 else 0,
        })
    return {
        'labels': labels,
        'values': values,
        'max_value': max_value,
        'points': points,
        'total': round(sum(values), 1),
    }


def _query_integrated_stats(cursor, wp_filter, period, anchor_raw):
    anchor, start_date, end_date, labels = _get_stats_period_bounds(period, anchor_raw)
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    wp_clause = ''
    wp_params = []
    if wp_filter != 'all':
        wp_clause = ' AND pr.workplace = ?'
        wp_params.append(wp_filter)

    product_rows = cursor.execute(
        f'''
        SELECT strftime('%m-%d', pr.production_date) as bucket,
               SUM(COALESCE(pr.actual_boxes, pr.planned_boxes, 0)) as qty
        FROM productions pr
        WHERE pr.production_date BETWEEN ? AND ?
          AND COALESCE(pr.actual_boxes, pr.planned_boxes, 0) > 0
          {wp_clause}
        GROUP BY pr.production_date
        ORDER BY pr.production_date
        ''',
        [start_str, end_str, *wp_params],
    ).fetchall()

    material_rows = cursor.execute(
        f'''
        SELECT strftime('%m-%d', pr.production_date) as bucket,
               SUM(COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0)) as qty
        FROM production_material_usage pmu
        JOIN productions pr ON pr.id = pmu.production_id
        WHERE pr.production_date BETWEEN ? AND ?
          AND pmu.material_id IS NOT NULL
          AND COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0) > 0
          {wp_clause}
        GROUP BY pr.production_date
        ORDER BY pr.production_date
        ''',
        [start_str, end_str, *wp_params],
    ).fetchall()

    raw_rows = cursor.execute(
        f'''
        SELECT strftime('%m-%d', pr.production_date) as bucket,
               SUM(COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0)) as qty
        FROM production_material_usage pmu
        JOIN productions pr ON pr.id = pmu.production_id
        WHERE pr.production_date BETWEEN ? AND ?
          AND (pmu.raw_material_id IS NOT NULL OR COALESCE(TRIM(pmu.raw_material_name), '') != '')
          AND COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0) > 0
          {wp_clause}
        GROUP BY pr.production_date
        ORDER BY pr.production_date
        ''',
        [start_str, end_str, *wp_params],
    ).fetchall()

    top_products = cursor.execute(
        f'''
        SELECT COALESCE(p.name, printf('상품 #%s', pr.product_id)) as name,
               SUM(COALESCE(pr.actual_boxes, pr.planned_boxes, 0)) as qty
        FROM productions pr
        LEFT JOIN products p ON p.id = pr.product_id
        WHERE pr.production_date BETWEEN ? AND ?
          AND COALESCE(pr.actual_boxes, pr.planned_boxes, 0) > 0
          {wp_clause}
        GROUP BY pr.product_id, p.name
        ORDER BY qty DESC, name ASC
        LIMIT 10
        ''',
        [start_str, end_str, *wp_params],
    ).fetchall()

    top_materials = cursor.execute(
        f'''
        SELECT COALESCE(m.name, printf('부자재 #%s', pmu.material_id)) as name,
               SUM(COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0)) as qty
        FROM production_material_usage pmu
        JOIN productions pr ON pr.id = pmu.production_id
        LEFT JOIN materials m ON m.id = pmu.material_id
        WHERE pr.production_date BETWEEN ? AND ?
          AND pmu.material_id IS NOT NULL
          AND COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0) > 0
          {wp_clause}
        GROUP BY pmu.material_id, m.name
        ORDER BY qty DESC, name ASC
        LIMIT 10
        ''',
        [start_str, end_str, *wp_params],
    ).fetchall()

    top_raws = cursor.execute(
        f'''
        SELECT COALESCE(NULLIF(TRIM(pmu.raw_material_name), ''), rm.name, printf('원초 #%s', pmu.raw_material_id)) as name,
               SUM(COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0)) as qty
        FROM production_material_usage pmu
        JOIN productions pr ON pr.id = pmu.production_id
        LEFT JOIN raw_materials rm ON rm.id = pmu.raw_material_id
        WHERE pr.production_date BETWEEN ? AND ?
          AND (pmu.raw_material_id IS NOT NULL OR COALESCE(TRIM(pmu.raw_material_name), '') != '')
          AND COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0) > 0
          {wp_clause}
        GROUP BY COALESCE(pmu.raw_material_id, -pmu.id), COALESCE(NULLIF(TRIM(pmu.raw_material_name), ''), rm.name)
        ORDER BY qty DESC, name ASC
        LIMIT 10
        ''',
        [start_str, end_str, *wp_params],
    ).fetchall()

    product_table = cursor.execute(
        f'''
        SELECT COALESCE(p.code, '-') as code,
               COALESCE(p.name, printf('상품 #%s', pr.product_id)) as name,
               SUM(COALESCE(pr.actual_boxes, pr.planned_boxes, 0)) as qty,
               COUNT(DISTINCT pr.production_date) as day_count,
               MAX(pr.production_date) as last_date
        FROM productions pr
        LEFT JOIN products p ON p.id = pr.product_id
        WHERE pr.production_date BETWEEN ? AND ?
          AND COALESCE(pr.actual_boxes, pr.planned_boxes, 0) > 0
          {wp_clause}
        GROUP BY pr.product_id, p.code, p.name
        ORDER BY qty DESC, name ASC
        LIMIT 100
        ''',
        [start_str, end_str, *wp_params],
    ).fetchall()

    material_table = cursor.execute(
        f'''
        SELECT COALESCE(m.code, printf('M%05d', pmu.material_id)) as code,
               COALESCE(m.name, printf('부자재 #%s', pmu.material_id)) as name,
               COALESCE(m.category, '-') as category,
               COALESCE(m.unit, '-') as unit,
               SUM(COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0)) as qty,
               COUNT(DISTINCT pr.production_date) as day_count,
               MAX(pr.production_date) as last_date
        FROM production_material_usage pmu
        JOIN productions pr ON pr.id = pmu.production_id
        LEFT JOIN materials m ON m.id = pmu.material_id
        WHERE pr.production_date BETWEEN ? AND ?
          AND pmu.material_id IS NOT NULL
          AND COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0) > 0
          {wp_clause}
        GROUP BY pmu.material_id, m.code, m.name, m.category, m.unit
        ORDER BY qty DESC, name ASC
        LIMIT 100
        ''',
        [start_str, end_str, *wp_params],
    ).fetchall()

    raw_table = cursor.execute(
        f'''
        SELECT COALESCE(rm.code, '-') as code,
               COALESCE(NULLIF(TRIM(pmu.raw_material_name), ''), rm.name, printf('원초 #%s', pmu.raw_material_id)) as name,
               COALESCE(MAX(NULLIF(TRIM(rm.ja_ho), '')), MAX(NULLIF(TRIM(rm.car_number), '')), '-') as lot_ref,
               SUM(COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0)) as qty,
               COUNT(DISTINCT pr.production_date) as day_count,
               MAX(pr.production_date) as last_date
        FROM production_material_usage pmu
        JOIN productions pr ON pr.id = pmu.production_id
        LEFT JOIN raw_materials rm ON rm.id = pmu.raw_material_id
        WHERE pr.production_date BETWEEN ? AND ?
          AND (pmu.raw_material_id IS NOT NULL OR COALESCE(TRIM(pmu.raw_material_name), '') != '')
          AND COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0) > 0
          {wp_clause}
        GROUP BY COALESCE(pmu.raw_material_id, -pmu.id), COALESCE(NULLIF(TRIM(pmu.raw_material_name), ''), rm.name), rm.code
        ORDER BY qty DESC, name ASC
        LIMIT 100
        ''',
        [start_str, end_str, *wp_params],
    ).fetchall()

    product_series = _build_stats_series(labels, {row['bucket']: row['qty'] for row in product_rows})
    material_series = _build_stats_series(labels, {row['bucket']: row['qty'] for row in material_rows})
    raw_series = _build_stats_series(labels, {row['bucket']: row['qty'] for row in raw_rows})

    return {
        'period': period,
        'anchor': anchor.strftime('%Y-%m-%d'),
        'start_date': start_str,
        'end_date': end_str,
        'summary': {
            'product_total': product_series['total'],
            'material_total': material_series['total'],
            'raw_total': raw_series['total'],
            'days_count': len(labels),
        },
        'product_series': product_series,
        'material_series': material_series,
        'raw_series': raw_series,
        'top_products': [{'name': row['name'], 'qty': round(float(row['qty'] or 0), 1)} for row in top_products],
        'top_materials': [{'name': row['name'], 'qty': round(float(row['qty'] or 0), 1)} for row in top_materials],
        'top_raws': [{'name': row['name'], 'qty': round(float(row['qty'] or 0), 1)} for row in top_raws],
        'product_table': [{
            'code': row['code'],
            'name': row['name'],
            'qty': round(float(row['qty'] or 0), 1),
            'day_count': int(row['day_count'] or 0),
            'last_date': row['last_date'] or '-',
        } for row in product_table],
        'material_table': [{
            'code': row['code'],
            'name': row['name'],
            'category': row['category'] or '-',
            'unit': row['unit'] or '-',
            'qty': round(float(row['qty'] or 0), 1),
            'day_count': int(row['day_count'] or 0),
            'last_date': row['last_date'] or '-',
        } for row in material_table],
        'raw_table': [{
            'code': row['code'],
            'name': row['name'],
            'lot_ref': row['lot_ref'] or '-',
            'qty': round(float(row['qty'] or 0), 1),
            'day_count': int(row['day_count'] or 0),
            'last_date': row['last_date'] or '-',
        } for row in raw_table],
    }


def _get_requirement_calculator_products(cursor):
    return cursor.execute(
        '''
        SELECT id, name, code, workplace
        FROM products
        ORDER BY workplace, name
        '''
    ).fetchall()


def _get_material_stock_rows(cursor):
    return cursor.execute(
        '''
        SELECT
            m.id,
            m.code,
            m.name,
            m.category,
            m.unit,
            COALESCE(m.current_stock, 0) AS workplace_stock,
            COALESCE(ls.quantity, 0) AS logistics_stock,
            COALESCE(m.current_stock, 0) + COALESCE(ls.quantity, 0) AS total_stock
        FROM materials m
        LEFT JOIN logistics_stocks ls
          ON ls.material_code = COALESCE(NULLIF(TRIM(m.code), ''), printf('M%05d', m.id))
        '''
    ).fetchall()


def _get_raw_stock_rows(cursor):
    return cursor.execute(
        '''
        SELECT
            id,
            code,
            name,
            COALESCE(current_stock, 0) AS current_stock
        FROM raw_materials
        '''
    ).fetchall()


def _sort_requirement_items(items):
    category_order = {
        '원초': 0,
        '기름': 1,
        '소금': 2,
        '내포': 3,
        '외포': 4,
        '박스': 5,
        '실리카': 6,
        '트레이': 7,
        '뚜껑': 8,
        '각대': 9,
        '기타': 10,
    }
    return sorted(
        items,
        key=lambda row: (
            category_order.get((row.get('category') or '').strip(), 99),
            str(row.get('code') or ''),
            str(row.get('name') or ''),
        ),
    )


def _build_integrated_requirement_payload(cursor, items):
    normalized_items = []
    for item in items or []:
        try:
            product_id = int(item.get('product_id') or 0)
            boxes = float(item.get('boxes') or 0)
        except (TypeError, ValueError, AttributeError):
            continue
        if product_id <= 0 or boxes <= 0:
            continue
        normalized_items.append({'product_id': product_id, 'boxes': boxes})

    if not normalized_items:
        return {
            'summary': {'raw': [], 'base': [], 'sub': []},
            'products': [],
            'rows': [],
        }

    product_ids = sorted({item['product_id'] for item in normalized_items})
    placeholders = ','.join(['?'] * len(product_ids))

    product_map = {
        row['id']: row for row in cursor.execute(
            f'''
            SELECT id, name, code, workplace
            FROM products
            WHERE id IN ({placeholders})
            ''',
            product_ids,
        ).fetchall()
    }

    bom_rows = cursor.execute(
        f'''
        SELECT
            b.product_id,
            b.material_id,
            b.raw_material_id,
            b.raw_material_name,
            COALESCE(b.sok_per_box, b.quantity_per_box, 0) AS raw_qty_per_box,
            COALESCE(b.quantity_per_box, 0) AS material_qty_per_box,
            m.code AS material_code,
            m.name AS material_name,
            m.category AS material_category,
            m.unit AS material_unit,
            rm.code AS raw_code,
            rm.name AS raw_name
        FROM bom b
        LEFT JOIN materials m ON m.id = b.material_id
        LEFT JOIN raw_materials rm ON rm.id = b.raw_material_id
        WHERE b.product_id IN ({placeholders})
        ORDER BY b.product_id, b.id
        ''',
        product_ids,
    ).fetchall()

    material_stock_map = {}
    for row in _get_material_stock_rows(cursor):
        material_stock_map[row['id']] = {
            'code': row['code'] or '',
            'name': row['name'] or '',
            'category': row['category'] or '기타',
            'unit': row['unit'] or '-',
            'current_stock': float(row['total_stock'] or 0),
        }

    raw_stock_map = {}
    for row in _get_raw_stock_rows(cursor):
        raw_stock_map[row['id']] = {
            'code': row['code'] or '',
            'name': row['name'] or '',
            'current_stock': float(row['current_stock'] or 0),
        }

    summary_map = {}
    product_sections = []
    export_rows = []

    for request_item in normalized_items:
        product = product_map.get(request_item['product_id'])
        if not product:
            continue

        product_rows = []
        for bom_row in [row for row in bom_rows if row['product_id'] == request_item['product_id']]:
            if bom_row['raw_material_id'] or (bom_row['raw_material_name'] and not bom_row['material_id']):
                raw_id = bom_row['raw_material_id']
                raw_info = raw_stock_map.get(raw_id, {})
                category = '원초'
                code = bom_row['raw_code'] or raw_info.get('code') or '-'
                name = bom_row['raw_name'] or bom_row['raw_material_name'] or raw_info.get('name') or '원초'
                unit = '속'
                current_stock = float(raw_info.get('current_stock') or 0)
                required_qty = float(bom_row['raw_qty_per_box'] or 0) * request_item['boxes']
            elif bom_row['material_id']:
                material_info = material_stock_map.get(bom_row['material_id'], {})
                category = bom_row['material_category'] or material_info.get('category') or '기타'
                code = bom_row['material_code'] or material_info.get('code') or '-'
                name = bom_row['material_name'] or material_info.get('name') or '부자재'
                unit = bom_row['material_unit'] or material_info.get('unit') or '-'
                current_stock = float(material_info.get('current_stock') or 0)
                required_qty = float(bom_row['material_qty_per_box'] or 0) * request_item['boxes']
            else:
                continue

            shortage_qty = max(required_qty - current_stock, 0)
            item_key = (category, code, name, unit)

            row_payload = {
                'category': category,
                'code': code,
                'name': name,
                'unit': unit,
                'current_stock': round(current_stock, 1),
                'required_qty': round(required_qty, 1),
                'shortage_qty': round(shortage_qty, 1),
            }
            product_rows.append(row_payload)

            if item_key not in summary_map:
                summary_map[item_key] = {
                    'category': category,
                    'code': code,
                    'name': name,
                    'unit': unit,
                    'current_stock': round(current_stock, 1),
                    'required_qty': 0.0,
                }
            summary_map[item_key]['required_qty'] += required_qty

            export_rows.append({
                'scope': '상품별',
                'product_code': product['code'] or '-',
                'product_name': product['name'] or '-',
                'workplace': product['workplace'] or '-',
                'boxes': round(request_item['boxes'], 1),
                **row_payload,
            })

        sorted_rows = _sort_requirement_items(product_rows)
        product_sections.append({
            'product_id': product['id'],
            'code': product['code'] or '-',
            'name': product['name'] or '-',
            'workplace': product['workplace'] or '-',
            'boxes': round(request_item['boxes'], 1),
            'raw_items': [row for row in sorted_rows if row['category'] == '원초'],
            'base_items': [row for row in sorted_rows if row['category'] in ('기름', '소금')],
            'sub_items': [row for row in sorted_rows if row['category'] not in ('원초', '기름', '소금')],
        })

    summary_rows = []
    for row in summary_map.values():
        row['required_qty'] = round(row['required_qty'], 1)
        row['shortage_qty'] = round(max(row['required_qty'] - row['current_stock'], 0), 1)
        summary_rows.append(row)

    summary_rows = _sort_requirement_items(summary_rows)
    for row in summary_rows:
        export_rows.append({
            'scope': '전체',
            'product_code': '-',
            'product_name': '전체 합계',
            'workplace': '-',
            'boxes': 0,
            **row,
        })

    return {
        'summary': {
            'raw': [row for row in summary_rows if row['category'] == '원초'],
            'base': [row for row in summary_rows if row['category'] in ('기름', '소금')],
            'sub': [row for row in summary_rows if row['category'] not in ('원초', '기름', '소금')],
        },
        'products': product_sections,
        'rows': export_rows,
    }


def _filter_integrated_requirement_payload(payload, selected_categories):
    categories = [str(category or '').strip() for category in (selected_categories or []) if str(category or '').strip()]
    if not categories:
        return payload

    category_set = set(categories)

    def _filter_rows(rows):
        return [row for row in (rows or []) if (row.get('category') or '').strip() in category_set]

    return {
        'summary': {
            'raw': _filter_rows((payload.get('summary') or {}).get('raw')),
            'base': _filter_rows((payload.get('summary') or {}).get('base')),
            'sub': _filter_rows((payload.get('summary') or {}).get('sub')),
        },
        'products': [
            {
                **product,
                'raw_items': _filter_rows(product.get('raw_items')),
                'base_items': _filter_rows(product.get('base_items')),
                'sub_items': _filter_rows(product.get('sub_items')),
            }
            for product in (payload.get('products') or [])
        ],
        'rows': _filter_rows(payload.get('rows')),
    }


@bp.route('/integrated-management')
@login_required
def integrated_management():
    """Auto-generated docstring."""
    if not session['user']['is_admin']:
        return "??????????????源낆┰?????????곸죩", 403

    tab = request.args.get('tab', 'products')  # products, raw_materials, materials, purchase_requests, requirements_calculator, stats, inventory_audit, audit_logs, db_backups
    wp_filter = request.args.get('wp', 'all')
    q = request.args.get('q', '').strip()
    rm_tab = request.args.get('rm_tab', 'active')
    stat_period = request.args.get('stat_period', 'month')
    if stat_period not in ('month', 'week'):
        stat_period = 'month'
    stat_view = request.args.get('stat_view', 'table')
    if stat_view not in ('table', 'graph'):
        stat_view = 'table'
    stat_anchor = (request.args.get('stat_anchor') or '').strip()
    keep_count = _parse_keep_count(request.args.get('keep_count'))
    selected_inventory_wps = []
    wps_param = (request.args.get('wps') or '').strip()
    if wps_param:
        selected_inventory_wps = [w.strip() for w in wps_param.split(',') if w.strip()]
    elif wp_filter != 'all':
        selected_inventory_wps = [wp_filter]

    conn = get_db()
    cursor = conn.cursor()
    stats = None
    calculator_products = []
    if tab in ('materials', 'purchase_requests'):
        _sync_material_stock_with_lots(conn)

    workplaces = WORKPLACES

    if tab == 'products':
        # ?????獄쏅챶留????????곗뒩筌? ????⑥ル????
        query = '''
            SELECT p.*, COUNT(b.id) as bom_count
            FROM products p 
            LEFT JOIN bom b ON p.id = b.product_id
            WHERE 1=1
        '''
        params = []
        if wp_filter == 'unassigned':
            query += ' AND (p.workplace IS NULL OR p.workplace = "")'
        elif wp_filter == 'all':
            pass
        else:
            query += ' AND p.workplace = ?'
            params.append(wp_filter)
        if q:
            query += ' AND (p.name LIKE ? OR p.code LIKE ? OR p.category LIKE ?)'
            like_q = f'%{q}%'
            params.extend([like_q, like_q, like_q])
        query += ' GROUP BY p.id ORDER BY p.workplace, p.name'
        cursor.execute(query, params)
        data = cursor.fetchall()


    elif tab == 'raw_materials':
        # ??: ???+?? ???? ???? 1?? 1? ??
        query = '''
            WITH raw_group AS (
                SELECT
                    MIN(id) as id,
                    workplace,
                    MIN(name) as name,
                    COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', MIN(id))) as code,
                    MIN(lot) as lot,
                    MAX(receiving_date) as receiving_date,
                    MIN(COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), ''))) as car_number,
                    COALESCE(SUM(COALESCE(total_stock, 0)), 0) as total_stock,
                    COALESCE(SUM(COALESCE(current_stock, 0)), 0) as current_stock,
                    COALESCE(SUM(COALESCE(used_quantity, 0)), 0) as used_quantity,
                    COUNT(*) as lot_count,
                    GROUP_CONCAT(DISTINCT lot) as lots_text,
                    GROUP_CONCAT(DISTINCT COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), ''))) as ja_ho_text
                FROM raw_materials
                GROUP BY workplace, COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id))
            )
            SELECT *
            FROM raw_group
            WHERE 1=1
        '''
        params = []
        if wp_filter != 'all':
            query += ' AND workplace = ?'
            params.append(wp_filter)
        if rm_tab == 'active':
            query += ' AND current_stock > 0'
        elif rm_tab == 'done':
            query += ' AND current_stock <= 0'
        if q:
            like_q = f'%{q}%'
            query += ' AND (name LIKE ? OR code LIKE ? OR COALESCE(lots_text, "") LIKE ? OR COALESCE(ja_ho_text, "") LIKE ?)'
            params.extend([like_q, like_q, like_q, like_q])
        query += ' ORDER BY workplace, current_stock DESC, receiving_date DESC'
        cursor.execute(query, params)
        data = cursor.fetchall()


    elif tab == 'materials':
        # ?????獄쏅챶留????????????⑥ル????
        query = '''
            SELECT
                m.*,
                s.name as supplier_name,
                COALESCE(ls.quantity, 0) as logistics_stock,
                COALESCE(SUM(COALESCE(ml.current_quantity, ml.quantity, 0)), 0) as lot_total_quantity,
                COUNT(ml.id) as lot_count
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            LEFT JOIN logistics_stocks ls ON ls.material_code = COALESCE(NULLIF(TRIM(m.code), ''), printf('M%05d', m.id))
            LEFT JOIN material_lots ml ON ml.material_id = m.id AND COALESCE(ml.is_disposed, 0) = 0
            WHERE 1=1
        '''
        params = []
        if wp_filter == 'unassigned':
            query += ' AND (m.workplace IS NULL OR m.workplace = "")'
        elif wp_filter == 'all':
            # ?????獄쏅챶留????????轅붽틓??????????釉랁닑???롪퍓媛??????브틯???筌먯룆???????
            pass
        else:
            # ???????????????轅붽틓??????????釉랁닑???롪퍓媛??????브틯???筌먯룆??????轅붽틓?????
            query += ' AND (m.workplace = ? OR m.workplace = ?)'
            params.extend([wp_filter, SHARED_WORKPLACE])
        if q:
            query += ' AND (m.name LIKE ? OR m.code LIKE ? OR m.category LIKE ? OR s.name LIKE ?)'
            like_q = f'%{q}%'
            params.extend([like_q, like_q, like_q, like_q])
        query += ' GROUP BY m.id ORDER BY m.code, m.name'
        cursor.execute(query, params)
        data = cursor.fetchall()

    elif tab == 'purchase_requests':
        # ?????獄쏅챶留???????밸븶筌믩끃???ル봿留싷┼??돘????????⑥ル????
        query = '''
            SELECT pr.*, m.name as material_name, m.code as material_code, m.unit,
                   s.name as supplier_name,
                   pr.ordered_by as ordered_by_name,
                   pr.received_by as received_by_name
            FROM purchase_requests pr
            JOIN materials m ON pr.material_id = m.id
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            WHERE 1=1
        '''
        params = []
        if wp_filter != 'all':
            query += ' AND pr.workplace IN (?, ?)'
            params.extend([wp_filter, SHARED_WORKPLACE])
        if q:
            query += ' AND (m.name LIKE ? OR s.name LIKE ? OR pr.status LIKE ?)'
            like_q = f'%{q}%'
            params.extend([like_q, like_q, like_q])
        query += ' ORDER BY pr.workplace, pr.status, pr.requested_at DESC'
        cursor.execute(query, params)
        data = cursor.fetchall()

    elif tab == 'requirements_calculator':
        data = []
        calculator_products = _get_requirement_calculator_products(cursor)

    elif tab == 'stats':
        data = []
        stats = _query_integrated_stats(cursor, wp_filter, stat_period, stat_anchor)

    elif tab == 'inventory_audit':
        # 월말 재고 조사용 작업장별 상세 리스트 (현재고 0 초과만)
        data = _query_inventory_audit_rows(cursor, selected_inventory_wps)

    elif tab == 'audit_logs':
        query = '''
            SELECT *
            FROM audit_logs
            WHERE 1=1
        '''
        params = []
        if wp_filter != 'all':
            query += ' AND workplace = ?'
            params.append(wp_filter)
        if q:
            query += ' AND (action LIKE ? OR entity LIKE ? OR name LIKE ? OR username LIKE ?)'
            like_q = f'%{q}%'
            params.extend([like_q, like_q, like_q, like_q])
        query += ' ORDER BY created_at DESC, id DESC LIMIT 500'
        cursor.execute(query, params)
        data = cursor.fetchall()
    elif tab == 'db_backups':
        data = _list_db_backups()
        if q:
            q_lower = q.lower()
            data = [row for row in data if q_lower in row['filename'].lower()]

    # ?????嶺뚮ㅎ?э㎗??耀붾굝?????????붾눀?袁⑸븸亦껋꼷伊???(????????????꾨굴??/?????곌떽釉붾???
    cursor.execute('SELECT id, name FROM suppliers ORDER BY name')
    suppliers = cursor.fetchall()

    conn.close()

    return render_template('integrated_management.html',
                          user=session['user'],
                          tab=tab,
                          data=data,
                          current_workplace=session.get('workplace') or 'all',
                          selected_inventory_wps=selected_inventory_wps,
                           workplaces=workplaces,
                           suppliers=suppliers,
                           wp_filter=wp_filter,
                           q=q,
                           rm_tab=rm_tab,
                           calculator_products=calculator_products,
                           stats=stats,
                           stat_period=stat_period,
                           stat_view=stat_view,
                           stat_anchor=(stats or {}).get('anchor', stat_anchor),
                           backup_keep_count=keep_count)


@bp.route('/integrated-management/requirements-calculator-data', methods=['POST'])
@admin_required
def integrated_requirements_calculator_data():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items') or []
    selected_categories = payload.get('categories') or []

    conn = get_db()
    try:
        result = _build_integrated_requirement_payload(conn.cursor(), items)
        result = _filter_integrated_requirement_payload(result, selected_categories)
    finally:
        conn.close()

    return jsonify({'ok': True, **result})


@bp.route('/integrated-management/requirements-calculator-export', methods=['POST'])
@admin_required
def integrated_requirements_calculator_export():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items') or []
    scope = (payload.get('scope') or 'summary').strip()
    selected_categories = payload.get('categories') or []

    conn = get_db()
    try:
        result = _build_integrated_requirement_payload(conn.cursor(), items)
        result = _filter_integrated_requirement_payload(result, selected_categories)
    finally:
        conn.close()

    if scope == 'products':
        rows = [row for row in (result.get('rows') or []) if row.get('scope') == '상품별']
    else:
        rows = [row for row in (result.get('rows') or []) if row.get('scope') == '전체']

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['구분', '상품코드', '상품명', '작업장', '생산 예상 박스수', '분류', '자재코드', '자재명', '단위', '현재고', '필요량', '부족량'])
    for row in rows:
        writer.writerow([
            row.get('scope') or '',
            row.get('product_code') or '',
            row.get('product_name') or '',
            row.get('workplace') or '',
            row.get('boxes') or 0,
            row.get('category') or '',
            row.get('code') or '',
            row.get('name') or '',
            row.get('unit') or '',
            row.get('current_stock') or 0,
            row.get('required_qty') or 0,
            row.get('shortage_qty') or 0,
        ])

    csv_bytes = ('\ufeff' + output.getvalue()).encode('utf-8')
    output.close()
    now_token = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"requirements_calculator_{scope}_{now_token}.csv"
    quoted = quote(filename)
    return Response(
        csv_bytes,
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': f"attachment; filename*=UTF-8''{quoted}"},
    )


@bp.route('/integrated-management/inventory-audit/export')
@admin_required
def integrated_inventory_audit_export():
    selected_inventory_wps = []
    wps_param = (request.args.get('wps') or '').strip()
    if wps_param:
        selected_inventory_wps = [w.strip() for w in wps_param.split(',') if w.strip()]

    conn = get_db()
    cursor = conn.cursor()
    try:
        rows = _query_inventory_audit_rows(cursor, selected_inventory_wps)
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['작업장', '카테고리', '코드', '제품명', '자호', '입고일', '현재고', '실재고'])
    for row in rows:
        writer.writerow(
            [
                row['workplace'] or '',
                row['inv_category'] or '',
                row['code'] or '',
                row['item_name'] or '',
                row['car_number'] or '',
                row['receiving_date'] or '',
                f"{float(row['current_stock'] or 0):.1f}",
                '',
            ]
        )

    csv_text = output.getvalue()
    output.close()
    csv_bytes = ('\ufeff' + csv_text).encode('utf-8')

    now_token = datetime.now().strftime('%Y%m%d_%H%M%S')
    scope = 'all' if not selected_inventory_wps else f"{len(selected_inventory_wps)}wps"
    filename = f"inventory_audit_{scope}_{now_token}.csv"
    quoted = quote(filename)

    return Response(
        csv_bytes,
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': f"attachment; filename*=UTF-8''{quoted}"},
    )


@bp.route('/integrated-management/inventory-audit/apply', methods=['POST'])
@admin_required
def integrated_inventory_audit_apply():
    payload = request.get_json(silent=True) or {}
    rows = payload.get('rows') or []
    if not isinstance(rows, list) or not rows:
        return jsonify({'ok': False, 'message': 'No inventory rows provided.'}), 400

    conn = get_db()
    cursor = conn.cursor()
    applied = []
    failed = []

    try:
        cursor.execute('BEGIN IMMEDIATE')

        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                failed.append({'index': idx, 'message': 'Invalid row payload.'})
                continue

            inv_type = (row.get('inv_type') or '').strip()
            inv_id_raw = row.get('inv_id')
            actual_stock_raw = row.get('actual_stock')

            try:
                inv_id = int(inv_id_raw)
            except (TypeError, ValueError):
                failed.append({'index': idx, 'message': 'Invalid inventory id.'})
                continue

            try:
                actual_stock = float(actual_stock_raw)
            except (TypeError, ValueError):
                failed.append({'index': idx, 'message': 'Invalid actual stock.'})
                continue

            if actual_stock < 0:
                failed.append({'index': idx, 'message': 'Actual stock must be >= 0.'})
                continue

            if inv_type == 'raw_material':
                cursor.execute(
                    '''
                    SELECT id, workplace, COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id)) as code
                    FROM raw_materials
                    WHERE id = ?
                    ''',
                    (inv_id,),
                )
                base = cursor.fetchone()
                if not base:
                    failed.append({'index': idx, 'inv_id': inv_id, 'message': 'Raw material lot not found.'})
                    continue

                code = base['code']
                workplace = base['workplace'] or ''

                cursor.execute(
                    '''
                    SELECT COALESCE(SUM(COALESCE(current_stock, 0)), 0) as total_stock
                    FROM raw_materials
                    WHERE COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id)) = ?
                      AND COALESCE(workplace, '') = ?
                    ''',
                    (code, workplace),
                )
                sum_row = cursor.fetchone()
                current_total = float((sum_row['total_stock'] if sum_row else 0) or 0)
                delta = actual_stock - current_total
                if abs(delta) < 1e-9:
                    continue

                cursor.execute(
                    '''
                    SELECT id, COALESCE(current_stock, 0) as current_stock
                    FROM raw_materials
                    WHERE COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id)) = ?
                      AND COALESCE(workplace, '') = ?
                    ORDER BY
                        CASE WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN 1 ELSE 0 END,
                        receiving_date ASC,
                        id ASC
                    LIMIT 1
                    ''',
                    (code, workplace),
                )
                target = cursor.fetchone()
                if not target:
                    failed.append({'index': idx, 'inv_id': inv_id, 'message': 'No target lot found for raw material.'})
                    continue

                next_stock = float(target['current_stock'] or 0) + delta
                if next_stock < -1e-9:
                    failed.append({'index': idx, 'inv_id': inv_id, 'message': 'Adjustment exceeds oldest raw lot stock.'})
                    continue

                safe_stock = max(next_stock, 0)
                cursor.execute(
                    '''
                    UPDATE raw_materials
                    SET current_stock = ?, total_stock = total_stock + ?
                    WHERE id = ?
                    ''',
                    (safe_stock, delta, target['id']),
                )
                cursor.execute(
                    '''
                    INSERT INTO raw_material_logs (raw_material_id, type, quantity, note, created_by)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (target['id'], 'adjustment', delta, f'inventory_audit_apply:{code}', session['user']['username']),
                )
                audit_log(
                    conn,
                    'inventory_audit_apply',
                    'raw_material',
                    target['id'],
                    {
                        'code': code,
                        'workplace': workplace,
                        'actual_stock': actual_stock,
                        'current_total': current_total,
                        'delta': delta,
                        'target_lot_id': target['id'],
                    },
                )
                applied.append({'index': idx, 'inv_type': inv_type, 'inv_id': inv_id})

            elif inv_type == 'material':
                cursor.execute(
                    '''
                    SELECT id, workplace, COALESCE(NULLIF(TRIM(code), ''), printf('M%05d', id)) as code,
                           COALESCE(current_stock, 0) as current_stock
                    FROM materials
                    WHERE id = ?
                    ''',
                    (inv_id,),
                )
                mat = cursor.fetchone()
                if not mat:
                    failed.append({'index': idx, 'inv_id': inv_id, 'message': 'Material not found.'})
                    continue

                cursor.execute(
                    '''
                    SELECT
                        COALESCE(SUM(COALESCE(current_quantity, quantity, 0)), 0) as lot_total,
                        COUNT(*) as lot_count
                    FROM material_lots
                    WHERE material_id = ?
                      AND COALESCE(is_disposed, 0) = 0
                    ''',
                    (inv_id,),
                )
                lot_sum = cursor.fetchone()
                lot_count = int((lot_sum['lot_count'] if lot_sum else 0) or 0)
                current_total = float((lot_sum['lot_total'] if lot_count > 0 else mat['current_stock']) or 0)
                delta = actual_stock - current_total
                if abs(delta) < 1e-9:
                    continue

                if lot_count > 0:
                    cursor.execute(
                        '''
                        SELECT id, lot, COALESCE(current_quantity, quantity, 0) as current_quantity
                        FROM material_lots
                        WHERE material_id = ?
                          AND COALESCE(is_disposed, 0) = 0
                        ORDER BY
                            CASE WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN 1 ELSE 0 END,
                            receiving_date ASC,
                            lot_seq ASC,
                            id ASC
                        LIMIT 1
                        ''',
                        (inv_id,),
                    )
                    target_lot = cursor.fetchone()
                    if not target_lot:
                        failed.append({'index': idx, 'inv_id': inv_id, 'message': 'No active material lot found.'})
                        continue

                    next_qty = float(target_lot['current_quantity'] or 0) + delta
                    if next_qty < -1e-9:
                        failed.append({'index': idx, 'inv_id': inv_id, 'message': 'Adjustment exceeds oldest material lot stock.'})
                        continue

                    safe_qty = max(next_qty, 0)
                    cursor.execute(
                        '''
                        UPDATE material_lots
                        SET current_quantity = ?, quantity = ?
                        WHERE id = ?
                        ''',
                        (safe_qty, safe_qty, target_lot['id']),
                    )
                    cursor.execute(
                        '''
                        INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
                        VALUES (?, ?, ?, ?, ?)
                        ''',
                        (target_lot['id'], inv_id, 'adjustment', delta, 'inventory_audit_apply'),
                    )
                    _sync_material_stock_with_lots(conn, inv_id)
                    target_entity_id = target_lot['id']
                else:
                    cursor.execute('UPDATE materials SET current_stock = ? WHERE id = ?', (actual_stock, inv_id))
                    target_entity_id = inv_id

                audit_log(
                    conn,
                    'inventory_audit_apply',
                    'material',
                    target_entity_id,
                    {
                        'code': mat['code'],
                        'workplace': mat['workplace'],
                        'actual_stock': actual_stock,
                        'current_total': current_total,
                        'delta': delta,
                        'material_id': inv_id,
                    },
                )
                applied.append({'index': idx, 'inv_type': inv_type, 'inv_id': inv_id})
            else:
                failed.append({'index': idx, 'inv_id': inv_id, 'message': 'Unsupported inventory type.'})

        cursor.execute('COMMIT')
    except Exception:
        try:
            cursor.execute('ROLLBACK')
        except Exception:
            pass
        conn.close()
        return jsonify({'ok': False, 'message': 'Failed to apply inventory audit.'}), 500

    conn.close()
    return jsonify(
        {
            'ok': True,
            'applied_count': len(applied),
            'failed_count': len(failed),
            'applied': applied,
            'failed': failed,
        }
    )


@bp.route('/integrated-management/raw-materials/activity')
@admin_required
def integrated_raw_material_activity():
    wp_filter = request.args.get('wp', 'all')
    date_param = (request.args.get('date') or '').strip()
    if date_param:
        try:
            target_date = datetime.strptime(date_param, '%Y-%m-%d').date()
        except ValueError:
            target_date = datetime.now().date()
    else:
        target_date = datetime.now().date()

    date_from_s = target_date.isoformat()
    date_to_s = target_date.isoformat()

    conn = get_db()
    cursor = conn.cursor()
    try:
        where_clause = ''
        where_params = []
        if wp_filter != 'all':
            where_clause = 'WHERE rm.workplace = ?'
            where_params.append(wp_filter)

        params = [*where_params, date_from_s, date_to_s, date_from_s, date_to_s, date_from_s, date_to_s]
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
        return jsonify(
            {
                'ok': True,
                'date': target_date.isoformat(),
                'date_from': date_from_s,
                'date_to': date_to_s,
                'rows': rows,
            }
        )
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500
    finally:
        conn.close()


@bp.route('/integrated-management/raw-materials/<int:raw_material_id>/detail')
@admin_required
def integrated_raw_material_detail(raw_material_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT id, workplace, name, code, sheets_per_sok, total_stock, current_stock, used_quantity
            FROM raw_materials
            WHERE id = ?
            ''',
            (raw_material_id,),
        )
        base = cursor.fetchone()
        if not base:
            return jsonify({'ok': False, 'message': 'Raw material not found.'}), 404

        base_code = (base['code'] or '').strip()
        if base_code:
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
                ORDER BY
                    CASE WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN 1 ELSE 0 END ASC,
                    receiving_date ASC,
                    id ASC
                ''',
                (base['workplace'], base_code),
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
                ORDER BY
                    CASE WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN 1 ELSE 0 END ASC,
                    receiving_date ASC,
                    id ASC
                ''',
                (base['workplace'], base['name']),
            )
        lots = [dict(row) for row in cursor.fetchall()]
        payload = dict(base)
        payload['lot_count'] = len(lots)
        payload['total_stock_sum'] = sum(float(row.get('total_stock') or 0) for row in lots)
        payload['current_stock_sum'] = sum(float(row.get('current_stock') or 0) for row in lots)
        payload['used_quantity_sum'] = sum(float(row.get('used_quantity') or 0) for row in lots)
        return jsonify({'ok': True, 'raw_material': payload, 'lots': lots})
    finally:
        conn.close()


@bp.route('/integrated-management/raw-material-lots/<int:lot_id>/update', methods=['POST'])
@admin_required
def integrated_update_raw_material_lot(lot_id):
    receiving_date = (request.form.get('receiving_date') or '').strip() or None
    ja_ho = (request.form.get('ja_ho') or request.form.get('car_number') or '').strip()
    sheets_per_sok = float(request.form.get('sheets_per_sok') or 0)
    total_stock = float(request.form.get('total_stock') or 0)
    current_stock = float(request.form.get('current_stock') or 0)

    if total_stock < 0 or current_stock < 0:
        return jsonify({'ok': False, 'message': 'Stock must be >= 0.'}), 400
    if current_stock > total_stock:
        return jsonify({'ok': False, 'message': 'Current stock cannot exceed total stock.'}), 400

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM raw_materials WHERE id = ?', (lot_id,))
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
        conn.commit()
        return jsonify({'ok': True, 'lot': lot})
    except Exception:
        conn.rollback()
        return jsonify({'ok': False, 'message': 'Failed to update raw lot.'}), 500
    finally:
        conn.close()


@bp.route('/integrated-management/db-backups/create', methods=['POST'])
@admin_required
def integrated_create_db_backup():
    keep_count = _parse_keep_count(request.form.get('keep_count'))
    conn = get_db()
    try:
        backup_name, deleted_count = _create_db_backup(keep_count)
        audit_log(
            conn,
            'create',
            'db_backup',
            0,
            {
                'backup_name': backup_name,
                'keep_count': keep_count,
                'deleted_old_backups': deleted_count,
            },
        )
        conn.commit()
    except Exception:
        conn.rollback()
        return "<script>alert('DB backup failed.'); history.back();</script>"
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='db_backups', keep_count=keep_count))


@bp.route('/integrated-management/db-backups/<path:filename>/download')
@admin_required
def integrated_download_db_backup(filename):
    safe_name = Path(filename).name
    if safe_name != filename or not safe_name.startswith('yemat_') or not safe_name.endswith('.db'):
        return "Invalid backup filename.", 400
    target = BACKUP_DIR / safe_name
    if not target.exists():
        return "Backup file not found.", 404
    return send_file(target, as_attachment=True, download_name=safe_name)


# Integrated management - add product
@bp.route('/integrated-management/products/add', methods=['POST'])
@login_required
def integrated_add_product():
    """Auto-generated docstring."""
    if not session['user']['is_admin']:
        return "??????????????源낆┰?????????곸죩", 403

    workplace = request.form.get('workplace')
    name = request.form.get('name')
    code = request.form.get('code')
    description = request.form.get('description')
    box_quantity = request.form.get('box_quantity', 1)
    expiry_months = request.form.get('expiry_months', 12)
    try:
        expiry_months = int(expiry_months)
    except (TypeError, ValueError):
        expiry_months = 12
    if expiry_months < 1 or expiry_months > 12:
        expiry_months = 12

    conn = get_db()
    cursor = conn.cursor()
    try:
        if code:
            cursor.execute("SELECT id FROM products WHERE code = ?", (code,))
            if cursor.fetchone():
                conn.close()
                return "<script>alert('???????쇨덫?? ???? ????⑥ル??????黎앸럽????룸돥??????????곗뒩筌? ?????諛몃마?????????筌?캉??'); history.back();</script>"
        cursor.execute('''
            INSERT INTO products (name, code, description, box_quantity, category, workplace)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (name, code, description, box_quantity, category, workplace))
        audit_log(
            conn,
            'create',
            'product',
            cursor.lastrowid,
            {
                'name': name,
                'code': code,
                'description': description,
                'box_quantity': box_quantity,
                'category': category,
                'workplace': workplace,
            },
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='products'))


# ???? ???癲ル슢????- ???????????꾨굴??
@bp.route('/integrated-management/raw-materials/add', methods=['POST'])
@login_required
def integrated_add_raw_material():
    """Auto-generated docstring."""
    if not session['user']['is_admin']:
        return "??????????????源낆┰?????????곸죩", 403

    workplace = request.form.get('workplace')
    code = (request.form.get('code') or '').strip()
    name = request.form.get('name')
    sheets_per_sok = request.form.get('sheets_per_sok') or 0
    receiving_date = request.form.get('receiving_date')
    ja_ho = (request.form.get('ja_ho') or request.form.get('car_number') or '').strip()
    total_stock = request.form.get('total_stock') or 0
    current_stock = request.form.get('current_stock') or 0

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            INSERT INTO raw_materials (name, code, lot, sheets_per_sok, receiving_date, ja_ho, car_number, total_stock, current_stock, used_quantity, workplace)
            VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, 0, ?)
            ''',
            (name, code, sheets_per_sok, receiving_date, ja_ho, ja_ho, total_stock, current_stock, workplace),
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
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='raw_materials'))


@bp.route('/integrated-management/raw-materials/update', methods=['POST'])
@admin_required
def integrated_update_raw_material():
    """Auto-generated docstring."""
    raw_id = request.form.get('id')
    workplace = request.form.get('workplace')
    code = (request.form.get('code') or '').strip()
    name = request.form.get('name')
    sheets_per_sok = request.form.get('sheets_per_sok') or 0
    receiving_date = request.form.get('receiving_date')
    ja_ho = (request.form.get('ja_ho') or request.form.get('car_number') or '').strip()
    total_stock = request.form.get('total_stock') or 0
    current_stock = request.form.get('current_stock') or 0

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM raw_materials WHERE id = ?', (raw_id,))
        before = cursor.fetchone()
        cursor.execute(
            '''
            UPDATE raw_materials
            SET workplace = ?, code = ?, name = ?, sheets_per_sok = ?, receiving_date = ?, ja_ho = ?, car_number = ?,
                total_stock = ?, current_stock = ?
            WHERE id = ?
        ''',
            (workplace, code, name, sheets_per_sok, receiving_date, ja_ho, ja_ho, total_stock, current_stock, raw_id),
        )
        final_code, lot = _ensure_raw_code_and_lot(cursor, raw_id, code, receiving_date, ja_ho)
        audit_log(
            conn,
            'update',
            'raw_material',
            raw_id,
            {
                'before': dict(before) if before else None,
                'after': {'code': final_code, 'lot': lot},
            },
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='raw_materials'))


# ???? ???癲ル슢????- ????????????꾨굴??
@bp.route('/integrated-management/materials/add', methods=['POST'])
@login_required
def integrated_add_material():
    """Auto-generated docstring."""
    if not session['user']['is_admin']:
        return "??????????????源낆┰?????????곸죩", 403

    workplace = request.form.get('workplace')
    custom_code = request.form.get('code', '').strip()
    supplier_id = request.form.get('supplier_id') or None
    name = request.form.get('name')
    category = request.form.get('category')
    category_clean = (category or '???????').strip()
    unit = request.form.get('unit')
    min_stock = float(request.form.get('min_stock') or 0)
    receiving_date = request.form.get('receiving_date')
    manufacture_date = request.form.get('manufacture_date')
    expiry_date = request.form.get('expiry_date')
    unit_price = float(request.form.get('unit_price') or 0)
    lot_quantity = float(request.form.get('quantity') or 0)
    target_workplace = SHARED_WORKPLACE if category_clean in SHARED_MATERIAL_CATEGORIES else workplace

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM materials WHERE name = ? AND workplace = ?", (name, target_workplace))
        if cursor.fetchone():
            conn.close()
            return redirect(url_for('admin.integrated_management', tab='materials'))

        if custom_code:
            cursor.execute("SELECT id FROM materials WHERE code = ?", (custom_code,))
            if cursor.fetchone():
                conn.close()
                return redirect(url_for('admin.integrated_management', tab='materials'))

        cursor.execute(
            '''
            INSERT INTO materials 
            (supplier_id, name, category, unit, current_stock, min_stock, workplace, unit_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
            (supplier_id, name, category_clean, unit, lot_quantity, min_stock, target_workplace, unit_price),
        )

        new_id = cursor.lastrowid
        final_code = custom_code if custom_code else f"M{new_id:05d}"
        cursor.execute(
            '''
            UPDATE materials
            SET code = ?
            WHERE id = ?
        ''',
            (final_code, new_id),
        )

        if receiving_date or lot_quantity:
            lot_seq = _next_lot_seq(cursor, new_id, receiving_date)
            lot = _build_material_lot(final_code, receiving_date, lot_seq)
            cursor.execute(
                '''
                INSERT INTO material_lots
                (material_id, lot, lot_seq, receiving_date, manufacture_date, expiry_date, unit_price, received_quantity, current_quantity, quantity, supplier_lot)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
                (new_id, lot, lot_seq, receiving_date, manufacture_date, expiry_date, unit_price, lot_quantity, lot_quantity, lot_quantity, ''),
            )
            lot_id = cursor.lastrowid
            cursor.execute(
                '''
                INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
                VALUES (?, ?, 'create', ?, ?)
            ''',
                (lot_id, new_id, lot_quantity, 'initial lot create'),
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
                'current_stock': lot_quantity,
                'min_stock': min_stock,
                'workplace': target_workplace,
                'receiving_date': receiving_date,
                'manufacture_date': manufacture_date,
                'expiry_date': expiry_date,
                'unit_price': unit_price,
            },
        )

        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='materials'))


@bp.route('/integrated-management/materials/update', methods=['POST'])
@admin_required
def integrated_update_material():
    """Auto-generated docstring."""
    material_id = request.form.get('id')
    workplace = request.form.get('workplace')
    code = request.form.get('code', '').strip()
    supplier_id = request.form.get('supplier_id') or None
    name = request.form.get('name')
    category = request.form.get('category')
    unit = request.form.get('unit')
    min_stock_raw = request.form.get('min_stock')
    moq_raw = request.form.get('moq')

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM materials WHERE id = ?', (material_id,))
        before = cursor.fetchone()
        if not before:
            conn.close()
            return redirect(url_for('admin.integrated_management', tab='materials'))

        current_code = (before['code'] or '').strip()
        final_code = code or current_code
        final_name = name or before['name']
        final_category = (category or before['category'] or '???????').strip()
        final_unit = unit or before['unit']
        final_min_stock = float(min_stock_raw) if min_stock_raw not in (None, '') else float(before['min_stock'] or 0)
        final_moq = float(moq_raw) if moq_raw not in (None, '') else float(before['moq'] or 0)
        base_workplace = workplace or before['workplace']
        target_workplace = SHARED_WORKPLACE if final_category in SHARED_MATERIAL_CATEGORIES else base_workplace

        if final_code:
            cursor.execute("SELECT id FROM materials WHERE code = ? AND id != ?", (final_code, material_id))
            if cursor.fetchone():
                conn.close()
                return "<script>alert('???????쇨덫?? ???? ????⑥ル??????黎앸럽????룸돥???????????????諛몃마?????????筌?캉??'); history.back();</script>"

        cursor.execute(
            '''
            UPDATE materials
            SET code = ?, name = ?, category = ?, unit = ?, supplier_id = ?,
                min_stock = ?, moq = ?, workplace = ?
            WHERE id = ?
        ''',
            (final_code, final_name, final_category, final_unit, supplier_id, final_min_stock, final_moq, target_workplace, material_id),
        )

        if final_code and final_code != current_code:
            cursor.execute('SELECT id, receiving_date, lot_seq FROM material_lots WHERE material_id = ?', (material_id,))
            lots = cursor.fetchall()
            for row in lots:
                updated_lot = _build_material_lot(final_code, row['receiving_date'], row['lot_seq'])
                cursor.execute('UPDATE material_lots SET lot = ? WHERE id = ?', (updated_lot, row['id']))

        audit_log(
            conn,
            'update',
            'material',
            material_id,
            {
                'before': dict(before) if before else None,
                'after': {
                    'code': final_code,
                    'name': final_name,
                    'category': final_category,
                    'unit': final_unit,
                    'supplier_id': supplier_id,
                    'min_stock': final_min_stock,
                    'moq': final_moq,
                    'workplace': target_workplace,
                },
            },
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='materials'))


@bp.route('/integrated-management/materials/assign-workplace', methods=['POST'])
@admin_required
def integrated_assign_material_workplace():
    """Auto-generated docstring."""
    material_id = request.form.get('id')
    workplace = request.form.get('workplace')
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT category, workplace FROM materials WHERE id = ?', (material_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return redirect(url_for('admin.integrated_management', tab='materials', wp='unassigned'))
        category = (row['category'] or '').strip()
        target_workplace = SHARED_WORKPLACE if category in SHARED_MATERIAL_CATEGORIES else workplace
        cursor.execute(
            'UPDATE materials SET workplace = ? WHERE id = ?',
            (target_workplace, material_id),
        )
        audit_log(
            conn,
            'update',
            'material',
            material_id,
            {'set_workplace': target_workplace},
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()
    return redirect(url_for('admin.integrated_management', tab='materials', wp='unassigned'))


@bp.route('/integrated-management/materials/reset-stock', methods=['POST'])
@admin_required
def integrated_reset_material_stock():
    conn = get_db()
    cursor = conn.cursor()
    try:
        def _table_exists(name):
            cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (name,))
            return cursor.fetchone() is not None

        cursor.execute(
            '''
            UPDATE material_lots
            SET current_quantity = 0,
                quantity = 0
            WHERE COALESCE(is_disposed, 0) = 0
            '''
        )
        lot_count = cursor.rowcount if cursor.rowcount is not None else 0
        cursor.execute('UPDATE materials SET current_stock = 0')
        material_count = cursor.rowcount if cursor.rowcount is not None else 0

        purchase_request_deleted = 0
        purchase_order_deleted = 0
        purchase_order_item_deleted = 0
        if _table_exists('purchase_requests'):
            cursor.execute('DELETE FROM purchase_requests')
            purchase_request_deleted = cursor.rowcount if cursor.rowcount is not None else 0
        if _table_exists('purchase_order_items'):
            cursor.execute('DELETE FROM purchase_order_items')
            purchase_order_item_deleted = cursor.rowcount if cursor.rowcount is not None else 0
        if _table_exists('purchase_orders'):
            cursor.execute('DELETE FROM purchase_orders')
            purchase_order_deleted = cursor.rowcount if cursor.rowcount is not None else 0

        audit_log(
            conn,
            'update',
            'material_stock',
            0,
            {
                'action': 'integrated_reset_stock',
                'materials_updated': material_count,
                'lots_updated': lot_count,
                'purchase_requests_deleted': purchase_request_deleted,
                'purchase_orders_deleted': purchase_order_deleted,
                'purchase_order_items_deleted': purchase_order_item_deleted,
            },
        )
        conn.commit()
    except Exception:
        conn.rollback()
        return "<script>alert('????????????????硫멸킐????????????쇨덫????????밸븶筌믩끃???ル봿留싷┼??돘????????????????곸죩.'); history.back();</script>"
    finally:
        conn.close()

    wp = request.form.get('wp', 'all')
    q = request.form.get('q', '').strip()
    return redirect(url_for('admin.integrated_management', tab='materials', wp=wp, q=q))


@bp.route('/integrated-management/materials/<int:material_id>/detail')
@admin_required
def integrated_material_detail(material_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        _sync_material_stock_with_lots(conn, material_id)
        cursor.execute(
            '''
            SELECT m.id, m.code, m.name, m.unit, m.current_stock, s.name as supplier_name
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            WHERE m.id = ?
        ''',
            (material_id,),
        )
        material = cursor.fetchone()
        if not material:
            return jsonify({'ok': False, 'message': 'Material not found.'}), 404

        cursor.execute(
            '''
            SELECT id, lot, lot_seq, receiving_date, manufacture_date, expiry_date, unit_price, received_quantity, current_quantity, supplier_lot, is_disposed
            FROM material_lots
            WHERE material_id = ?
              AND COALESCE(is_disposed, 0) = 0
              AND COALESCE(current_quantity, quantity, 0) > 0
            ORDER BY receiving_date DESC, lot_seq DESC, id DESC
        ''',
            (material_id,),
        )
        lots = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            '''
            SELECT
                COALESCE(p.production_date, substr(pmu.created_at, 1, 10)) as use_date,
                ('PROD-' || pmu.production_id) as production_no,
                COALESCE(pmu.actual_quantity, 0) as used_quantity,
                COALESCE(m.current_stock, 0) as remaining_quantity,
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
            LEFT JOIN materials m ON m.id = pmu.material_id
            WHERE pmu.material_id = ?
              AND COALESCE(pmu.actual_quantity, 0) > 0
              AND COALESCE(p.status, '') = '완료'
            ORDER BY COALESCE(p.production_date, pmu.created_at) DESC
            LIMIT 200
        ''',
            (material_id,),
        )
        usage_logs = [dict(row) for row in cursor.fetchall()]

        payload = dict(material)
        payload['total_quantity'] = sum(float(row.get('current_quantity') or 0) for row in lots)
        return jsonify({'ok': True, 'material': payload, 'lots': lots, 'usage_logs': usage_logs})
    finally:
        conn.close()


@bp.route('/integrated-management/material-lots/add', methods=['POST'])
@admin_required
def integrated_add_material_lot():
    material_id = request.form.get('material_id')
    receiving_date = request.form.get('receiving_date')
    manufacture_date = request.form.get('manufacture_date')
    expiry_date = request.form.get('expiry_date')
    unit_price = float(request.form.get('unit_price') or 0)
    received_quantity = _round_to_1_decimal(request.form.get('received_quantity') or request.form.get('quantity') or 0)
    current_quantity = _round_to_1_decimal(request.form.get('current_quantity') or received_quantity)
    supplier_lot = (request.form.get('supplier_lot') or '').strip()

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT id, code FROM materials WHERE id = ?', (material_id,))
        material = cursor.fetchone()
        if not material:
            return jsonify({'ok': False, 'message': '????????耀붾굝????????????????源낆┰?????????곸죩.'}), 404

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
        cursor.execute('UPDATE materials SET current_stock = current_stock + ?, unit_price = ? WHERE id = ?', (current_quantity, unit_price, material_id))
        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'create', ?, ?)
        ''',
            (lot_id, material_id, current_quantity, lot),
        )
        audit_log(conn, 'create', 'material_lot', lot_id, {'material_id': material_id, 'lot': lot, 'received_quantity': received_quantity, 'current_quantity': current_quantity, 'supplier_lot': supplier_lot})
        conn.commit()
        return jsonify({'ok': True, 'lot': lot})
    except Exception:
        conn.rollback()
        return jsonify({'ok': False, 'message': '????癲????????꾨굴?? ?????????쇨덫????????밸븶筌믩끃???ル봿留싷┼??돘????????????????곸죩.'}), 500
    finally:
        conn.close()


@bp.route('/integrated-management/material-lots/<int:lot_id>/update', methods=['POST'])
@admin_required
def integrated_update_material_lot(lot_id):
    receiving_date = request.form.get('receiving_date')
    manufacture_date = request.form.get('manufacture_date')
    expiry_date = request.form.get('expiry_date')
    unit_price = float(request.form.get('unit_price') or 0)
    received_quantity = _round_to_1_decimal(request.form.get('received_quantity') or request.form.get('quantity') or 0)
    current_quantity = _round_to_1_decimal(request.form.get('current_quantity') or received_quantity)
    supplier_lot = (request.form.get('supplier_lot') or '').strip()

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM material_lots WHERE id = ?', (lot_id,))
        before = cursor.fetchone()
        if not before:
            return jsonify({'ok': False, 'message': '????癲????筌롫㈇????耀붾굝????????????????源낆┰?????????곸죩.'}), 404

        cursor.execute('SELECT code FROM materials WHERE id = ?', (before['material_id'],))
        material = cursor.fetchone()
        lot = _build_material_lot(material['code'] if material else '', receiving_date, before['lot_seq'])

        qty_delta = current_quantity - float(before['current_quantity'] or before['quantity'] or 0)
        cursor.execute(
            '''
            UPDATE material_lots
            SET lot = ?, receiving_date = ?, manufacture_date = ?, expiry_date = ?, unit_price = ?, received_quantity = ?, current_quantity = ?, supplier_lot = ?, quantity = ?, is_disposed = 0, disposed_at = NULL
            WHERE id = ?
        ''',
            (lot, receiving_date, manufacture_date, expiry_date, unit_price, received_quantity, current_quantity, supplier_lot, current_quantity, lot_id),
        )
        cursor.execute('UPDATE materials SET current_stock = current_stock + ?, unit_price = ? WHERE id = ?', (qty_delta, unit_price, before['material_id']))
        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'update', ?, ?)
        ''',
            (lot_id, before['material_id'], current_quantity, lot),
        )
        audit_log(conn, 'update', 'material_lot', lot_id, {'before': dict(before), 'after': {'lot': lot, 'received_quantity': received_quantity, 'current_quantity': current_quantity, 'supplier_lot': supplier_lot}})
        conn.commit()
        return jsonify({'ok': True, 'lot': lot})
    except Exception:
        conn.rollback()
        return jsonify({'ok': False, 'message': '????癲???????곌떽釉붾???????????쇨덫????????밸븶筌믩끃???ル봿留싷┼??돘????????????????곸죩.'}), 500
    finally:
        conn.close()


@bp.route('/integrated-management/material-lots/<int:lot_id>/delete', methods=['POST'])
@admin_required
def integrated_delete_material_lot(lot_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM material_lots WHERE id = ?', (lot_id,))
        lot = cursor.fetchone()
        if not lot:
            return jsonify({'ok': False, 'message': '????癲????筌롫㈇????耀붾굝????????????????源낆┰?????????곸죩.'}), 404

        current_qty = float(lot['current_quantity'] or lot['quantity'] or 0)
        cursor.execute(
            '''
            UPDATE material_lots
            SET is_disposed = 1, disposed_at = CURRENT_TIMESTAMP, current_quantity = 0, quantity = 0
            WHERE id = ?
        ''',
            (lot_id,),
        )
        cursor.execute('UPDATE materials SET current_stock = current_stock - ? WHERE id = ?', (current_qty, lot['material_id']))
        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'dispose', ?, ?)
        ''',
            (lot_id, lot['material_id'], current_qty, lot['lot']),
        )
        audit_log(conn, 'delete', 'material_lot', lot_id, {'before': dict(lot), 'disposed_quantity': current_qty})
        conn.commit()
        return jsonify({'ok': True})
    except Exception:
        conn.rollback()
        return jsonify({'ok': False, 'message': '????癲????????????????쇨덫????????밸븶筌믩끃???ル봿留싷┼??돘????????????????곸죩.'}), 500
    finally:
        conn.close()


@bp.route('/integrated-management/products/update', methods=['POST'])
@admin_required
def integrated_update_product():
    """Auto-generated docstring."""
    product_id = request.form.get('id')
    workplace = request.form.get('workplace')
    name = request.form.get('name')
    code = request.form.get('code')
    description = request.form.get('description')
    box_quantity = request.form.get('box_quantity', 1)
    category = request.form.get('category', '???????')

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM products WHERE id = ?', (product_id,))
        before = cursor.fetchone()
        if code:
            cursor.execute("SELECT id FROM products WHERE code = ? AND id != ?", (code, product_id))
            if cursor.fetchone():
                conn.close()
                return "<script>alert('???????쇨덫?? ???? ????⑥ル??????黎앸럽????룸돥??????????곗뒩筌? ?????諛몃마?????????筌?캉??'); history.back();</script>"
        cursor.execute(
            '''
            UPDATE products
            SET workplace = ?, name = ?, code = ?, description = ?, box_quantity = ?, expiry_months = ?
            WHERE id = ?
        ''',
            (workplace, name, code, description, box_quantity, expiry_months, product_id),
        )
        audit_log(
            conn,
            'update',
            'product',
            product_id,
            {'before': dict(before) if before else None},
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='products'))


@bp.route('/integrated-management/purchase-requests/update', methods=['POST'])
@admin_required
def integrated_update_purchase_request():
    """Auto-generated docstring."""
    req_id = request.form.get('id')
    workplace = request.form.get('workplace')
    status = request.form.get('status')
    requested_quantity = float(request.form.get('requested_quantity') or 0)
    ordered_quantity = float(request.form.get('ordered_quantity') or 0)
    received_quantity = float(request.form.get('received_quantity') or 0)
    expected_delivery_date = request.form.get('expected_delivery_date')
    note = request.form.get('note')

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM purchase_requests WHERE id = ?', (req_id,))
        prev = cursor.fetchone()
        if not prev:
            conn.close()
            return redirect(url_for('admin.integrated_management', tab='purchase_requests'))

        prev_status = prev['status']
        material_id = prev['material_id']

        cursor.execute(
            '''
            UPDATE purchase_requests
            SET workplace = ?, status = ?, requested_quantity = ?, ordered_quantity = ?,
                received_quantity = ?, expected_delivery_date = ?, note = ?
            WHERE id = ?
        ''',
            (
                workplace,
                status,
                requested_quantity,
                ordered_quantity,
                received_quantity,
                expected_delivery_date,
                note,
                req_id,
            ),
        )

        # ?????獄?塋??????獄쏅챶留???????ш내?℡ㅇ?????????????밸븶筌믩끃????
        if status == '??껎?袁⑥┷' and prev_status != '??껎?袁⑥┷' and received_quantity > 0:
            cursor.execute(
                'UPDATE materials SET current_stock = current_stock + ? WHERE id = ?',
                (received_quantity, material_id),
            )
            cursor.execute(
                '''
                UPDATE purchase_requests
                SET received_at = COALESCE(received_at, CURRENT_TIMESTAMP),
                    received_by = COALESCE(received_by, ?)
                WHERE id = ?
            ''',
                (session['user'].get('name'), req_id),
            )

        audit_log(
            conn,
            'update',
            'purchase_request',
            req_id,
            {
                'before': dict(prev),
                'after': {
                    'workplace': workplace,
                    'status': status,
                    'requested_quantity': requested_quantity,
                    'ordered_quantity': ordered_quantity,
                    'received_quantity': received_quantity,
                    'expected_delivery_date': expected_delivery_date,
                    'note': note,
                },
            },
        )

        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='purchase_requests'))


# ???? ???癲ル슢????- ????(??????⑤８???
@bp.route('/integrated-management/<table>/<int:item_id>/delete', methods=['POST'])
@login_required
def integrated_delete_item(table, item_id):
    """Auto-generated docstring."""
    if not session['user']['is_admin']:
        return "??????????????源낆┰?????????곸죩", 403

    allowed_tables = ['products', 'raw_materials', 'materials', 'purchase_requests']
    if table not in allowed_tables:
        return "??濡?굘?????逾??곕뾼????덈펲.", 400

    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute(f'SELECT * FROM {table} WHERE id = ?', (item_id,))
        before = cursor.fetchone()
        cursor.execute(f'DELETE FROM {table} WHERE id = ?', (item_id,))
        audit_log(
            conn,
            'delete',
            table,
            item_id,
            {'before': dict(before) if before else None},
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        return f"???????????ㅼ뒩?? {str(e)}", 400
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab=table))
