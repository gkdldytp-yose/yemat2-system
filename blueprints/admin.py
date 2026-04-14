from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
import csv
import io
import re
import zipfile
from urllib.parse import quote
from xml.sax.saxutils import escape as xml_escape

from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify, send_file, Response

from core import (
    get_db,
    login_required,
    admin_required,
    WORKPLACES,
    LOGISTICS_WORKPLACE,
    SHARED_WORKPLACE,
    SHARED_MATERIAL_CATEGORIES,
    audit_log,
)
from blueprints.production import _delete_production_record

bp = Blueprint('admin', __name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / 'yemat.db'
BACKUP_DIR = PROJECT_ROOT / 'backups'
BACKUP_KEEP_DEFAULT = 10

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


def _can_manage_material_lots():
    user = session.get('user') or {}
    role = user.get('role', 'readonly')
    workplace = (session.get('workplace') or '').strip()
    return bool(user.get('is_admin')) or role == 'logistics' or workplace == LOGISTICS_WORKPLACE


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


def _normalize_date_token(value):
    raw = (value or '').strip()
    if not raw:
        return '00000000'
    return raw.replace('-', '')


def _round_to_1_decimal(value):
    return round(float(value or 0) + 1e-9, 1)


def _xlsx_escape_text(value):
    text = '' if value is None else str(value)
    text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', '', text)
    return xml_escape(text)


def _xlsx_column_name(index):
    result = ''
    current = int(index)
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result = chr(65 + remainder) + result
    return result or 'A'


def _build_simple_xlsx(sheet_name, headers, rows):
    buffer = io.BytesIO()

    def _cell_xml(row_idx, col_idx, value):
        cell_ref = f"{_xlsx_column_name(col_idx)}{row_idx}"
        if value is None or value == '':
            return f'<c r="{cell_ref}" t="inlineStr"><is><t></t></is></c>'
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return f'<c r="{cell_ref}"><v>{value}</v></c>'
        return f'<c r="{cell_ref}" t="inlineStr"><is><t>{_xlsx_escape_text(value)}</t></is></c>'

    all_rows = [headers] + rows
    max_cols = max((len(r) for r in all_rows), default=0)
    cols_xml = ''.join(
        f'<col min="{idx}" max="{idx}" width="18" customWidth="1"/>'
        for idx in range(1, max_cols + 1)
    )
    rows_xml = []
    for row_idx, row in enumerate(all_rows, start=1):
        cell_xml = ''.join(_cell_xml(row_idx, col_idx, value) for col_idx, value in enumerate(row, start=1))
        rows_xml.append(f'<row r="{row_idx}">{cell_xml}</row>')

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<cols>{cols_xml}</cols>'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        '</worksheet>'
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets><sheet name="{_xlsx_escape_text(sheet_name)}" sheetId="1" r:id="rId1"/></sheets>'
        '</workbook>'
    )
    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        '</Relationships>'
    )
    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '</Types>'
    )
    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><name val="Malgun Gothic"/></font></fonts>'
        '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        '</styleSheet>'
    )

    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('[Content_Types].xml', content_types_xml)
        zf.writestr('_rels/.rels', root_rels_xml)
        zf.writestr('xl/workbook.xml', workbook_xml)
        zf.writestr('xl/_rels/workbook.xml.rels', workbook_rels_xml)
        zf.writestr('xl/worksheets/sheet1.xml', sheet_xml)
        zf.writestr('xl/styles.xml', styles_xml)

    buffer.seek(0)
    return buffer


def _normalize_requirement_sub_category(category):
    raw = (category or '').strip()
    if raw == '박스':
        return 'box'
    if raw == '내포':
        return 'inner'
    if raw == '외포':
        return 'outer'
    if raw in ('실리카', '실리카겔'):
        return 'silica'
    if raw == '트레이':
        return 'tray'
    return 'etc'


def _requirement_sub_sort_key(category):
    order = {
        'inner': 1,
        'outer': 2,
        'box': 3,
        'silica': 4,
        'tray': 5,
        'etc': 6,
    }
    return order.get(_normalize_requirement_sub_category(category), 99)


def _build_integrated_requirement_payload(cursor, product_inputs):
    product_box_map = {}
    product_name_map = {}

    for item in product_inputs:
        try:
            product_id = int(item.get('product_id') or 0)
            boxes = float(item.get('boxes') or 0)
        except (TypeError, ValueError):
            continue
        if product_id <= 0 or boxes <= 0:
            continue
        product_box_map[product_id] = product_box_map.get(product_id, 0.0) + boxes

    if not product_box_map:
        return {
            'ok': True,
            'summary': {'raw': [], 'base': [], 'sub': [], 'sub_groups': {'box': [], 'inner': [], 'outer': [], 'silica': [], 'tray': [], 'etc': []}},
            'products': [],
        }

    product_ids = list(product_box_map.keys())
    placeholders = ','.join(['?'] * len(product_ids))

    cursor.execute(
        f'''
        SELECT id, name, code, workplace
        FROM products
        WHERE id IN ({placeholders})
        ''',
        product_ids,
    )
    for row in cursor.fetchall():
        product_name_map[int(row['id'])] = {
            'name': row['name'],
            'code': row['code'],
            'workplace': row['workplace'],
        }

    cursor.execute(
        f'''
        SELECT
            b.product_id,
            b.raw_material_id,
            b.material_id,
            COALESCE(b.quantity_per_box, 0) as quantity_per_box,
            rm.name as raw_name,
            COALESCE(NULLIF(TRIM(rm.code), ''), printf('RM%05d', rm.id)) as raw_code,
            m.name as material_name,
            COALESCE(NULLIF(TRIM(m.code), ''), printf('M%05d', m.id)) as material_code,
            COALESCE(m.category, '') as material_category,
            COALESCE(NULLIF(TRIM(m.unit), ''), '개') as material_unit
        FROM bom b
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
        GROUP BY COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id))
        '''
    )
    raw_stock_map = {str(r['code']): float(r['stock'] or 0) for r in cursor.fetchall()}

    cursor.execute(
        '''
        SELECT
            COALESCE(NULLIF(TRIM(code), ''), printf('M%05d', id)) as code,
            COALESCE(SUM(COALESCE(current_stock, 0)), 0) as stock
        FROM materials
        GROUP BY COALESCE(NULLIF(TRIM(code), ''), printf('M%05d', id))
        '''
    )
    material_stock_map = {str(r['code']): float(r['stock'] or 0) for r in cursor.fetchall()}

    cursor.execute(
        '''
        SELECT material_code, COALESCE(SUM(quantity), 0) as stock
        FROM logistics_stocks
        GROUP BY material_code
        '''
    )
    for row in cursor.fetchall():
        code = str(row['material_code'] or '').strip()
        material_stock_map[code] = material_stock_map.get(code, 0.0) + float(row['stock'] or 0)

    summary_raw = {}
    summary_base = {}
    summary_sub = {}
    summary_sub_groups = {'box': {}, 'inner': {}, 'outer': {}, 'silica': {}, 'tray': {}, 'etc': {}}
    product_detail = {}
    seen_product_raw_keys = set()

    def _upsert_item(target, key, code, name, unit, stock, required, category=''):
        if key not in target:
            target[key] = {
                'code': code or '-',
                'name': name or '-',
                'category': category or '',
                'unit': unit or '개',
                'stock': float(stock or 0),
                'required': 0.0,
            }
        target[key]['required'] += float(required or 0)

    for row in bom_rows:
        product_id = int(row.get('product_id') or 0)
        if product_id <= 0 or product_id not in product_box_map:
            continue

        qty_per_box = float(row.get('quantity_per_box') or 0)
        if qty_per_box <= 0:
            continue

        need_qty = qty_per_box * float(product_box_map.get(product_id) or 0)
        if need_qty <= 0:
            continue

        if product_id not in product_detail:
            info = product_name_map.get(product_id) or {}
            product_detail[product_id] = {
                'product_id': product_id,
                'product_name': info.get('name') or f'상품 {product_id}',
                'product_code': info.get('code') or '',
                'workplace': info.get('workplace') or '-',
                'planned_boxes': float(product_box_map.get(product_id) or 0),
                'raw_map': {},
                'base_map': {},
                'sub_map': {},
            }

        if row.get('raw_material_id'):
            code = str(row.get('raw_code') or '')
            name = row.get('raw_name') or code or '원초'
            raw_key = (product_id, code or name)
            if raw_key in seen_product_raw_keys:
                continue
            seen_product_raw_keys.add(raw_key)
            stock = raw_stock_map.get(code, 0.0)
            _upsert_item(summary_raw, code or name, code, name, '속', stock, need_qty, '원초')
            _upsert_item(product_detail[product_id]['raw_map'], code or name, code, name, '속', stock, need_qty, '원초')
            continue

        if row.get('material_id'):
            code = str(row.get('material_code') or '')
            name = row.get('material_name') or code or '부자재'
            category = (row.get('material_category') or '').strip()
            unit = row.get('material_unit') or '개'
            stock = material_stock_map.get(code, 0.0)
            is_base = category in ('기름', '소금')
            target_summary = summary_base if is_base else summary_sub
            target_product = product_detail[product_id]['base_map'] if is_base else product_detail[product_id]['sub_map']
            _upsert_item(target_summary, code or name, code, name, unit, stock, need_qty, category)
            _upsert_item(target_product, code or name, code, name, unit, stock, need_qty, category)
            if not is_base:
                sub_key = _normalize_requirement_sub_category(category)
                _upsert_item(summary_sub_groups[sub_key], code or name, code, name, unit, stock, need_qty, category)

    def _to_sorted_list(data_map, mode='default'):
        rows = []
        for item in data_map.values():
            stock = float(item.get('stock') or 0)
            required = float(item.get('required') or 0)
            shortage = required - stock
            if shortage < 0:
                shortage = 0.0
            rows.append(
                {
                    'code': item.get('code') or '-',
                    'name': item.get('name') or '-',
                    'category': item.get('category') or '',
                    'unit': item.get('unit') or '개',
                    'stock': round(stock, 2),
                    'required': round(required, 2),
                    'shortage': round(shortage, 2),
                }
            )
        if mode == 'sub':
            rows.sort(
                key=lambda x: (
                    _requirement_sub_sort_key(x.get('category')),
                    str(x.get('code') or ''),
                    str(x.get('name') or ''),
                )
            )
        else:
            rows.sort(key=lambda x: (x['shortage'] > 0, x['shortage'], x['required'], x['name']), reverse=True)
        return rows

    products_payload = []
    for item in product_detail.values():
        products_payload.append(
            {
                'product_id': item['product_id'],
                'product_name': item['product_name'],
                'product_code': item.get('product_code') or '',
                'workplace': item['workplace'],
                'planned_boxes': round(float(item.get('planned_boxes') or 0), 2),
                'raw_items': _to_sorted_list(item['raw_map']),
                'base_items': _to_sorted_list(item['base_map']),
                'sub_items': _to_sorted_list(item['sub_map'], mode='sub'),
            }
        )
    products_payload.sort(key=lambda x: x['product_name'])

    return {
        'ok': True,
        'summary': {
            'raw': _to_sorted_list(summary_raw),
            'base': _to_sorted_list(summary_base),
            'sub': _to_sorted_list(summary_sub, mode='sub'),
            'sub_groups': {
                'box': _to_sorted_list(summary_sub_groups['box'], mode='sub'),
                'inner': _to_sorted_list(summary_sub_groups['inner'], mode='sub'),
                'outer': _to_sorted_list(summary_sub_groups['outer'], mode='sub'),
                'silica': _to_sorted_list(summary_sub_groups['silica'], mode='sub'),
                'tray': _to_sorted_list(summary_sub_groups['tray'], mode='sub'),
                'etc': _to_sorted_list(summary_sub_groups['etc'], mode='sub'),
            },
        },
        'products': products_payload,
    }


def _filter_integrated_requirement_payload(payload, selected_categories):
    categories = [str(cat or '').strip() for cat in (selected_categories or []) if str(cat or '').strip()]
    if not categories:
        return payload

    allowed = set(categories)

    def _filter_rows(rows):
        return [row for row in (rows or []) if str((row or {}).get('category') or '').strip() in allowed]

    summary = payload.get('summary') or {}
    sub_groups = summary.get('sub_groups') or {}
    filtered_products = []
    for product in payload.get('products') or []:
        filtered_products.append(
            {
                **product,
                'raw_items': _filter_rows(product.get('raw_items') or []),
                'base_items': _filter_rows(product.get('base_items') or []),
                'sub_items': _filter_rows(product.get('sub_items') or []),
            }
        )

    return {
        **payload,
        'selected_categories': categories,
        'summary': {
            'raw': _filter_rows(summary.get('raw') or []),
            'base': _filter_rows(summary.get('base') or []),
            'sub': _filter_rows(summary.get('sub') or []),
            'sub_groups': {
                'box': _filter_rows(sub_groups.get('box') or []),
                'inner': _filter_rows(sub_groups.get('inner') or []),
                'outer': _filter_rows(sub_groups.get('outer') or []),
                'silica': _filter_rows(sub_groups.get('silica') or []),
                'tray': _filter_rows(sub_groups.get('tray') or []),
                'etc': _filter_rows(sub_groups.get('etc') or []),
            },
        },
        'products': filtered_products,
    }


def _build_requirement_export_rows(payload, mode):
    rows = []
    summary = payload.get('summary') or {}
    products = payload.get('products') or []

    if mode == 'products':
        for product in products:
            base_info = [
                '상품별',
                product.get('product_code') or '',
                product.get('product_name') or '',
                product.get('workplace') or '',
                float(product.get('planned_boxes') or 0),
            ]
            for section_name, items in (
                ('원초', product.get('raw_items') or []),
                ('원자재', product.get('base_items') or []),
                ('부자재', product.get('sub_items') or []),
            ):
                for item in items:
                    rows.append(
                        base_info + [
                            section_name,
                            item.get('category') or '',
                            item.get('code') or '',
                            item.get('name') or '',
                            item.get('unit') or '',
                            float(item.get('stock') or 0),
                            float(item.get('required') or 0),
                            float(item.get('shortage') or 0),
                        ]
                    )
    else:
        summary_sections = [
            ('원초', '원초', summary.get('raw') or []),
            ('원자재', '원자재', summary.get('base') or []),
        ]
        for sub_key, label in (
            ('inner', '내포'),
            ('outer', '외포'),
            ('box', '박스'),
            ('silica', '실리카'),
            ('tray', '트레이'),
            ('etc', '기타'),
        ):
            summary_sections.append(('부자재', label, ((summary.get('sub_groups') or {}).get(sub_key) or [])))

        for section_name, detail_name, items in summary_sections:
            for item in items:
                rows.append(
                    [
                        '전체',
                        '',
                        '',
                        '',
                        '',
                        section_name,
                        detail_name if section_name == '부자재' else (item.get('category') or detail_name),
                        item.get('code') or '',
                        item.get('name') or '',
                        item.get('unit') or '',
                        float(item.get('stock') or 0),
                        float(item.get('required') or 0),
                        float(item.get('shortage') or 0),
                    ]
                )

    return rows


def _build_material_lot(material_code, receiving_date, lot_seq):
    code = (material_code or '').strip() or 'NO_CODE'
    seq = int(lot_seq or 1)
    return f"{code}-{_normalize_date_token(receiving_date)}-{seq:03d}"


def _get_inv_location_id(cursor, location_name):
    target_name = (location_name or '').strip()
    if target_name in {LOGISTICS_WORKPLACE, '물류창고'}:
        row = cursor.execute(
            '''
            SELECT id
            FROM inv_locations
            WHERE name = '물류창고'
               OR COALESCE(workplace_code, '') IN ('WH', ?)
               OR COALESCE(loc_type, '') = 'WAREHOUSE'
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
        WHERE name = ? OR workplace_code = ?
        ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, id
        LIMIT 1
        ''',
        (location_name, location_name, location_name),
    ).fetchone()
    return int(row['id']) if row else None


def _upsert_inv_material_balance(cursor, location_id, material_lot_id, qty):
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
            (qty, location_id, material_lot_id),
        )
    else:
        cursor.execute(
            '''
            INSERT INTO inv_material_lot_balances (location_id, material_lot_id, qty, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''',
            (location_id, material_lot_id, qty),
        )


def _next_material_lot_seq(cursor, material_id, receiving_date):
    row = cursor.execute(
        '''
        SELECT COALESCE(MAX(lot_seq), 0) AS max_seq
        FROM material_lots
        WHERE material_id = ? AND COALESCE(receiving_date, '') = ?
        ''',
        (material_id, receiving_date),
    ).fetchone()
    return int((row['max_seq'] if row else 0) or 0) + 1


def _get_workplace_material_stock(cursor, material_id, workplace):
    location_id = _get_inv_location_id(cursor, workplace)
    if not location_id:
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
        (location_id, material_id),
    ).fetchone()
    return float((row['qty'] if row else 0) or 0)


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


def _query_inventory_audit_rows(
    cursor,
    selected_inventory_wps,
    inventory_type='all',
    inventory_search_field='all',
    inventory_category='',
    inventory_product_id='',
    inventory_q='',
):
    filters = []
    filter_params = []
    logistics_only = selected_inventory_wps == [LOGISTICS_WORKPLACE]
    if logistics_only:
        filters.append('COALESCE(t.logistics_stock, 0) > 0')
    else:
        filters.append('COALESCE(t.workplace_stock, 0) > 0')
    if selected_inventory_wps:
        placeholders = ','.join(['?'] * len(selected_inventory_wps))
        filters.append(f"COALESCE(t.workplace, '') IN ({placeholders})")
        filter_params.extend(selected_inventory_wps)
    if inventory_type == 'raw':
        filters.append("t.inv_type = 'raw_material'")
    elif inventory_type == 'material':
        filters.append("t.inv_type = 'material'")
    if inventory_category:
        filters.append("COALESCE(t.inv_category, '') = ?")
        filter_params.append(inventory_category)
    if inventory_product_id:
        filters.append(
            '''
            EXISTS (
                SELECT 1
                FROM bom b
                WHERE b.product_id = ?
                  AND (
                      (t.inv_type = 'material' AND b.material_id = t.inv_id)
                      OR
                      (t.inv_type = 'raw_material' AND b.raw_material_id = t.inv_id)
                  )
            )
            '''
        )
        filter_params.append(inventory_product_id)
    if inventory_q:
        like_q = f'%{inventory_q}%'
        if inventory_search_field == 'code':
            filters.append('COALESCE(t.code, "") LIKE ?')
            filter_params.append(like_q)
        elif inventory_search_field == 'name':
            filters.append('COALESCE(t.item_name, "") LIKE ?')
            filter_params.append(like_q)
        elif inventory_search_field == 'category':
            filters.append('COALESCE(t.inv_category, "") LIKE ?')
            filter_params.append(like_q)
        elif inventory_search_field == 'workplace':
            filters.append('COALESCE(t.workplace, "") LIKE ?')
            filter_params.append(like_q)
        else:
            filters.append('(COALESCE(t.code, "") LIKE ? OR COALESCE(t.item_name, "") LIKE ? OR COALESCE(t.inv_category, "") LIKE ? OR COALESCE(t.workplace, "") LIKE ?)')
            filter_params.extend([like_q, like_q, like_q, like_q])

    filter_clause = f"WHERE {' AND '.join(filters)}" if filters else ''

    cursor.execute(
        f'''
        SELECT *
        FROM (
            SELECT
                'raw_material' as inv_type,
                rm.id as inv_id,
                rm.workplace as workplace,
                '\uc6d0\ucd08' as inv_category,
                COALESCE(NULLIF(TRIM(rm.code), ''), printf('RM%05d', rm.id)) as code,
                rm.name as item_name,
                rm.car_number as car_number,
                rm.receiving_date as receiving_date,
                COALESCE(rm.current_stock, 0) as workplace_stock,
                0 as logistics_stock,
                COALESCE(rm.current_stock, 0) as total_stock,
                0 as type_order,
                0 as cat_order
            FROM raw_materials rm

            UNION ALL

            SELECT
                'material' as inv_type,
                m.id as inv_id,
                COALESCE(wp_stock.workplace_name, m.workplace) as workplace,
                COALESCE(NULLIF(TRIM(m.category), ''), '\uae30\ud0c0') as inv_category,
                COALESCE(NULLIF(TRIM(m.code), ''), printf('M%05d', m.id)) as code,
                m.name as item_name,
                '' as car_number,
                '' as receiving_date,
                COALESCE(wp_stock.qty, 0) as workplace_stock,
                0 as logistics_stock,
                COALESCE(wp_stock.qty, 0) as total_stock,
                1 as type_order,
                CASE
                    WHEN COALESCE(m.category, '') = '\ub0b4\ud3ec' THEN 1
                    WHEN COALESCE(m.category, '') = '\uc678\ud3ec' THEN 2
                    WHEN COALESCE(m.category, '') = '\ubc15\uc2a4' THEN 3
                    WHEN COALESCE(m.category, '') = '\uc2e4\ub9ac\uce74' THEN 4
                    WHEN COALESCE(m.category, '') = '\ud2b8\ub808\uc774' THEN 5
                    WHEN COALESCE(m.category, '') = '\uae30\ub984' THEN 6
                    WHEN COALESCE(m.category, '') = '\uc18c\uae08' THEN 7
                    ELSE 99
                END as cat_order
            FROM materials m
            JOIN (
                SELECT
                    ml.material_id,
                    COALESCE(l.name, l.workplace_code) AS workplace_name,
                    COALESCE(SUM(b.qty), 0) AS qty
                FROM inv_material_lot_balances b
                JOIN material_lots ml ON ml.id = b.material_lot_id
                JOIN inv_locations l ON l.id = b.location_id
                WHERE COALESCE(ml.is_disposed, 0) = 0
                  AND COALESCE(l.loc_type, '') = 'WORKPLACE'
                  AND COALESCE(b.qty, 0) > 0
                GROUP BY ml.material_id, COALESCE(l.name, l.workplace_code)
            ) wp_stock
                ON wp_stock.material_id = m.id

            UNION ALL

            SELECT
                'material' as inv_type,
                m.id as inv_id,
                ? as workplace,
                COALESCE(NULLIF(TRIM(m.category), ''), '\uae30\ud0c0') as inv_category,
                COALESCE(NULLIF(TRIM(m.code), ''), printf('M%05d', m.id)) as code,
                m.name as item_name,
                '' as car_number,
                '' as receiving_date,
                0 as workplace_stock,
                COALESCE(log_stock.qty, 0) as logistics_stock,
                COALESCE(log_stock.qty, 0) as total_stock,
                1 as type_order,
                CASE
                    WHEN COALESCE(m.category, '') = '\ub0b4\ud3ec' THEN 1
                    WHEN COALESCE(m.category, '') = '\uc678\ud3ec' THEN 2
                    WHEN COALESCE(m.category, '') = '\ubc15\uc2a4' THEN 3
                    WHEN COALESCE(m.category, '') = '\uc2e4\ub9ac\uce74' THEN 4
                    WHEN COALESCE(m.category, '') = '\ud2b8\ub808\uc774' THEN 5
                    WHEN COALESCE(m.category, '') = '\uae30\ub984' THEN 6
                    WHEN COALESCE(m.category, '') = '\uc18c\uae08' THEN 7
                    ELSE 99
                END as cat_order
            FROM materials m
            JOIN (
                SELECT
                    ml.material_id,
                    COALESCE(SUM(b.qty), 0) AS qty
                FROM inv_material_lot_balances b
                JOIN material_lots ml ON ml.id = b.material_lot_id
                JOIN inv_locations l ON l.id = b.location_id
                WHERE COALESCE(ml.is_disposed, 0) = 0
                  AND COALESCE(b.qty, 0) > 0
                  AND (
                        COALESCE(l.name, '') = '물류창고'
                     OR COALESCE(l.workplace_code, '') IN ('WH', ?)
                     OR COALESCE(l.loc_type, '') = 'WAREHOUSE'
                  )
                GROUP BY ml.material_id
            ) log_stock
                ON log_stock.material_id = m.id
        ) t
        {filter_clause}
        ORDER BY t.type_order ASC, t.code ASC, t.cat_order ASC, t.item_name ASC
        ''',
        [LOGISTICS_WORKPLACE, LOGISTICS_WORKPLACE, *filter_params],
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


@bp.route('/integrated-management')
@login_required
def integrated_management():
    """Auto-generated docstring."""
    if not session['user']['is_admin']:
        return "??????????????源낆┰?????????곸죩", 403

    tab = request.args.get('tab', 'products')  # products, raw_materials, materials, productions, purchase_requests, requirements_calculator, inventory_audit, db_backups
    if tab in ('stats', 'audit_logs'):
        tab = 'products'
    wp_filter = request.args.get('wp', 'all')
    q = request.args.get('q', '').strip()
    selected_product_id = (request.args.get('product_id') or '').strip()
    selected_material_search_field = (request.args.get('material_search_field') or 'all').strip() or 'all'
    selected_material_type = (request.args.get('material_type') or 'all').strip() or 'all'
    selected_material_category = (request.args.get('material_category') or '').strip()
    rm_tab = request.args.get('rm_tab', 'active')
    prod_tab = request.args.get('prod_tab', 'active')
    if prod_tab not in ('active', 'done', 'temp'):
        prod_tab = 'active'
    stat_period = request.args.get('stat_period', 'month')
    if stat_period not in ('month', 'week'):
        stat_period = 'month'
    stat_view = request.args.get('stat_view', 'table')
    if stat_view not in ('table', 'graph'):
        stat_view = 'table'
    stat_anchor = (request.args.get('stat_anchor') or '').strip()
    keep_count = _parse_keep_count(request.args.get('keep_count'))
    inventory_type = (request.args.get('inventory_type') or 'all').strip() or 'all'
    if inventory_type not in ('all', 'raw', 'material'):
        inventory_type = 'all'
    inventory_search_field = (request.args.get('inventory_search_field') or 'all').strip() or 'all'
    inventory_category = (request.args.get('inventory_category') or '').strip()
    inventory_product_id = (request.args.get('inventory_product_id') or '').strip()
    inventory_q = (request.args.get('inventory_q') or '').strip()
    inventory_wp = (request.args.get('inventory_wp') or 'all').strip() or 'all'
    selected_inventory_wps = []
    if inventory_wp != 'all':
        selected_inventory_wps = [inventory_wp]

    conn = get_db()
    cursor = conn.cursor()
    stats = None
    if tab in ('materials', 'purchase_requests'):
        _sync_material_stock_with_lots(conn)

    workplaces = WORKPLACES
    filter_products = []
    calculator_products = []
    production_counts = {'active': 0, 'done': 0, 'temp': 0}
    material_category_options = []

    if tab in ('materials', 'inventory_audit'):
        cursor.execute(
            '''
            SELECT DISTINCT category
            FROM materials
            WHERE category IS NOT NULL AND TRIM(category) <> ''
            ORDER BY category
            '''
        )
        material_category_options = [row['category'] for row in cursor.fetchall() if row['category']]
        material_category_options = [cat for cat in material_category_options if str(cat).strip() not in {'??', '??'}]
        material_category_options = sorted(material_category_options, key=_material_category_sort_key)
        product_wp_filter = wp_filter if tab == 'materials' else 'all'
        if product_wp_filter in ('all', SHARED_WORKPLACE, 'unassigned'):
            cursor.execute(
                '''
                SELECT id, name, workplace
                FROM products
                ORDER BY name
                '''
            )
        else:
            cursor.execute(
                '''
                SELECT id, name, workplace
                FROM products
                WHERE workplace = ?
                ORDER BY name
                ''',
                (product_wp_filter,),
            )
        filter_products = [dict(row) for row in cursor.fetchall()]
    elif tab == 'requirements_calculator':
        cursor.execute(
            '''
            SELECT id, code, name, category, workplace
            FROM products
            ORDER BY workplace, category, name
            '''
        )
        calculator_products = [dict(row) for row in cursor.fetchall()]

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


    elif tab == 'productions':
        query = '''
            SELECT
                pr.id,
                pr.workplace,
                pr.production_date,
                pr.status,
                pr.planned_boxes,
                pr.actual_boxes,
                pr.work_time,
                pr.personnel_note,
                pr.supply_people,
                pr.packing_people,
                pr.outer_packing_people,
                p.code as product_code,
                p.name as product_name,
                EXISTS(
                    SELECT 1
                    FROM production_material_usage pmu
                    WHERE pmu.production_id = pr.id
                      AND (
                          pmu.actual_quantity IS NOT NULL
                          OR pmu.loss_quantity IS NOT NULL
                          OR pmu.yield_rate IS NOT NULL
                      )
                ) as has_usage_save
            FROM productions pr
            LEFT JOIN products p ON pr.product_id = p.id
            WHERE 1=1
        '''
        params = []
        if wp_filter != 'all':
            query += ' AND pr.workplace = ?'
            params.append(wp_filter)
        if q:
            like_q = f'%{q}%'
            query += ' AND (p.name LIKE ? OR p.code LIKE ? OR pr.production_date LIKE ? OR pr.status LIKE ? OR pr.workplace LIKE ? OR CAST(pr.id AS TEXT) LIKE ?)'
            params.extend([like_q, like_q, like_q, like_q, like_q, like_q])
        query += ' ORDER BY pr.production_date DESC, pr.id DESC'
        cursor.execute(query, params)
        all_rows = [dict(row) for row in cursor.fetchall()]

        for row in all_rows:
            status = (row.get('status') or '').strip()
            if status == '완료' or '완료' in status:
                row['list_type'] = 'done'
                row['display_status'] = '생산완료'
            else:
                has_temp_save = bool(row.get('has_usage_save')) or any(
                    row.get(key) not in (None, '', 0, 0.0)
                    for key in ('actual_boxes', 'work_time', 'personnel_note', 'supply_people', 'packing_people', 'outer_packing_people')
                )
                if has_temp_save:
                    row['list_type'] = 'temp'
                    row['display_status'] = '임시저장'
                else:
                    row['list_type'] = 'active'
                    row['display_status'] = '생산중'

        production_counts = {
            'active': sum(1 for row in all_rows if row.get('list_type') == 'active'),
            'done': sum(1 for row in all_rows if row.get('list_type') == 'done'),
            'temp': sum(1 for row in all_rows if row.get('list_type') == 'temp'),
        }
        data = [row for row in all_rows if row.get('list_type') == prod_tab]

    elif tab == 'materials':
        query = '''
            SELECT
                m.*,
                s.name as supplier_name,
                COALESCE(SUM(
                    CASE
                        WHEN COALESCE(ml.current_quantity, ml.quantity, 0) > 0 THEN COALESCE(ml.current_quantity, ml.quantity, 0)
                        ELSE 0
                    END
                ), 0) as lot_total_quantity,
                COALESCE(SUM(
                    CASE
                        WHEN COALESCE(ml.current_quantity, ml.quantity, 0) > 0 THEN 1
                        ELSE 0
                    END
                ), 0) as lot_count
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            LEFT JOIN material_lots ml ON ml.material_id = m.id AND COALESCE(ml.is_disposed, 0) = 0
            WHERE 1=1
        '''
        params = []
        if wp_filter == 'unassigned':
            query += ' AND (m.workplace IS NULL OR m.workplace = "")'
        elif wp_filter == 'all':
            pass
        elif wp_filter == LOGISTICS_WORKPLACE:
            query += ' AND m.workplace = ?'
            params.append(LOGISTICS_WORKPLACE)
        elif wp_filter == SHARED_WORKPLACE:
            query += ' AND m.workplace = ?'
            params.append(SHARED_WORKPLACE)
        else:
            query += ' AND (m.workplace = ? OR m.workplace = ?)'
            params.extend([wp_filter, SHARED_WORKPLACE])
        if selected_material_type == 'raw_like':
            query += " AND COALESCE(m.category, '') IN ('기름', '소금')"
        elif selected_material_type == 'material_only':
            query += " AND COALESCE(m.category, '') NOT IN ('??', '??')"
        if selected_material_category:
            query += ' AND COALESCE(m.category, "") = ?'
            params.append(selected_material_category)
        if q:
            like_q = f'%{q}%'
            if selected_material_search_field == 'code':
                query += ' AND m.code LIKE ?'
                params.append(like_q)
            elif selected_material_search_field == 'name':
                query += ' AND m.name LIKE ?'
                params.append(like_q)
            elif selected_material_search_field == 'supplier':
                query += ' AND COALESCE(s.name, "") LIKE ?'
                params.append(like_q)
            elif selected_material_search_field == 'category':
                query += ' AND COALESCE(m.category, "") LIKE ?'
                params.append(like_q)
            elif selected_material_search_field == 'unit':
                query += ' AND COALESCE(m.unit, "") LIKE ?'
                params.append(like_q)
            else:
                query += ' AND (m.name LIKE ? OR m.code LIKE ? OR m.category LIKE ? OR COALESCE(s.name, "") LIKE ? OR COALESCE(m.unit, "") LIKE ?)'
                params.extend([like_q, like_q, like_q, like_q, like_q])
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
        query += '''
            GROUP BY m.id
            ORDER BY
                CASE COALESCE(m.workplace, '')
                    WHEN '1? ??' THEN 1
                    WHEN '2? ?? 1?' THEN 2
                    WHEN '2? ?? 2?' THEN 3
                    WHEN '1? ??' THEN 4
                    WHEN '??' THEN 5
                    WHEN '??' THEN 6
                    ELSE 99
                END,
                CASE COALESCE(m.category, '')
                    WHEN '??' THEN 1
                    WHEN '??' THEN 2
                    WHEN '??' THEN 3
                    WHEN '??' THEN 4
                    WHEN '??' THEN 5
                    WHEN '???' THEN 6
                    WHEN '???' THEN 7
                    ELSE 99
                END,
                COALESCE(m.code, ''),
                m.name
        '''
        cursor.execute(query, params)
        rows = [dict(row) for row in cursor.fetchall()]
        material_ids = [int(row['id']) for row in rows if row.get('id')]
        workplace_stock_maps = {wp: _get_material_stock_map_for_location(cursor, material_ids, wp) for wp in workplaces}
        data = []
        for item in rows:
            material_id = int(item.get('id') or 0)
            stock_by_workplace = {}
            workplace_total = 0.0
            for wp_name in workplaces:
                qty = round(float(workplace_stock_maps.get(wp_name, {}).get(material_id, 0) or 0), 1)
                stock_by_workplace[wp_name] = qty
                workplace_total += qty
            item['stock_by_workplace'] = stock_by_workplace
            item['workplace_total_stock'] = round(workplace_total, 1)
            item['logistics_stock'] = 0.0
            item['total_stock'] = round(workplace_total, 1)
            item['lot_total_quantity'] = item['total_stock']
            data.append(item)
        data.sort(key=_material_row_sort_key)

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

    elif tab == 'stats':
        data = []
        stats = _query_integrated_stats(cursor, wp_filter, stat_period, stat_anchor)
    elif tab == 'requirements_calculator':
        data = []

    elif tab == 'inventory_audit':
        # 월말 재고 조사용 작업장별 상세 리스트 (현재고 0 초과만)
        data = _query_inventory_audit_rows(
            cursor,
            selected_inventory_wps,
            inventory_type,
            inventory_search_field,
            inventory_category,
            inventory_product_id,
            inventory_q,
        )

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
                           inventory_type=inventory_type,
                           inventory_search_field=inventory_search_field,
                           inventory_category=inventory_category,
                           inventory_product_id=inventory_product_id,
                           inventory_q=inventory_q,
                           inventory_wp=inventory_wp,
                           workplaces=workplaces,
                           suppliers=suppliers,
                           wp_filter=wp_filter,
                           q=q,
                           selected_product_id=selected_product_id,
                           selected_material_search_field=selected_material_search_field,
                           selected_material_type=selected_material_type,
                           selected_material_category=selected_material_category,
                           material_category_options=material_category_options,
                           filter_products=filter_products,
                           calculator_products=calculator_products,
                           can_manage_material_lots=_can_manage_material_lots(),
                           rm_tab=rm_tab,
                           prod_tab=prod_tab,
                           production_counts=production_counts,
                           stats=stats,
                           stat_period=stat_period,
                           stat_view=stat_view,
                           stat_anchor=(stats or {}).get('anchor', stat_anchor),
                           backup_keep_count=keep_count)


@bp.route('/integrated-management/inventory-audit/export')
@admin_required
def integrated_inventory_audit_export():
    inventory_type = (request.args.get('inventory_type') or 'all').strip() or 'all'
    if inventory_type not in ('all', 'raw', 'material'):
        inventory_type = 'all'
    inventory_search_field = (request.args.get('inventory_search_field') or 'all').strip() or 'all'
    inventory_category = (request.args.get('inventory_category') or '').strip()
    inventory_product_id = (request.args.get('inventory_product_id') or '').strip()
    inventory_q = (request.args.get('inventory_q') or '').strip()
    inventory_wp = (request.args.get('inventory_wp') or 'all').strip() or 'all'
    selected_inventory_wps = []
    if inventory_wp != 'all':
        selected_inventory_wps = [inventory_wp]

    conn = get_db()
    cursor = conn.cursor()
    try:
        rows = _query_inventory_audit_rows(
            cursor,
            selected_inventory_wps,
            inventory_type,
            inventory_search_field,
            inventory_category,
            inventory_product_id,
            inventory_q,
        )
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['\uc791\uc5c5\uc7a5', '\uce74\ud14c\uace0\ub9ac', '\ucf54\ub4dc', '\ud488\uba85', '\uc790\ud638', '\uc785\uace0\uc77c', '\uc791\uc5c5\uc7a5 \uc7ac\uace0', '\ucd1d\uc7ac\uace0', '\uc2e4\uc7ac\uace0(\uc791\uc5c5\uc7a5)'])
    for row in rows:
        writer.writerow(
            [
                row['workplace'] or '',
                row['inv_category'] or '',
                row['code'] or '',
                row['item_name'] or '',
                row['car_number'] or '',
                row['receiving_date'] or '',
                f"{float(row['workplace_stock'] or 0):.1f}",
                f"{float(row['total_stock'] or 0):.1f}",
                '',
            ]
        )

    csv_text = output.getvalue()
    output.close()
    csv_bytes = ('\ufeff' + csv_text).encode('utf-8')

    now_token = datetime.now().strftime('%Y%m%d_%H%M%S')
    scope_parts = [inventory_type]
    if selected_inventory_wps:
        scope_parts.append(selected_inventory_wps[0])
    if inventory_category:
        scope_parts.append(inventory_category)
    scope = '_'.join(scope_parts)
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
    username = (session.get('user') or {}).get('username') or (session.get('user') or {}).get('name') or 'system'
    today = datetime.now().strftime('%Y-%m-%d')

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
                    (target['id'], 'adjustment', delta, f'inventory_audit_apply:{code}', username),
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
                    SELECT id, name, workplace, unit, COALESCE(NULLIF(TRIM(code), ''), printf('M%05d', id)) as code,
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

                workplace = (mat['workplace'] or '').strip()
                if not workplace:
                    failed.append({'index': idx, 'inv_id': inv_id, 'message': 'Material workplace is not set.'})
                    continue

                workplace_location_id = _get_inv_location_id(cursor, workplace)
                if not workplace_location_id:
                    failed.append({'index': idx, 'inv_id': inv_id, 'message': 'Workplace inventory location not found.'})
                    continue

                current_total = _get_workplace_material_stock(cursor, inv_id, workplace)
                delta = actual_stock - current_total
                if abs(delta) < 1e-9:
                    cursor.execute('UPDATE materials SET current_stock = ? WHERE id = ?', (actual_stock, inv_id))
                    continue

                if delta > 0:
                    target_lot = cursor.execute(
                        '''
                        SELECT ml.id, ml.lot, COALESCE(ml.quantity, 0) AS quantity, COALESCE(ml.current_quantity, 0) AS current_quantity,
                               COALESCE(ml.received_quantity, 0) AS received_quantity,
                               COALESCE(b.qty, 0) AS location_qty
                        FROM material_lots ml
                        LEFT JOIN inv_material_lot_balances b
                          ON b.material_lot_id = ml.id AND b.location_id = ?
                        WHERE ml.material_id = ?
                          AND COALESCE(ml.is_disposed, 0) = 0
                          AND COALESCE(ml.receiving_date, '') = ?
                        ORDER BY ml.id DESC
                        LIMIT 1
                        ''',
                        (workplace_location_id, inv_id, today),
                    ).fetchone()

                    if target_lot:
                        cursor.execute(
                            '''
                            UPDATE material_lots
                            SET quantity = COALESCE(quantity, 0) + ?,
                                received_quantity = COALESCE(received_quantity, 0) + ?,
                                current_quantity = COALESCE(current_quantity, 0) + ?
                            WHERE id = ?
                            ''',
                            (delta, delta, delta, target_lot['id']),
                        )
                        new_loc_qty = float(target_lot['location_qty'] or 0) + delta
                        _upsert_inv_material_balance(cursor, workplace_location_id, target_lot['id'], new_loc_qty)
                        lot_id = target_lot['id']
                    else:
                        lot_seq = _next_material_lot_seq(cursor, inv_id, today)
                        lot_code = _build_material_lot(mat['code'], today, lot_seq)
                        cursor.execute(
                            '''
                            INSERT INTO material_lots (
                                material_id, lot, receiving_date, unit_price,
                                quantity, lot_seq, received_quantity, current_quantity, supplier_lot
                            ) VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?)
                            ''',
                            (inv_id, lot_code, today, delta, lot_seq, delta, delta, 'inventory_audit'),
                        )
                        lot_id = cursor.lastrowid
                        _upsert_inv_material_balance(cursor, workplace_location_id, lot_id, delta)

                    cursor.execute(
                        '''
                        INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
                        VALUES (?, ?, 'adjustment', ?, ?)
                        ''',
                        (lot_id, inv_id, delta, 'inventory_audit_apply_workplace_plus'),
                    )
                else:
                    decrease = abs(delta)
                    lots = cursor.execute(
                        '''
                        SELECT ml.id, COALESCE(ml.current_quantity, ml.quantity, 0) AS current_quantity, COALESCE(b.qty, 0) AS location_qty
                        FROM inv_material_lot_balances b
                        JOIN material_lots ml ON ml.id = b.material_lot_id
                        WHERE b.location_id = ?
                          AND ml.material_id = ?
                          AND COALESCE(ml.is_disposed, 0) = 0
                          AND COALESCE(b.qty, 0) > 0
                        ORDER BY CASE WHEN ml.receiving_date IS NULL OR TRIM(ml.receiving_date) = '' THEN 1 ELSE 0 END,
                                 ml.receiving_date ASC,
                                 ml.lot_seq ASC,
                                 ml.id ASC
                        ''',
                        (workplace_location_id, inv_id),
                    ).fetchall()
                    available = sum(float(lot['location_qty'] or 0) for lot in lots)
                    if available + 1e-9 < decrease:
                        failed.append({'index': idx, 'inv_id': inv_id, 'message': 'Adjustment exceeds workplace lot stock.'})
                        continue

                    remaining = decrease
                    touched_lot_id = None
                    for lot in lots:
                        if remaining <= 0:
                            break
                        lot_qty = float(lot['location_qty'] or 0)
                        move_qty = min(lot_qty, remaining)
                        new_loc_qty = lot_qty - move_qty
                        new_current_qty = max(float(lot['current_quantity'] or 0) - move_qty, 0)
                        _upsert_inv_material_balance(cursor, workplace_location_id, lot['id'], new_loc_qty)
                        cursor.execute(
                            'UPDATE material_lots SET current_quantity = ? WHERE id = ?',
                            (new_current_qty, lot['id']),
                        )
                        cursor.execute(
                            '''
                            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
                            VALUES (?, ?, 'adjustment', ?, ?)
                            ''',
                            (lot['id'], inv_id, -move_qty, 'inventory_audit_apply_workplace_minus'),
                        )
                        touched_lot_id = lot['id']
                        remaining -= move_qty
                    lot_id = touched_lot_id or inv_id

                cursor.execute('UPDATE materials SET current_stock = ? WHERE id = ?', (actual_stock, inv_id))
                audit_log(
                    conn,
                    'inventory_audit_apply',
                    'material',
                    lot_id,
                    {
                        'code': mat['code'],
                        'workplace': workplace,
                        'actual_stock': actual_stock,
                        'current_total': current_total,
                        'delta': delta,
                        'material_id': inv_id,
                        'applied_to_workplace_lot': True,
                        'audit_date': today,
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
                  AND COALESCE(current_stock, 0) > 0
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
                  AND COALESCE(current_stock, 0) > 0
                ORDER BY
                    CASE WHEN receiving_date IS NULL OR TRIM(receiving_date) = '' THEN 1 ELSE 0 END ASC,
                    receiving_date ASC,
                    id ASC
                ''',
                (base['workplace'], base['name']),
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
                    COALESCE(rml.created_at, '') as log_date,
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
                ORDER BY rml.created_at DESC, rml.id DESC
                LIMIT 200
                ''',
                raw_ids,
            )
            usage_logs = [dict(row) for row in cursor.fetchall()]

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
    wp_filter = request.form.get('wp', 'all')
    q = (request.form.get('q') or '').strip()
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
            return redirect(url_for('admin.integrated_management', tab='materials', wp=wp_filter, q=q))

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

    return redirect(url_for('admin.integrated_management', tab='materials', wp=wp_filter, q=q))


@bp.route('/integrated-management/materials/assign-workplace', methods=['POST'])
@admin_required
def integrated_assign_material_workplace():
    """Auto-generated docstring."""
    material_id = request.form.get('id')
    workplace = request.form.get('workplace')
    wp_filter = (request.form.get('wp') or 'unassigned').strip() or 'unassigned'
    q = (request.form.get('q') or '').strip()
    product_id = (request.form.get('product_id') or '').strip()
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT category, workplace FROM materials WHERE id = ?', (material_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return redirect(url_for('admin.integrated_management', tab='materials', wp=wp_filter, q=q, product_id=product_id or None))
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
    return redirect(url_for('admin.integrated_management', tab='materials', wp=wp_filter, q=q, product_id=product_id or None))


@bp.route('/integrated-management/materials/bulk-assign-workplace', methods=['POST'])
@admin_required
def integrated_bulk_assign_material_workplace():
    target_workplace = (request.form.get('target_workplace') or '').strip()
    material_ids = request.form.getlist('material_ids[]')
    wp_filter = (request.form.get('wp') or 'all').strip() or 'all'
    q = (request.form.get('q') or '').strip()
    product_id = (request.form.get('product_id') or '').strip()

    valid_ids = []
    for raw_id in material_ids:
        try:
            material_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if material_id > 0:
            valid_ids.append(material_id)

    if not target_workplace or not valid_ids:
        return redirect(url_for('admin.integrated_management', tab='materials', wp=wp_filter, q=q, product_id=product_id or None))

    conn = get_db()
    cursor = conn.cursor()
    try:
        placeholders = ','.join(['?'] * len(valid_ids))
        cursor.execute(
            f'''
            SELECT id, category, workplace
            FROM materials
            WHERE id IN ({placeholders})
            ''',
            valid_ids,
        )
        rows = cursor.fetchall()
        for row in rows:
            category = (row['category'] or '').strip()
            resolved_workplace = SHARED_WORKPLACE if category in SHARED_MATERIAL_CATEGORIES else target_workplace
            cursor.execute(
                'UPDATE materials SET workplace = ? WHERE id = ?',
                (resolved_workplace, row['id']),
            )
            audit_log(
                conn,
                'update',
                'material',
                row['id'],
                {
                    'bulk_set_workplace': resolved_workplace,
                    'selected_target_workplace': target_workplace,
                },
            )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='materials', wp=wp_filter, q=q, product_id=product_id or None))


@bp.route('/integrated-management/products/bulk-assign-workplace', methods=['POST'])
@admin_required
def integrated_bulk_assign_product_workplace():
    target_workplace = (request.form.get('target_workplace') or '').strip()
    product_ids = request.form.getlist('product_ids[]')
    wp_filter = (request.form.get('wp') or 'all').strip() or 'all'
    q = (request.form.get('q') or '').strip()

    valid_ids = []
    for raw_id in product_ids:
        try:
            product_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if product_id > 0:
            valid_ids.append(product_id)

    if not target_workplace or not valid_ids:
        return redirect(url_for('admin.integrated_management', tab='products', wp=wp_filter, q=q))

    conn = get_db()
    cursor = conn.cursor()
    try:
        placeholders = ','.join(['?'] * len(valid_ids))
        cursor.execute(
            f'''
            SELECT id, workplace
            FROM products
            WHERE id IN ({placeholders})
            ''',
            valid_ids,
        )
        rows = cursor.fetchall()
        for row in rows:
            cursor.execute(
                'UPDATE products SET workplace = ? WHERE id = ?',
                (target_workplace, row['id']),
            )
            audit_log(
                conn,
                'update',
                'product',
                row['id'],
                {
                    'bulk_set_workplace': target_workplace,
                    'before_workplace': row['workplace'],
                },
            )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='products', wp=wp_filter, q=q))


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

        logistics_stock_deleted = 0
        balance_deleted = 0
        txn_deleted = 0
        lot_log_deleted = 0
        if _table_exists('logistics_stocks'):
            cursor.execute('DELETE FROM logistics_stocks')
            logistics_stock_deleted = cursor.rowcount if cursor.rowcount is not None else 0
        if _table_exists('inv_material_lot_balances'):
            cursor.execute('DELETE FROM inv_material_lot_balances')
            balance_deleted = cursor.rowcount if cursor.rowcount is not None else 0
        if _table_exists('inv_material_txns'):
            cursor.execute('DELETE FROM inv_material_txns')
            txn_deleted = cursor.rowcount if cursor.rowcount is not None else 0
        if _table_exists('material_lot_logs'):
            cursor.execute('DELETE FROM material_lot_logs')
            lot_log_deleted = cursor.rowcount if cursor.rowcount is not None else 0

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
                'logistics_stocks_deleted': logistics_stock_deleted,
                'lot_balances_deleted': balance_deleted,
                'material_txns_deleted': txn_deleted,
                'material_lot_logs_deleted': lot_log_deleted,
                'purchase_requests_deleted': purchase_request_deleted,
                'purchase_orders_deleted': purchase_order_deleted,
                'purchase_order_items_deleted': purchase_order_item_deleted,
            },
        )
        conn.commit()
    except Exception:
        conn.rollback()
        return "<script>alert('?? ??? ? ??? ??????. ?? ??? ???.'); history.back();</script>"
    finally:
        conn.close()

    wp = request.form.get('wp', 'all')
    q = request.form.get('q', '').strip()
    return redirect(url_for('admin.integrated_management', tab='materials', wp=wp, q=q))


@bp.route('/integrated-management/productions/delete-all', methods=['POST'])
@admin_required
def integrated_delete_all_productions():
    wp_filter = request.form.get('wp', 'all')
    q = (request.form.get('q') or '').strip()
    prod_tab = (request.form.get('prod_tab') or 'done').strip()
    if prod_tab not in ('active', 'done', 'temp'):
        prod_tab = 'done'

    conn = get_db()
    cursor = conn.cursor()
    try:
        query = '''
            SELECT
                pr.id,
                pr.workplace,
                pr.production_date,
                pr.status,
                pr.actual_boxes,
                pr.work_time,
                pr.personnel_note,
                pr.supply_people,
                pr.packing_people,
                pr.outer_packing_people,
                p.name as product_name,
                p.code as product_code,
                EXISTS(
                    SELECT 1
                    FROM production_material_usage pmu
                    WHERE pmu.production_id = pr.id
                      AND (
                          pmu.actual_quantity IS NOT NULL
                          OR pmu.loss_quantity IS NOT NULL
                          OR pmu.yield_rate IS NOT NULL
                      )
                ) as has_usage_save
            FROM productions pr
            LEFT JOIN products p ON pr.product_id = p.id
            WHERE 1=1
        '''
        params = []
        if wp_filter != 'all':
            query += ' AND pr.workplace = ?'
            params.append(wp_filter)
        if q:
            like_q = f'%{q}%'
            query += ' AND (p.name LIKE ? OR p.code LIKE ? OR pr.production_date LIKE ? OR pr.status LIKE ? OR pr.workplace LIKE ? OR CAST(pr.id AS TEXT) LIKE ?)'
            params.extend([like_q, like_q, like_q, like_q, like_q, like_q])
        query += ' ORDER BY pr.production_date DESC, pr.id DESC'
        cursor.execute(query, params)
        rows = [dict(row) for row in cursor.fetchall()]

        target_ids = []
        for row in rows:
            status = (row.get('status') or '').strip()
            if status == '완료' or '완료' in status:
                list_type = 'done'
            else:
                has_temp_save = bool(row.get('has_usage_save')) or any(
                    row.get(key) not in (None, '', 0, 0.0)
                    for key in ('actual_boxes', 'work_time', 'personnel_note', 'supply_people', 'packing_people', 'outer_packing_people')
                )
                list_type = 'temp' if has_temp_save else 'active'
            if list_type == prod_tab:
                target_ids.append(int(row['id']))

        for production_id in target_ids:
            _delete_production_record(conn, production_id, session.get('user_id'))

        audit_log(
            conn,
            'delete',
            'production_bulk',
            0,
            {
                'prod_tab': prod_tab,
                'wp_filter': wp_filter,
                'keyword': q,
                'deleted_count': len(target_ids),
            },
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        return f"<script>alert('생산건 전체 삭제 중 오류가 발생했습니다: {str(e)}'); history.back();</script>"
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab='productions', wp=wp_filter, q=q, prod_tab=prod_tab))


@bp.route('/integrated-management/materials/<int:material_id>/detail')
@admin_required
def integrated_material_detail(material_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
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

        can_manage_lots = _can_manage_material_lots()
        allowed_locations = []
        if can_manage_lots:
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
            workplace_name = (material['workplace'] or '').strip()
            workplace_location_id = _get_inv_location_id(cursor, workplace_name) if workplace_name else None
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
                ORDER BY CASE WHEN l.name = '물류창고' THEN 1 ELSE 0 END,
                         CASE WHEN ml.receiving_date IS NULL OR TRIM(ml.receiving_date) = '' THEN 1 ELSE 0 END,
                         ml.receiving_date DESC,
                         ml.lot_seq DESC,
                         ml.id DESC
                """,
                (material_id, *allowed_locations),
            )
            lots = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
SELECT
                COALESCE(p.production_date, substr(pmu.created_at, 1, 10)) as use_date,
                ('PROD-' || pmu.production_id) as production_no,
                COALESCE(prd.name, '-') as product_name,
                COALESCE(ml.lot, '-') as lot,
                COALESCE(pmlu.quantity, pmu.actual_quantity, 0) as used_quantity,
                CASE
                    WHEN pmlu.id IS NOT NULL THEN COALESCE(ml.current_quantity, 0)
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

        cursor.execute(
            """
            SELECT
                COALESCE(ml.receiving_date, substr(mll.created_at, 1, 10)) as receive_date,
                COALESCE(ml.manufacture_date, '-') as manufacture_date,
                COALESCE(ml.expiry_date, '-') as expiry_date,
                COALESCE(mll.quantity, 0) as received_quantity,
                CASE COALESCE(mll.action, '')
                    WHEN 'create' THEN '신규 입고'
                    WHEN 'issue_request_complete' THEN '불출 입고 완료'
                    ELSE COALESCE(mll.action, '-')
                END as action_label,
                COALESCE(mll.note, '-') as note
            FROM material_lot_logs mll
            LEFT JOIN material_lots ml ON ml.id = mll.material_lot_id
            WHERE mll.material_id = ?
              AND COALESCE(mll.action, '') IN ('create', 'issue_request_complete')
              AND COALESCE(mll.quantity, 0) > 0
            ORDER BY
                COALESCE(ml.receiving_date, substr(mll.created_at, 1, 10)) DESC,
                mll.id DESC
            LIMIT 200
            """,
            (material_id,),
        )
        receive_logs = [dict(row) for row in cursor.fetchall()]

        payload = dict(material)
        payload['total_quantity'] = sum(float(row.get('location_quantity') or 0) for row in lots)
        payload['can_manage_lots'] = bool(can_manage_lots)
        return jsonify({'ok': True, 'material': payload, 'lots': lots, 'usage_logs': usage_logs, 'receive_logs': receive_logs})
    finally:
        conn.close()


@bp.route('/integrated-management/material-lots/add', methods=['POST'])
@login_required
def integrated_add_material_lot():
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

    conn = get_db()
    cursor = conn.cursor()
    try:
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
        cursor.execute('UPDATE materials SET current_stock = current_stock + ?, unit_price = ? WHERE id = ?', (current_quantity, unit_price, material_id))

        current_workplace = (session.get('workplace') or '').strip()
        target_location_name = '\ubb3c\ub958\ucc3d\uace0' if current_workplace == LOGISTICS_WORKPLACE else (current_workplace or (material['workplace'] or '').strip())
        target_location_id = _get_inv_location_id(cursor, target_location_name)
        if target_location_id:
            _upsert_inv_material_balance(cursor, target_location_id, lot_id, current_quantity)

        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'create', ?, ?)
        ''',
            (lot_id, material_id, current_quantity, lot),
        )
        audit_log(conn, 'create', 'material_lot', lot_id, {'material_id': material_id, 'lot': lot, 'received_quantity': received_quantity, 'current_quantity': current_quantity, 'supplier_lot': supplier_lot})
        conn.commit()
        return jsonify({'ok': True, 'lot': lot, 'lot_id': lot_id})
    except Exception:
        conn.rollback()
        return jsonify({'ok': False, 'message': '?? ?? ? ??? ??????.'}), 500
    finally:
        conn.close()

@bp.route('/integrated-management/material-lots/<int:lot_id>/update', methods=['POST'])
@login_required
def integrated_update_material_lot(lot_id):
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

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM material_lots WHERE id = ?', (lot_id,))
        before = cursor.fetchone()
        if not before:
            return jsonify({'ok': False, 'message': '\ub85c\ud2b8\ub97c \ucc3e\uc744 \uc218 \uc5c6\uc2b5\ub2c8\ub2e4.'}), 404

        cursor.execute('SELECT code FROM materials WHERE id = ?', (before['material_id'],))
        material = cursor.fetchone()
        lot = _build_material_lot(material['code'] if material else '', receiving_date, before['lot_seq'])

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
                _upsert_inv_material_balance(cursor, location_id_int, lot_id, current_quantity)

        cursor.execute(
            'SELECT COALESCE(SUM(qty), 0) AS total_qty FROM inv_material_lot_balances WHERE material_lot_id = ?',
            (lot_id,),
        )
        total_row = cursor.fetchone()
        lot_total = _round_to_1_decimal((total_row['total_qty'] if total_row else 0) or 0)
        cursor.execute('UPDATE material_lots SET current_quantity = ?, quantity = ? WHERE id = ?', (lot_total, lot_total, lot_id))
        cursor.execute('UPDATE materials SET current_stock = current_stock + ?, unit_price = ? WHERE id = ?', (lot_total - previous_lot_quantity, unit_price, before['material_id']))
        cursor.execute(
            '''
            INSERT INTO material_lot_logs (material_lot_id, material_id, action, quantity, note)
            VALUES (?, ?, 'update', ?, ?)
        ''',
            (lot_id, before['material_id'], lot_total, lot),
        )
        audit_log(conn, 'update', 'material_lot', lot_id, {'before': dict(before), 'after': {'lot': lot, 'received_quantity': received_quantity, 'current_quantity': lot_total, 'supplier_lot': supplier_lot}})
        conn.commit()
        return jsonify({'ok': True, 'lot': lot})
    except Exception as e:
        conn.rollback()
        return jsonify({'ok': False, 'message': f'\ub85c\ud2b8 \uc218\uc815 \uc911 \uc624\ub958\uac00 \ubc1c\uc0dd\ud588\uc2b5\ub2c8\ub2e4: {e}'}), 500
    finally:
        conn.close()


@bp.route('/integrated-management/material-lots/<int:lot_id>/delete', methods=['POST'])
@login_required
def integrated_delete_material_lot(lot_id):
    if not _can_manage_material_lots():
        return jsonify({'ok': False, 'message': '로트 관리 권한이 없습니다.'}), 403
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
    category = (request.form.get('category') or '기타').strip() or '기타'
    sheets_per_pack = request.form.get('sheets_per_pack')
    cuts_per_sheet = request.form.get('cuts_per_sheet')
    sok_per_box = _round_to_1_decimal(request.form.get('sok_per_box') or 0)
    expiry_months = request.form.get('expiry_months', 12)

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
            SET workplace = ?, name = ?, code = ?, description = ?, box_quantity = ?,
                category = ?, sheets_per_pack = ?, cuts_per_sheet = ?, sok_per_box = ?, expiry_months = ?
            WHERE id = ?
        ''',
            (
                workplace,
                name,
                code,
                description,
                box_quantity,
                category,
                sheets_per_pack,
                cuts_per_sheet,
                sok_per_box,
                expiry_months,
                product_id,
            ),
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


@bp.route('/integrated-management/requirements-calculator-data', methods=['POST'])
@admin_required
def integrated_requirements_calculator_data():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items') or []
    selected_categories = payload.get('categories') or []

    conn = get_db()
    cursor = conn.cursor()
    try:
        result = _build_integrated_requirement_payload(cursor, items)
        return jsonify(_filter_integrated_requirement_payload(result, selected_categories))
    finally:
        conn.close()


@bp.route('/integrated-management/requirements-calculator-export', methods=['POST'])
@admin_required
def integrated_requirements_calculator_export():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items') or []
    selected_categories = payload.get('categories') or []
    mode = 'products' if (payload.get('mode') or '').strip() == 'products' else 'summary'

    conn = get_db()
    cursor = conn.cursor()
    try:
        result = _build_integrated_requirement_payload(cursor, items)
        result = _filter_integrated_requirement_payload(result, selected_categories)
    finally:
        conn.close()

    headers = [
        '구분',
        '상품코드',
        '상품명',
        '작업장',
        '생산 예상 박스수',
        '자재대분류',
        '자재세부분류',
        '자재코드',
        '자재명',
        '단위',
        '현재고',
        '필요량',
        '부족량',
    ]
    rows = _build_requirement_export_rows(result, mode)
    workbook = _build_simple_xlsx('원부자재계산기', headers, rows)
    now_token = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'requirements_calculator_{mode}_{now_token}.xlsx'
    return send_file(
        workbook,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )


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


# 통합관리 공통 삭제 라우트
@bp.route('/integrated-management/<table>/<int:item_id>/delete', methods=['POST'])
@login_required
def integrated_delete_item(table, item_id):
    """통합관리 항목을 삭제한다."""
    if not session['user']['is_admin']:
        return "관리자만 접근할 수 있습니다.", 403

    allowed_tables = ['products', 'raw_materials', 'materials', 'purchase_requests']
    if table not in allowed_tables:
        return "삭제할 수 없는 대상입니다.", 400

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
        return f"삭제 중 오류가 발생했습니다: {str(e)}", 400
    finally:
        conn.close()

    return redirect(url_for('admin.integrated_management', tab=table))
