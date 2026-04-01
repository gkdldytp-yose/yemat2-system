import shutil
import sqlite3
import sys
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "yemat.db"
XLSX_PATH = ROOT.parent / "stock_import.xlsx"
TODAY = date.today().isoformat()
TODAY_TOKEN = TODAY.replace("-", "")
WORKPLACE = "물류"

NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}


def load_workbook_rows(path: Path):
    with zipfile.ZipFile(path) as zf:
        shared = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", NS):
                shared.append("".join(node.text or "" for node in si.findall(".//a:t", NS)))

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}

        sheets = []
        for sheet in workbook.find("a:sheets", NS):
            rid = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            target = "xl/" + rel_map[rid]
            root = ET.fromstring(zf.read(target))
            rows = []
            for row in root.findall(".//a:sheetData/a:row", NS):
                values = []
                for cell in row.findall("a:c", NS):
                    cell_type = cell.attrib.get("t")
                    value_node = cell.find("a:v", NS)
                    inline_node = cell.find("a:is", NS)
                    if inline_node is not None:
                        values.append("".join(node.text or "" for node in inline_node.findall(".//a:t", NS)))
                    elif value_node is None:
                        values.append("")
                    elif cell_type == "s":
                        values.append(shared[int(value_node.text)])
                    else:
                        values.append(value_node.text)
                rows.append(values)
            sheets.append((sheet.attrib.get("name", ""), rows))
        return sheets


def clean_text(value):
    return (value or "").strip()


def parse_decimal(value):
    raw = clean_text(value).replace(",", "")
    if not raw:
        return Decimal("0")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return Decimal("0")


def read_stock_rows(path: Path):
    stock_by_code = defaultdict(Decimal)
    meta_by_code = {}
    for sheet_name, rows in load_workbook_rows(path):
        if not rows:
            continue
        header = [clean_text(x) for x in rows[0]]
        code_idx = next((i for i, name in enumerate(header) if name == "코드"), None)
        supplier_idx = next((i for i, name in enumerate(header) if name == "업체명"), None)
        name_idx = next((i for i, name in enumerate(header) if name == "제품명"), None)
        name_code_idx = next((i for i, name in enumerate(header) if name == "이름 코드"), None)
        category_idx = next((i for i, name in enumerate(header) if name == "구분"), None)
        stock_idx = next((i for i, name in enumerate(header) if name == "현재고"), None)
        if code_idx is None or stock_idx is None:
            continue

        for row in rows[1:]:
            code = clean_text(row[code_idx] if code_idx < len(row) else "")
            if not code:
                continue
            stock_by_code[code] += parse_decimal(row[stock_idx] if stock_idx < len(row) else "")
            meta_by_code.setdefault(
                code,
                {
                    "sheet": sheet_name,
                    "supplier_name": clean_text(row[supplier_idx] if supplier_idx is not None and supplier_idx < len(row) else ""),
                    "name": clean_text(row[name_idx] if name_idx is not None and name_idx < len(row) else ""),
                    "name_code": clean_text(row[name_code_idx] if name_code_idx is not None and name_code_idx < len(row) else ""),
                    "category": clean_text(row[category_idx] if category_idx is not None and category_idx < len(row) else ""),
                },
            )
    return stock_by_code, meta_by_code


def infer_unit(code, category, name):
    code = clean_text(code).upper()
    category = clean_text(category)
    if code.startswith("O") or category == "기름":
        return "kg"
    if code.startswith("B") or category == "소금":
        return "kg"
    if code.startswith("P01") or category == "내포":
        return "롤"
    if code.startswith("P02") or category == "외포":
        return "롤"
    return "ea"


def ensure_supplier(cur, supplier_name):
    name = clean_text(supplier_name)
    if not name:
        return None
    row = cur.execute("SELECT id FROM suppliers WHERE name = ? LIMIT 1", (name,)).fetchone()
    if row:
        return int(row["id"])
    cur.execute(
        "INSERT INTO suppliers (code, name, contact, address, note) VALUES (?, ?, '', '', '')",
        (None, name),
    )
    supplier_id = int(cur.lastrowid)
    cur.execute("UPDATE suppliers SET code = ? WHERE id = ?", (f"SUP{supplier_id:04d}", supplier_id))
    return supplier_id


def get_logistics_location_id(cur):
    row = cur.execute(
        """
        SELECT id
        FROM inv_locations
        WHERE name = ?
           OR (loc_type = 'WAREHOUSE' AND COALESCE(workplace_code, '') = 'WH')
        ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, id
        LIMIT 1
        """,
        ("물류창고", "물류창고"),
    ).fetchone()
    return int(row["id"]) if row else None


def next_lot_seq(cur, material_id, receiving_date):
    row = cur.execute(
        "SELECT COALESCE(MAX(lot_seq), 0) + 1 AS next_seq FROM material_lots WHERE material_id = ? AND receiving_date = ?",
        (material_id, receiving_date),
    ).fetchone()
    return int(row["next_seq"] or 1) if row else 1


def build_lot(code, lot_seq):
    return f"{clean_text(code) or 'NO_CODE'}-{TODAY_TOKEN}-{int(lot_seq):03d}"


def backup_database():
    backup_path = ROOT / f"yemat_backup_before_logistics_material_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copy2(DB_PATH, backup_path)
    return backup_path


def upsert_material(cur, code, meta, qty):
    supplier_id = ensure_supplier(cur, meta.get("supplier_name"))
    unit = infer_unit(code, meta.get("category"), meta.get("name"))
    row = cur.execute("SELECT id FROM materials WHERE code = ? LIMIT 1", (code,)).fetchone()
    if row:
        material_id = int(row["id"])
        cur.execute(
            """
            UPDATE materials
            SET supplier_id = ?, name = ?, category = ?, spec = ?, unit = ?, current_stock = ?, workplace = ?
            WHERE id = ?
            """,
            (
                supplier_id,
                clean_text(meta.get("name")) or code,
                clean_text(meta.get("category")),
                clean_text(meta.get("name_code")),
                unit,
                qty,
                WORKPLACE,
                material_id,
            ),
        )
        return material_id, False
    cur.execute(
        """
        INSERT INTO materials (
            supplier_id, code, name, category, spec, unit, moq, lead_time,
            unit_price, current_stock, min_stock, workplace
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            supplier_id,
            code,
            clean_text(meta.get("name")) or code,
            clean_text(meta.get("category")),
            clean_text(meta.get("name_code")),
            unit,
            0,
            0,
            0,
            qty,
            0,
            WORKPLACE,
        ),
    )
    return int(cur.lastrowid), True


def replace_material_stock_with_logistics(cur, material_id, code, name, unit, qty, logistics_location_id):
    active_lots = cur.execute(
        """
        SELECT id
        FROM material_lots
        WHERE material_id = ?
          AND COALESCE(is_disposed, 0) = 0
        """,
        (material_id,),
    ).fetchall()
    if active_lots:
        lot_ids = [int(row["id"]) for row in active_lots]
        cur.execute(
            f"""
            UPDATE material_lots
            SET is_disposed = 1,
                disposed_at = CURRENT_TIMESTAMP,
                current_quantity = 0,
                quantity = 0
            WHERE id IN ({",".join("?" for _ in lot_ids)})
            """,
            lot_ids,
        )
        cur.execute(
            f"DELETE FROM inv_material_lot_balances WHERE material_lot_id IN ({','.join('?' for _ in lot_ids)})",
            lot_ids,
        )

    cur.execute("DELETE FROM logistics_stocks WHERE material_code = ?", (code,))
    cur.execute(
        """
        INSERT INTO logistics_stocks (material_code, material_name, unit, quantity, updated_by)
        VALUES (?, ?, ?, ?, ?)
        """,
        (code, name, unit, qty, "excel_import"),
    )

    lot_seq = next_lot_seq(cur, material_id, TODAY)
    lot = build_lot(code, lot_seq)
    cur.execute(
        """
        INSERT INTO material_lots (
            material_id, lot, receiving_date, manufacture_date, expiry_date,
            unit_price, quantity, lot_seq, received_quantity, current_quantity,
            supplier_lot, is_disposed
        )
        VALUES (?, ?, ?, NULL, NULL, 0, ?, ?, ?, ?, NULL, 0)
        """,
        (material_id, lot, TODAY, qty, lot_seq, qty, qty),
    )
    lot_id = int(cur.lastrowid)
    if logistics_location_id is not None:
        cur.execute(
            """
            INSERT INTO inv_material_lot_balances (location_id, material_lot_id, qty, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (logistics_location_id, lot_id, qty),
        )


def run_import():
    if not XLSX_PATH.exists():
        print(f"Excel not found: {XLSX_PATH}")
        return 1
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return 1

    stock_by_code, meta_by_code = read_stock_rows(XLSX_PATH)
    backup_path = backup_database()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    logistics_location_id = get_logistics_location_id(cur)

    created_materials = 0
    updated_materials = 0
    created_lots = 0

    try:
        for code in sorted(stock_by_code.keys()):
            qty = float(stock_by_code[code])
            meta = meta_by_code.get(code, {})
            material_id, created = upsert_material(cur, code, meta, qty)
            if created:
                created_materials += 1
            else:
                updated_materials += 1
            row = cur.execute("SELECT code, name, unit FROM materials WHERE id = ?", (material_id,)).fetchone()
            replace_material_stock_with_logistics(
                cur,
                material_id,
                clean_text(row["code"]),
                clean_text(row["name"]),
                clean_text(row["unit"]),
                qty,
                logistics_location_id,
            )
            created_lots += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"backup={backup_path}")
    print(f"created_materials={created_materials}")
    print(f"updated_materials={updated_materials}")
    print(f"created_lots={created_lots}")
    print(f"codes={len(stock_by_code)}")
    return 0


if __name__ == "__main__":
    sys.exit(run_import())
