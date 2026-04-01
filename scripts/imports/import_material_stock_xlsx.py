import shutil
import sqlite3
import sys
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "yemat.db"
XLSX_PATH = ROOT.parent / "현재고DB.xlsx"
TODAY = date.today().isoformat()
TODAY_TOKEN = TODAY.replace("-", "")

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
            name = sheet.attrib.get("name", "")
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
            sheets.append((name, rows))
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


def read_stock_by_code(path: Path):
    stock_by_code = defaultdict(Decimal)
    row_samples = {}
    for sheet_name, rows in load_workbook_rows(path):
        if not rows:
            continue
        header = [clean_text(x) for x in rows[0]]
        code_idx = next((i for i, name in enumerate(header) if name == "코드"), None)
        supplier_idx = next((i for i, name in enumerate(header) if name == "업체명"), None)
        name_idx = next((i for i, name in enumerate(header) if name == "제품명"), None)
        stock_idx = next((i for i, name in enumerate(header) if name == "현재고"), None)
        category_idx = next((i for i, name in enumerate(header) if name == "구분"), None)
        if code_idx is None or stock_idx is None:
            continue
        for row in rows[1:]:
            code = clean_text(row[code_idx] if code_idx < len(row) else "")
            if not code:
                continue
            qty = parse_decimal(row[stock_idx] if stock_idx < len(row) else "")
            stock_by_code[code] += qty
            row_samples.setdefault(
                code,
                {
                    "sheet": sheet_name,
                    "qty": qty,
                    "supplier": clean_text(row[supplier_idx] if supplier_idx is not None and supplier_idx < len(row) else ""),
                    "name": clean_text(row[name_idx] if name_idx is not None and name_idx < len(row) else ""),
                    "category": clean_text(row[category_idx] if category_idx is not None and category_idx < len(row) else ""),
                },
            )
    return stock_by_code, row_samples


def next_lot_seq(cur, material_id, receiving_date):
    row = cur.execute(
        "SELECT COALESCE(MAX(lot_seq), 0) + 1 AS next_seq FROM material_lots WHERE material_id = ? AND receiving_date = ?",
        (material_id, receiving_date),
    ).fetchone()
    return int(row["next_seq"] or 1) if row else 1


def build_lot(code, lot_seq):
    material_code = clean_text(code) or "NO_CODE"
    return f"{material_code}-{TODAY_TOKEN}-{int(lot_seq):03d}"


def get_logistics_location_id(cur):
    row = cur.execute(
        """
        SELECT id
        FROM inv_locations
        WHERE name = '물류창고'
           OR (loc_type = 'WAREHOUSE' AND COALESCE(workplace_code, '') = 'WH')
        ORDER BY CASE WHEN name = '물류창고' THEN 0 ELSE 1 END, id
        LIMIT 1
        """
    ).fetchone()
    return int(row["id"]) if row else None


def infer_unit(category):
    mapping = {
        "내포": "롤",
        "외포": "롤",
        "박스": "ea",
        "트레이": "ea",
        "실리카": "ea",
        "강판": "ea",
        "스티커": "ea",
        "기름": "kg",
        "소금": "kg",
    }
    return mapping.get(category, "ea")


def infer_workplace(code, name, category):
    text = clean_text(name)
    if code.startswith("Z10"):
        return "공통"
    if "자반" in text:
        return "1동 자반"
    if "미니재래식탁12g" in text or "감태식탁" in text:
        return "2동 신관 2층"
    if any(token in text for token in ["도시락", "전장", "김밥김", "30매"]):
        return "2동 신관 1층"
    if "식탁" in text:
        return "1동 조미"
    if category in {"내포", "외포", "박스"}:
        return "2동 신관 1층"
    return "공통"


def ensure_material_exists(cur, code, meta):
    row = cur.execute("SELECT id FROM materials WHERE code = ?", (code,)).fetchone()
    if row:
        return False
    category = clean_text(meta.get("category"))
    name = clean_text(meta.get("name")) or code
    workplace = infer_workplace(code, name, category)
    unit = infer_unit(category)
    cur.execute(
        """
        INSERT INTO materials (
            supplier_id, code, name, category, spec, unit, moq, lead_time,
            unit_price, current_stock, min_stock, workplace
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            None,
            code,
            name,
            category,
            clean_text(meta.get("supplier")),
            unit,
            0,
            0,
            0,
            0,
            0,
            workplace,
        ),
    )
    return True


def backup_database():
    backup_path = ROOT / f"yemat_backup_before_stock_import_{TODAY_TOKEN}.db"
    if not backup_path.exists():
        shutil.copy2(DB_PATH, backup_path)
    return backup_path


def sync_stock():
    if not XLSX_PATH.exists():
        print(f"Excel not found: {XLSX_PATH}")
        return 1
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return 1

    stock_by_code, row_samples = read_stock_by_code(XLSX_PATH)
    backup_path = backup_database()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    logistics_location_id = get_logistics_location_id(cur)
    matched = 0
    created_lots = 0
    disposed_lots = 0
    created_materials = 0
    skipped = []

    try:
        for code, meta in sorted(row_samples.items()):
            if ensure_material_exists(cur, code, meta):
                created_materials += 1

        for code, qty_decimal in sorted(stock_by_code.items()):
            material = cur.execute(
                "SELECT id, code, name, unit FROM materials WHERE code = ?",
                (code,),
            ).fetchone()
            if not material:
                skipped.append((code, qty_decimal))
                continue

            qty = float(qty_decimal)
            material_id = int(material["id"])

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
                disposed_lots += len(lot_ids)
                cur.execute(
                    f"DELETE FROM inv_material_lot_balances WHERE material_lot_id IN ({','.join('?' for _ in lot_ids)})",
                    lot_ids,
                )

            lot_seq = next_lot_seq(cur, material_id, TODAY)
            lot = build_lot(material["code"], lot_seq)
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
            created_lots += 1

            cur.execute(
                "UPDATE materials SET current_stock = ? WHERE id = ?",
                (qty, material_id),
            )

            if logistics_location_id is not None:
                cur.execute(
                    """
                    INSERT INTO inv_material_lot_balances (location_id, material_lot_id, qty, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (logistics_location_id, lot_id, qty),
                )

            matched += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"backup={backup_path}")
    print(f"created_materials={created_materials}")
    print(f"matched={matched}")
    print(f"created_lots={created_lots}")
    print(f"disposed_lots={disposed_lots}")
    print(f"skipped={len(skipped)}")
    for code, qty in skipped[:50]:
        print(f"skip {code} qty={qty}")
    return 0


if __name__ == "__main__":
    sys.exit(sync_stock())
