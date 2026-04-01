import re
import shutil
import sqlite3
import sys
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "yemat.db"
XLSX_PATH = ROOT.parent / "bom_import.xlsx"

NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}

COMMON_MATERIALS = {
    "참기름": {"code": "O01", "name": "참기름_조미", "category": "기름", "unit": "kg", "workplace": "물류"},
    "해바라기": {"code": "O04", "name": "고올레산 해바라기유", "category": "기름", "unit": "kg", "workplace": "물류"},
    "유기해바라기": {"code": "O05", "name": "유기농 고올레산 해바라기유", "category": "기름", "unit": "kg", "workplace": "물류"},
    "옥배유": {"code": "O06", "name": "옥배유", "category": "기름", "unit": "kg", "workplace": "물류"},
    "올리브유": {"code": "O07", "name": "혼합 올리브유", "category": "기름", "unit": "kg", "workplace": "물류"},
    "포도씨유": {"code": "O08", "name": "포도씨유", "category": "기름", "unit": "kg", "workplace": "물류"},
    "정제소금": {"code": "B01", "name": "정제소금(한주)", "category": "소금", "unit": "kg", "workplace": "물류"},
    "맛소금": {"code": "B03", "name": "맛소금", "category": "소금", "unit": "kg", "workplace": "물류"},
    "천일염": {"code": "B05", "name": "천일염", "category": "소금", "unit": "kg", "workplace": "물류"},
    "식탁(중) 트레이": {"code": "T02", "name": "트레이 식탁(중)", "category": "트레이", "unit": "ea", "workplace": "물류"},
    "도시락 43mm": {"code": "T04", "name": "트레이 도시락43mm", "category": "트레이", "unit": "ea", "workplace": "물류"},
    "식탁(대)트레이": {"code": "T01", "name": "트레이 식탁(대)", "category": "트레이", "unit": "ea", "workplace": "물류"},
    "군함말이 트레이": {"code": "T03", "name": "군함말이 트레이", "category": "트레이", "unit": "ea", "workplace": "물류"},
    "1g 줄": {"code": "S10", "name": "실리카겔 1g 줄", "category": "실리카", "unit": "ea", "workplace": "물류"},
    "4g 줄": {"code": "S40", "name": "실리카겔 4g 줄", "category": "실리카", "unit": "ea", "workplace": "물류"},
    "4g 컷": {"code": "S41", "name": "4g 컷", "category": "실리카", "unit": "ea", "workplace": "물류"},
    "앵글1240(앵글1)": {"code": "Z01A010", "name": "앵글1240(앵글1)", "category": "앵글", "unit": "ea", "workplace": "물류"},
    "앵글1150(앵글2)": {"code": "Z01A002", "name": "앵글1150(앵글2)", "category": "앵글", "unit": "ea", "workplace": "물류"},
}

PRODUCT_SPEC_HEADERS = {
    "내포": {"category": "내포", "unit": "롤", "prefix": "P01"},
    "외포": {"category": "외포", "unit": "롤", "prefix": "P02"},
    "박스": {"category": "박스", "unit": "ea", "prefix": "P03"},
    "뚜껑": {"category": "뚜껑", "unit": "ea", "prefix": "P04"},
    "파우치": {"category": "파우치", "unit": "ea", "prefix": "P05"},
}

RAW_CODE_RULES = [
    ("감태", "A04"),
    ("곱창", "A05"),
    ("유기", "A02"),
    ("김밥", "A01"),
]


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
            yield sheet.attrib.get("name", ""), rows


def clean_text(value):
    return (value or "").replace("\xa0", " ").strip()


def parse_qty(value):
    raw = clean_text(value).replace(",", "")
    if not raw or raw == "#REF!":
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def normalize_text(value):
    text = clean_text(value).lower()
    text = text.replace("&", " ")
    text = re.sub(r"[()*_/\\-]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize(value):
    text = normalize_text(value)
    return [token for token in re.split(r"[^0-9a-z가-힣]+", text) if token]


def score_material_match(product_name, header, material_name):
    p_tokens = tokenize(product_name)
    m_tokens = tokenize(material_name)
    score = 0
    for token in p_tokens:
        if token in m_tokens:
            score += 4 if any(ch.isdigit() for ch in token) else 2
        elif any(token in mt or mt in token for mt in m_tokens):
            score += 1
    header_key = normalize_text(header)
    material_key = normalize_text(material_name)
    if header_key and header_key in material_key:
        score += 3
    for special in ["대만", "일본", "중국", "국내", "미국", "캐나다", "필리핀", "유기", "감태", "곱창", "김밥", "광푸웬", "수니쿡", "해피", "니코니코", "예맛"]:
        if special in product_name and special in material_name:
            score += 3
    return score


def backup_database():
    backup_path = ROOT / f"yemat_backup_before_bom_reimport_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copy2(DB_PATH, backup_path)
    return backup_path


def ensure_material(cur, meta):
    row = cur.execute("SELECT id FROM materials WHERE code = ?", (meta["code"],)).fetchone()
    if row:
        cur.execute(
            """
            UPDATE materials
            SET name = ?, category = ?, unit = ?, workplace = ?
            WHERE id = ?
            """,
            (meta["name"], meta["category"], meta["unit"], meta["workplace"], int(row["id"])),
        )
        return int(row["id"]), False
    cur.execute(
        """
        INSERT INTO materials (
            supplier_id, code, name, category, spec, unit, moq, lead_time,
            unit_price, current_stock, min_stock, workplace
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (None, meta["code"], meta["name"], meta["category"], "", meta["unit"], 0, 0, 0, 0, 0, meta["workplace"]),
    )
    return int(cur.lastrowid), True


def ensure_imp_material(cur, product_name, header, header_meta, workplace):
    name = f"{clean_text(product_name)} {header}"
    row = cur.execute(
        "SELECT id, code FROM materials WHERE name = ? LIMIT 1",
        (name,),
    ).fetchone()
    if row:
        cur.execute(
            "UPDATE materials SET category = ?, unit = ?, workplace = ? WHERE id = ?",
            (header_meta["category"], header_meta["unit"], workplace, int(row["id"])),
        )
        return int(row["id"]), row["code"], False
    row = cur.execute("SELECT code FROM materials WHERE code LIKE 'IMP%' ORDER BY code DESC LIMIT 1").fetchone()
    next_num = int((row["code"] or "IMP00000")[3:]) + 1 if row and row["code"] else 1
    code = f"IMP{next_num:05d}"
    cur.execute(
        """
        INSERT INTO materials (
            supplier_id, code, name, category, spec, unit, moq, lead_time,
            unit_price, current_stock, min_stock, workplace
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (None, code, name, header_meta["category"], "", header_meta["unit"], 0, 0, 0, 0, 0, workplace),
    )
    return int(cur.lastrowid), code, True


def choose_raw_material_id(cur, product_name):
    raw_rows = cur.execute("SELECT id, code, name FROM raw_materials ORDER BY id").fetchall()
    name = clean_text(product_name)
    chosen_code = "A01"
    for keyword, code in RAW_CODE_RULES:
        if keyword in name:
            chosen_code = code
            break
    for row in raw_rows:
        if clean_text(row["code"]) == chosen_code:
            return int(row["id"])
    return int(raw_rows[0]["id"]) if raw_rows else None


def product_specific_candidate(cur, category):
    return [
        dict(row)
        for row in cur.execute(
            "SELECT id, code, name, workplace FROM materials WHERE category = ? ORDER BY code, id",
            (category,),
        ).fetchall()
    ]


def find_or_create_product_material(cur, product_name, header, workplace):
    header_meta = PRODUCT_SPEC_HEADERS[header]
    candidates = product_specific_candidate(cur, header_meta["category"])
    best = None
    best_score = -1
    for item in candidates:
        score = score_material_match(product_name, header, item["name"])
        if score > best_score:
            best_score = score
            best = item
    if best and best_score >= 5:
        return int(best["id"]), best["code"], False
    return ensure_imp_material(cur, product_name, header, header_meta, workplace)


def choose_common_material(header, product_name):
    if header == "해바라기" and "유기" in clean_text(product_name):
        return COMMON_MATERIALS["유기해바라기"]
    return COMMON_MATERIALS[header]


def run_import():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return 1
    if not XLSX_PATH.exists():
        print(f"Excel not found: {XLSX_PATH}")
        return 1

    backup_path = backup_database()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    created_materials = 0
    bom_rows_added = 0
    skipped_products = []
    product_workplace_map = {}
    material_workplaces = defaultdict(set)

    # Clear BOM for full reimport.
    cur.execute("DELETE FROM bom")

    products = {
        clean_text(row["code"]): dict(row)
        for row in cur.execute("SELECT id, code, name, workplace FROM products WHERE code IS NOT NULL AND TRIM(code) <> ''").fetchall()
    }

    header = None
    for _sheet_name, rows in load_workbook_rows(XLSX_PATH):
        if not rows:
            continue
        header = [clean_text(x) for x in rows[0]]
        for row in rows[1:]:
            product_code = clean_text(row[0] if len(row) > 0 else "")
            product_name = clean_text(row[1] if len(row) > 1 else "")
            if not product_code:
                continue
            product = products.get(product_code)
            if not product:
                skipped_products.append(product_code)
                continue
            product_id = int(product["id"])
            workplace = clean_text(product["workplace"])
            product_workplace_map[product_id] = workplace

            # Raw material from sok_per_box.
            sok_per_box = parse_qty(row[5] if len(row) > 5 else "")
            raw_material_id = choose_raw_material_id(cur, product_name)
            if raw_material_id and sok_per_box > 0:
                cur.execute(
                    """
                    INSERT INTO bom (product_id, material_id, raw_material_id, quantity_per_box, sok_per_box)
                    VALUES (?, NULL, ?, ?, ?)
                    """,
                    (product_id, raw_material_id, sok_per_box, sok_per_box),
                )
                bom_rows_added += 1

            for idx, col_name in enumerate(header[6:], start=6):
                qty = parse_qty(row[idx] if idx < len(row) else "")
                if qty <= 0:
                    continue

                material_id = None
                material_code = None
                created = False

                if col_name in COMMON_MATERIALS:
                    common_meta = choose_common_material(col_name, product_name)
                    material_id, created = ensure_material(cur, common_meta)
                    material_code = common_meta["code"]
                elif col_name in PRODUCT_SPEC_HEADERS:
                    material_id, material_code, created = find_or_create_product_material(cur, product_name, col_name, workplace)
                    if material_code:
                        material_workplaces[material_code].add(workplace)
                else:
                    continue

                if created:
                    created_materials += 1

                cur.execute(
                    """
                    INSERT INTO bom (product_id, material_id, raw_material_id, quantity_per_box, sok_per_box)
                    VALUES (?, ?, NULL, ?, NULL)
                    """,
                    (product_id, material_id, qty),
                )
                bom_rows_added += 1

    # Move product-specific materials to matching workplace where possible.
    for material_code, workplaces in material_workplaces.items():
        row = cur.execute("SELECT category FROM materials WHERE code = ?", (material_code,)).fetchone()
        if not row:
            continue
        target_workplace = next(iter(workplaces)) if len(workplaces) == 1 else "공통"
        cur.execute("UPDATE materials SET workplace = ? WHERE code = ?", (target_workplace, material_code))

    conn.commit()
    conn.close()

    print(f"backup={backup_path}")
    print(f"created_materials={created_materials}")
    print(f"bom_rows_added={bom_rows_added}")
    print(f"skipped_products={len(set(skipped_products))}")
    for code in sorted(set(skipped_products))[:30]:
        print(f"skip {code}")
    return 0


if __name__ == "__main__":
    sys.exit(run_import())
