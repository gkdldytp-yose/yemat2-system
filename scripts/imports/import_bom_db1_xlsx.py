import re
import sqlite3
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "yemat.db"
XLSX_PATH = ROOT.parent / "BOM DB_1.xlsx"

NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}

COMMON_HEADER_CODE_MAP = {
    "옥배유": "O06",
    "올리브유": "O07",
    "포도씨유": "O08",
    "정제소금": "B01",
    "맛소금": "B03",
    "천일염": "B05",
    "도시락 43mm": "T04",
    "1g 줄": "S10",
    "4g 줄": "S40",
    "4g 컷": "S41",
    "앵글1240(앵글1)": "Z01A010",
    "앵글1150(앵글2)": "Z01A002",
}

HEADER_META = {
    "참기름": {"category": "기름", "unit": "kg", "kind": "common"},
    "해바라기": {"category": "기름", "unit": "kg", "kind": "common"},
    "옥배유": {"category": "기름", "unit": "kg", "kind": "common"},
    "올리브유": {"category": "기름", "unit": "kg", "kind": "common"},
    "포도씨유": {"category": "기름", "unit": "kg", "kind": "common"},
    "정제소금": {"category": "소금", "unit": "kg", "kind": "common"},
    "맛소금": {"category": "소금", "unit": "kg", "kind": "common"},
    "천일염": {"category": "소금", "unit": "kg", "kind": "common"},
    "식탁(중) 트레이": {"category": "트레이", "unit": "개", "kind": "packaging"},
    "도시락 43mm": {"category": "트레이", "unit": "개", "kind": "common"},
    "식탁(대)트레이": {"category": "트레이", "unit": "개", "kind": "packaging"},
    "군함말이 트레이": {"category": "트레이", "unit": "개", "kind": "packaging"},
    "1g 줄": {"category": "실리카", "unit": "개", "kind": "common"},
    "4g 줄": {"category": "실리카", "unit": "개", "kind": "common"},
    "4g 컷": {"category": "실리카", "unit": "개", "kind": "common"},
    "내포": {"category": "내포", "unit": "개", "kind": "packaging"},
    "외포": {"category": "외포", "unit": "개", "kind": "packaging"},
    "박스": {"category": "박스", "unit": "개", "kind": "packaging"},
    "뚜껑": {"category": "포장재", "unit": "개", "kind": "packaging"},
    "앵글1240(앵글1)": {"category": "소모품", "unit": "개", "kind": "common"},
    "앵글1150(앵글2)": {"category": "소모품", "unit": "개", "kind": "common"},
    "파우치": {"category": "포장재", "unit": "개", "kind": "packaging"},
}

PRODUCT_CATEGORY_DEFAULT_WORKPLACE = {
    "도시락": "2동 신관 1층",
    "전장": "2동 신관 1층",
    "김밥": "2동 신관 1층",
    "생김": "2동 신관 1층",
    "무트레이": "2동 신관 1층",
    "자반": "1동 자반",
}


def load_xlsx_rows(path: Path):
    with zipfile.ZipFile(path) as zf:
        shared = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", NS):
                shared.append("".join(t.text or "" for t in si.findall(".//a:t", NS)))

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        sheet = workbook.find("a:sheets", NS)[0]
        target = "xl/" + rel_map[sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]]
        root = ET.fromstring(zf.read(target))
        rows = []
        for row in root.findall(".//a:sheetData/a:row", NS):
            values = []
            for cell in row.findall("a:c", NS):
                cell_type = cell.attrib.get("t")
                value_node = cell.find("a:v", NS)
                inline_node = cell.find("a:is", NS)
                if inline_node is not None:
                    values.append("".join(t.text or "" for t in inline_node.findall(".//a:t", NS)))
                elif value_node is None:
                    values.append("")
                elif cell_type == "s":
                    values.append(shared[int(value_node.text)])
                else:
                    values.append(value_node.text)
            rows.append(values)
        return rows


def parse_number(value):
    raw = (value or "").strip()
    if not raw or raw in {"0", "0.0", "#REF!"}:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def normalize_text(value):
    text = (value or "").replace("＆", "&").replace("　", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_key(value):
    text = normalize_text(value).lower()
    text = re.sub(r"\([^)]*\)", " ", text)
    text = text.replace("*", " ").replace("x", " ").replace("×", " ")
    text = re.sub(r"[^0-9a-z가-힣]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def token_list(value):
    stop = {
        "국내", "일본", "중국", "대만", "미국", "캐나다", "필리핀", "호주", "영국",
        "예맛", "해피식품", "고아사", "홈플러스", "t", "s", "r", "and",
    }
    return [token for token in normalize_key(value).split() if token and token not in stop]


def infer_workplace(product_name, category):
    name = normalize_text(product_name)
    if "유기" in name:
        return "2동 신관 2층"
    if category == "식탁":
        if any(keyword in name for keyword in ["감태", "곱창", "미니", "조미하지 않은", "조미하지않은"]):
            return "2동 신관 2층"
        return "1동 조미"
    return PRODUCT_CATEGORY_DEFAULT_WORKPLACE.get(category, "1동 조미")


def choose_oil_code(product_name, header):
    if header == "참기름":
        return "O03" if "유기" in normalize_text(product_name) else "O01"
    if header == "해바라기":
        return "O05" if "유기" in normalize_text(product_name) else "O04"
    return COMMON_HEADER_CODE_MAP.get(header)


def material_score(product_name, header, material_row):
    score = 0
    product_tokens = token_list(product_name)
    material_tokens = token_list(material_row["name"])
    material_name = normalize_text(material_row["name"])
    for token in product_tokens:
        if token in material_tokens:
            score += 4 if re.search(r"\d", token) else 2
        elif any(token in mt or mt in token for mt in material_tokens):
            score += 1
    if header in material_name:
        score += 3
    if header == "뚜껑" and "뚜껑" in material_name:
        score += 3
    if header == "파우치" and "파우치" in material_name:
        score += 3
    if "박스" in header and "박스" in material_name:
        score += 2
    if "트레이" in header and "트레이" in material_name:
        score += 2
    return score


def next_material_code(cur):
    cur.execute("SELECT code FROM materials WHERE code LIKE 'IMP%' ORDER BY code DESC LIMIT 1")
    row = cur.fetchone()
    last_num = int((row["code"] or "IMP00000")[3:]) if row and row["code"] else 0
    return f"IMP{last_num + 1:05d}"


def get_material_by_code(cur, code):
    cur.execute("SELECT * FROM materials WHERE code = ?", (code,))
    return cur.fetchone()


def find_existing_packaging_material(cur, product_name, workplace, header, category):
    if category == "소모품":
        workplaces = [workplace, "기타", "공통"]
    else:
        workplaces = [workplace, "공통"]
    cur.execute(
        """
        SELECT *
        FROM materials
        WHERE category = ?
          AND workplace IN ({})
        ORDER BY workplace, name
        """.format(",".join("?" for _ in workplaces)),
        (category, *workplaces),
    )
    candidates = cur.fetchall()
    scored = [(material_score(product_name, header, row), row) for row in candidates]
    scored = [item for item in scored if item[0] > 0]
    if not scored:
        return None
    scored.sort(key=lambda item: (item[0], item[1]["id"]), reverse=True)
    best_score, best_row = scored[0]
    if best_score < 5:
        return None
    return best_row


def ensure_material(cur, product_name, workplace, header):
    meta = HEADER_META[header]
    if header in ("참기름", "해바라기"):
        code = choose_oil_code(product_name, header)
        row = get_material_by_code(cur, code)
        if row:
            return row

    if header in COMMON_HEADER_CODE_MAP:
        row = get_material_by_code(cur, COMMON_HEADER_CODE_MAP[header])
        if row:
            return row

    if meta["kind"] == "common":
        cur.execute(
            """
            SELECT *
            FROM materials
            WHERE name = ? OR code = ?
            LIMIT 1
            """,
            (header, COMMON_HEADER_CODE_MAP.get(header, "")),
        )
        row = cur.fetchone()
        if row:
            return row

    existing = find_existing_packaging_material(cur, product_name, workplace, header, meta["category"])
    if existing:
        return existing

    material_name = f"{normalize_text(product_name)} {header}"
    cur.execute(
        """
        SELECT *
        FROM materials
        WHERE name = ?
        LIMIT 1
        """,
        (material_name,),
    )
    row = cur.fetchone()
    if row:
        return row

    material_workplace = workplace
    if meta["kind"] == "common":
        material_workplace = "공통"
    elif meta["category"] == "소모품":
        material_workplace = "기타"

    code = next_material_code(cur)
    cur.execute(
        """
        INSERT INTO materials (code, name, category, unit, workplace, current_stock, min_stock)
        VALUES (?, ?, ?, ?, ?, 0, 0)
        """,
        (code, material_name, meta["category"], meta["unit"], material_workplace),
    )
    cur.execute("SELECT * FROM materials WHERE id = last_insert_rowid()")
    return cur.fetchone()


def upsert_product(cur, code, name, category, box_quantity, sok_per_box, workplace):
    cur.execute("SELECT * FROM products WHERE code = ?", (code,))
    row = cur.fetchone()
    if row:
        cur.execute(
            """
            UPDATE products
            SET name = ?, category = ?, box_quantity = ?, sok_per_box = ?, workplace = ?
            WHERE id = ?
            """,
            (name, category, int(box_quantity or 1), float(sok_per_box or 0), workplace, row["id"]),
        )
        product_id = row["id"]
        created = False
    else:
        cur.execute(
            """
            INSERT INTO products (name, code, category, box_quantity, sok_per_box, workplace)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (name, code, category, int(box_quantity or 1), float(sok_per_box or 0), workplace),
        )
        product_id = cur.lastrowid
        created = True
    return product_id, created


def upsert_bom_item(cur, product_id, material_id, quantity_per_box):
    cur.execute(
        """
        SELECT id
        FROM bom
        WHERE product_id = ? AND material_id = ?
        LIMIT 1
        """,
        (product_id, material_id),
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            "UPDATE bom SET quantity_per_box = ? WHERE id = ?",
            (float(quantity_per_box), row["id"]),
        )
        return False
    cur.execute(
        """
        INSERT INTO bom (product_id, material_id, quantity_per_box)
        VALUES (?, ?, ?)
        """,
        (product_id, material_id, float(quantity_per_box)),
    )
    return True


def main():
    if not XLSX_PATH.exists():
        raise FileNotFoundError(XLSX_PATH)

    rows = load_xlsx_rows(XLSX_PATH)
    header = rows[0]
    material_headers = header[6:]
    data_rows = rows[1:]

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    created_products = 0
    updated_products = 0
    created_material_ids = set()
    inserted_bom = 0
    updated_bom = 0

    for row in data_rows:
        if len(row) < 6:
            continue
        code = normalize_text(row[0])
        name = normalize_text(row[1])
        category = normalize_text(row[2]) or "기타"
        box_quantity = parse_number(row[4]) or 1
        sok_per_box = parse_number(row[5]) or 0
        if not code or not name:
            continue

        workplace = infer_workplace(name, category)
        product_id, created = upsert_product(cur, code, name, category, box_quantity, sok_per_box, workplace)
        if created:
            created_products += 1
        else:
            updated_products += 1

        for idx, header_name in enumerate(material_headers, start=6):
            qty = parse_number(row[idx] if idx < len(row) else "")
            if qty <= 0:
                continue
            material = ensure_material(cur, name, workplace, header_name)
            if material["code"].startswith("IMP"):
                created_material_ids.add(int(material["id"]))
            created_bom_row = upsert_bom_item(cur, product_id, material["id"], qty)
            if created_bom_row:
                inserted_bom += 1
            else:
                updated_bom += 1

    conn.commit()
    conn.close()

    print(
        f"created_products={created_products} "
        f"updated_products={updated_products} "
        f"created_materials={len(created_material_ids)} "
        f"inserted_bom={inserted_bom} "
        f"updated_bom={updated_bom}"
    )


if __name__ == "__main__":
    main()
