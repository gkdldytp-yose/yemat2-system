import sqlite3
from pathlib import Path


DB_PATH = Path(__file__).resolve().parents[2] / "yemat.db"
WORKPLACE = "2동 신관 1층"
PRODUCT_CATEGORY = "세트"
DEFAULT_UNIT = "ea"
DEFAULT_BOM_QTY = 1.0


PRODUCTS = [
    {
        "code": "A-KR-V001V1",
        "name": "신안 1004 캔 세트 3입",
        "description": "신안1004돌김 캔 낱캔 3개를 세트로 포장한 완제품",
        "box_quantity": 1,
        "set_item_type": "finished",
    },
    {
        "code": "A-KR-V001V2",
        "name": "신안 1004 캔 세트 6입",
        "description": "신안1004돌김 캔 낱캔 6개를 세트로 포장한 완제품",
        "box_quantity": 1,
        "set_item_type": "finished",
    },
    {
        "code": "C-KR-R001",
        "name": "감태김 도시락(세트용,6)",
        "description": "감태김선물세트 구성용 도시락 제품",
        "box_quantity": 1,
        "set_item_type": "semi",
    },
    {
        "code": "C-KR-R002",
        "name": "감태김 전장(세트용,5)",
        "description": "감태김선물세트 구성용 전장 제품",
        "box_quantity": 1,
        "set_item_type": "semi",
    },
    {
        "code": "C-KR-R003",
        "name": "감태김선물세트",
        "description": "캔 3개, 도시락 6개, 전장 5개를 넣는 선물세트 완제품",
        "box_quantity": 1,
        "set_item_type": "finished",
    },
    {
        "code": "C-KR-U001",
        "name": "신안1004돌김 캔 낱캔",
        "description": "신안 1004 캔 세트 구성용 낱캔 반제품",
        "box_quantity": 1,
        "set_item_type": "semi",
    },
]


MATERIALS = [
    {
        "code": "Z03A003",
        "name": "세트박스 신안1004돌김 캔 3입",
        "category": "박스",
    },
    {
        "code": "Z03A004",
        "name": "세트박스 신안1004돌김 캔 6입",
        "category": "박스",
    },
    {
        "code": "Z03A005",
        "name": "세트박스 감태김선물세트",
        "category": "박스",
    },
    {
        "code": "Z04A001",
        "name": "가방 캔 3입",
        "category": "가방",
    },
    {
        "code": "Z04A002",
        "name": "가방 캔 6입",
        "category": "가방",
    },
    {
        "code": "Z13A001",
        "name": "간지 선물세트 패드",
        "category": "패드",
    },
    {
        "code": "Z15A001",
        "name": "기름종이 원형",
        "category": "종이",
    },
    {
        "code": None,
        "name": "(상)감태, 신안 공용 401PE(고무뚜껑)(신규)",
        "category": "캔",
    },
    {
        "code": None,
        "name": "(상)감태, 신안 공용 35g401END(실뚜껑)(신규)",
        "category": "캔",
    },
    {
        "code": None,
        "name": "(하)신안1004돌김캔세트6입박스",
        "category": "박스",
    },
    {
        "code": None,
        "name": "(상) 신안1004돌김캔퍼트6입(신규)",
        "category": "박스",
    },
    {
        "code": None,
        "name": "(상) 신안1004돌김캔패드3입(신규)",
        "category": "패드",
    },
    {
        "code": None,
        "name": "(상)신안1004돌김캔세트3입밑판(하)",
        "category": "박스",
    },
    {
        "code": None,
        "name": "(상)고-감태김선물세트_인박스_도시락_A",
        "category": "박스",
    },
    {
        "code": None,
        "name": "(상)고-감태김선물세트_인박스_전장_B",
        "category": "박스",
    },
    {
        "code": None,
        "name": "(상)고-감태김선물세트_슬리브",
        "category": "박스",
    },
    {
        "code": None,
        "name": "(상)고-감태김선물세트_하석",
        "category": "박스",
    },
    {
        "code": None,
        "name": "(상)고-감태김선물세트_패드",
        "category": "패드",
    },
]


PRODUCT_BOM = {
    "A-KR-V001V1": [
        "세트박스 신안1004돌김 캔 3입",
        "가방 캔 3입",
        "(상) 신안1004돌김캔패드3입(신규)",
        "(상)신안1004돌김캔세트3입밑판(하)",
    ],
    "A-KR-V001V2": [
        "세트박스 신안1004돌김 캔 6입",
        "가방 캔 6입",
        "(하)신안1004돌김캔세트6입박스",
        "(상) 신안1004돌김캔퍼트6입(신규)",
    ],
    "C-KR-R003": [
        "세트박스 감태김선물세트",
        "간지 선물세트 패드",
        "(상)고-감태김선물세트_인박스_도시락_A",
        "(상)고-감태김선물세트_인박스_전장_B",
        "(상)고-감태김선물세트_슬리브",
        "(상)고-감태김선물세트_하석",
        "(상)고-감태김선물세트_패드",
    ],
}

COMPONENT_BOM = {
    "A-KR-V001V1": [
        {"component_code": "C-KR-U001", "quantity": 3.0},
    ],
    "A-KR-V001V2": [
        {"component_code": "C-KR-U001", "quantity": 6.0},
    ],
    "C-KR-R003": [
        {"component_code": "C-KR-R001", "quantity": 6.0},
        {"component_code": "C-KR-R002", "quantity": 5.0},
    ],
}


def upsert_product(cur, payload):
    row = cur.execute(
        "SELECT id FROM products WHERE code = ?",
        (payload["code"],),
    ).fetchone()
    params = (
        payload["name"],
        payload["description"],
        payload["box_quantity"],
        PRODUCT_CATEGORY,
        WORKPLACE,
        payload.get("set_item_type") or "",
    )
    if row:
        cur.execute(
            """
            UPDATE products
            SET name = ?, description = ?, box_quantity = ?, category = ?, workplace = ?, set_item_type = ?
            WHERE id = ?
            """,
            params + (int(row["id"]),),
        )
        return int(row["id"]), "updated"

    cur.execute(
        """
        INSERT INTO products (
            name, code, description, box_quantity, category, workplace,
            sheets_per_pack, cuts_per_sheet, set_item_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["name"],
            payload["code"],
            payload["description"],
            payload["box_quantity"],
            PRODUCT_CATEGORY,
            WORKPLACE,
            1,
            1,
            payload.get("set_item_type") or "",
        ),
    )
    return int(cur.lastrowid), "inserted"


def upsert_material(cur, payload):
    row = None
    code = payload.get("code")
    if code:
        row = cur.execute(
            "SELECT id FROM materials WHERE code = ?",
            (code,),
        ).fetchone()
    if not row:
        row = cur.execute(
            """
            SELECT id
            FROM materials
            WHERE name = ?
              AND COALESCE(workplace, '') = ?
            LIMIT 1
            """,
            (payload["name"], WORKPLACE),
        ).fetchone()

    params = (
        code,
        payload["name"],
        payload["category"],
        DEFAULT_UNIT,
        WORKPLACE,
    )
    if row:
        cur.execute(
            """
            UPDATE materials
            SET code = ?, name = ?, category = ?, unit = ?, workplace = ?
            WHERE id = ?
            """,
            params + (int(row["id"]),),
        )
        return int(row["id"]), "updated"

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
            payload["name"],
            payload["category"],
            "",
            DEFAULT_UNIT,
            0,
            0,
            0,
            0,
            0,
            WORKPLACE,
        ),
    )
    return int(cur.lastrowid), "inserted"


def upsert_bom(cur, product_id, material_id):
    row = cur.execute(
        """
        SELECT id
        FROM bom
        WHERE product_id = ?
          AND material_id = ?
          AND raw_material_id IS NULL
        LIMIT 1
        """,
        (product_id, material_id),
    ).fetchone()
    if row:
        cur.execute(
            """
            UPDATE bom
            SET quantity_per_box = ?, quantity_per_box_expr = ''
            WHERE id = ?
            """,
            (DEFAULT_BOM_QTY, int(row["id"])),
        )
        return "updated"

    cur.execute(
        """
        INSERT INTO bom (product_id, material_id, quantity_per_box, quantity_per_box_expr)
        VALUES (?, ?, ?, '')
        """,
        (product_id, material_id, DEFAULT_BOM_QTY),
    )
    return "inserted"


def upsert_component_bom(cur, product_id, component_product_id, quantity):
    row = cur.execute(
        """
        SELECT id
        FROM bom
        WHERE product_id = ?
          AND component_product_id = ?
        LIMIT 1
        """,
        (product_id, component_product_id),
    ).fetchone()
    if row:
        cur.execute(
            """
            UPDATE bom
            SET quantity_per_box = ?, quantity_per_box_expr = ''
            WHERE id = ?
            """,
            (quantity, int(row["id"])),
        )
        return "updated"

    cur.execute(
        """
        INSERT INTO bom (product_id, component_product_id, quantity_per_box, quantity_per_box_expr)
        VALUES (?, ?, ?, '')
        """,
        (product_id, component_product_id, quantity),
    )
    return "inserted"


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    product_ids = {}
    material_ids = {}
    product_stats = {"inserted": 0, "updated": 0}
    material_stats = {"inserted": 0, "updated": 0}
    bom_stats = {"inserted": 0, "updated": 0}

    for product in PRODUCTS:
        product_id, status = upsert_product(cur, product)
        product_ids[product["code"]] = product_id
        product_stats[status] += 1

    for material in MATERIALS:
        material_id, status = upsert_material(cur, material)
        material_ids[material["name"]] = material_id
        material_stats[status] += 1

    for product_code, material_names in PRODUCT_BOM.items():
        product_id = product_ids[product_code]
        for material_name in material_names:
            material_id = material_ids[material_name]
            status = upsert_bom(cur, product_id, material_id)
            bom_stats[status] += 1

    for product_code, component_rows in COMPONENT_BOM.items():
        product_id = product_ids[product_code]
        for row in component_rows:
            status = upsert_component_bom(
                cur,
                product_id,
                product_ids[row["component_code"]],
                float(row["quantity"]),
            )
            bom_stats[status] += 1

    conn.commit()
    conn.close()

    print(
        "products inserted={inserted} updated={updated}".format(**product_stats)
    )
    print(
        "materials inserted={inserted} updated={updated}".format(**material_stats)
    )
    print("bom inserted={inserted} updated={updated}".format(**bom_stats))


if __name__ == "__main__":
    main()
