"""
모든 라우트에 workplace 필터를 자동으로 추가
주요 라우트들의 쿼리를 수정합니다
"""

import re

def add_workplace_filters():
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # 패턴 정의: SELECT 문에서 WHERE 절이 있는 경우
    patterns = [
        # products 테이블
        (r"(FROM products[^W]*WHERE[^;]+)", r"\1 AND products.workplace = ?"),
        (r"(FROM products p[^W]*WHERE[^;]+)", r"\1 AND p.workplace = ?"),
        
        # raw_materials 테이블  
        (r"(FROM raw_materials[^W]*WHERE[^;]+)", r"\1 AND raw_materials.workplace = ?"),
        (r"(FROM raw_materials rm[^W]*WHERE[^;]+)", r"\1 AND rm.workplace = ?"),
        
        # materials 테이블
        (r"(FROM materials[^W]*WHERE[^;]+)", r"\1 AND materials.workplace = ?"),
        (r"(FROM materials m[^W]*WHERE[^;]+)", r"\1 AND m.workplace = ?"),
        
        # productions 테이블
        (r"(FROM productions[^W]*WHERE[^;]+)", r"\1 AND productions.workplace = ?"),
        (r"(FROM productions pr[^W]*WHERE[^;]+)", r"\1 AND pr.workplace = ?"),
        
        # production_schedules 테이블
        (r"(FROM production_schedules[^W]*WHERE[^;]+)", r"\1 AND production_schedules.workplace = ?"),
        (r"(FROM production_schedules ps[^W]*WHERE[^;]+)", r"\1 AND ps.workplace = ?"),
        
        # purchase_requests 테이블
        (r"(FROM purchase_requests[^W]*WHERE[^;]+)", r"\1 AND purchase_requests.workplace = ?"),
        (r"(FROM purchase_requests pr[^W]*WHERE[^;]+)", r"\1 AND pr.workplace = ?"),
    ]
    
    for pattern, replacement in patterns:
        content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)
    
    # WHERE 절이 없는 경우는 수동으로 처리해야 함
    
    if content != original:
        with open('app.py', 'w', encoding='utf-8') as f:
            f.write(content)
        print("✅ workplace 필터 추가 완료")
        print(f"변경된 라인 수: {len(content.splitlines()) - len(original.splitlines())}")
    else:
        print("⚠️  변경사항 없음")

if __name__ == '__main__':
    add_workplace_filters()
