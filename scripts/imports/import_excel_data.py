import pandas as pd
from sqlalchemy.orm import Session
from database import SessionLocal, init_db
from models import Supplier, Material, User
from passlib.context import CryptContext
import re

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def clean_value(value):
    """값 정리 함수"""
    if pd.isna(value) or value == '-':
        return None
    if isinstance(value, str):
        return value.strip()
    return value

def extract_number(text):
    """문자열에서 숫자 추출"""
    if pd.isna(text) or text == '-':
        return None
    if isinstance(text, (int, float)):
        return text
    match = re.search(r'(\d+)', str(text))
    return int(match.group(1)) if match else None

def import_suppliers_and_materials(db: Session):
    """발주.xlsx의 단가표 시트에서 업체 및 부자재 정보 가져오기"""
    print("=" * 80)
    print("발주.xlsx - 단가표 시트 데이터 import 시작...")
    print("=" * 80)
    
    # 엑셀 파일 읽기
    df = pd.read_excel('/mnt/user-data/uploads/발주.xlsx', sheet_name='단가표', header=None)
    
    # 데이터는 4행부터 시작
    # 컬럼 구조:
    # 0: 사용여부?, 1: 비고, 2: 업체명, 3: 품명, 4: 규격, 5: 단위, 
    # 6-12: 기타, 13: MOQ, 14: Lead time, 15: 모품목 품명
    
    suppliers_dict = {}  # 업체명을 키로 사용
    materials_count = 0
    
    for idx in range(4, len(df)):
        row = df.iloc[idx]
        
        # 업체명 추출
        supplier_name = clean_value(row[2])
        if not supplier_name:
            continue
            
        # 품명 확인
        material_name = clean_value(row[3])
        if not material_name:
            continue
        
        # 업체 등록 (중복 방지)
        if supplier_name not in suppliers_dict:
            # 업체 코드는 일단 자동 생성 (발주내역에서 매칭 필요)
            supplier = Supplier(
                code=f"S{len(suppliers_dict)+1:05d}",
                name=supplier_name
            )
            db.add(supplier)
            db.flush()  # ID 생성
            suppliers_dict[supplier_name] = supplier
            print(f"✓ 업체 등록: {supplier_name} (코드: {supplier.code})")
        else:
            supplier = suppliers_dict[supplier_name]
        
        # 부자재 정보 추출
        spec = clean_value(row[4])  # 규격
        unit = clean_value(row[5])  # 단위
        moq = clean_value(row[13])  # MOQ
        lead_time = clean_value(row[14])  # Lead time
        
        # 카테고리 자동 분류
        category = "기타"
        material_name_lower = material_name.lower()
        if "내포" in material_name:
            category = "내포"
        elif "외포" in material_name:
            category = "외포"
        elif "트레이" in material_name:
            category = "트레이"
        elif "박스" in material_name:
            category = "박스"
        elif "뚜껑" in material_name:
            category = "뚜껑"
        elif "각대" in material_name:
            category = "각대"
        elif "실리카" in material_name:
            category = "실리카"
        elif "기름" in material_name or "오일" in material_name:
            category = "기름"
        elif "소금" in material_name:
            category = "소금"
        elif "김" in material_name or "원초" in material_name:
            category = "원초"
        
        # 부자재 코드 생성 (일단 자동, 나중에 발주내역과 매칭 필요)
        material_code = f"M{materials_count+1:05d}"
        
        # 부자재 등록
        material = Material(
            supplier_id=supplier.id,
            code=material_code,
            name=material_name,
            category=category,
            spec=spec,
            unit=unit,
            moq=moq,
            lead_time=lead_time,
            unit_price=0,  # 단가는 발주내역에서 가져와야 함
            current_stock=0,
            min_stock=0
        )
        db.add(material)
        materials_count += 1
        
        if materials_count % 50 == 0:
            print(f"  → {materials_count}개 부자재 처리 중...")
    
    db.commit()
    print(f"\n✓ 총 {len(suppliers_dict)}개 업체, {materials_count}개 부자재 등록 완료!")
    return suppliers_dict

def import_purchase_orders():
    """발주.xlsx의 발주내역 시트에서 발주 정보 가져오기"""
    print("\n" + "=" * 80)
    print("발주.xlsx - 발주내역 시트 데이터 분석...")
    print("=" * 80)
    
    df = pd.read_excel('/mnt/user-data/uploads/발주.xlsx', sheet_name='발주내역', header=None)
    
    # 헤더는 1행, 데이터는 3행부터
    print("발주내역 컬럼 구조:")
    headers = df.iloc[1]
    for i, header in enumerate(headers):
        if pd.notna(header):
            print(f"  열{i}: {header}")
    
    print(f"\n총 {len(df) - 3}건의 발주 내역 발견")
    print("(발주내역 import는 추후 단계에서 진행)")

def create_admin_user(db: Session):
    """관리자 계정 생성"""
    print("\n" + "=" * 80)
    print("관리자 계정 생성...")
    print("=" * 80)
    
    # 기존 admin 계정 확인
    existing_admin = db.query(User).filter(User.username == "admin").first()
    if existing_admin:
        print("⚠ 관리자 계정이 이미 존재합니다.")
        return
    
    # admin 계정 생성
    hashed_password = pwd_context.hash("1111")
    admin_user = User(
        username="admin",
        hashed_password=hashed_password,
        is_admin=True
    )
    db.add(admin_user)
    db.commit()
    print("✓ 관리자 계정 생성 완료!")
    print("  - 아이디: admin")
    print("  - 비밀번호: 1111")

def main():
    """메인 실행 함수"""
    print("\n" + "=" * 80)
    print("예맛 생산관리 시스템 - 데이터베이스 초기화")
    print("=" * 80 + "\n")
    
    # 데이터베이스 초기화
    print("데이터베이스 테이블 생성 중...")
    init_db()
    print("✓ 데이터베이스 테이블 생성 완료!\n")
    
    # 세션 생성
    db = SessionLocal()
    
    try:
        # 1. 관리자 계정 생성
        create_admin_user(db)
        
        # 2. 업체 및 부자재 데이터 import
        import_suppliers_and_materials(db)
        
        # 3. 발주내역 분석 (실제 import는 추후)
        import_purchase_orders()
        
        print("\n" + "=" * 80)
        print("✓ 데이터 import 완료!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n✗ 에러 발생: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    main()
