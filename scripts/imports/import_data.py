"""
Excel data import script
"""
import pandas as pd
import re
from database import SessionLocal, init_db
from models import Supplier, Material, Product, BOM
from auth import get_password_hash


def clean_value(value):
    """데이터 정리 함수"""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        return value.strip()
    return value


def parse_price_sheet():
    """발주.xlsx - 단가표 시트 파싱"""
    db = SessionLocal()
    
    try:
        # 엑셀 읽기
        df = pd.read_excel('/mnt/user-data/uploads/발주.xlsx', sheet_name='단가표', header=None)
        
        suppliers_dict = {}
        materials_list = []
        
        # 데이터 파싱 (4번째 행부터 실제 데이터)
        for idx in range(4, len(df)):
            row = df.iloc[idx]
            
            # 사용 여부 체크 (첫 번째 컬럼)
            if pd.isna(row[0]) or row[0] == 0:
                continue
                
            supplier_name = clean_value(row[2])
            if not supplier_name:
                continue
                
            material_name = clean_value(row[3])
            specification = clean_value(row[4])
            unit = clean_value(row[5])
            unit_price = row[6] if not pd.isna(row[6]) else 0
            supplier_code = clean_value(row[7])
            material_code = clean_value(row[8])
            
            # MOQ와 Lead time (컬럼 13, 14)
            moq_str = clean_value(row[13])
            lead_time_str = clean_value(row[14])
            
            # MOQ 파싱
            moq = None
            if moq_str:
                moq_match = re.search(r'(\d+)', str(moq_str))
                if moq_match:
                    moq = int(moq_match.group(1))
            
            # Lead time 파싱
            lead_time = None
            if lead_time_str:
                lead_time_match = re.search(r'(\d+)', str(lead_time_str))
                if lead_time_match:
                    lead_time = int(lead_time_match.group(1))
            
            # 카테고리 분류
            category = "기타"
            if material_name:
                if any(x in material_name for x in ["내포", "포장"]):
                    category = "내포"
                elif "외포" in material_name:
                    category = "외포"
                elif any(x in material_name for x in ["박스", "BOX"]):
                    category = "박스"
                elif "트레이" in material_name:
                    category = "트레이"
                elif any(x in material_name for x in ["실리카", "건조제"]):
                    category = "실리카"
            
            # 업체 정보 저장
            if supplier_name and supplier_name not in suppliers_dict:
                suppliers_dict[supplier_name] = {
                    'code': supplier_code if supplier_code else f"S{len(suppliers_dict)+1:04d}",
                    'name': supplier_name
                }
            
            # 부자재 정보 저장
            if material_name and material_code:
                materials_list.append({
                    'supplier_name': supplier_name,
                    'code': material_code,
                    'name': material_name,
                    'category': category,
                    'specification': specification,
                    'unit': unit,
                    'moq': moq,
                    'lead_time': lead_time,
                    'unit_price': float(unit_price) if unit_price else 0
                })
        
        print(f"✓ 파싱 완료: 업체 {len(suppliers_dict)}개, 부자재 {len(materials_list)}개")
        
        # 업체 DB에 저장
        for supplier_data in suppliers_dict.values():
            existing = db.query(Supplier).filter(Supplier.name == supplier_data['name']).first()
            if not existing:
                supplier = Supplier(**supplier_data)
                db.add(supplier)
        
        db.commit()
        print("✓ 업체 정보 저장 완료")
        
        # 부자재 DB에 저장
        for material_data in materials_list:
            supplier = db.query(Supplier).filter(Supplier.name == material_data['supplier_name']).first()
            if supplier:
                existing = db.query(Material).filter(Material.code == material_data['code']).first()
                if not existing:
                    material = Material(
                        supplier_id=supplier.id,
                        code=material_data['code'],
                        name=material_data['name'],
                        category=material_data['category'],
                        specification=material_data['specification'],
                        unit=material_data['unit'],
                        moq=material_data['moq'],
                        lead_time=material_data['lead_time'],
                        unit_price=material_data['unit_price']
                    )
                    db.add(material)
        
        db.commit()
        print("✓ 부자재 정보 저장 완료")
        
    except Exception as e:
        print(f"✗ 오류 발생: {e}")
        db.rollback()
    finally:
        db.close()


def parse_stock_ledger():
    """통합_생산관리.xlsx - 물류 수불대장 파싱 (현재 재고)"""
    db = SessionLocal()
    
    try:
        df = pd.read_excel('/mnt/user-data/uploads/통합_생산관리.xlsx', sheet_name='물류 수불대장')
        
        for idx, row in df.iterrows():
            code = clean_value(row['코드'])
            current_stock = row['현재고'] if not pd.isna(row['현재고']) else 0
            
            if code:
                # 부자재 찾기
                material = db.query(Material).filter(Material.code == code).first()
                if material:
                    material.current_stock = float(current_stock)
        
        db.commit()
        print("✓ 재고 정보 업데이트 완료")
        
    except Exception as e:
        print(f"✗ 재고 정보 업데이트 오류: {e}")
        db.rollback()
    finally:
        db.close()


def parse_bom_db():
    """통합_생산관리.xlsx - DB 시트 파싱 (BOM 정보)"""
    db = SessionLocal()
    
    try:
        df = pd.read_excel('/mnt/user-data/uploads/통합_생산관리.xlsx', sheet_name='DB', header=None)
        
        # 13번째 행이 헤더
        header_row = 13
        headers = df.iloc[header_row].tolist()
        
        # 부자재 카테고리 매핑
        oil_categories = ['참기름', '해바라기', '옥배유', '올리브유', '포도씨유']
        powder_categories = ['정제소금', '맛소금', '천일염']
        tray_categories = ['미니도시락', '도시락 43mm', '도시락 37mm', '식탁(중) 트레이', '식탁 트레이']
        silica_categories = ['1g 컷', '1g 줄', '2g 줄', '4g 줄', '4g 컷', '6g 줄', '7g 줄']
        packaging_categories = ['1차포장', '2차포장', '박스', '뚜껑']
        
        products_count = 0
        bom_count = 0
        
        # 14번째 행부터 실제 상품 데이터
        for idx in range(14, len(df)):
            row = df.iloc[idx]
            
            product_code = clean_value(row[0])
            product_name = clean_value(row[1])
            box_quantity = row[2] if not pd.isna(row[2]) else 0
            sheets = row[3] if not pd.isna(row[3]) else 0
            cutting = row[4] if not pd.isna(row[4]) else 0
            
            if not product_code or not product_name:
                continue
            
            if "상품종료" in str(product_code) or pd.isna(product_code):
                continue
            
            # 상품 저장
            existing_product = db.query(Product).filter(Product.code == product_code).first()
            if not existing_product:
                product = Product(
                    code=product_code,
                    name=product_name,
                    box_quantity=int(box_quantity) if box_quantity else 0,
                    sheets_per_pack=int(sheets) if sheets else 0,
                    cutting=int(cutting) if cutting else 0
                )
                db.add(product)
                db.commit()
                db.refresh(product)
                products_count += 1
            else:
                product = existing_product
            
            # BOM 데이터 파싱
            # 오일류 (컬럼 10-15)
            for i, oil_name in enumerate(oil_categories, start=10):
                qty = row[i] if not pd.isna(row[i]) else 0
                if qty and qty > 0:
                    # 오일 부자재 찾기
                    material = db.query(Material).filter(
                        Material.name.like(f"%{oil_name}%"),
                        Material.category == "기름"
                    ).first()
                    
                    if material:
                        existing_bom = db.query(BOM).filter(
                            BOM.product_id == product.id,
                            BOM.material_id == material.id
                        ).first()
                        
                        if not existing_bom:
                            bom = BOM(
                                product_id=product.id,
                                material_id=material.id,
                                quantity_per_box=float(qty)
                            )
                            db.add(bom)
                            bom_count += 1
            
            # 분말류 (컬럼 21-23)
            for i, powder_name in enumerate(powder_categories, start=21):
                qty = row[i] if not pd.isna(row[i]) else 0
                if qty and qty > 0:
                    material = db.query(Material).filter(
                        Material.name.like(f"%{powder_name}%"),
                        Material.category == "소금"
                    ).first()
                    
                    if material:
                        existing_bom = db.query(BOM).filter(
                            BOM.product_id == product.id,
                            BOM.material_id == material.id
                        ).first()
                        
                        if not existing_bom:
                            bom = BOM(
                                product_id=product.id,
                                material_id=material.id,
                                quantity_per_box=float(qty)
                            )
                            db.add(bom)
                            bom_count += 1
        
        db.commit()
        print(f"✓ BOM 정보 저장 완료: 상품 {products_count}개, BOM {bom_count}개")
        
    except Exception as e:
        print(f"✗ BOM 정보 파싱 오류: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


def create_admin_user():
    """관리자 계정 생성"""
    db = SessionLocal()
    
    try:
        # 기존 admin 계정 확인
        admin = db.query(User).filter(User.username == "admin").first()
        
        if not admin:
            admin = User(
                username="admin",
                hashed_password=get_password_hash("1111"),
                is_admin=True
            )
            db.add(admin)
            db.commit()
            print("✓ 관리자 계정 생성 완료 (ID: admin, PW: 1111)")
        else:
            print("✓ 관리자 계정이 이미 존재합니다.")
    
    except Exception as e:
        print(f"✗ 관리자 계정 생성 오류: {e}")
        db.rollback()
    finally:
        db.close()


def import_all_data():
    """모든 데이터 임포트"""
    print("=" * 60)
    print("맛 상사 예맛 생산관리 시스템 - 데이터 임포트")
    print("=" * 60)
    
    # 1. DB 초기화
    print("\n[1] 데이터베이스 초기화...")
    init_db()
    
    # 2. 관리자 계정 생성
    print("\n[2] 관리자 계정 생성...")
    create_admin_user()
    
    # 3. 발주.xlsx - 단가표 파싱
    print("\n[3] 발주.xlsx - 단가표 파싱...")
    parse_price_sheet()
    
    # 4. 통합_생산관리.xlsx - 재고 정보 파싱
    print("\n[4] 통합_생산관리.xlsx - 재고 정보 파싱...")
    parse_stock_ledger()
    
    # 5. 통합_생산관리.xlsx - BOM 정보 파싱
    print("\n[5] 통합_생산관리.xlsx - BOM 정보 파싱...")
    parse_bom_db()
    
    print("\n" + "=" * 60)
    print("✓ 모든 데이터 임포트 완료!")
    print("=" * 60)


if __name__ == "__main__":
    import_all_data()
