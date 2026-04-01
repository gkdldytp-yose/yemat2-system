from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Text, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class User(Base):
    """사용자 테이블"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now)

class Supplier(Base):
    """업체 테이블"""
    __tablename__ = "suppliers"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True)  # 업체 코드 (예: 00146)
    name = Column(String, index=True)  # 업체명
    contact = Column(String, nullable=True)  # 연락처
    address = Column(String, nullable=True)  # 주소
    note = Column(Text, nullable=True)  # 비고
    created_at = Column(DateTime, default=datetime.now)
    
    materials = relationship("Material", back_populates="supplier")
    orders = relationship("PurchaseOrder", back_populates="supplier")

class Material(Base):
    """부자재 테이블"""
    __tablename__ = "materials"
    
    id = Column(Integer, primary_key=True, index=True)
    supplier_id = Column(Integer, ForeignKey("suppliers.id"))
    code = Column(String, unique=True, index=True)  # 품목코드
    name = Column(String, index=True)  # 부자재명/품명
    category = Column(String)  # 카테고리 (내포, 외포, 트레이, 박스, 뚜껑, 각대, 원초, 기름, 소금, 실리카 등)
    spec = Column(String, nullable=True)  # 규격 (예: 340*1000)
    unit = Column(String)  # 단위 (R/L, EA, KG 등)
    moq = Column(String, nullable=True)  # 최소 주문 수량 (Minimum Order Quantity)
    lead_time = Column(String, nullable=True)  # 리드타임 (예: 12일)
    unit_price = Column(Float, default=0)  # 단가
    current_stock = Column(Float, default=0)  # 현재 재고
    min_stock = Column(Float, default=0)  # 최소 재고
    created_at = Column(DateTime, default=datetime.now)
    
    supplier = relationship("Supplier", back_populates="materials")
    bom_items = relationship("BOM", back_populates="material")
    
class Product(Base):
    """상품 테이블"""
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)  # 상품명
    code = Column(String, unique=True, nullable=True)  # 상품 코드
    description = Column(Text, nullable=True)  # 상품 설명
    box_quantity = Column(Integer, default=1)  # 박스당 개수
    created_at = Column(DateTime, default=datetime.now)
    
    bom_items = relationship("BOM", back_populates="product")
    productions = relationship("Production", back_populates="product")

class BOM(Base):
    """BOM (Bill of Materials) - 상품별 부자재 구성"""
    __tablename__ = "bom"
    
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"))
    material_id = Column(Integer, ForeignKey("materials.id"))
    quantity_per_box = Column(Float)  # 박스당 필요 수량
    created_at = Column(DateTime, default=datetime.now)
    
    product = relationship("Product", back_populates="bom_items")
    material = relationship("Material", back_populates="bom_items")

class Production(Base):
    """생산 기록 테이블"""
    __tablename__ = "productions"
    
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"))
    production_date = Column(Date)  # 생산일
    planned_boxes = Column(Integer)  # 계획 박스 수
    actual_boxes = Column(Integer, nullable=True)  # 실제 생산 박스 수
    status = Column(String, default="계획")  # 상태: 계획, 진행중, 완료
    note = Column(Text, nullable=True)  # 비고
    created_at = Column(DateTime, default=datetime.now)
    
    product = relationship("Product", back_populates="productions")
    material_usage = relationship("ProductionMaterialUsage", back_populates="production")

class ProductionMaterialUsage(Base):
    """생산별 부자재 사용 기록"""
    __tablename__ = "production_material_usage"
    
    id = Column(Integer, primary_key=True, index=True)
    production_id = Column(Integer, ForeignKey("productions.id"))
    material_id = Column(Integer, ForeignKey("materials.id"))
    expected_quantity = Column(Float)  # 예상 사용량
    actual_quantity = Column(Float, nullable=True)  # 실제 사용량
    loss_quantity = Column(Float, nullable=True)  # 로스량
    created_at = Column(DateTime, default=datetime.now)
    
    production = relationship("Production", back_populates="material_usage")
    material = relationship("Material")

class PurchaseOrder(Base):
    """발주 테이블"""
    __tablename__ = "purchase_orders"
    
    id = Column(Integer, primary_key=True, index=True)
    supplier_id = Column(Integer, ForeignKey("suppliers.id"))
    order_date = Column(Date)  # 발주일
    expected_delivery_date = Column(Date, nullable=True)  # 입고예정일
    actual_delivery_date = Column(Date, nullable=True)  # 실제입고일
    status = Column(String, default="발주")  # 상태: 발주, 입고완료, 취소
    requester = Column(String, nullable=True)  # 발주요청자
    order_number = Column(String, nullable=True)  # 발주 NO
    note = Column(Text, nullable=True)  # 비고
    created_at = Column(DateTime, default=datetime.now)
    
    supplier = relationship("Supplier", back_populates="orders")
    items = relationship("PurchaseOrderItem", back_populates="purchase_order")

class PurchaseOrderItem(Base):
    """발주 상세 항목"""
    __tablename__ = "purchase_order_items"
    
    id = Column(Integer, primary_key=True, index=True)
    purchase_order_id = Column(Integer, ForeignKey("purchase_orders.id"))
    material_id = Column(Integer, ForeignKey("materials.id"))
    quantity = Column(Float)  # 발주 수량
    unit_price = Column(Float)  # 단가
    total_price = Column(Float)  # 합계액
    received_quantity = Column(Float, nullable=True)  # 입고 수량
    created_at = Column(DateTime, default=datetime.now)
    
    purchase_order = relationship("PurchaseOrder", back_populates="items")
    material = relationship("Material")

class ProductionSchedule(Base):
    """생산 스케줄 테이블"""
    __tablename__ = "production_schedules"
    
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"))
    scheduled_date = Column(Date)  # 예정일
    planned_boxes = Column(Integer)  # 계획 박스 수
    status = Column(String, default="예정")  # 상태: 예정, 진행중, 완료, 취소
    note = Column(Text, nullable=True)  # 비고
    created_at = Column(DateTime, default=datetime.now)
    
    product = relationship("Product")
