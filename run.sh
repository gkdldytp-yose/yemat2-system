#!/bin/bash
echo "=========================================="
echo "🍱 김공자 생산관리 시스템 시작"
echo "=========================================="
echo ""
echo "접속 주소: http://localhost:8000"
echo "관리자 계정: admin / 1111"
echo ""
echo "종료하려면 Ctrl+C를 누르세요"
echo ""

cd "$(dirname "$0")"
python3 app.py
