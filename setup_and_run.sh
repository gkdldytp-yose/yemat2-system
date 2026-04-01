#!/bin/bash

echo "================================================================"
echo "🍱 김공자 생산관리 시스템 - 자동 설치 및 실행"
echo "================================================================"
echo ""

# 현재 디렉토리 저장
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 1. templates 폴더 확인
echo "📁 폴더 구조 확인 중..."
if [ ! -d "templates" ]; then
    echo "❌ templates 폴더가 없습니다!"
    echo ""
    echo "해결 방법:"
    echo "1. 압축 파일을 다시 다운로드하세요"
    echo "2. 압축 해제 시 모든 파일을 포함하도록 하세요"
    echo "3. 또는 아래 파일들이 있는지 확인하세요:"
    echo "   - templates/login.html"
    echo "   - templates/dashboard.html"
    echo "   - templates/base.html"
    echo "   등..."
    exit 1
fi

# 2. 필수 파일 확인
echo "✓ templates 폴더 존재"

REQUIRED_FILES=(
    "templates/login.html"
    "templates/dashboard.html"
    "templates/base.html"
    "app.py"
    "yemat.db"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ $file 파일이 없습니다!"
        exit 1
    fi
    echo "✓ $file"
done

echo ""
echo "================================================================"
echo "📦 Flask 설치 확인 중..."
echo "================================================================"
echo ""

# 3. Flask 설치 확인 및 설치
if ! python3 -c "import flask" 2>/dev/null; then
    echo "Flask가 설치되지 않았습니다. 설치 중..."
    pip3 install flask --user
    
    if [ $? -ne 0 ]; then
        echo ""
        echo "❌ Flask 설치 실패!"
        echo "다음 명령어를 직접 실행해보세요:"
        echo "  pip3 install flask"
        echo "또는:"
        echo "  python3 -m pip install flask --user"
        exit 1
    fi
    echo "✓ Flask 설치 완료!"
else
    echo "✓ Flask 이미 설치됨"
fi

echo ""
echo "================================================================"
echo "🚀 서버 시작!"
echo "================================================================"
echo ""
echo "접속 URL: http://localhost:8000"
echo "관리자 계정: admin / 1111"
echo ""
echo "종료하려면 Ctrl+C를 누르세요"
echo ""
echo "================================================================"
echo ""

# 4. 서버 실행
python3 app.py
