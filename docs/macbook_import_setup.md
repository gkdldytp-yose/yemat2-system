# MacBook 실행 가이드

이 프로젝트를 집 MacBook에서 받아서 `엑셀 임포트` 기능까지 사용하려면 아래 순서대로 실행하면 됩니다.

## 1. 코드 받기

```bash
git clone <저장소주소>
cd yemat1/yemat1
```

이미 clone 되어 있으면:

```bash
git pull
```

## 2. 가상환경 만들기

```bash
python3 -m venv venv
source venv/bin/activate
```

활성화 후에는 프롬프트 앞에 `(venv)`가 보입니다.

## 3. 패키지 설치

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

현재 `requirements.txt`에는 엑셀 임포트에 필요한 아래 패키지가 이미 포함되어 있습니다.

- `pandas`
- `openpyxl`

즉, MacBook에서는 `requirements.txt`만 설치하면 엑셀 임포트 기능까지 바로 사용할 수 있습니다.

## 4. 서버 실행

```bash
python app.py
```

## 5. 브라우저 접속

보통 아래 주소로 접속합니다.

```text
http://127.0.0.1:8080
```

또는 프로젝트 설정에 따라:

```text
http://localhost:8080
```

## 6. 엑셀 임포트 사용 위치

- `통합관리`
- `엑셀 임포트`

## 자주 쓰는 전체 명령

처음 설정:

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python app.py
```

그다음부터:

```bash
cd yemat1/yemat1
source venv/bin/activate
python app.py
```

## 확인용 명령

설치 확인:

```bash
python -c "import pandas, openpyxl; print('OK', pandas.__version__, openpyxl.__version__)"
```

## 참고

- `엑셀 임포트`는 `pandas + openpyxl`가 있어야 업로드/파싱이 됩니다.
- 템플릿 다운로드만 볼 때는 앱 자체는 뜰 수 있지만, 실제 임포트 검증/적용은 위 패키지가 필요합니다.
