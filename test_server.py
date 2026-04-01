from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return '''
    <html>
    <head>
        <meta charset="UTF-8">
        <title>테스트 성공!</title>
    </head>
    <body style="font-family: Arial; text-align: center; padding: 50px;">
        <h1>🎉 Flask 서버가 정상 작동합니다!</h1>
        <p>이제 본 시스템을 실행해보세요.</p>
    </body>
    </html>
    '''

if __name__ == '__main__':
    print("\n" + "="*60)
    print("테스트 서버 시작!")
    print("브라우저에서 http://localhost:5000 접속하세요")
    print("="*60 + "\n")
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
