# yemat2 Quick Start

## 1. Move into the project folder

```powershell
cd yemat2
```

## 2. Activate the virtual environment

If `venv` already exists:

```powershell
.\venv\Scripts\Activate.ps1
```

If you need to create it first:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

## 3. Install dependencies

```powershell
pip install -r requirements.txt
```

## 4. Run the server

```powershell
python app.py
```

Then open:

```text
http://localhost:8080
```

Default admin account:

```text
ID: admin
PW: 1111
```

## 5. Optional settings

You can change the port or secret key before starting the app:

```powershell
$env:YEMAT_PORT="5000"
$env:YEMAT_SECRET_KEY="change-this-secret"
python app.py
```

## Troubleshooting

If Flask is missing:

```powershell
pip install Flask
```

If Excel import packages are missing:

```powershell
pip install pandas openpyxl
```

If the app runs but the page does not open:

- Try `http://127.0.0.1:8080`
- Change `YEMAT_PORT` if the current port is already in use
- Check that the database file and template folder are present
