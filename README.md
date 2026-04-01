# yemat2

This folder is the lite edition of the Yemat production management system.
It is intended to evolve from the `yemat1` final first-phase build into a simpler operational version.

## Project layout

- `app.py`: Flask entry point
- `blueprints/`: feature blueprints
- `templates/`: Jinja templates
- `static/`: static assets
- `scripts/`: setup, maintenance, and import scripts
- `yemat.db`: local SQLite database

## Quick start

```powershell
cd yemat2
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Open `http://localhost:8080` in your browser.

## Environment variables

- `YEMAT_HOST`: default `0.0.0.0`
- `YEMAT_PORT`: default `8080`
- `YEMAT_SECRET_KEY`: overrides the default Flask secret key

Example:

```powershell
$env:YEMAT_PORT="5000"
$env:YEMAT_SECRET_KEY="replace-this"
python app.py
```

## Suggested next steps

- Remove logistics hub and purchase flows from the remaining routes
- Rework issue requests so the requester confirms received quantity and closes the request
- Keep workplace-level stock as the only sub-material inventory concept
- Reduce or remove heavyweight material and purchasing flows
- Keep only the minimum screens needed for product, production, and core operations
- Split more logic out of large blueprint files
- Move runtime-only data into a dedicated app config or `instance/` folder
- Collect tests under a `tests/` directory for easier validation
