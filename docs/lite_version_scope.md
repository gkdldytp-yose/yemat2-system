# yemat2 Lite Version Scope

## Goal

`yemat2` is not a direct continuation of `yemat1`.
It is the lightweight version of the system, built from the first-phase final version of `yemat1`.

The main direction is:

- keep production management simple
- reduce the depth of the material concept
- avoid large purchasing, lot, and logistics workflows unless they are truly required

## Recommended product direction

Keep these areas as the default core:

- login and user access
- dashboard
- product master
- simple BOM or recipe mapping
- production entry and production history
- print/output screens only if they support production work directly

Reduce or remove these areas first:

- advanced material lot tracking
- supplier management
- purchase order workflow
- logistics issue/request workflow
- multi-workplace stock branching
- detailed import flows that only exist for full material operations

## Suggested simplification rule

Instead of treating sub-materials as full inventory objects with lot, supplier, purchase, and logistics state,
handle them in one of these lighter ways:

1. Product recipe reference only
2. Simple item list with current stock only
3. Manual note-level management outside the main workflow

## Candidate modules to review first

- `blueprints/materials.py`
- `blueprints/inventory.py`
- `templates/materials.html`
- `templates/purchase_orders.html`
- `templates/raw_materials.html`
- `blueprints/imports.py`

## Migration strategy

Phase 1:

- define the lite feature set
- hide or disconnect screens that do not belong in the lite version
- keep the app bootable while trimming unused routes

Phase 2:

- simplify database usage around materials and BOM
- remove dependencies between production and advanced purchasing/logistics flows

Phase 3:

- clean up templates, navigation, and imports
- add focused tests for the retained lite workflows

## Working assumption for Codex

Until we decide otherwise, the default assumption for `yemat2` will be:

- production-centered app
- reduced sub-material management
- no expansion of complex purchasing/logistics features
