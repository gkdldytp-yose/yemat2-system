# Lite Inventory Rules

## Core decisions

The lite version uses a much smaller sub-material workflow than `yemat1`.

1. No logistics stock concept
2. No purchase flow
3. No logistics manager role
4. Workplace stock is managed by each workplace
5. An issue request is closed by the requester after real receipt and stock confirmation

## Operational interpretation

- A workplace requests needed stock
- The real handoff happens in the field
- The requesting user checks what was actually received
- The requesting user enters the real received quantity
- The request is marked complete only after that confirmation

## Implications for implementation

- remove logistics-only screens from navigation
- stop routing users into logistics-specific pages on login
- stop showing purchase and logistics notifications as the default app behavior
- simplify role choices for new approvals and user management
- redesign issue request status flow around requester confirmation

## Next backend target

The next major refactor should convert the current logistics-centered issue flow into:

- request created
- stock handed over
- requester confirms actual quantity
- workplace stock updated
- request completed
