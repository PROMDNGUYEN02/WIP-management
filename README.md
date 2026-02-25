# WIP-management

Codebase is now structured as Event-Driven Clean Architecture + MVVM + Single-Writer Store in `wip_management/`.

## Package layout

`wip_management/`

1. `__main__.py`
2. `config.py`
3. `domain/`
4. `application/`
5. `infrastructure/`
6. `presentation/`

Detailed sub-structure:

1. `domain/models/tray.py`, `domain/models/trolley.py`
2. `domain/events.py`, `domain/rules/classifier.py`, `domain/rules/grouping_rules.py`, `domain/state_machine.py`
3. `application/ports.py`
4. `application/use_cases/ingest_signals.py`, `recompute_projection.py`, `auto_group.py`, `manual_group.py`
5. `application/services/orchestrator.py`
6. `application/state/state_store.py`, `reducers.py`, `snapshots.py`
7. `infrastructure/sqlserver/connection.py`, `ccu_repo.py`, `fpc_repo.py`, `delta_tracker.py`
8. `infrastructure/messaging/event_bus.py`
9. `infrastructure/realtime/ws_server.py`
10. `infrastructure/persistence/state_repo.py`
11. `presentation/api/http.py`, `presentation/api/ws.py`
12. `presentation/ui/viewmodels.py`, `presentation/ui/mapper.py`

## Run

1. Activate venv:
   `venv\Scripts\activate`
2. Install deps:
   `pip install -e .`
3. Configure `.env` (already present in this repo).
   - `LOG_LEVEL=DEBUG` for detailed diagnostics
   - `LOG_FILE=wip_management.log` (default)
   - `SHARED_STATE_ENABLED=true` to share manual/auto grouping state across users
   - `SHARED_STATE_DIR=...` shared folder path
4. Start app:
   `python -m wip_management`

## SQL driver note

Default is compatible with built-in Windows SQL driver:

`SQL_DRIVER=SQL Server`

If connection fails with IM002:

1. Check installed drivers:
   `venv\Scripts\python -c "import pyodbc; print(pyodbc.drivers())"`
2. Set `SQL_DRIVER` in `.env` to one installed driver.
