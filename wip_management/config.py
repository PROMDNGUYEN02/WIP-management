# wip_management/config.py
from __future__ import annotations

import tempfile
from datetime import datetime, time, timedelta
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # APPLICATION SETTINGS
    # ═══════════════════════════════════════════════════════════════════════════
    app_name: str = "WIP Management"
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    log_file: str = "wip_management.log"
    log_to_console: bool = True
    log_sql_preview_chars: int = Field(default=220, ge=80, le=1200)

    # Shared state for multi-instance coordination
    shared_state_enabled: bool = True
    shared_state_dir: str = (
        r"C:\Users\PROMDNGUYEN02\OneDrive - Techtronic Industries Co. Ltd"
        r"\AES_Mass production - WIP Management"
    )
    shared_state_file: str = "grouping_state.json"
    grouping_sync_interval_seconds: float = Field(default=3.0, ge=0.5, le=120.0)

    # ═══════════════════════════════════════════════════════════════════════════
    # DATABASE CONNECTION SETTINGS
    # ═══════════════════════════════════════════════════════════════════════════
    sql_server: str = "10.148.144.75"
    sql_port: int = 1433
    sql_database: str = "yntti"
    sql_user: str = "sa"
    sql_password: str = "123456"
    sql_driver: str = "auto"
    sql_trust_server_certificate: bool = True
    sql_schema: str = "dbo"

    # ═══════════════════════════════════════════════════════════════════════════
    # TABLE NAMES
    # ═══════════════════════════════════════════════════════════════════════════
    ccu_table: str = "TT_WO_RPARAM_RECORD_CCU"
    fpc_table: str = "TT_WO_RPARAM_RECORD_FPC"
    tray_event_norm_table: str = "WIP_TRAY_EVENT_NORM"
    trolley_session_table: str = "WIP_TROLLEY_SESSION"
    tray_timeline_table: str = "WIP_TRAY_TIMELINE"
    tray_trolley_map_table: str = "WIP_TRAY_TROLLEY_MAP"
    record_filter_keyword: str = ""

    # ═══════════════════════════════════════════════════════════════════════════
    # COLUMN MAPPINGS
    # ═══════════════════════════════════════════════════════════════════════════
    tray_id_column: str = "MANUFACTURE_CODE"
    collected_time_column: str = "COLLECTED_TIME"
    update_time_column: str = "COLLECTED_TIME"
    ccu_lot_column: str = "LOT_NO"
    ccu_start_column: str = "START_TIME"
    ccu_end_column: str = "END_TIME"
    ccu_record_json_column: str = "RECORD_JSON"
    fpc_lot_column: str = "LOT_NO"
    fpc_start_column: str = "START_TIME"
    fpc_end_column: str = "END_TIME"
    fpc_record_json_column: str = "RECORD_JSON"
    ccu_json_tray_id_key: str = "TRAYID"
    ccu_json_pos_key: str = "TRAY_POS_NO"
    fpc_json_tray_id_key: str = "Tray_barcode"
    fpc_json_pos_key: str = "Cell_position"

    # ═══════════════════════════════════════════════════════════════════════════
    # TRAY ID EXTRACTION — JSON_VALUE vs LIKE scan
    # ═══════════════════════════════════════════════════════════════════════════
    # MANUFACTURE_CODE contains machine codes (e.g. 'RMP1.06092ASY01'),
    # NOT tray IDs. Tray ID is in RECORD_JSON $.TRAYID.paramValue.
    #
    # Options:
    #   - "" (empty): JSON_VALUE() extraction (SQL Server 2016+, recommended)
    #   - "LIKE": RECORD_JSON LIKE '%tray_id%' scan (slow)
    #   - "<column>": Indexed computed column (fastest, requires DBA setup)
    # ═══════════════════════════════════════════════════════════════════════════
    ccu_tray_id_db_column: str = Field(
        default="",
        description=(
            "Strategy for tray ID lookups in CCU table. "
            "Empty = JSON_VALUE(). 'LIKE' = LIKE scan. "
            "Or a computed column name."
        ),
    )
    fpc_tray_id_db_column: str = Field(
        default="",
        description="Strategy for tray ID lookups in FPC table.",
    )
    ccu_json_tray_id_path: str = Field(
        default="$.TRAYID.paramValue",
        description="JSON path for CCU tray ID extraction",
    )
    fpc_json_tray_id_path: str = Field(
        default="$.Tray_barcode.paramValue",
        description="JSON path for FPC tray ID extraction",
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # FPC ROW FILTERING
    # ═══════════════════════════════════════════════════════════════════════════
    fpc_filter_is_his: bool = Field(
        default=False,
        description="Filter FPC rows to IS_HIS='0'. Disabled: 100%% active.",
    )
    fpc_is_his_column: str = "IS_HIS"
    fpc_is_his_active_value: str = "0"
    fpc_filter_del_flag: bool = Field(
        default=False,
        description="Exclude DEL_FLAG='1'. Disabled: 0 deleted rows.",
    )
    fpc_del_flag_column: str = "DEL_FLAG"
    fpc_del_flag_deleted_value: str = "1"

    # ═══════════════════════════════════════════════════════════════════════════
    # UI DATA WINDOW — Today Only Mode (Option B)
    # ═══════════════════════════════════════════════════════════════════════════
    # Show tray if: CCU_time >= today_00:00 OR FPC_time >= today_00:00
    #
    # CCU needs extra lookback to cover overnight aging:
    #   Tray CCU at 22:00 yesterday → 5h aging → FPC at 03:00 today
    #   FPC is today ✅ → show tray
    #   But CCU record is at 18:00-22:00 yesterday → need buffer
    #
    # Fetch windows:
    #   FPC: [today_00:00, now]
    #   CCU: [today_00:00 - buffer, now]
    #
    # UI filter after fetch:
    #   Show if ccu_time >= today_00:00 OR fpc_time >= today_00:00
    # ═══════════════════════════════════════════════════════════════════════════
    ui_show_today_only: bool = Field(
        default=True,
        description=(
            "When True, UI shows trays with CCU >= today OR FPC >= today. "
            "CCU fetches extra buffer hours before midnight to cover aging."
        ),
    )
    ccu_today_lookback_buffer_hours: float = Field(
        default=6.0,
        ge=1.0,
        le=12.0,
        description=(
            "Extra CCU lookback before midnight when ui_show_today_only=True. "
            "Covers overnight aging: 5h max aging + 1h safety = 6h. "
            "Tray with CCU at 22:00 yesterday + 5h aging → FPC 03:00 today "
            "requires CCU fetch from 18:00 yesterday."
        ),
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # DATA WINDOW SETTINGS — Rolling Mode (when today_only=False)
    # ═══════════════════════════════════════════════════════════════════════════
    initial_load_start_hour: int = 0
    initial_load_lookback_hours: float = Field(
        default=0.0,
        ge=0.0,
        le=8784.0,
        description=(
            "Manual lookback override in hours. "
            "Set 0 for auto-calculation: "
            "  Today mode: hours_since_midnight + ccu_buffer. "
            "  Rolling mode: ui_data_window_days * 24 + 2."
        ),
    )
    ui_data_window_days: int = Field(default=1, ge=1, le=365)

    # ═══════════════════════════════════════════════════════════════════════════
    # FPC INITIAL LOAD TIMEOUT
    # ═══════════════════════════════════════════════════════════════════════════
    fpc_initial_load_timeout_seconds: float = Field(
        default=120.0,
        ge=30.0,
        le=1800.0,
        description=(
            "Hard timeout for FPC initial load. "
            "If exceeded, publishes CCU-only data and catches up via delta."
        ),
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # POLL & REFRESH SETTINGS
    # ═══════════════════════════════════════════════════════════════════════════
    delta_poll_interval_seconds: float = Field(default=10.0, ge=0.5, le=60.0)
    delta_poll_idle_interval_seconds: float = Field(default=30.0, ge=1.0, le=300.0)
    delta_overlap_seconds: float = Field(default=5.0, ge=0.0, le=120.0)

    initial_partial_publish_min_changed: int = Field(default=50, ge=1, le=5000)
    initial_partial_publish_max_interval_seconds: float = Field(
        default=3.0, ge=0.1, le=60.0
    )

    peek_latest_cache_ttl_seconds: float = Field(default=20.0, ge=0.0, le=120.0)
    refresh_peek_enabled: bool = True
    refresh_peek_disable_for_legacy_driver: bool = True

    # ═══════════════════════════════════════════════════════════════════════════
    # CCU BACKFILL SETTINGS
    # ═══════════════════════════════════════════════════════════════════════════
    ccu_backfill_cooldown_seconds: float = Field(default=20.0, ge=0.5, le=300.0)
    ccu_backfill_retry_seconds: float = Field(default=60.0, ge=5.0, le=600.0)
    ccu_backfill_allow_targeted_lookup: bool = True
    ccu_backfill_targeted_first_max_trays: int = Field(default=100, ge=1, le=2000)
    ccu_backfill_targeted_batch_enabled: bool = True
    ccu_backfill_targeted_batch_max_trays: int = Field(default=50, ge=2, le=256)
    ccu_backfill_targeted_batch_rows_per_tray: int = Field(default=50, ge=20, le=400)
    ccu_backfill_targeted_batch_max_rows: int = Field(default=5000, ge=500, le=100000)
    ccu_backfill_lookback_hours: float = Field(default=24.0, ge=1.0, le=8784.0)
    ccu_backfill_step_hours: float = Field(default=2.0, ge=0.25, le=24.0)

    ccu_backfill_max_attempts: int = Field(
        default=5, ge=1, le=50,
        description="Max backfill attempts per tray before giving up.",
    )
    ccu_backfill_give_up_minutes: float = Field(
        default=15.0, ge=1.0, le=120.0,
        description="Give up on tray's CCU backfill after this many minutes.",
    )
    ccu_backfill_backoff_base_seconds: float = Field(
        default=30.0, ge=5.0, le=300.0,
        description="Base seconds for exponential backoff.",
    )
    ccu_backfill_backoff_max_seconds: float = Field(
        default=300.0, ge=30.0, le=1800.0,
        description="Max seconds between backfill retries.",
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SQL PERFORMANCE SETTINGS
    # ═══════════════════════════════════════════════════════════════════════════
    sql_query_timeout_seconds: int = Field(default=60, ge=2, le=300)
    sql_max_concurrent_queries: int = Field(
        default=4, ge=1, le=32,
        description="Max concurrent SQL queries (lanes for legacy driver).",
    )
    sql_allow_legacy_parallel_queries: bool = Field(
        default=True,
        description="Allow parallel queries with legacy SQL Server driver.",
    )
    sql_slow_query_threshold_ms: int = Field(default=2000, ge=100, le=30000)

    # ═══════════════════════════════════════════════════════════════════════════
    # CACHE SETTINGS
    # ═══════════════════════════════════════════════════════════════════════════
    tray_detail_cache_ttl_seconds: float = Field(default=600.0, ge=0.0, le=3600.0)
    tray_detail_cache_max_entries: int = Field(default=1000, ge=0, le=5000)

    local_cache_enabled: bool = Field(default=True)
    local_cache_dir: str = Field(default="")
    local_cache_max_entries: int = Field(default=5000, ge=100, le=50000)
    local_cache_ttl_hours: float = Field(default=24.0, ge=1.0, le=168.0)

    tray_detail_narrow_window_hours: float = Field(default=2.0, ge=0.5, le=24.0)
    tray_detail_medium_window_hours: float = Field(default=8.0, ge=1.0, le=48.0)
    tray_detail_narrow_min_results: int = Field(default=10, ge=1, le=100)

    # ═══════════════════════════════════════════════════════════════════════════
    # PERFORMANCE SETTINGS
    # ═══════════════════════════════════════════════════════════════════════════
    ccu_backfill_targeted_workers: int = Field(default=2, ge=1, le=64)
    refresh_skip_while_backfill: bool = True
    initial_fpc_publish_requires_ccu: bool = True

    max_parallel_workers: int = Field(default=2, ge=1, le=64)
    max_fetch_batch: int = Field(default=10000, ge=100, le=200000)
    event_queue_size: int = Field(default=5000, ge=1000, le=500000)
    ui_snapshot_limit: int = Field(default=10000, ge=100, le=200000)

    state_store_max_trays: int = Field(default=50000, ge=1000, le=500000)
    state_store_memory_check_interval: int = Field(default=500, ge=100, le=10000)
    payload_intern_enabled: bool = Field(default=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # UI COALESCING SETTINGS
    # ═══════════════════════════════════════════════════════════════════════════
    ui_coalesce_window_ms: int = Field(default=150, ge=5, le=1000)
    ui_coalesce_max_batch: int = Field(default=50, ge=10, le=5000)

    # ═══════════════════════════════════════════════════════════════════════════
    # TROLLEY & GROUPING SETTINGS
    # ═══════════════════════════════════════════════════════════════════════════
    trolley_max_trays: int = Field(default=6, ge=1, le=200)
    target_aging_hours: float = Field(default=4.0, ge=0.1, le=72.0)
    target_aging_tolerance_hours: float = Field(default=1.0, ge=0.0, le=24.0)
    assembly_auto_trolley_count: int = Field(default=1, ge=1, le=20)
    auto_group_default_enabled: bool = False
    auto_group_gap_minutes: int = Field(default=10, ge=0, le=240)
    total_trolley_count: int = Field(default=99, ge=1, le=999)

    # ═══════════════════════════════════════════════════════════════════════════
    # SNAPSHOT & PERSISTENCE
    # ═══════════════════════════════════════════════════════════════════════════
    snapshot_file: str = ".state_snapshot.json"

    # ═══════════════════════════════════════════════════════════════════════════
    # HELPER METHODS
    # ═══════════════════════════════════════════════════════════════════════════

    def build_odbc_dsn(self, driver: str | None = None) -> str:
        selected_driver = (driver or self.sql_driver).strip()
        dsn = (
            f"DRIVER={{{selected_driver}}};"
            f"SERVER={self.sql_server},{self.sql_port};"
            f"DATABASE={self.sql_database};"
            f"UID={self.sql_user};"
            f"PWD={self.sql_password};"
        )
        if "ODBC Driver" in selected_driver and "SQL Server" in selected_driver:
            trust = "yes" if self.sql_trust_server_certificate else "no"
            dsn += f"TrustServerCertificate={trust};"
        return dsn

    def calculate_load_windows(self) -> tuple[datetime, datetime, datetime]:
        """
        Calculate (ccu_start, fpc_start, end_time) for initial load.

        Today mode (ui_show_today_only=True):
          FPC: [today 00:00, now]
          CCU: [today 00:00 - buffer, now]

          Buffer covers overnight aging:
            CCU 22:00 yesterday + 5h aging → FPC 03:00 today
            Need CCU from 18:00 yesterday (6h buffer)

        Rolling mode (ui_show_today_only=False):
          Both: [now - lookback_hours, now]
        """
        now = datetime.now()

        if self.ui_show_today_only:
            today_start = datetime.combine(now.date(), time.min)
            fpc_start = today_start
            ccu_start = today_start - timedelta(
                hours=self.ccu_today_lookback_buffer_hours
            )
            return ccu_start, fpc_start, now

        # Rolling mode
        if self.initial_load_lookback_hours > 0:
            lookback = self.initial_load_lookback_hours
        else:
            lookback = float(self.ui_data_window_days * 24 + 2)

        start = now - timedelta(hours=lookback)
        return start, start, now

    def get_today_start(self) -> datetime:
        """Returns today 00:00:00 for UI display filtering."""
        return datetime.combine(datetime.now().date(), time.min)

    def get_effective_lookback_hours(self, window_days: int | None = None) -> float:
        days = window_days if window_days is not None else self.ui_data_window_days
        days = max(1, min(365, days))
        return float(days * 24 + 2)

    def get_local_cache_path(self) -> Path:
        if self.local_cache_dir:
            cache_dir = Path(self.local_cache_dir)
        else:
            cache_dir = Path(tempfile.gettempdir()) / "wip_management_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    def get_tray_detail_time_tiers(self) -> list[tuple[float, int]]:
        return [
            (self.tray_detail_narrow_window_hours, 200),
            (self.tray_detail_medium_window_hours, 500),
            (self.initial_load_lookback_hours, 1000),
        ]

    def get_ccu_backfill_next_delay(self, attempt: int) -> float:
        delay = self.ccu_backfill_backoff_base_seconds * (2.0 ** attempt)
        return min(delay, self.ccu_backfill_backoff_max_seconds)

    def to_performance_summary(self) -> dict[str, object]:
        ccu_start, fpc_start, end = self.calculate_load_windows()
        return {
            "sql_max_concurrent_queries": self.sql_max_concurrent_queries,
            "sql_query_timeout_seconds": self.sql_query_timeout_seconds,
            "max_fetch_batch": self.max_fetch_batch,
            "max_parallel_workers": self.max_parallel_workers,
            "delta_poll_interval_seconds": self.delta_poll_interval_seconds,
            "tray_detail_cache_ttl_seconds": self.tray_detail_cache_ttl_seconds,
            "tray_detail_cache_max_entries": self.tray_detail_cache_max_entries,
            "local_cache_enabled": self.local_cache_enabled,
            "local_cache_ttl_hours": self.local_cache_ttl_hours,
            "payload_intern_enabled": self.payload_intern_enabled,
            "ui_coalesce_window_ms": self.ui_coalesce_window_ms,
            "fpc_filter_is_his": self.fpc_filter_is_his,
            "fpc_filter_del_flag": self.fpc_filter_del_flag,
            "fpc_initial_load_timeout_seconds": self.fpc_initial_load_timeout_seconds,
            "ccu_tray_id_db_column": self.ccu_tray_id_db_column or "JSON_VALUE",
            "ccu_json_tray_id_path": self.ccu_json_tray_id_path,
            "ccu_backfill_max_attempts": self.ccu_backfill_max_attempts,
            "ccu_backfill_give_up_minutes": self.ccu_backfill_give_up_minutes,
            # Today mode info
            "ui_show_today_only": self.ui_show_today_only,
            "ccu_today_lookback_buffer_hours": self.ccu_today_lookback_buffer_hours,
            "load_window_ccu_start": ccu_start.isoformat(),
            "load_window_fpc_start": fpc_start.isoformat(),
            "load_window_end": end.isoformat(),
        }


# ═══════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════════════════
settings = Settings()


def log_settings_summary() -> None:
    import logging
    _log = logging.getLogger(__name__)
    _log.info("Performance settings: %s", settings.to_performance_summary())
    _log.info("Cache directory: %s", settings.get_local_cache_path())