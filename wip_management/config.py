from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "WIP Management"
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    log_file: str = "wip_management.log"
    log_to_console: bool = True
    log_sql_preview_chars: int = Field(default=220, ge=80, le=1200)
    shared_state_enabled: bool = True
    shared_state_dir: str = (
        r"C:\Users\PROMDNGUYEN02\OneDrive - Techtronic Industries Co. Ltd\AES_Mass production - WIP Management"
    )
    shared_state_file: str = "grouping_state.json"
    grouping_sync_interval_seconds: float = Field(default=3.0, ge=0.5, le=120.0)

    sql_server: str = "10.148.144.75"
    sql_port: int = 1433
    sql_database: str = "yntti"
    sql_user: str = "sa"
    sql_password: str = "123456"
    sql_driver: str = "SQL Server"
    sql_trust_server_certificate: bool = True
    sql_schema: str = "dbo"
    ccu_table: str = "TT_WO_RPARAM_RECORD_CCU"
    fpc_table: str = "TT_WO_RPARAM_RECORD_FPC"
    record_filter_keyword: str = ""

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

    initial_load_start_hour: int = 0
    delta_poll_interval_seconds: float = Field(default=1.0, ge=0.1)
    delta_poll_idle_interval_seconds: float = Field(default=15.0, ge=1.0, le=300.0)
    delta_overlap_seconds: float = Field(default=2.0, ge=0.0, le=120.0)
    ccu_backfill_cooldown_seconds: float = Field(default=10.0, ge=0.5, le=300.0)
    sql_query_timeout_seconds: int = Field(default=15, ge=2, le=300)
    sql_max_concurrent_queries: int = Field(default=1, ge=1, le=16)
    tray_detail_cache_ttl_seconds: float = Field(default=120.0, ge=0.0, le=3600.0)
    tray_detail_cache_max_entries: int = Field(default=128, ge=0, le=2000)

    max_parallel_workers: int = Field(default=8, ge=1, le=64)
    max_fetch_batch: int = Field(default=5000, ge=100, le=100000)
    event_queue_size: int = Field(default=20000, ge=1000, le=500000)
    ui_snapshot_limit: int = Field(default=50000, ge=100, le=200000)
    ui_coalesce_window_ms: int = Field(default=40, ge=5, le=1000)
    ui_coalesce_max_batch: int = Field(default=300, ge=10, le=5000)
    trolley_max_trays: int = Field(default=6, ge=1, le=200)
    target_aging_hours: float = Field(default=4.0, ge=0.1, le=72.0)
    target_aging_tolerance_hours: float = Field(default=1.0, ge=0.0, le=24.0)
    assembly_auto_trolley_count: int = Field(default=1, ge=1, le=20)
    auto_group_default_enabled: bool = False

    snapshot_file: str = ".state_snapshot.json"

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


settings = Settings()
