-- strategy_open_monitor_eval
CREATE TABLE `strategy_open_monitor_eval` (
  `monitor_date` date NOT NULL,
  `sig_date` date NOT NULL,
  `run_id` varchar(64) NOT NULL,
  `asof_trade_date` date DEFAULT NULL,
  `live_trade_date` date DEFAULT NULL,
  `signal_age` int(11) DEFAULT NULL,
  `valid_days` int(11) DEFAULT NULL,
  `code` varchar(64) NOT NULL,
  `live_gap_pct` double DEFAULT NULL,
  `live_pct_change` double DEFAULT NULL,
  `live_intraday_vol_ratio` double DEFAULT NULL,
  `dev_ma5` double DEFAULT NULL,
  `dev_ma20` double DEFAULT NULL,
  `dev_ma5_atr` double DEFAULT NULL,
  `dev_ma20_atr` double DEFAULT NULL,
  `runup_from_sigclose` double DEFAULT NULL,
  `runup_from_sigclose_atr` double DEFAULT NULL,
  `runup_ref_price` double DEFAULT NULL,
  `runup_ref_source` varchar(32) DEFAULT NULL,
  `entry_exposure_cap` double DEFAULT NULL,
  `env_index_score` double DEFAULT NULL,
  `env_regime` varchar(32) DEFAULT NULL,
  `env_position_hint` double DEFAULT NULL,
  `env_final_gate_action` varchar(16) DEFAULT NULL,
  `env_index_snapshot_hash` varchar(64) DEFAULT NULL,
  `signal_strength` double DEFAULT NULL,
  `strength_delta` double DEFAULT NULL,
  `strength_trend` varchar(16) DEFAULT NULL,
  `strength_note` varchar(512) DEFAULT NULL,
  `signal_kind` varchar(16) DEFAULT NULL,
  `sig_signal` varchar(16) DEFAULT NULL,
  `sig_reason` varchar(255) DEFAULT NULL,
  `state` varchar(32) DEFAULT NULL,
  `status_reason` varchar(255) DEFAULT NULL,
  `action` varchar(16) DEFAULT NULL,
  `action_reason` varchar(255) DEFAULT NULL,
  `rule_hits_json` text,
  `summary_line` varchar(512) DEFAULT NULL,
  `risk_tag` varchar(255) DEFAULT NULL,
  `risk_note` varchar(255) DEFAULT NULL,
  `checked_at` datetime(6) DEFAULT NULL,
  `snapshot_hash` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`monitor_date`,`sig_date`,`code`,`run_id`),
  UNIQUE KEY `ux_open_monitor_run` (`monitor_date`,`sig_date`,`code`,`run_id`),
  KEY `idx_open_monitor_strength_time` (`monitor_date`,`code`,`checked_at`),
  KEY `idx_open_monitor_code_time` (`code`,`checked_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- strategy_open_monitor_quote
CREATE TABLE `strategy_open_monitor_quote` (
  `monitor_date` date NOT NULL,
  `run_id` varchar(64) NOT NULL,
  `code` varchar(20) NOT NULL,
  `live_trade_date` date DEFAULT NULL,
  `live_open` double DEFAULT NULL,
  `live_high` double DEFAULT NULL,
  `live_low` double DEFAULT NULL,
  `live_latest` double DEFAULT NULL,
  `live_volume` double DEFAULT NULL,
  `live_amount` double DEFAULT NULL,
  PRIMARY KEY (`monitor_date`,`run_id`,`code`),
  UNIQUE KEY `ux_open_monitor_quote_run` (`monitor_date`,`run_id`,`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- strategy_open_monitor_env
CREATE TABLE `strategy_open_monitor_env` (
  `monitor_date` date NOT NULL,
  `run_id` varchar(64) NOT NULL,
  `checked_at` datetime(6) DEFAULT NULL,
  `env_weekly_asof_trade_date` date DEFAULT NULL,
  `env_weekly_risk_level` varchar(16) DEFAULT NULL,
  `env_weekly_scene` varchar(32) DEFAULT NULL,
  `env_weekly_gate_action` varchar(16) DEFAULT NULL,
  `env_weekly_gate_policy` varchar(16) DEFAULT NULL,
  `env_final_gate_action` varchar(16) DEFAULT NULL,
  `env_index_snapshot_hash` varchar(32) DEFAULT NULL,
  `env_final_cap_pct` double DEFAULT NULL,
  `env_final_reason_json` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`monitor_date`,`run_id`),
  UNIQUE KEY `ux_env_snapshot_run` (`monitor_date`,`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- strategy_env_index_snapshot
CREATE TABLE `strategy_env_index_snapshot` (
  `snapshot_hash` varchar(32) NOT NULL,
  `monitor_date` date DEFAULT NULL,
  `run_id` varchar(64) DEFAULT NULL,
  `checked_at` datetime(6) DEFAULT NULL,
  `index_code` varchar(16) DEFAULT NULL,
  `asof_trade_date` date DEFAULT NULL,
  `live_trade_date` date DEFAULT NULL,
  `asof_close` double DEFAULT NULL,
  `asof_ma20` double DEFAULT NULL,
  `asof_ma60` double DEFAULT NULL,
  `asof_macd_hist` double DEFAULT NULL,
  `asof_atr14` double DEFAULT NULL,
  `live_open` double DEFAULT NULL,
  `live_high` double DEFAULT NULL,
  `live_low` double DEFAULT NULL,
  `live_latest` double DEFAULT NULL,
  `live_pct_change` double DEFAULT NULL,
  `live_volume` double DEFAULT NULL,
  `live_amount` double DEFAULT NULL,
  `dev_ma20_atr` double DEFAULT NULL,
  `gate_action` varchar(16) DEFAULT NULL,
  `gate_reason` varchar(255) DEFAULT NULL,
  `position_cap` double DEFAULT NULL,
  PRIMARY KEY (`snapshot_hash`),
  UNIQUE KEY `uk_env_index_snapshot` (`monitor_date`,`run_id`,`index_code`,`checked_at`),
  KEY `idx_env_index_snapshot_run` (`monitor_date`,`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

