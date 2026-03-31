# -*- coding: utf-8 -*-
"""
===================================
日志配置模块 - 统一的日志系统初始化
===================================

职责：
1. 提供统一的日志格式和配置常量
2. 支持控制台 + 文件（常规/调试）三层日志输出
3. 自动降低第三方库日志级别
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional


LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(pathname)s:%(lineno)d | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 强制使用 Asia/Shanghai (UTC+8) 时区，避免云端日志显示 UTC 时间
_SHANGHAI_TZ = timezone(timedelta(hours=8))


class RelativePathFormatter(logging.Formatter):
    """自定义 Formatter：相对路径 + 强制 Asia/Shanghai (UTC+8) 时间戳"""

    def __init__(self, fmt=None, datefmt=None, relative_to=None):
        super().__init__(fmt, datefmt)
        self.relative_to = Path(relative_to) if relative_to else Path.cwd()

    def formatTime(self, record: logging.LogRecord, datefmt=None) -> str:
        """覆盖时间格式化，强制输出 UTC+8 时间而非系统 UTC 时间。"""
        ct = datetime.fromtimestamp(record.created, tz=_SHANGHAI_TZ)
        if datefmt:
            return ct.strftime(datefmt)
        return ct.strftime(LOG_DATE_FORMAT)

    def format(self, record):
        # 将绝对路径转为相对路径
        try:
            record.pathname = str(Path(record.pathname).relative_to(self.relative_to))
        except ValueError:
            # 如果无法转换为相对路径，保持原样
            pass
        return super().format(record)



# 默认需要降低日志级别的第三方库
DEFAULT_QUIET_LOGGERS = [
    'urllib3',
    'sqlalchemy',
    'google',
    'httpx',
]
LOG_RETENTION_DAYS = 3


def _cleanup_old_logs(log_path: Path, retention_days: int = LOG_RETENTION_DAYS) -> None:
    """删除 retention_days 之前的历史日志文件。"""
    if retention_days <= 0 or not log_path.exists():
        return

    cutoff = datetime.now() - timedelta(days=retention_days)
    for candidate in log_path.glob("*.log*"):
        try:
            modified_at = datetime.fromtimestamp(candidate.stat().st_mtime)
        except OSError:
            continue
        if modified_at < cutoff:
            try:
                candidate.unlink()
            except OSError:
                logging.getLogger(__name__).warning("删除旧日志失败：%s", candidate)


def setup_logging(
    log_prefix: str = "app",
    log_dir: str = "./logs",
    console_level: Optional[int] = None,
    debug: bool = False,
    extra_quiet_loggers: Optional[List[str]] = None,
) -> None:
    """
    统一的日志系统初始化

    配置三层日志输出：
    1. 控制台：根据 debug 参数或 console_level 设置级别
    2. 常规日志文件：INFO 级别，5MB 轮转，保留 2 个备份
    3. 自动删除 3 天前历史日志文件

    Args:
        log_prefix: 日志文件名前缀（如 "api_server" -> api_server_20240101.log）
        log_dir: 日志文件目录，默认 ./logs
        console_level: 控制台日志级别（可选，优先于 debug 参数）
        debug: 是否启用调试模式（控制台输出 DEBUG 级别）
        extra_quiet_loggers: 额外需要降低日志级别的第三方库列表
    """
    # 确定控制台日志级别
    if console_level is not None:
        level = console_level
    else:
        level = logging.INFO if debug else logging.WARNING

    # 创建日志目录
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    _cleanup_old_logs(log_path)

    # 日志文件路径（按日期分文件）
    today_str = datetime.now().strftime('%Y%m%d')
    log_file = log_path / f"{log_prefix}_{today_str}.log"

    # 配置根 logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 清除已有 handler，避免重复添加
    if root_logger.handlers:
        root_logger.handlers.clear()
    # 创建相对路径 Formatter（相对于项目根目录）
    project_root = Path.cwd()
    rel_formatter = RelativePathFormatter(
        LOG_FORMAT, LOG_DATE_FORMAT, relative_to=project_root
    )
    # Handler 1: 控制台输出
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(rel_formatter)
    root_logger.addHandler(console_handler)

    # Handler 2: 常规日志文件（INFO 级别，5MB 轮转）
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=2,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(rel_formatter)
    root_logger.addHandler(file_handler)

    # 降低第三方库的日志级别
    quiet_loggers = DEFAULT_QUIET_LOGGERS.copy()
    if extra_quiet_loggers:
        quiet_loggers.extend(extra_quiet_loggers)

    for logger_name in quiet_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # 输出初始化完成信息（使用相对路径）
    try:
        rel_log_path = log_path.resolve().relative_to(project_root)
    except ValueError:
        rel_log_path = log_path

    try:
        rel_log_file = log_file.resolve().relative_to(project_root)
    except ValueError:
        rel_log_file = log_file

    logging.info(f"日志系统初始化完成，日志目录: {rel_log_path}")
    logging.info(f"常规日志: {rel_log_file}")
