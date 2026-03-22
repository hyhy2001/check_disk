"""
Formatter Module (DEPRECATED)

This compatibility shim is kept for backward compatibility.
Import directly from src.formatters.* instead.
"""

from src.formatters.base_formatter import BaseFormatter
from src.formatters.table_formatter import TableFormatter
from src.formatters.report_formatter import ReportFormatter

__all__ = ['BaseFormatter', 'TableFormatter', 'ReportFormatter']