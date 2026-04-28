"""
Formatter Module (DEPRECATED)

This compatibility shim is kept for backward compatibility.
Import directly from .formatters.* instead.
"""

from .formatters.base_formatter import BaseFormatter
from .formatters.table_formatter import TableFormatter
from .formatters.report_formatter import ReportFormatter

__all__ = ['BaseFormatter', 'TableFormatter', 'ReportFormatter']