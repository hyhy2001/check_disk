"""
Formatter Module (DEPRECATED)

This compatibility shim is kept for backward compatibility.
Import directly from .formatters.* instead.
"""

from .formatters.base_formatter import BaseFormatter
from .formatters.report_formatter import ReportFormatter
from .formatters.table_formatter import TableFormatter

__all__ = ['BaseFormatter', 'TableFormatter', 'ReportFormatter']
