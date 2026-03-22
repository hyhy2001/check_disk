"""
Base Formatter Module

Contains the base formatter class with common formatting functionality.
"""

import shutil
from typing import Dict, Any, List, Optional, Tuple
from src.utils import create_usage_bar as _create_usage_bar_util


class BaseFormatter:
    """Base class with common formatting functionality."""

    def __init__(self):
        """Initialize the formatter."""
        self.terminal_width, self.terminal_height = shutil.get_terminal_size((80, 24))

    def _truncate_text(self, text: str, width: int) -> str:
        """Truncate text to fit within width."""
        if len(text) <= width:
            return text
        return text[:width-3] + "..."

    def _ljust_text(self, text: str, width: int) -> str:
        """Left-justify text to width."""
        padding = width - len(text)
        if padding <= 0:
            return text
        return text + " " * padding

    def _center_text(self, text: str, width: int) -> str:
        """Center text within width."""
        padding = width - len(text)
        if padding <= 0:
            return text
        left_pad = padding // 2
        right_pad = padding - left_pad
        return " " * left_pad + text + " " * right_pad

    def _create_usage_bar(self, percent: float, width: int = 20) -> str:
        """Create a visual bar representing disk usage percentage."""
        return _create_usage_bar_util(percent, width)