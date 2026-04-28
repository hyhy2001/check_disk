"""
Table Formatter Module

Contains the TableFormatter class for creating and displaying tables.
"""

from typing import Dict, List, Optional

from .base_formatter import BaseFormatter


class TableFormatter(BaseFormatter):
    """Helper class for formatting and displaying tables."""

    def format_table(self, headers: List[str], rows: List[List[str]],
                    title: Optional[str] = None, max_width: Optional[int] = None) -> str:
        """
        Format data as a table using ASCII grid style.

        Args:
            headers: List of column headers
            rows: List of rows, each row is a list of strings
            title: Optional title for the table
            max_width: Maximum width for the table (defaults to terminal width)

        Returns:
            Formatted table as a string
        """
        if max_width is None:
            max_width = self.terminal_width

        # Calculate and adjust column widths
        col_widths = self._calculate_column_widths(headers, rows, max_width)

        # Build the table components
        table_components = self._build_table_components(col_widths)

        # Assemble the table
        return self._assemble_table(headers, rows, col_widths, table_components, title)

    def _calculate_column_widths(self, headers: List[str], rows: List[List[str]], max_width: int) -> List[int]:
        """Calculate and adjust column widths based on content and terminal constraints."""
        # Calculate initial column widths
        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, cell in enumerate(row):
                if i < len(col_widths):
                    col_widths[i] = max(col_widths[i], len(str(cell)))

        # Adjust column widths if the table is too wide
        total_width = sum(col_widths) + (3 * len(headers)) - 1
        if total_width > max_width:
            # Calculate how much we need to reduce
            excess = total_width - max_width

            # Distribute the reduction proportionally among columns
            total_width_original = total_width
            for i in range(len(col_widths)):
                proportion = col_widths[i] / total_width_original
                reduction = int(excess * proportion)
                col_widths[i] = max(5, col_widths[i] - reduction)  # Ensure minimum width of 5

            # Recalculate total width
            total_width = sum(col_widths) + (3 * len(headers)) - 1

            # If still too wide, reduce the last column more
            if total_width > max_width and len(col_widths) > 0:
                col_widths[-1] = max(5, col_widths[-1] - (total_width - max_width))

        return col_widths

    def _build_table_components(self, col_widths: List[int]) -> Dict[str, str]:
        """Build the ASCII table components."""
        # ASCII grid style characters
        horizontal = "-"
        vertical = "|"
        top_left = "+"
        top_right = "+"
        bottom_left = "+"
        bottom_right = "+"
        top_t = "+"
        bottom_t = "+"
        left_t = "+"
        right_t = "+"
        cross = "+"

        # Table borders
        top_border = top_left + top_t.join(horizontal * (w + 2) for w in col_widths) + top_right
        header_sep = left_t + cross.join(horizontal * (w + 2) for w in col_widths) + right_t
        bottom_border = bottom_left + bottom_t.join(horizontal * (w + 2) for w in col_widths) + bottom_right

        return {
            "top_border": top_border,
            "header_sep": header_sep,
            "bottom_border": bottom_border,
            "vertical": vertical
        }

    def _assemble_table(self, headers: List[str], rows: List[List[str]],
                       col_widths: List[int], components: Dict[str, str], title: Optional[str]) -> str:
        """Assemble the final table string."""
        result = []

        # Add title if provided
        if title:
            # Create a centered title
            title_text = f" {title} "
            title_len = len(title_text)
            total_table_width = len(components["top_border"])
            padding = (total_table_width - title_len) // 2
            result.append(f"+{'-' * padding}{title_text}{'-' * (total_table_width - padding - title_len - 2)}+")
        else:
            result.append(components["top_border"])

        # Add header row
        header_cells = []
        for i, h in enumerate(headers):
            header_str = self._center_text(h, col_widths[i])
            header_cells.append(f" {header_str} ")
        result.append(f"{components['vertical']}{components['vertical'].join(header_cells)}{components['vertical']}")

        # Add header separator
        result.append(components["header_sep"])

        # Add data rows
        for row in rows:
            row_cells = []
            for i, cell in enumerate(row):
                if i < len(col_widths):
                    cell_str = str(cell)
                    cell_str = self._truncate_text(cell_str, col_widths[i])
                    cell_str = self._ljust_text(cell_str, col_widths[i])
                    row_cells.append(f" {cell_str} ")
            result.append(f"{components['vertical']}{components['vertical'].join(row_cells)}{components['vertical']}")

        # Add bottom border
        result.append(components["bottom_border"])

        return "\n".join(result)

    def _create_multicolumn_rows(self, items: List[str], num_rows: int, num_columns: int) -> List[List[str]]:
        """Create rows for a multi-column display with sequential numbering."""
        rows = []
        for i in range(num_rows):
            row = []
            for j in range(num_columns):
                idx = j * num_rows + i  # This creates a top-to-bottom ordering
                if idx < len(items):
                    # Add sequential number before item
                    row.append(f"{idx+1}. {items[idx]}")
                else:
                    row.append("")
            rows.append(row)
        return rows
