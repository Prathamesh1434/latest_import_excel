package com.excelutility.gui;

import com.excelutility.core.CellDifference;
import com.excelutility.core.MismatchType;
import com.excelutility.core.RowResult;
import com.excelutility.core.RowComparisonStatus;

import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import java.awt.*;
import java.util.Map;

/**
 * A custom table cell renderer for displaying comparison results with advanced,
 * cell-level color-coding based on the type of mismatch.
 */
public class ResultTableRenderer extends DefaultTableCellRenderer {

    // Define the color palette for different mismatch types
    private static final Color COLOR_NUMERIC_MISMATCH = new Color(255, 255, 153); // Light Yellow
    private static final Color COLOR_STRING_MISMATCH = new Color(255, 179, 179);  // Light Red/Pink
    private static final Color COLOR_BLANK_MISMATCH = new Color(255, 204, 153);   // Orange
    private static final Color COLOR_TYPE_MISMATCH = new Color(221, 179, 255);   // Light Purple

    // Row-level colors
    private static final Color COLOR_MISMATCH_ROW_BG = new Color(255, 255, 230); // A very light yellow for the row background
    private static final Color COLOR_SOURCE_ONLY = new Color(230, 255, 230); // Light Green
    private static final Color COLOR_TARGET_ONLY = new Color(255, 230, 230); // Light Pink

    @Override
    public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
        Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
        c.setFont(new Font("Lucida Sans Unicode", Font.PLAIN, 12));

        if (!(table.getModel() instanceof ResultTableModel)) {
            return c;
        }

        ResultTableModel model = (ResultTableModel) table.getModel();
        RowResult rowResult = model.getRowResult(row);

        // Default state
        c.setBackground(table.getBackground());
        setToolTipText(null);

        if (rowResult == null) {
            return c;
        }

        RowComparisonStatus status = rowResult.getStatus();

        switch (status) {
            case MATCHED_MISMATCHED:
                c.setBackground(COLOR_MISMATCH_ROW_BG);
                Map<Integer, CellDifference> diffs = rowResult.getDifferences();

                // The key in the diffs map is the source column index.
                // We assume the table model's column index aligns, as it doesn't add special columns.
                if (diffs != null && diffs.containsKey(column)) {
                    CellDifference diff = diffs.get(column);

                    // Set cell-specific color based on mismatch type
                    c.setBackground(getColorForMismatch(diff.getMismatchType()));

                    // Set tooltip to show the difference
                    setToolTipText("<html><b>Source:</b> " + escapeHtml(diff.getSourceValue()) + "<br><b>Target:</b> " + escapeHtml(diff.getTargetValue()) + "</html>");
                }
                break;
            case SOURCE_ONLY:
                c.setBackground(COLOR_SOURCE_ONLY);
                break;
            case TARGET_ONLY:
                c.setBackground(COLOR_TARGET_ONLY);
                break;
            case MATCHED_IDENTICAL:
            default:
                // Already set to default background
                break;
        }

        if (isSelected) {
            c.setBackground(table.getSelectionBackground());
        }

        return c;
    }

    private Color getColorForMismatch(MismatchType type) {
        if (type == null) return COLOR_MISMATCH_ROW_BG; // Fallback color
        switch (type) {
            case NUMERIC:
                return COLOR_NUMERIC_MISMATCH;
            case STRING:
                return COLOR_STRING_MISMATCH;
            case BLANK_VS_NON_BLANK:
                return COLOR_BLANK_MISMATCH;
            case TYPE_MISMATCH:
                return COLOR_TYPE_MISMATCH;
            default:
                return COLOR_MISMATCH_ROW_BG;
        }
    }

    private String escapeHtml(Object obj) {
        if (obj == null) return "<i>null</i>";
        // Basic HTML escaping
        return obj.toString()
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}
