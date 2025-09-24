package com.excelutility.gui;

import com.excelutility.core.ComparisonResult;
import com.excelutility.core.RowResult;
import com.excelutility.core.RowComparisonStatus;

import javax.swing.table.AbstractTableModel;
import java.util.Collections;
import java.util.List;

public class ResultTableModel extends AbstractTableModel {

    private ComparisonResult comparisonResult;

    public ResultTableModel() {
        // Initialize with an empty result to avoid null checks everywhere
        this.comparisonResult = new ComparisonResult(Collections.emptyList(), Collections.emptyList());
    }

    public void setComparisonResult(ComparisonResult result) {
        this.comparisonResult = (result != null) ? result : new ComparisonResult(Collections.emptyList(), Collections.emptyList());
        fireTableStructureChanged();
    }

    public ComparisonResult getComparisonResult() {
        return this.comparisonResult;
    }

    public void clear() {
        setComparisonResult(null);
    }

    @Override
    public int getRowCount() {
        return comparisonResult.getRowResults().size();
    }

    @Override
    public int getColumnCount() {
        return comparisonResult.getFinalHeaders().size();
    }

    @Override
    public String getColumnName(int column) {
        return comparisonResult.getFinalHeaders().get(column);
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        RowResult rowResult = getRowResult(rowIndex);
        if (rowResult == null) return "";

        // Display data from source, or target if source is null (i.e., for TARGET_ONLY rows)
        List<Object> data = rowResult.getStatus() == RowComparisonStatus.TARGET_ONLY
                            ? rowResult.getTargetRowData()
                            : rowResult.getSourceRowData();

        if (data != null && columnIndex < data.size()) {
            return data.get(columnIndex);
        }
        return "";
    }

    public RowResult getRowResult(int rowIndex) {
        if (comparisonResult != null && rowIndex >= 0 && rowIndex < comparisonResult.getRowResults().size()) {
            return comparisonResult.getRowResults().get(rowIndex);
        }
        return null;
    }
}
