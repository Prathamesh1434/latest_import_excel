package com.excelutility.gui;

import javax.swing.table.AbstractTableModel;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table model for displaying generated test cases.
 */
public class TestCaseTableModel extends AbstractTableModel {

    private final List<String> columnNames = List.of("ID", "Description", "Source File", "Target File");
    private final List<Map<String, Object>> testCases = new ArrayList<>();

    public void addTestCase(Map<String, Object> testCase) {
        testCases.add(testCase);
        fireTableRowsInserted(testCases.size() - 1, testCases.size() - 1);
    }

    public void clear() {
        testCases.clear();
        fireTableDataChanged();
    }

    @Override
    public int getRowCount() {
        return testCases.size();
    }

    @Override
    public int getColumnCount() {
        return columnNames.size();
    }

    @Override
    public String getColumnName(int column) {
        return columnNames.get(column);
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        Map<String, Object> testCase = testCases.get(rowIndex);
        switch (columnIndex) {
            case 0:
                return testCase.get("id");
            case 1:
                return testCase.get("description");
            case 2:
                return new File(testCase.get("sourceFile").toString()).getName();
            case 3:
                return new File(testCase.get("targetFile").toString()).getName();
            default:
                return null;
        }
    }
}
