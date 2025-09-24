package com.excelutility.gui;

import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import javax.swing.table.AbstractTableModel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ColumnMappingTableModel extends AbstractTableModel {

    private final String[] columnNames = {"Source Column", "Target Column", "Is Key", "Ignore"};
    private List<Object[]> data = new ArrayList<>();
    private static final double FUZZY_MATCH_THRESHOLD = 0.8;

    public void setSourceColumns(List<String> sourceColumns, List<String> targetColumns) {
        data.clear();
        List<String> availableTargetCols = new ArrayList<>(targetColumns);
        JaroWinklerSimilarity fuzzyMatcher = new JaroWinklerSimilarity();

        // Pass 1: Exact and Trimmed/Case-Insensitive Matches
        for (String sourceCol : sourceColumns) {
            String bestMatch = availableTargetCols.stream()
                    .filter(t -> t.trim().equalsIgnoreCase(sourceCol.trim()))
                    .findFirst()
                    .orElse(null);

            if (bestMatch != null) {
                data.add(new Object[]{sourceCol, bestMatch, false, false});
                availableTargetCols.remove(bestMatch);
            } else {
                // Add placeholder for fuzzy matching pass
                data.add(new Object[]{sourceCol, null, false, false});
            }
        }

        // Pass 2: Fuzzy Matching for remaining columns
        for (Object[] rowData : data) {
            if (rowData[1] == null) { // If not mapped in pass 1
                String sourceCol = (String) rowData[0];
                String bestFuzzyMatch = null;
                double highestScore = 0.0;

                for (String targetCol : availableTargetCols) {
                    double score = fuzzyMatcher.apply(sourceCol.toLowerCase(), targetCol.toLowerCase());
                    if (score > highestScore) {
                        highestScore = score;
                        bestFuzzyMatch = targetCol;
                    }
                }

                if (highestScore > FUZZY_MATCH_THRESHOLD) {
                    rowData[1] = bestFuzzyMatch;
                    availableTargetCols.remove(bestFuzzyMatch);
                }
            }
        }

        fireTableDataChanged();
    }

    public List<Object[]> getMappingData() {
        return data;
    }

    public void setMappings(Map<String, String> mappings, List<String> keyColumns) {
        for (Object[] rowData : data) {
            String sourceColumn = (String) rowData[0];
            rowData[1] = mappings.getOrDefault(sourceColumn, null);
            rowData[2] = keyColumns.contains(sourceColumn);
        }
        fireTableDataChanged();
    }

    @Override
    public int getRowCount() {
        return data.size();
    }

    @Override
    public int getColumnCount() {
        return columnNames.length;
    }

    @Override
    public String getColumnName(int column) {
        return columnNames[column];
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        if (columnIndex == 2 || columnIndex == 3) {
            return Boolean.class;
        }
        return String.class;
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
        return columnIndex > 0;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        return data.get(rowIndex)[columnIndex];
    }

    @Override
    public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
        data.get(rowIndex)[columnIndex] = aValue;
        fireTableCellUpdated(rowIndex, columnIndex);
    }
}
