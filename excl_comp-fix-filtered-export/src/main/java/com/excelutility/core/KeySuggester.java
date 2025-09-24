package com.excelutility.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A utility to analyze data and suggest potential key columns.
 */
public class KeySuggester {

    public static class KeySuggestion implements Comparable<KeySuggestion> {
        private final String columnName;
        private final double uniqueness; // 0.0 to 1.0

        public KeySuggestion(String columnName, double uniqueness) {
            this.columnName = columnName;
            this.uniqueness = uniqueness;
        }

        public String getColumnName() { return columnName; }
        public double getUniqueness() { return uniqueness; }

        @Override
        public int compareTo(KeySuggestion other) {
            return Double.compare(other.uniqueness, this.uniqueness); // Sort descending
        }

        @Override
        public String toString() {
            return String.format("%s (%.2f%% unique)", columnName, uniqueness * 100);
        }
    }

    public static List<KeySuggestion> suggestKeys(List<List<Object>> data, List<Object> headers) {
        if (data == null || data.isEmpty() || headers == null || headers.isEmpty()) {
            return Collections.emptyList();
        }

        List<KeySuggestion> suggestions = new ArrayList<>();
        int numRows = data.size();

        for (int colIndex = 0; colIndex < headers.size(); colIndex++) {
            Set<Object> uniqueValues = new HashSet<>();
            for (List<Object> row : data) {
                if (colIndex < row.size()) {
                    uniqueValues.add(row.get(colIndex));
                }
            }
            double uniqueness = (double) uniqueValues.size() / numRows;
            suggestions.add(new KeySuggestion(headers.get(colIndex).toString(), uniqueness));
        }

        Collections.sort(suggestions);
        return suggestions;
    }
}
