package com.excelutility.core;

import java.util.List;
import java.util.Map;

/**
 * A data class representing the result of a single row comparison.
 * It holds the status, the key used for matching, the original data, and any differences found.
 */
public class RowResult {
    private final RowComparisonStatus status;
    private final String matchKey;
    private final List<Object> sourceRowData;
    private final List<Object> targetRowData;
    private final Map<Integer, CellDifference> differences; // Key is the column index

    public RowResult(RowComparisonStatus status, String matchKey, List<Object> sourceRowData, List<Object> targetRowData, Map<Integer, CellDifference> differences) {
        this.status = status;
        this.matchKey = matchKey;
        this.sourceRowData = sourceRowData;
        this.targetRowData = targetRowData;
        this.differences = differences;
    }

    // Getters
    public RowComparisonStatus getStatus() { return status; }
    public String getMatchKey() { return matchKey; }
    public List<Object> getSourceRowData() { return sourceRowData; }
    public List<Object> getTargetRowData() { return targetRowData; }
    public Map<Integer, CellDifference> getDifferences() { return differences; }
}
