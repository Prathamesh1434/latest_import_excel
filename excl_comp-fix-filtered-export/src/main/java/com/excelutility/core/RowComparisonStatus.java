package com.excelutility.core;

/**
 * Represents the result status of a row comparison.
 */
public enum RowComparisonStatus {
    /**
     * The rows are identical based on the comparison criteria.
     */
    MATCHED_IDENTICAL,

    /**
     * The rows match on their keys but have differences in other columns.
     */
    MATCHED_MISMATCHED,

    /**
     * The row is present in the source (File 1) but not in the target (File 2).
     */
    SOURCE_ONLY,

    /**
     * The row is present in the target (File 2) but not in the source (File 1).
     */
    TARGET_ONLY,

    /**
     * The row is a duplicate within its own file, based on the defined key.
     */
    DUPLICATE
}
