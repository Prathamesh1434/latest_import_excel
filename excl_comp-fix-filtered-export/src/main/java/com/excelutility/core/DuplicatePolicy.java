package com.excelutility.core;

/**
 * Defines how to handle duplicate rows found within a single file (based on the matching key).
 */
public enum DuplicatePolicy {
    /**
     * Report all duplicate rows.
     */
    REPORT_ALL,

    /**
     * Keep only the first occurrence and report the rest as duplicates.
     */
    KEEP_FIRST,

    /**
     * Keep only the last occurrence and report the rest as duplicates.
     */
    KEEP_LAST,

    /**
     * Aggregate the values from duplicate rows (e.g., sum, average).
     * Note: This is an advanced feature and may not be implemented in the initial version.
     */
    AGGREGATE
}
