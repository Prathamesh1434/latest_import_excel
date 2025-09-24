package com.excelutility.core;

/**
 * Defines the strategy used to match rows between the source and target files.
 */
public enum RowMatchStrategy {
    /**
     * Match rows based on their index (row number).
     */
    BY_ROW_ORDER,

    /**
     * Match rows based on the values in one or more key columns.
     */
    BY_PRIMARY_KEY,

    /**
     * Match rows using fuzzy string comparison on key columns.
     */
    BY_FUZZY_KEY
}
