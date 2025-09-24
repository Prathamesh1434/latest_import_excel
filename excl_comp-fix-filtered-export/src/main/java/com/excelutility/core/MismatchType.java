package com.excelutility.core;

/**
 * Defines the type of mismatch found between two cells.
 * This will be used for color-coding in the results view.
 */
public enum MismatchType {
    /**
     * A mismatch between two numeric values.
     */
    NUMERIC,
    /**
     * A mismatch between two string values.
     */
    STRING,
    /**
     * A mismatch where one cell is blank/null and the other is not.
     */
    BLANK_VS_NON_BLANK,
    /**
     * A mismatch where the data types of the cells are different (e.g., String vs. Number).
     */
    TYPE_MISMATCH
}
