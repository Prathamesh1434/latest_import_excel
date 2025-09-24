package com.excelutility.core;

/**
 * Represents the difference found in a single cell between two matched rows.
 * This version is designed to support typed mismatches for color-coding.
 */
public class CellDifference {
    private final String columnName;
    private final Object sourceValue;
    private final Object targetValue;
    private final MismatchType mismatchType;

    public CellDifference(String columnName, Object sourceValue, Object targetValue, MismatchType mismatchType) {
        this.columnName = columnName;
        this.sourceValue = sourceValue;
        this.targetValue = targetValue;
        this.mismatchType = mismatchType;
    }

    public String getColumnName() {
        return columnName;
    }

    public Object getSourceValue() {
        return sourceValue;
    }

    public Object getTargetValue() {
        return targetValue;
    }

    public MismatchType getMismatchType() {
        return mismatchType;
    }

    @Override
    public String toString() {
        return "CellDifference{" +
                "columnName='" + columnName + '\'' +
                ", sourceValue=" + sourceValue +
                ", targetValue=" + targetValue +
                ", mismatchType=" + mismatchType +
                '}';
    }
}
