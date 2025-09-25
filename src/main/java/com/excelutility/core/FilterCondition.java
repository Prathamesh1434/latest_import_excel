package com.excelutility.core;

/**
 * Represents a single filter condition (e.g., "Column 'Name' CONTAINS 'John'").
 */
public class FilterCondition {
    private String columnName;
    private Operator operator;
    private Object value; // Can be a single value, a List for IN_LIST, or a range
    private boolean caseInsensitive = true;

    // No-arg constructor for Jackson deserialization
    public FilterCondition() {}

    // Constructors, Getters, and Setters
    public FilterCondition(String columnName, Operator operator, Object value) {
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
    }

    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }
    public Operator getOperator() { return operator; }
    public void setOperator(Operator operator) { this.operator = operator; }
    public Object getValue() { return value; }
    public void setValue(Object value) { this.value = value; }
    public boolean isCaseInsensitive() { return caseInsensitive; }
    public void setCaseInsensitive(boolean caseInsensitive) { this.caseInsensitive = caseInsensitive; }
}
