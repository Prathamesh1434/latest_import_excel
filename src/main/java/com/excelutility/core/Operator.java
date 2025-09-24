package com.excelutility.core;

/**
 * Represents the operators that can be used in a filter condition.
 */
public enum Operator {
    EQUALS("Equals"),
    NOT_EQUALS("Not Equals"),
    CONTAINS("Contains"),
    NOT_CONTAINS("Not Contains"),
    STARTS_WITH("Starts With"),
    ENDS_WITH("Ends With"),
    REGEX_MATCH("Regex Match"),
    IN_LIST("In List"),
    NOT_IN_LIST("Not In List"),
    IS_NULL("Is Null"),
    IS_NOT_NULL("Is Not Null"),
    GREATER_THAN("Greater Than"),
    LESS_THAN("Less Than"),
    GREATER_OR_EQUAL("Greater Than or Equal"),
    LESS_OR_EQUAL("Less Than or Equal"),
    BETWEEN("Between");

    private final String displayName;

    Operator(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }
}
