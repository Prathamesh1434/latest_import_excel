package com.excelutility.core.expression;

import com.excelutility.core.FilteringService;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

import java.util.List;

/**
 * Represents a node in a filter expression tree. This can be either a single rule
 * or a group of other expressions.
 */
@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = GroupNode.class, name = "group"),
    @JsonSubTypes.Type(value = RuleNode.class, name = "rule")
})
public interface FilterExpression {

    /**
     * Evaluates this expression node against a single row of data.
     *
     * @param row     The row of data to evaluate.
     * @param header  The list of header strings for mapping columns.
     * @param service The filtering service instance, used to access shared logic.
     * @return True if the row matches the expression, false otherwise.
     */
    boolean evaluate(List<Object> row, List<String> header, FilteringService service);

    /**
     * @return The user-defined or auto-generated name of the node.
     */
    String getName();

    /**
     * @return A descriptive name for the expression, for logging and display.
     */
    String getDescriptiveName();
}
