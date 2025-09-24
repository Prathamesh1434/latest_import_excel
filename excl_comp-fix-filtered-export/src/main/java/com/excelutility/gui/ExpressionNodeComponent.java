package com.excelutility.gui;

import com.excelutility.core.expression.FilterExpression;

/**
 * An interface for UI components that can be mapped to a node
 * in the backend FilterExpression tree.
 */
public interface ExpressionNodeComponent {

    /**
     * Builds and returns the backend representation of this UI component.
     * @return A {@link FilterExpression} object.
     */
    FilterExpression getExpression();
}
