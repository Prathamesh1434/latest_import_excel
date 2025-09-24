package com.excelutility.gui;

import com.excelutility.core.Operator;
import javax.swing.*;
import java.util.List;

/**
 * A panel representing a single filter condition row in the Filter Builder UI.
 */
public class FilterConditionPanel extends JPanel {

    private final JComboBox<String> columnCombo;
    private final JComboBox<Operator> operatorCombo;
    private final JTextField valueField;
    private final JButton removeButton;

    public FilterConditionPanel(List<String> availableColumns) {
        columnCombo = new JComboBox<>(availableColumns.toArray(new String[0]));
        operatorCombo = new JComboBox<>(Operator.values());
        valueField = new JTextField(15);
        removeButton = new JButton("-");

        add(new JLabel("Column:"));
        add(columnCombo);
        add(new JLabel("Operator:"));
        add(operatorCombo);
        add(new JLabel("Value:"));
        add(valueField);
        add(removeButton);
    }

    public JButton getRemoveButton() {
        return removeButton;
    }

    /**
     * Constructs a FilterCondition object from the current state of the UI components.
     * @return A new FilterCondition, or null if the state is invalid.
     */
    public com.excelutility.core.FilterCondition getFilterCondition() {
        String columnName = (String) columnCombo.getSelectedItem();
        Operator operator = (Operator) operatorCombo.getSelectedItem();
        String value = valueField.getText();

        if (columnName == null || operator == null) {
            return null; // Not a valid condition if column or operator is not selected
        }

        // For operators that don't need a value, ignore the value field
        if (operator == Operator.IS_NULL || operator == Operator.IS_NOT_NULL) {
            return new com.excelutility.core.FilterCondition(columnName, operator, null);
        }

        return new com.excelutility.core.FilterCondition(columnName, operator, value);
    }
}
