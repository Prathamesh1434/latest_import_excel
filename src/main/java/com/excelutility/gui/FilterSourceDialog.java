package com.excelutility.gui;

import com.excelutility.core.FilterRule;
import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import java.awt.*;

/**
 * A dialog that prompts the user to choose how to use a selected cell from the filter-values file.
 * The user can choose to filter by the cell's literal value or by its column's name.
 */
public class FilterSourceDialog extends JDialog {

    private FilterRule.SourceType selectedType;
    private String selectedValue;

    /**
     * Constructs the dialog.
     *
     * @param owner      The parent frame.
     * @param value      The value of the selected cell.
     * @param columnName The name of the column of the selected cell.
     */
    public FilterSourceDialog(Frame owner, String value, String columnName) {
        super(owner, "Select Filter Source", true);
        setLayout(new MigLayout("wrap 1", "[grow]", "[]15[]15[]"));

        String displayValue = (value == null || value.trim().isEmpty()) ? "<Empty>" : value;

        JLabel infoLabel = new JLabel(String.format("<html>You selected cell with value '<b>%s</b>' in column '<b>%s</b>'.<br>How would you like to use this selection?</html>", displayValue, columnName));
        add(infoLabel, "growx");

        JButton byValueButton = new JButton(String.format("Filter by Value: '%s'", displayValue));
        JButton byColumnButton = new JButton(String.format("Filter using Column Name: '%s'", columnName));

        add(byValueButton, "growx");
        add(byColumnButton, "growx");

        byValueButton.addActionListener(e -> {
            this.selectedType = FilterRule.SourceType.BY_VALUE;
            this.selectedValue = (value == null) ? "" : value;
            setVisible(false);
        });

        byColumnButton.addActionListener(e -> {
            this.selectedType = FilterRule.SourceType.BY_COLUMN;
            this.selectedValue = columnName;
            setVisible(false);
        });

        pack();
        setLocationRelativeTo(owner);
        setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
    }

    /**
     * @return The {@link FilterRule.SourceType} selected by the user.
     */
    public FilterRule.SourceType getSelectedType() {
        return selectedType;
    }

    /**
     * @return The string value associated with the user's choice (either the cell value or the column name).
     */
    public String getSelectedValue() {
        return selectedValue;
    }
}
