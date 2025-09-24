package com.excelutility.gui;

import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A dialog that allows the user to select a subset of columns for viewing or exporting.
 */
public class ColumnSelectionDialog extends JDialog {

    private final List<JCheckBox> checkBoxes = new ArrayList<>();
    private List<String> selectedColumns = Collections.emptyList();
    private boolean cancelled = true;

    /**
     * Constructs a new ColumnSelectionDialog.
     * @param owner The parent frame.
     * @param availableColumns The list of all possible column names to choose from.
     */
    public ColumnSelectionDialog(Frame owner, List<String> availableColumns) {
        super(owner, "Select Columns for Output", true);
        setLayout(new BorderLayout(10, 10));

        // --- Checkbox Panel ---
        JPanel checkPanel = new JPanel(new MigLayout("wrap 1"));
        for (String colName : availableColumns) {
            JCheckBox checkBox = new JCheckBox(colName, true); // Default to selected
            checkBoxes.add(checkBox);
            checkPanel.add(checkBox);
        }
        JScrollPane scrollPane = new JScrollPane(checkPanel);
        scrollPane.setBorder(BorderFactory.createTitledBorder("Available Columns"));
        add(scrollPane, BorderLayout.CENTER);

        // --- Button Panel ---
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton okButton = new JButton("OK");
        JButton cancelButton = new JButton("Cancel");
        buttonPanel.add(okButton);
        buttonPanel.add(cancelButton);

        // --- Select All / Deselect All Panel ---
        JPanel selectionControlPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton selectAllButton = new JButton("Select All");
        JButton deselectAllButton = new JButton("Deselect All");
        selectionControlPanel.add(selectAllButton);
        selectionControlPanel.add(deselectAllButton);

        JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(selectionControlPanel, BorderLayout.WEST);

        JPanel southPanel = new JPanel(new BorderLayout());
        southPanel.add(buttonPanel, BorderLayout.EAST);

        add(topPanel, BorderLayout.NORTH);
        add(southPanel, BorderLayout.SOUTH);

        // --- Action Listeners ---
        selectAllButton.addActionListener(e -> setAllSelected(true));
        deselectAllButton.addActionListener(e -> setAllSelected(false));

        okButton.addActionListener(e -> {
            selectedColumns = new ArrayList<>();
            for (JCheckBox checkBox : checkBoxes) {
                if (checkBox.isSelected()) {
                    selectedColumns.add(checkBox.getText());
                }
            }
            if (selectedColumns.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Please select at least one column.", "Selection Required", JOptionPane.WARNING_MESSAGE);
                return;
            }
            this.cancelled = false;
            setVisible(false);
        });

        cancelButton.addActionListener(e -> {
            this.cancelled = true;
            setVisible(false);
        });

        setSize(400, 500);
        setLocationRelativeTo(owner);
    }

    private void setAllSelected(boolean selected) {
        for (JCheckBox checkBox : checkBoxes) {
            checkBox.setSelected(selected);
        }
    }

    public List<String> getSelectedColumns() {
        return selectedColumns;
    }

    public boolean isCancelled() {
        return cancelled;
    }
}
