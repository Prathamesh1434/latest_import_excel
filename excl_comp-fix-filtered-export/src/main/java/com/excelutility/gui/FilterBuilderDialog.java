package com.excelutility.gui;

import com.excelutility.core.FilterCondition;
import com.excelutility.core.FilterGroup;
import com.excelutility.core.FilterManager;
import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;

public class FilterBuilderDialog extends JDialog {

    private final List<String> availableColumns;
    private final JPanel conditionsPanel;
    private FilterGroup filterGroup = new FilterGroup(); // The root group
    private boolean applied = false;

    public FilterBuilderDialog(Frame owner, List<String> availableColumns) {
        super(owner, "Filter Builder", true);
        this.availableColumns = availableColumns;
        setSize(800, 400);
        setLocationRelativeTo(owner);
        setLayout(new BorderLayout(10, 10));

        conditionsPanel = new JPanel();
        conditionsPanel.setLayout(new BoxLayout(conditionsPanel, BoxLayout.Y_AXIS));
        add(new JScrollPane(conditionsPanel), BorderLayout.CENTER);

        // --- Bottom Button Panel ---
        JPanel bottomPanel = new JPanel(new MigLayout("fillx", "[left][right]"));

        JPanel leftButtons = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton addConditionButton = new JButton("+ Add Condition");
        leftButtons.add(addConditionButton);

        JPanel rightButtons = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton saveButton = new JButton("Save Filter");
        JButton applyButton = new JButton("Apply");
        JButton cancelButton = new JButton("Cancel");
        rightButtons.add(saveButton);
        rightButtons.add(applyButton);
        rightButtons.add(cancelButton);

        bottomPanel.add(leftButtons, "growx");
        bottomPanel.add(rightButtons, "growx");
        add(bottomPanel, BorderLayout.SOUTH);

        // --- Action Listeners ---
        addConditionButton.addActionListener(e -> addConditionRow());
        applyButton.addActionListener(e -> applyFilter());
        saveButton.addActionListener(e -> saveFilter());
        cancelButton.addActionListener(e -> dispose());

        // Add an initial condition row
        addConditionRow();
    }

    private void addConditionRow() {
        FilterConditionPanel newRow = new FilterConditionPanel(availableColumns);
        newRow.getRemoveButton().addActionListener(e -> {
            conditionsPanel.remove(newRow);
            revalidate();
            repaint();
        });
        conditionsPanel.add(newRow);
        revalidate();
        repaint();
    }

    private void buildFilterGroupFromUI() {
        this.filterGroup = new FilterGroup(); // Reset
        for (Component comp : conditionsPanel.getComponents()) {
            if (comp instanceof FilterConditionPanel) {
                FilterCondition condition = ((FilterConditionPanel) comp).getFilterCondition();
                if (condition != null) {
                    filterGroup.addCondition(condition);
                }
            }
        }
    }

    private void applyFilter() {
        buildFilterGroupFromUI();
        this.applied = true;
        dispose();
    }

    private void saveFilter() {
        buildFilterGroupFromUI();
        if (filterGroup.getConditions().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Cannot save an empty filter.", "Save Filter", JOptionPane.WARNING_MESSAGE);
            return;
        }

        String filterName = JOptionPane.showInputDialog(this, "Enter a name for this filter:", "Save Filter", JOptionPane.PLAIN_MESSAGE);
        if (filterName != null && !filterName.trim().isEmpty()) {
            FilterManager.getInstance().saveFilter(filterName, filterGroup);
            JOptionPane.showMessageDialog(this, "Filter '" + filterName + "' saved successfully.", "Success", JOptionPane.INFORMATION_MESSAGE);
        }
    }

    public FilterGroup getFilterGroup() {
        return filterGroup;
    }

    public boolean isApplied() {
        return applied;
    }
}
