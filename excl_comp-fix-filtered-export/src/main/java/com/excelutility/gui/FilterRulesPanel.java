package com.excelutility.gui;

import com.excelutility.core.ConcatenationMode;
import com.excelutility.core.FilterRule;
import com.excelutility.core.FilteringService;
import net.miginfocom.swing.MigLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

/**
 * A panel that displays a list of currently configured filter rules and provides actions for managing them.
 */
public class FilterRulesPanel extends JPanel {

    private static final Logger logger = LoggerFactory.getLogger(FilterRulesPanel.class);
    private final DefaultTableModel tableModel;
    private final List<FilterRule> rules = new ArrayList<>();
    private final JTable rulesTable;
    private final FilteringService filteringService;
    private final FilterFilePanel dataFilePanel;
    private final JButton refreshButton;

    /**
     * Constructs the panel containing the filter rules table and action buttons.
     *
     * @param filteringService The service used to perform filtering and counting operations.
     * @param dataFilePanel    The panel containing information about the data file to be filtered.
     */
    public FilterRulesPanel(FilteringService filteringService, FilterFilePanel dataFilePanel) {
        this.filteringService = filteringService;
        this.dataFilePanel = dataFilePanel;

        setLayout(new MigLayout("fill, insets 5", "[grow]", "[grow][]"));
        setBorder(BorderFactory.createTitledBorder("Configured Filters"));

        tableModel = new DefaultTableModel(new String[]{"Filter Type", "Filter Value", "Target Column", "Match Count"}, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false; // Make table cells non-editable
            }
        };
        rulesTable = new JTable(tableModel);
        rulesTable.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);

        add(new JScrollPane(rulesTable), "grow, wrap");

        JPanel buttonPanel = new JPanel(new MigLayout("insets 0", "[][][]"));
        refreshButton = new JButton("Refresh Counts");
        JButton deleteButton = new JButton("Delete Selected");
        JButton clearButton = new JButton("Clear All Filters");

        buttonPanel.add(refreshButton);
        buttonPanel.add(deleteButton);
        buttonPanel.add(clearButton);

        add(buttonPanel, "align right");

        clearButton.addActionListener(e -> clearRules());
        deleteButton.addActionListener(e -> deleteSelectedRules());
        refreshButton.addActionListener(e -> refreshCounts());
    }

    /**
     * Iterates through all configured rules and updates their match counts in the table.
     * This is done in a background thread to avoid freezing the UI.
     */
    private void refreshCounts() {
        String dataFilePath = dataFilePanel.getFilePath();
        String sheetName = dataFilePanel.getSelectedSheet();
        if (dataFilePath == null || dataFilePath.trim().isEmpty() || sheetName == null) {
            JOptionPane.showMessageDialog(this, "Please select a data file and sheet before refreshing counts.", "Data File Required", JOptionPane.WARNING_MESSAGE);
            return;
        }

        refreshButton.setText("Calculating...");
        refreshButton.setEnabled(false);

        // Use a SwingWorker to perform counting in the background
        new SwingWorker<Void, int[]>() {
            @Override
            protected Void doInBackground() throws Exception {
                List<Integer> headerRows = dataFilePanel.getHeaderRowIndices();
                ConcatenationMode concatMode = dataFilePanel.getConcatenationMode();

                for (int i = 0; i < rules.size(); i++) {
                    try {
                        int count = filteringService.countMatches(dataFilePath, sheetName, headerRows, concatMode, rules.get(i));
                        publish(new int[]{i, count});
                    } catch (Exception e) {
                        logger.error("Failed to count matches for rule: {}", rules.get(i), e);
                        publish(new int[]{i, -1}); // Use -1 to indicate an error
                    }
                }
                return null;
            }

            @Override
            protected void process(List<int[]> chunks) {
                // Update the table on the EDT
                for (int[] chunk : chunks) {
                    int rowIndex = chunk[0];
                    int count = chunk[1];
                    tableModel.setValueAt(count >= 0 ? String.valueOf(count) : "Error", rowIndex, 3);
                }
            }

            @Override
            protected void done() {
                refreshButton.setText("Refresh Counts");
                refreshButton.setEnabled(true);
                try {
                    get(); // Check for exceptions during doInBackground
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Error refreshing filter counts.", e);
                    JOptionPane.showMessageDialog(FilterRulesPanel.this, "An error occurred while calculating match counts: " + e.getCause().getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
                }
            }
        }.execute();
    }

    /**
     * Deletes the currently selected rules from the table and the internal list.
     */
    private void deleteSelectedRules() {
        int[] selectedViewRows = rulesTable.getSelectedRows();
        if (selectedViewRows.length == 0) {
            return; // Nothing selected
        }

        int[] modelRows = new int[selectedViewRows.length];
        for (int i = 0; i < selectedViewRows.length; i++) {
            modelRows[i] = rulesTable.convertRowIndexToModel(selectedViewRows[i]);
        }
        Arrays.sort(modelRows);

        for (int i = modelRows.length - 1; i >= 0; i--) {
            int modelRow = modelRows[i];
            tableModel.removeRow(modelRow);
            rules.remove(modelRow);
        }
    }

    /**
     * Adds a new filter rule to the internal list and updates the display table.
     * @param rule The {@link FilterRule} to add.
     */
    public void addRule(FilterRule rule) {
        rules.add(rule);
        Vector<Object> row = new Vector<>();
        row.add(rule.getSourceType().toString());
        row.add(rule.getSourceValue());
        row.add(rule.getTargetColumn());
        row.add("N/A"); // Placeholder for match count
        tableModel.addRow(row);
    }

    /**
     * Removes all filter rules from the list and clears the display table.
     */
    public void clearRules() {
        rules.clear();
        tableModel.setRowCount(0);
    }

    /**
     * Gets a copy of the list of currently configured filter rules.
     * @return A new list containing the active {@link FilterRule} objects.
     */
    public List<FilterRule> getRules() {
        return new ArrayList<>(rules);
    }
}
