package com.excelutility.gui;

import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.awt.*;
import java.util.Collections;
import java.util.List;

/**
 * A dialog that allows the user to select one or more target columns from a list
 * to apply a filter to. It includes a search field to filter the list of columns.
 */
public class FilterTargetDialog extends JDialog {

    private JList<String> columnList;
    private List<String> selectedColumns = Collections.emptyList();
    private JCheckBox trimWhitespaceCheckbox;
    private final List<String> allColumns;
    private boolean cancelled = true; // Default to cancelled state

    /**
     * Constructs the dialog with a default title.
     *
     * @param owner            The parent frame.
     * @param availableColumns The complete list of column names to display for selection.
     */
    public FilterTargetDialog(Frame owner, List<String> availableColumns) {
        this(owner, availableColumns, "Select Target Column(s) for Filter");
    }

    /**
     * Constructs the dialog with a custom title.
     *
     * @param owner            The parent frame.
     * @param availableColumns The complete list of column names to display for selection.
     * @param title            The custom title for the dialog window.
     */
    public FilterTargetDialog(Frame owner, List<String> availableColumns, String title) {
        super(owner, title, true);
        this.allColumns = availableColumns;
        initComponents();
    }

    private void initComponents() {
        setLayout(new MigLayout("fill, wrap 1", "[grow]", "[][][grow][]"));

        add(new JLabel("Select one or more columns from the data file to apply the filter to:"), "growx");

        JTextField searchField = new JTextField();
        add(new JLabel("Search:"));
        add(searchField, "growx");

        columnList = new JList<>(allColumns.toArray(new String[0]));
        columnList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        add(new JScrollPane(columnList), "grow");

        trimWhitespaceCheckbox = new JCheckBox("Trim whitespace from target column(s) before filtering", true);
        add(trimWhitespaceCheckbox, "growx, gaptop 5");

        searchField.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) { filterList(searchField.getText()); }
            @Override
            public void removeUpdate(DocumentEvent e) { filterList(searchField.getText()); }
            @Override
            public void changedUpdate(DocumentEvent e) { filterList(searchField.getText()); }
        });

        JButton okButton = new JButton("OK");
        okButton.addActionListener(e -> {
            selectedColumns = columnList.getSelectedValuesList();
            if (selectedColumns.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Please select at least one target column.", "Selection Required", JOptionPane.WARNING_MESSAGE);
                return;
            }
            this.cancelled = false; // Mark as not cancelled
            setVisible(false);
        });

        JButton cancelButton = new JButton("Cancel");
        cancelButton.addActionListener(e -> {
            // "cancelled" remains true by default
            setVisible(false);
        });

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        buttonPanel.add(okButton);
        buttonPanel.add(cancelButton);
        add(buttonPanel, "growx, right");

        setSize(400, 500);
        setLocationRelativeTo(getOwner());
        setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
    }

    private void filterList(String searchTerm) {
        String term = searchTerm.toLowerCase();
        DefaultListModel<String> model = new DefaultListModel<>();
        for (String col : allColumns) {
            if (col.toLowerCase().contains(term)) {
                model.addElement(col);
            }
        }
        columnList.setModel(model);
    }

    /**
     * @return The list of column names selected by the user.
     */
    public List<String> getSelectedColumns() {
        return selectedColumns;
    }

    /**
     * @return True if the user selected the "Trim Whitespace" option, false otherwise.
     */
    public boolean isTrimWhitespaceSelected() {
        return trimWhitespaceCheckbox.isSelected();
    }

    /**
     * @return True if the dialog was cancelled (closed without pressing OK), false otherwise.
     */
    public boolean isCancelled() {
        return cancelled;
    }
}
