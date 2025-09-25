package com.excelutility.gui;

import com.excelutility.core.FilterGroup;
import com.excelutility.core.FilterManager;
import com.excelutility.io.ExcelReader;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FileConfigPanel extends JPanel {

    // ... (fields remain the same, plus new ones)
    private final JTextField fileField = new JTextField();
    private final JComboBox<String> sheetCombo = new JComboBox<>();
    private final JRadioButton noHeaderRadio = new JRadioButton("No Header");
    private final JRadioButton singleHeaderRadio = new JRadioButton("Single Header Row", true);
    private final JRadioButton multiHeaderRadio = new JRadioButton("Multi-row Header");
    private final JSpinner singleHeaderSpinner = new JSpinner(new SpinnerNumberModel(1, 1, 100, 1));
    private final JButton autoSuggestButton;
    private final JButton filterButton;
    private final JButton openButton;
    private final JButton previewButton;
    private final JComboBox<String> savedFiltersCombo;
    private final JButton loadFilterButton;
    private final JButton clearFilterButton;
    private final JButton detectHeaderButton;

    private FilterGroup activeFilterGroup;
    private List<Integer> headerRowIndices = new ArrayList<>();
    private com.excelutility.core.ConcatenationMode concatenationMode = com.excelutility.core.ConcatenationMode.LEAF_ONLY;
    private List<String> availableColumns;
    private File selectedFile;
    private final Component parent;

    public FileConfigPanel(String title, Component parent) {
        this.parent = parent;
        setLayout(new MigLayout("fillx", "[][grow][][]", ""));
        setBorder(BorderFactory.createTitledBorder(title));

        // --- UI Components ---
        fileField.setEditable(true);
        openButton = new JButton("Browse...");
        previewButton = new JButton("Preview");
        autoSuggestButton = new JButton("Auto-Suggest Keys");
        filterButton = new JButton("Filter...");
        savedFiltersCombo = new JComboBox<>();
        loadFilterButton = new JButton("Load");
        clearFilterButton = new JButton("Clear");
        detectHeaderButton = new JButton("Detect Header");

        // --- Layout ---
        add(new JLabel("File:"));
        add(fileField, "growx");
        add(openButton);
        add(previewButton, "wrap");
        add(new JLabel("Sheet:"));
        add(sheetCombo, "growx, span 3, wrap");

        add(new JSeparator(), "span, growx, wrap, gaptop 5");
        add(new JLabel("Header Options:"), "span, wrap, gaptop 5");
        add(noHeaderRadio, "split 3");
        add(singleHeaderRadio);
        add(multiHeaderRadio, "wrap");
        add(new JLabel("Row Index:"), "gapleft 20");
        add(singleHeaderSpinner, "wrap");
        add(detectHeaderButton, "span, growx, wrap, gaptop 5");

        add(new JSeparator(), "span, growx, wrap, gaptop 5");

        add(new JLabel("Actions:"), "gaptop 5");
        add(autoSuggestButton, "span 3, split 2, growx");
        add(filterButton, "wrap");

        add(new JLabel("Saved Filters:"), "gaptop 5");
        add(savedFiltersCombo, "span 3, split 3, growx");
        add(loadFilterButton);
        add(clearFilterButton, "wrap");

        // --- Action Listeners ---
        openButton.addActionListener(e -> selectFile());
        detectHeaderButton.addActionListener(e -> detectHeader());
        filterButton.addActionListener(e -> openFilterBuilder());
        loadFilterButton.addActionListener(e -> loadSelectedFilter());
        clearFilterButton.addActionListener(e -> clearActiveFilter());

        noHeaderRadio.addActionListener(e -> singleHeaderSpinner.setEnabled(false));
        singleHeaderRadio.addActionListener(e -> singleHeaderSpinner.setEnabled(true));
        multiHeaderRadio.addActionListener(e -> singleHeaderSpinner.setEnabled(false));

        updateSavedFilters();
    }

    private void detectHeader() {
        if (selectedFile == null || getSelectedSheet() == null) {
            JOptionPane.showMessageDialog(this, "Please select a file and sheet first.", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }
        try (FileInputStream fis = new FileInputStream(selectedFile);
             Workbook workbook = WorkbookFactory.create(fis)) {
            Sheet sheet = workbook.getSheet(getSelectedSheet());
            if (sheet != null) {
                HeaderDetectionDialog dialog = new HeaderDetectionDialog((Frame) SwingUtilities.getWindowAncestor(this), sheet);
                dialog.setVisible(true);
                if (dialog.isConfirmed()) {
                    this.headerRowIndices = dialog.getSelectedHeaderRowIndices();
                    this.concatenationMode = dialog.getConcatenationMode();
                    JOptionPane.showMessageDialog(this, "Header rows set to: " + headerRowIndices.toString(), "Header Detection", JOptionPane.INFORMATION_MESSAGE);
                }
            }
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "Error reading file for header detection: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    // ... (rest of the methods: selectFile, populateSheetCombo, etc. remain the same)
    private void selectFile() {
        JFileChooser chooser = new JFileChooser();
        FileNameExtensionFilter excelFilter = new FileNameExtensionFilter("Excel Files (*.xls, *.xlsx)", "xls", "xlsx");
        chooser.addChoosableFileFilter(excelFilter);
        chooser.setFileFilter(excelFilter);
        if (chooser.showOpenDialog(parent) == JFileChooser.APPROVE_OPTION) {
            this.selectedFile = chooser.getSelectedFile();
            fileField.setText(selectedFile.getAbsolutePath());
            populateSheetCombo(null);
        }
    }

    private void populateSheetCombo(Runnable onComplete) {
        if (selectedFile == null) {
            if (onComplete != null) onComplete.run();
            return;
        }

        sheetCombo.removeAllItems();
        sheetCombo.addItem("Loading sheets...");
        sheetCombo.setEnabled(false);
        setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));

        SwingWorker<List<String>, Void> worker = new SwingWorker<List<String>, Void>() {
            @Override
            protected List<String> doInBackground() throws Exception {
                return ExcelReader.getSheetNames(selectedFile.getAbsolutePath());
            }

            @Override
            protected void done() {
                sheetCombo.setEnabled(true);
                setCursor(Cursor.getDefaultCursor());
                sheetCombo.removeAllItems();

                try {
                    List<String> sheetNames = get();
                    if (sheetNames.isEmpty()) {
                        sheetCombo.addItem("No sheets found in file");
                    } else {
                        for (String name : sheetNames) {
                            sheetCombo.addItem(name);
                        }
                        sheetCombo.setSelectedIndex(0);
                        loadHeadersForFilter();
                    }
                } catch (Exception e) {
                    sheetCombo.addItem("Error loading sheets!");
                    String errorMessage = "An error occurred while reading the Excel file:\n" +
                            e.getClass().getSimpleName() + ": " + e.getMessage() + "\n\n" +
                            "Please ensure the file is a valid, unencrypted Excel file and that you have permission to read it.";
                    JOptionPane.showMessageDialog(parent, errorMessage, "Error Reading File", JOptionPane.ERROR_MESSAGE);
                } finally {
                    if (onComplete != null) {
                        SwingUtilities.invokeLater(onComplete);
                    }
                }
            }
        };

        worker.execute();
    }

    private void loadHeadersForFilter() {
        try {
            if (getFilePath() != null && !getFilePath().isEmpty() && getSelectedSheet() != null) {
                List<List<Object>> headerData = ExcelReader.read(getFilePath(), getSelectedSheet(), true);
                if (!headerData.isEmpty()) {
                    this.availableColumns = headerData.get(0).stream().map(Object::toString).collect(Collectors.toList());
                } else {
                    this.availableColumns = null;
                }
            }
        } catch (Exception e) {
            this.availableColumns = null;
        }
    }

    private void openFilterBuilder() {
        if (availableColumns == null || availableColumns.isEmpty()) {
            loadHeadersForFilter();
        }
        if (availableColumns == null || availableColumns.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Could not load column headers. Please check the file and sheet selection.", "No Columns Found", JOptionPane.WARNING_MESSAGE);
            return;
        }
        FilterBuilderDialog dialog = new FilterBuilderDialog((Frame) SwingUtilities.getWindowAncestor(this), availableColumns);
        dialog.setVisible(true);

        if (dialog.isApplied()) {
            this.activeFilterGroup = dialog.getFilterGroup();
            JOptionPane.showMessageDialog(this, "Filter has been set and will be applied on the next comparison.", "Filter Set", JOptionPane.INFORMATION_MESSAGE);
        }
        updateSavedFilters();
    }

    private void loadSelectedFilter() {
        String selectedFilterName = (String) savedFiltersCombo.getSelectedItem();
        if (selectedFilterName == null) {
            JOptionPane.showMessageDialog(this, "No saved filter selected.", "Load Filter", JOptionPane.WARNING_MESSAGE);
            return;
        }
        this.activeFilterGroup = FilterManager.getInstance().getFilter(selectedFilterName);
        JOptionPane.showMessageDialog(this, "Filter '" + selectedFilterName + "' loaded and will be applied on the next comparison.", "Filter Loaded", JOptionPane.INFORMATION_MESSAGE);
    }

    private void clearActiveFilter() {
        this.activeFilterGroup = null;
        JOptionPane.showMessageDialog(this, "Active filter has been cleared.", "Filter Cleared", JOptionPane.INFORMATION_MESSAGE);
    }

    public void updateSavedFilters() {
        savedFiltersCombo.removeAllItems();
        FilterManager.getInstance().getSavedFilterNames().forEach(savedFiltersCombo::addItem);
    }

    // --- Public Getters and Setters ---
    public List<Integer> getHeaderRowIndices() { return headerRowIndices; }
    public void setHeaderRowIndices(List<Integer> indices) { this.headerRowIndices = indices; }
    public com.excelutility.core.ConcatenationMode getConcatenationMode() { return concatenationMode; }
    public void setConcatenationMode(com.excelutility.core.ConcatenationMode mode) { this.concatenationMode = mode; }
    public String getFilePath() { return fileField.getText(); }
    public String getSelectedSheet() { return sheetCombo.getSelectedItem() != null ? sheetCombo.getSelectedItem().toString() : null; }
    public void setFilePath(String path) {
        setFilePath(path, null);
    }

    public void setFilePath(String path, Runnable onSheetsLoaded) {
        if (path != null && !path.isEmpty()) {
            this.selectedFile = new File(path);
            fileField.setText(path);
            populateSheetCombo(onSheetsLoaded);
        } else {
            this.selectedFile = null;
            fileField.setText("");
            sheetCombo.removeAllItems();
            if (onSheetsLoaded != null) {
                onSheetsLoaded.run();
            }
        }
    }
    public void setSelectedSheet(String sheetName) { sheetCombo.setSelectedItem(sheetName); }
    public FilterGroup getActiveFilterGroup() { return activeFilterGroup; }
    public void setActiveFilterGroup(FilterGroup group) {
        this.activeFilterGroup = group;
        if (group != null) {
            filterButton.setText("Filter (Active)");
            filterButton.setForeground(Color.BLUE);
        } else {
            filterButton.setText("Filter...");
            filterButton.setForeground(null);
        }
    }
    public JComboBox<String> getSheetCombo() { return sheetCombo; }
    public JButton getAutoSuggestButton() { return autoSuggestButton; }
    public JButton getPreviewButton() { return previewButton; }
    public void addSheetSelectionListener(java.awt.event.ActionListener listener) { sheetCombo.addActionListener(listener); }
}
