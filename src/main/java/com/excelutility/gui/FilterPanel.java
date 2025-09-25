package com.excelutility.gui;

import com.excelutility.core.*;
import com.excelutility.core.expression.FilterExpression;
import com.excelutility.io.ExcelReader;
import com.excelutility.io.ProfileService;
import com.excelutility.io.SimpleExcelWriter;
import net.miginfocom.swing.MigLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

public class FilterPanel extends JPanel {

    private static final Logger logger = LoggerFactory.getLogger(FilterPanel.class);

    private final FilterFilePanel dataFilePanel;
    private final FilterFilePanel filterValuesFilePanel;
    private final JTable dataPreviewTable;
    private final JTable filterValuesPreviewTable;
    private final DefaultTableModel dataPreviewModel;
    private final DefaultTableModel filterValuesPreviewModel;
    private final FilterExpressionBuilderPanel filterExpressionBuilderPanel;
    private final FilteringService filteringService = new FilteringService();
    private JLabel totalMatchesLabel;
    private JTabbedPane unifiedDataViewTabs;
    private final ProfileService profileService = new ProfileService("filter-profiles");
    private boolean isDirty = false;

    private enum ProcessDestination { VIEW, CALCULATE_ONLY }

    private Color selectedColor = Color.YELLOW;
    private final AppContainer appContainer;

    public FilterPanel(AppContainer appContainer) {
        this.appContainer = appContainer;
        setLayout(new BorderLayout());

        // --- Top Panel for File Selection ---
        JPanel topPanel = new JPanel(new MigLayout("fillx", "[grow][grow]"));
        dataFilePanel = new FilterFilePanel("Data File (to be filtered)", this);
        filterValuesFilePanel = new FilterFilePanel("Filter Values File", this);
        topPanel.add(dataFilePanel, "growx");
        topPanel.add(filterValuesFilePanel, "growx, wrap");
        JButton previewButton = new JButton("Load & Preview Files");
        topPanel.add(previewButton, "span, center");
        add(topPanel, BorderLayout.NORTH);

        // --- Main Content Panel ---
        JPanel mainContentPanel = new JPanel(new MigLayout("fill, insets 5",
                "[sg preview, grow, fill][sg preview, grow, fill][grow, fill]",
                "[grow 60, fill][grow 40, fill]"));
        add(mainContentPanel, BorderLayout.CENTER);

        // --- Top Row ---
        dataPreviewModel = new DefaultTableModel();
        dataPreviewTable = new JTable(dataPreviewModel);
        configureTable(dataPreviewTable);
        JScrollPane dataPreviewScroll = new JScrollPane(dataPreviewTable);
        dataPreviewScroll.setBorder(BorderFactory.createTitledBorder("Data Preview (First 50 rows)"));
        mainContentPanel.add(dataPreviewScroll, "grow");

        filterValuesPreviewModel = new DefaultTableModel();
        filterValuesPreviewTable = new JTable(filterValuesPreviewModel);
        configureTable(filterValuesPreviewTable);
        filterValuesPreviewTable.setCellSelectionEnabled(true);
        JScrollPane filterValuesPreviewScroll = new JScrollPane(filterValuesPreviewTable);
        filterValuesPreviewScroll.setBorder(BorderFactory.createTitledBorder("Filter Values Preview (Full Data)"));
        mainContentPanel.add(filterValuesPreviewScroll, "grow");

        filterExpressionBuilderPanel = new FilterExpressionBuilderPanel(this);
        JScrollPane builderScrollPane = new JScrollPane(filterExpressionBuilderPanel);
        builderScrollPane.setBorder(BorderFactory.createTitledBorder("Filter Logic Builder"));
        mainContentPanel.add(builderScrollPane, "grow, wrap");

        // --- Bottom Row ---
        unifiedDataViewTabs = new JTabbedPane();
        unifiedDataViewTabs.setBorder(BorderFactory.createTitledBorder("Unified Data View"));
        JPanel unifiedDataViewPlaceholder = new JPanel(new BorderLayout());
        unifiedDataViewTabs.addTab("Unified", unifiedDataViewPlaceholder);
        mainContentPanel.add(unifiedDataViewTabs, "span 2, grow");

        JPanel actionPanel = new JPanel(new MigLayout("wrap 1, fillx, insets 10", "[grow, fill]"));
        actionPanel.setBorder(BorderFactory.createTitledBorder("Actions"));
        JButton addGroupButton = new JButton("Add Group");
        JButton calculateButton = new JButton("Calculate Total");
        JButton viewButton = new JButton("View Overall Result");
        JButton downloadButton = new JButton("Download Filtered Results");
        JButton colorButton = new JButton("Set Highlight Color");
        totalMatchesLabel = new JLabel("Total Matches: N/A");
        actionPanel.add(addGroupButton);
        actionPanel.add(calculateButton, "gaptop 10");
        actionPanel.add(viewButton);
        actionPanel.add(totalMatchesLabel, "gaptop 5");
        actionPanel.add(downloadButton, "gaptop 10");
        actionPanel.add(colorButton);
        JButton exitButton = new JButton("Exit");
        actionPanel.add(exitButton, "gaptop 20, align right");
        mainContentPanel.add(actionPanel, "grow");

        // --- Action Listeners ---
        previewButton.addActionListener(e -> loadPreviews());
        colorButton.addActionListener(e -> chooseColor());
        downloadButton.addActionListener(e -> startMultiSheetExportProcess());
        viewButton.addActionListener(e -> startFilterProcess(ProcessDestination.VIEW));
        calculateButton.addActionListener(e -> startFilterProcess(ProcessDestination.CALCULATE_ONLY));
        exitButton.addActionListener(e -> exitApplication());

        addGroupButton.addActionListener(e -> {
            LogicalGroupPanel rootGroup = filterExpressionBuilderPanel.getRootGroup();
            ActionListener deleteListener = event -> {
                LogicalGroupPanel sourceGroup = (LogicalGroupPanel) event.getSource();
                rootGroup.removeComponent(sourceGroup);
                updateFilterResults();
            };
            String groupName = com.excelutility.core.AutoNamingService.suggestGroupName();
            LogicalGroupPanel newGroup = new LogicalGroupPanel(groupName, deleteListener);
            newGroup.getAddRuleButton().addActionListener(ev -> createFilterFromSelection(newGroup));
            rootGroup.addComponent(newGroup);
            isDirty = true;
        });

        filterExpressionBuilderPanel.getRootGroup().getAddRuleButton().addActionListener(ev -> createFilterFromSelection(filterExpressionBuilderPanel.getRootGroup()));

        filterValuesPreviewTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) {
                    createFilterFromSelection(filterExpressionBuilderPanel.getRootGroup());
                }
            }
        });
    }

    private void configureTable(JTable table) {
        table.setShowGrid(true);
        table.setGridColor(Color.LIGHT_GRAY);
        table.setAutoCreateRowSorter(true);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        table.setFont(new Font("Lucida Sans Unicode", Font.PLAIN, 12));
    }

    private void adjustColumnWidths(JTable table) {
        for (int column = 0; column < table.getColumnCount(); column++) {
            int width = 150;
            for (int row = 0; row < table.getRowCount(); row++) {
                TableCellRenderer renderer = table.getCellRenderer(row, column);
                Component comp = table.prepareRenderer(renderer, row, column);
                width = Math.max(comp.getPreferredSize().width + 10, width);
            }
            table.getColumnModel().getColumn(column).setPreferredWidth(width);
        }
    }

    private void chooseColor() {
        Color newColor = JColorChooser.showDialog(this, "Choose Highlight Color", selectedColor);
        if (newColor != null) {
            selectedColor = newColor;
        }
    }

    void updateFilterResults() {
        startFilterProcess(ProcessDestination.VIEW);
        isDirty = true;
    }

    private void startFilterProcess(ProcessDestination destination) {
        FilterExpression expression = filterExpressionBuilderPanel.getRootGroup().getExpression();
        String dataFilePath = dataFilePanel.getFilePath();
        String sheetName = dataFilePanel.getSelectedSheet();
        if (dataFilePath == null || dataFilePath.trim().isEmpty() || sheetName == null) {
            if (destination != ProcessDestination.CALCULATE_ONLY) {
                JOptionPane.showMessageDialog(this, "Please select a data file and sheet first.", "Data File Required", JOptionPane.WARNING_MESSAGE);
            }
            return;
        }

        totalMatchesLabel.setText("Total Matches: Calculating...");

        new SwingWorker<List<List<Object>>, Void>() {
            @Override
            protected List<List<Object>> doInBackground() throws Exception {
                return filteringService.filter(dataFilePath, sheetName, dataFilePanel.getHeaderRowIndices(), dataFilePanel.getConcatenationMode(), expression);
            }

            @Override
            protected void done() {
                try {
                    List<List<Object>> filteredData = get();
                    int recordCount = filteredData.isEmpty() ? 0 : filteredData.size() - 1;
                    logger.info("FilterApply: expression='{}' -> matched={}", expression.getDescriptiveName(), recordCount);
                    totalMatchesLabel.setText("Total Matches: " + recordCount);

                    if (destination == ProcessDestination.CALCULATE_ONLY) {
                        return;
                    }

                    if (destination == ProcessDestination.VIEW) {
                        List<String> allColumns = dataFilePanel.getColumnNames();
                        List<List<Object>> resultData = projectColumns(filteredData, allColumns);
                        updateUnifiedViewTab(resultData, allColumns);
                    }
                } catch (Exception e) {
                    logger.error("Filtering process failed.", e);
                    totalMatchesLabel.setText("Total Matches: Error");
                    JOptionPane.showMessageDialog(FilterPanel.this, "Failed to apply filters: " + e.getCause().getMessage(), "Filtering Error", JOptionPane.ERROR_MESSAGE);
                }
            }
        }.execute();
    }

    private void startMultiSheetExportProcess() {
        String dataFilePath = dataFilePanel.getFilePath();
        String sheetName = dataFilePanel.getSelectedSheet();
        if (dataFilePath == null || dataFilePath.trim().isEmpty() || sheetName == null) {
            JOptionPane.showMessageDialog(this, "Please select a data file and sheet first.", "Data File Required", JOptionPane.WARNING_MESSAGE);
            return;
        }

        final FilterExpression expression = filterExpressionBuilderPanel.getRootGroup().getExpression();

        // Final check if there's anything to export
        if (expression == null || (expression instanceof com.excelutility.core.expression.GroupNode && ((com.excelutility.core.expression.GroupNode) expression).getChildren().isEmpty())) {
            JOptionPane.showMessageDialog(this, "No filters to export. Add rules or groups to the builder.", "Export Error", JOptionPane.WARNING_MESSAGE);
            return;
        }

        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("Save Multi-Sheet Export");
        chooser.setFileFilter(new FileNameExtensionFilter("Excel Workbook (*.xlsx)", "xlsx"));
        chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);

        if (chooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            File fileToSave = chooser.getSelectedFile();
            String filePath = fileToSave.getAbsolutePath();
            if (!filePath.toLowerCase().endsWith(".xlsx")) {
                filePath += ".xlsx";
            }
            final String finalFilePath = filePath;

            new SwingWorker<Void, Void>() {
                @Override
                protected Void doInBackground() throws Exception {
                    // Get the filtered data
                    Map<String, List<List<Object>>> filteredData = filteringService.getUnifiedFilteredData(
                            dataFilePath,
                            sheetName,
                            dataFilePanel.getHeaderRowIndices(),
                            dataFilePanel.getConcatenationMode(),
                            expression
                    );

                    // Generate the summary sheet data
                    List<List<Object>> summaryData = new ArrayList<>();
                    summaryData.add(List.of("Filter Logic"));
                    summaryData.add(List.of(expression.getDescriptiveName()));
                    filteredData.put("filter rule", summaryData);

                    // Remove the duplicate "unified" sheet before writing
                    filteredData.remove("unified");

                    SimpleExcelWriter.writeFilteredResults(finalFilePath, filteredData, true, selectedColor);
                    return null;
                }

                @Override
                protected void done() {
                    try {
                        get();
                        JOptionPane.showMessageDialog(FilterPanel.this, "Multi-sheet export completed successfully!", "Export Complete", JOptionPane.INFORMATION_MESSAGE);
                    } catch (Exception e) {
                        logger.error("Multi-sheet export failed.", e);
                        JOptionPane.showMessageDialog(FilterPanel.this, "Failed to export results: " + e.getCause().getMessage(), "Export Error", JOptionPane.ERROR_MESSAGE);
                    }
                }
            }.execute();
        }
    }

    private void updateUnifiedViewTab(List<List<Object>> data, List<String> headers) {
        JPanel unifiedViewPanel = (JPanel) unifiedDataViewTabs.getComponentAt(0);
        unifiedViewPanel.removeAll();

        if (data.isEmpty() || data.size() <= 1) {
            unifiedViewPanel.add(new JLabel("No matching records found."), BorderLayout.CENTER);
        } else {
            JTable resultTable = new JTable();
            configureTable(resultTable);
            JScrollPane scrollPane = new JScrollPane(resultTable);
            unifiedViewPanel.add(scrollPane, BorderLayout.CENTER);
            Vector<String> headerVector = new Vector<>(headers);
            Vector<Vector<Object>> dataVector = new Vector<>();
            List<List<Object>> dataRows = data.subList(1, data.size());
            for (List<Object> row : dataRows) {
                dataVector.add(new Vector<>(row));
            }
            resultTable.setModel(new DefaultTableModel(dataVector, headerVector));
            adjustColumnWidths(resultTable);
        }
        unifiedViewPanel.revalidate();
        unifiedViewPanel.repaint();
        unifiedDataViewTabs.setSelectedIndex(0);
    }

    public void previewRule(FilterRulePanel rulePanel) {
        runPreviewFilter(rulePanel.getExpression(), rulePanel::setRecordCount, rulePanel.getRuleName() + " Preview");
    }

    public void previewGroup(LogicalGroupPanel groupPanel) {
        runPreviewFilter(groupPanel.getExpression(), groupPanel::setRecordCount, groupPanel.getName() + " Preview");
    }

    private void runPreviewFilter(FilterExpression expression, java.util.function.Consumer<Integer> countConsumer, String tabTitle) {
        String dataFilePath = dataFilePanel.getFilePath();
        String sheetName = dataFilePanel.getSelectedSheet();
        if (dataFilePath == null || dataFilePath.trim().isEmpty() || sheetName == null) {
            JOptionPane.showMessageDialog(this, "Please select a data file and sheet first.", "Data File Required", JOptionPane.WARNING_MESSAGE);
            return;
        }

        new SwingWorker<List<List<Object>>, Void>() {
            @Override
            protected List<List<Object>> doInBackground() throws Exception {
                return filteringService.filter(dataFilePath, sheetName, dataFilePanel.getHeaderRowIndices(), dataFilePanel.getConcatenationMode(), expression);
            }

            @Override
            protected void done() {
                try {
                    List<List<Object>> resultData = get();
                    int recordCount = resultData.isEmpty() ? 0 : resultData.size() - 1;
                    countConsumer.accept(recordCount);
                    if (recordCount > 0) {
                        addPreviewTab(tabTitle, resultData);
                    } else {
                        JOptionPane.showMessageDialog(FilterPanel.this, "No records match the preview criteria.", "No Matches", JOptionPane.INFORMATION_MESSAGE);
                    }
                } catch (Exception e) {
                    logger.error("Preview filtering process failed.", e);
                    countConsumer.accept(-1);
                    JOptionPane.showMessageDialog(FilterPanel.this, "Failed to apply preview filter: " + e.getCause().getMessage(), "Filtering Error", JOptionPane.ERROR_MESSAGE);
                }
            }
        }.execute();
    }

    private void addPreviewTab(String title, List<List<Object>> data) {
        JPanel contentPanel = new JPanel(new BorderLayout());
        JTable table = new JTable();
        configureTable(table);
        JScrollPane scrollPane = new JScrollPane(table);
        contentPanel.add(scrollPane, BorderLayout.CENTER);

        if (!data.isEmpty()) {
            Vector<String> headers = data.get(0).stream().map(Object::toString).collect(Collectors.toCollection(Vector::new));
            Vector<Vector<Object>> dataVector = new Vector<>();
            for (int i = 1; i < data.size(); i++) {
                dataVector.add(new Vector<>(data.get(i)));
            }
            table.setModel(new DefaultTableModel(dataVector, headers));
            adjustColumnWidths(table);
        }

        JPanel tabComponent = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        tabComponent.setOpaque(false);
        JLabel tabLabel = new JLabel(title + " ");
        JButton closeButton = new JButton("x");
        closeButton.setMargin(new Insets(0, 2, 0, 2));
        closeButton.setToolTipText("Close this tab");
        tabComponent.add(tabLabel);
        tabComponent.add(closeButton);

        int tabIndex = unifiedDataViewTabs.getTabCount();
        unifiedDataViewTabs.insertTab(title, null, contentPanel, "Preview for " + title, tabIndex);
        unifiedDataViewTabs.setTabComponentAt(tabIndex, tabComponent);
        unifiedDataViewTabs.setSelectedIndex(tabIndex);
        closeButton.addActionListener(e -> unifiedDataViewTabs.remove(contentPanel));
    }

    private List<List<Object>> projectColumns(List<List<Object>> data, List<String> columnsToKeep) {
        if (data.isEmpty() || columnsToKeep.isEmpty()) {
            return data;
        }
        List<List<Object>> projectedData = new ArrayList<>();
        List<String> originalHeader = data.get(0).stream().map(Object::toString).collect(Collectors.toList());
        List<Integer> indicesToKeep = new ArrayList<>();
        List<Object> newHeader = new ArrayList<>();
        for (String column : columnsToKeep) {
            int index = originalHeader.indexOf(column);
            if (index != -1) {
                indicesToKeep.add(index);
                newHeader.add(column);
            }
        }
        projectedData.add(newHeader);
        for (int i = 1; i < data.size(); i++) {
            List<Object> originalRow = data.get(i);
            List<Object> projectedRow = new ArrayList<>();
            for (int index : indicesToKeep) {
                projectedRow.add(index < originalRow.size() ? originalRow.get(index) : null);
            }
            projectedData.add(projectedRow);
        }
        return projectedData;
    }

    private void createFilterFromSelection(LogicalGroupPanel targetGroup) {
        int[] selectedRows = filterValuesPreviewTable.getSelectedRows();
        int[] selectedCols = filterValuesPreviewTable.getSelectedColumns();
        if (selectedRows.length == 0 || selectedCols.length == 0) {
            JOptionPane.showMessageDialog(this, "Please select one or more cells in the 'Filter Values' table first.", "Selection Required", JOptionPane.WARNING_MESSAGE);
            return;
        }
        List<String> targetColumns = dataFilePanel.getColumnNames();
        if (targetColumns.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Could not retrieve column names from the data file. Please ensure it is loaded correctly.", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }
        for (int row : selectedRows) {
            for (int col : selectedCols) {
                Object cellValueObj = filterValuesPreviewTable.getValueAt(row, col);
                String cellValue = (cellValueObj == null) ? "" : cellValueObj.toString();
                String columnName = filterValuesPreviewTable.getColumnName(col);
                String dialogTitle = String.format("Step 1/2: Select Target for Cell [%d, %d] (Value: %s)", row, col, cellValue);
                FilterTargetDialog targetDialog = new FilterTargetDialog((Frame) SwingUtilities.getWindowAncestor(this), targetColumns, dialogTitle);
                targetDialog.setVisible(true);
                if (targetDialog.isCancelled()) continue;
                List<String> selectedTargets = targetDialog.getSelectedColumns();
                boolean trim = targetDialog.isTrimWhitespaceSelected();
                if (selectedTargets.isEmpty()) continue;
                FilterSourceDialog sourceDialog = new FilterSourceDialog((Frame) SwingUtilities.getWindowAncestor(this), cellValue, columnName);
                sourceDialog.setVisible(true);
                FilterRule.SourceType sourceType = sourceDialog.getSelectedType();
                if (sourceType == null) continue;
                String sourceValue = sourceDialog.getSelectedValue();
                for (String target : selectedTargets) {
                    FilterRule rule = new FilterRule(sourceType, sourceValue, target, trim);
                    logger.info("FilterCreate: targetColumn='{}', op=EQUALS, value='{}', trim={}", target, sourceValue, trim);
                    filterExpressionBuilderPanel.addRuleToGroup(targetGroup, rule);
                }
                updateFilterResults();
            }
        }
    }

    private void loadPreviews() {
        loadTableData(dataFilePanel, dataPreviewModel, 50, "Error loading data preview", dataPreviewTable, true);
        loadTableData(filterValuesFilePanel, filterValuesPreviewModel, -1, "Error loading filter values preview", filterValuesPreviewTable, false);
    }

    public JMenuBar createMenuBar() {
        JMenuBar menuBar = new JMenuBar();
        JMenu fileMenu = new JMenu("File");
        JMenuItem backItem = new JMenuItem("Back to Mode Selection");
        backItem.addActionListener(e -> appContainer.navigateTo("modeSelection"));
        fileMenu.add(backItem);
        fileMenu.addSeparator();

        JMenuItem saveProfileItem = new JMenuItem("Save Profile...");
        saveProfileItem.setAccelerator(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_S, getShortcutMask()));
        saveProfileItem.addActionListener(e -> saveFilterProfile());
        fileMenu.add(saveProfileItem);

        JMenuItem manageProfilesItem = new JMenuItem("Manage Profiles...");
        manageProfilesItem.addActionListener(e -> openProfileManager());
        fileMenu.add(manageProfilesItem);

        fileMenu.addSeparator();
        JMenuItem exitItem = new JMenuItem("Exit");
        exitItem.addActionListener(e -> exitApplication());
        fileMenu.add(exitItem);
        menuBar.add(fileMenu);
        return menuBar;
    }

    private int getShortcutMask() {
        String javaVersion = System.getProperty("java.version");
        if (javaVersion.startsWith("1.8")) {
            return Toolkit.getDefaultToolkit().getMenuShortcutKeyMask();
        } else {
            try {
                // Using reflection to support Java 9+ while compiling with Java 8
                java.lang.reflect.Method method = Toolkit.class.getMethod("getMenuShortcutKeyMaskEx");
                return (int) method.invoke(Toolkit.getDefaultToolkit());
            } catch (Exception e) {
                // Fallback for any unexpected issues
                return Toolkit.getDefaultToolkit().getMenuShortcutKeyMask();
            }
        }
    }

    private void saveFilterProfile() {
        String profileName = JOptionPane.showInputDialog(this, "Enter a name for this profile:", "Save Profile", JOptionPane.PLAIN_MESSAGE);
        if (profileName == null || profileName.trim().isEmpty()) {
            return;
        }

        try {
            ComparisonProfile profile = new ComparisonProfile();
            profile.setSourceFilePath(dataFilePanel.getFilePath());
            profile.setSourceSheetName(dataFilePanel.getSelectedSheet());
            profile.setSourceHeaderRows(dataFilePanel.getHeaderRowIndices());
            profile.setSourceConcatenationMode(dataFilePanel.getConcatenationMode());
            profile.setTargetFilePath(filterValuesFilePanel.getFilePath());
            profile.setTargetSheetName(filterValuesFilePanel.getSelectedSheet());
            profile.setTargetHeaderRows(filterValuesFilePanel.getHeaderRowIndices());
            profile.setTargetConcatenationMode(filterValuesFilePanel.getConcatenationMode());
            profile.setFilterBuilderState(filterExpressionBuilderPanel.getState());

            profileService.saveProfile(profile, profileName);
            isDirty = false;
            JOptionPane.showMessageDialog(this, "Profile '" + profileName + "' saved successfully.", "Success", JOptionPane.INFORMATION_MESSAGE);
        } catch (Exception e) {
            logger.error("Failed to save profile", e);
            JOptionPane.showMessageDialog(this, "Error saving profile: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void openProfileManager() {
        ProfileManagerDialog dialog = new ProfileManagerDialog((Frame) SwingUtilities.getWindowAncestor(this), profileService, this);
        dialog.setVisible(true);
    }


    public void loadProfile(String profileName) {
        try {
            ComparisonProfile profile = profileService.loadProfile(profileName);

            // Handle missing files
            String sourcePath = profile.getSourceFilePath();
            if (sourcePath != null && !new File(sourcePath).exists()) {
                sourcePath = promptForNewFilePath(sourcePath, "Data File");
                if (sourcePath == null) return; // User cancelled
                profile.setSourceFilePath(sourcePath);
            }

            String targetPath = profile.getTargetFilePath();
            if (targetPath != null && !new File(targetPath).exists()) {
                targetPath = promptForNewFilePath(targetPath, "Filter Values File");
                if (targetPath == null) return; // User cancelled
                profile.setTargetFilePath(targetPath);
            }

            dataFilePanel.setFileAndSheet(profile.getSourceFilePath(), profile.getSourceSheetName());
            dataFilePanel.setHeaderSelection(profile.getSourceHeaderRows(), profile.getSourceConcatenationMode());

            filterValuesFilePanel.setFileAndSheet(profile.getTargetFilePath(), profile.getTargetSheetName());
            filterValuesFilePanel.setHeaderSelection(profile.getTargetHeaderRows(), profile.getTargetConcatenationMode());

            filterExpressionBuilderPanel.rebuildFromState(profile.getFilterBuilderState());

            loadPreviews();
            updateFilterResults();
            isDirty = false; // Loading a profile makes it "not dirty"

            logger.info("Profile '{}' loaded.", profileName);
            JOptionPane.showMessageDialog(this, "Profile '" + profileName + "' loaded successfully.", "Success", JOptionPane.INFORMATION_MESSAGE);
        } catch (Exception e) {
            logger.error("Failed to load profile", e);
            JOptionPane.showMessageDialog(this, "Error loading profile: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private String promptForNewFilePath(String originalPath, String fileDescription) {
        String message = String.format("The %s file was not found at the specified path:\n%s\nPlease locate the file.", fileDescription, originalPath);
        JOptionPane.showMessageDialog(this, message, "File Not Found", JOptionPane.WARNING_MESSAGE);

        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Locate " + fileDescription);
        fileChooser.setFileFilter(new FileNameExtensionFilter("Excel Files", "xlsx", "xls"));
        int result = fileChooser.showOpenDialog(this);

        if (result == JFileChooser.APPROVE_OPTION) {
            return fileChooser.getSelectedFile().getAbsolutePath();
        } else {
            return null; // User cancelled
        }
    }

    private void exitApplication() {
        if (isDirty) {
            String[] options = {"Save & Exit", "Exit without Saving", "Cancel"};
            int response = JOptionPane.showOptionDialog(this,
                    "You have unsaved changes to your profile. Do you want to Save before exiting?",
                    "Exit Application",
                    JOptionPane.DEFAULT_OPTION,
                    JOptionPane.WARNING_MESSAGE,
                    null,
                    options,
                    options[0]);

            switch (response) {
                case 0: // Save & Exit
                    saveFilterProfile();
                    if (!isDirty) {
                        System.exit(0);
                    }
                    break;
                case 1: // Exit without Saving
                    System.exit(0);
                    break;
                case 2: // Cancel
                default:
                    // Do nothing
                    break;
            }
        } else {
            System.exit(0);
        }
    }

    private void loadTableData(FilterFilePanel panel, DefaultTableModel model, int rowLimit, String errorTitle, JTable table, boolean useStreaming) {
        String filePath = panel.getFilePath();
        String sheetName = panel.getSelectedSheet();
        if (filePath == null || filePath.trim().isEmpty() || sheetName == null) {
            JOptionPane.showMessageDialog(this, "Please select a file and a sheet.", "File Not Selected", JOptionPane.WARNING_MESSAGE);
            return;
        }
        new SwingWorker<List<List<Object>>, Void>() {
            @Override
            protected List<List<Object>> doInBackground() throws Exception {
                if (rowLimit > 0) {
                    return ExcelReader.readPreview(filePath, sheetName, rowLimit);
                } else {
                    return ExcelReader.read(filePath, sheetName, useStreaming);
                }
            }

            @Override
            protected void done() {
                try {
                    List<List<Object>> data = get();
                    if (data == null || data.isEmpty()) {
                        model.setDataVector(new Vector<>(), new Vector<>());
                        return;
                    }
                    List<String> headers = panel.getColumnNames();
                    Vector<String> headerVector = new Vector<>(headers);
                    model.setColumnIdentifiers(headerVector);
                    int headerRowCount = panel.getHeaderRowIndices().isEmpty() ? 1 : panel.getHeaderRowIndices().size();
                    Vector<Vector<Object>> dataVector = new Vector<>();
                    if (data.size() > headerRowCount) {
                        List<List<Object>> dataRows = data.subList(headerRowCount, data.size());
                        for (List<Object> row : dataRows) {
                            dataVector.add(new Vector<>(row));
                        }
                    }
                    model.setDataVector(dataVector, headerVector);
                    adjustColumnWidths(table);
                } catch (Exception e) {
                    JOptionPane.showMessageDialog((Frame) SwingUtilities.getWindowAncestor(FilterPanel.this), "Could not load data: " + e.getMessage(), errorTitle, JOptionPane.ERROR_MESSAGE);
                }
            }
        }.execute();
    }
}
