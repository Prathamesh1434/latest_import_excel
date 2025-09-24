package com.excelutility.gui;

import com.excelutility.core.ComparisonProfile;
import com.excelutility.core.ComparisonResult;
import com.excelutility.core.ComparisonService;
import com.excelutility.core.KeySuggester;
import com.excelutility.io.ExcelReader;
import com.excelutility.io.ProfileService;
import com.excelutility.io.SimpleExcelWriter;
import net.miginfocom.swing.MigLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

public class ComparePanel extends JPanel {

    private static final Logger logger = LoggerFactory.getLogger(ComparePanel.class);
    private final ComparisonProfile profile = new ComparisonProfile();
    private final ComparisonService comparisonService = new ComparisonService();
    private final ProfileService profileService = new ProfileService("profiles");

    private final FileConfigPanel sourceFilePanel;
    private final FileConfigPanel targetFilePanel;
    private final ColumnMappingPanel columnMappingPanel;
    private final JTable resultsTable;
    private final ResultTableModel resultsTableModel;
    private final SummaryPanel summaryPanel;
    private final JLabel statusLabel;

    private final DefaultTableModel sourcePreviewModel;
    private final DefaultTableModel targetPreviewModel;

    private List<String> sourceHeaders;
    private List<String> targetHeaders;
    private final AppContainer appContainer;

    public ComparePanel(AppContainer appContainer) {
        this.appContainer = appContainer;
        setLayout(new BorderLayout());

        // --- Main Panel ---
        JPanel mainPanel = new JPanel(new MigLayout("fill", "[grow]", "[][grow]"));

        // --- Top Control Panel ---
        JPanel topPanel = new JPanel(new MigLayout("fillx", "[grow][grow]"));
        sourceFilePanel = new FileConfigPanel("Source File", this);
        targetFilePanel = new FileConfigPanel("Target File", this);
        topPanel.add(sourceFilePanel, "growx");
        topPanel.add(targetFilePanel, "growx");
        mainPanel.add(topPanel, "dock north");

        // --- Center Split Pane ---
        JSplitPane centerSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        centerSplit.setResizeWeight(0.5);

        // --- Top part of Center Split: Mappings and Previews ---
        columnMappingPanel = new ColumnMappingPanel();

        // Preview Panels
        sourcePreviewModel = new DefaultTableModel();
        JTable sourcePreviewTable = new JTable(sourcePreviewModel);
        JScrollPane sourcePreviewScroll = new JScrollPane(sourcePreviewTable);
        sourcePreviewScroll.setBorder(BorderFactory.createTitledBorder("Source Preview"));

        targetPreviewModel = new DefaultTableModel();
        JTable targetPreviewTable = new JTable(targetPreviewModel);
        JScrollPane targetPreviewScroll = new JScrollPane(targetPreviewTable);
        targetPreviewScroll.setBorder(BorderFactory.createTitledBorder("Target Preview"));

        JSplitPane previewSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, sourcePreviewScroll, targetPreviewScroll);
        previewSplit.setResizeWeight(0.5);

        JSplitPane topSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, columnMappingPanel, previewSplit);
        topSplit.setResizeWeight(0.5);

        // --- Bottom part of Center Split: Results Table ---
        resultsTableModel = new ResultTableModel();
        resultsTable = new JTable(resultsTableModel);
        resultsTable.setDefaultRenderer(Object.class, new ResultTableRenderer());
        JScrollPane resultsScrollPane = new JScrollPane(resultsTable);
        resultsScrollPane.setBorder(BorderFactory.createTitledBorder("Comparison Results"));

        summaryPanel = new SummaryPanel();

        JSplitPane bottomSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, resultsScrollPane, summaryPanel);
        bottomSplit.setResizeWeight(0.8);

        centerSplit.setTopComponent(topSplit);
        centerSplit.setBottomComponent(bottomSplit);

        mainPanel.add(centerSplit, "grow");
        add(mainPanel, BorderLayout.CENTER);

        // --- Status Bar ---
        JPanel statusPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        statusLabel = new JLabel("Ready.");
        statusPanel.add(statusLabel);
        add(statusPanel, BorderLayout.SOUTH);

        // --- Action Listeners ---
        sourceFilePanel.getAutoSuggestButton().addActionListener(e -> autoSuggestKeys(true));
        targetFilePanel.getAutoSuggestButton().addActionListener(e -> autoSuggestKeys(false));
        sourceFilePanel.getPreviewButton().addActionListener(e -> showPreview(true));
        targetFilePanel.getPreviewButton().addActionListener(e -> showPreview(false));
        sourceFilePanel.getSheetCombo().addActionListener(e -> {
            if (e.getActionCommand().equals("comboBoxChanged") && sourceFilePanel.getSheetCombo().getSelectedItem() != null) {
                loadHeaders(true);
            }
        });
        targetFilePanel.getSheetCombo().addActionListener(e -> {
            if (e.getActionCommand().equals("comboBoxChanged") && targetFilePanel.getSheetCombo().getSelectedItem() != null) {
                loadHeaders(false);
            }
        });
    }

    public JMenuBar createMenuBar() {
        JMenuBar menuBar = new JMenuBar();
        JMenu fileMenu = new JMenu("File");

        JMenuItem backItem = new JMenuItem("Back to Mode Selection");
        backItem.addActionListener(e -> appContainer.navigateTo("modeSelection"));
        fileMenu.add(backItem);
        fileMenu.addSeparator();

        JMenuItem saveProfileItem = new JMenuItem("Save Profile As...");
        saveProfileItem.addActionListener(e -> saveProfile());
        fileMenu.add(saveProfileItem);

        JMenuItem loadProfileItem = new JMenuItem("Load Profile...");
        loadProfileItem.addActionListener(e -> loadProfile());
        fileMenu.add(loadProfileItem);

        JMenuItem exportResultsItem = new JMenuItem("Export Results...");
        exportResultsItem.addActionListener(e -> exportResults());
        fileMenu.add(exportResultsItem);

        fileMenu.addSeparator();

        JMenuItem exitItem = new JMenuItem("Exit");
        exitItem.addActionListener(e -> System.exit(0));
        fileMenu.add(exitItem);
        menuBar.add(fileMenu);

        JMenu editMenu = new JMenu("Edit");
        JMenuItem profileManagerItem = new JMenuItem("Profile Manager...");
        profileManagerItem.addActionListener(e -> openProfileManager());
        editMenu.add(profileManagerItem);
        menuBar.add(editMenu);

        JMenu toolsMenu = new JMenu("Tools");
        JMenuItem runMenuItem = new JMenuItem("Run Comparison");
        runMenuItem.addActionListener(e -> runComparison());
        toolsMenu.add(runMenuItem);
        JMenuItem testGenItem = new JMenuItem("Generate Test Cases...");
        testGenItem.addActionListener(e -> openTestCaseGenerator());
        toolsMenu.add(testGenItem);
        menuBar.add(toolsMenu);

        return menuBar;
    }

    private void loadHeaders(boolean isSource) {
        FileConfigPanel panel = isSource ? sourceFilePanel : targetFilePanel;
        String filePath = panel.getFilePath();
        String sheetName = panel.getSelectedSheet();

        if (filePath == null || filePath.trim().isEmpty() || sheetName == null) {
            return;
        }

        try (org.apache.poi.ss.usermodel.Workbook workbook = org.apache.poi.ss.usermodel.WorkbookFactory.create(new java.io.File(filePath))) {
            org.apache.poi.ss.usermodel.Sheet sheet = workbook.getSheet(sheetName);
            if (sheet == null) return;

            List<Integer> headerRows = panel.getHeaderRowIndices();
            if (headerRows == null || headerRows.isEmpty()) {
                headerRows = java.util.Collections.singletonList(0);
            }

            List<String> canonicalHeaders = com.excelutility.core.CanonicalNameBuilder.buildCanonicalHeaders(
                sheet, headerRows, panel.getConcatenationMode(), " | ");

            if (isSource) {
                this.sourceHeaders = canonicalHeaders;
            } else {
                this.targetHeaders = canonicalHeaders;
            }

            if (this.sourceHeaders != null && this.targetHeaders != null) {
                columnMappingPanel.setColumns(this.sourceHeaders, this.targetHeaders);
            }
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "Error processing headers from file: " + filePath, "Header Read Error", JOptionPane.ERROR_MESSAGE);
            e.printStackTrace();
        }
    }

    private boolean validateGuiBeforeRun() {
        if (sourceFilePanel.getFilePath().trim().isEmpty() || targetFilePanel.getFilePath().trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Both Source and Target files must be selected.", "Validation Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }
        if (sourceFilePanel.getSelectedSheet() == null || targetFilePanel.getSelectedSheet() == null) {
            JOptionPane.showMessageDialog(this, "A sheet must be selected for both files.", "Validation Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }
        if (columnMappingPanel.getKeyColumns().isEmpty()) {
            JOptionPane.showMessageDialog(this, "At least one key column must be selected in the 'Column Mappings' panel.", "Validation Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }
        return true;
    }

    private void runComparison() {
        if (!validateGuiBeforeRun()) {
            return; // Stop if validation fails
        }

        updateProfileFromGui();

        logger.info("Starting comparison with profile: {}", profile);
        statusLabel.setText("Running comparison...");
        summaryPanel.clearSummary();
        resultsTableModel.clear();

        new SwingWorker<ComparisonResult, Void>() {
            @Override
            protected ComparisonResult doInBackground() throws Exception {
                return comparisonService.compare(profile);
            }

            @Override
            protected void done() {
                try {
                    ComparisonResult result = get();
                    resultsTableModel.setComparisonResult(result);
                    summaryPanel.updateSummary(result.getStats());
                    statusLabel.setText("Comparison complete. Found " + result.getStats().matchedMismatched + " mismatches and " + (result.getStats().sourceOnly + result.getStats().targetOnly) + " unmatched rows.");
                } catch (Exception e) {
                    statusLabel.setText("Error during comparison.");
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    logger.error("Comparison failed with exception.", cause);
                    ErrorDialog dialog = new ErrorDialog((Frame) SwingUtilities.getWindowAncestor(ComparePanel.this), "Comparison Failed", cause.getMessage(), cause);
                    dialog.setVisible(true);
                }
            }
        }.execute();
    }

    private void showPreview(boolean isSource) {
        FileConfigPanel panel = isSource ? sourceFilePanel : targetFilePanel;
        String filePath = panel.getFilePath();
        String sheetName = panel.getSelectedSheet();

        if (filePath == null || filePath.trim().isEmpty() || sheetName == null) {
            JOptionPane.showMessageDialog(this, "Please select a file and a sheet to preview.", "Cannot Preview", JOptionPane.WARNING_MESSAGE);
            return;
        }

        statusLabel.setText("Loading preview...");

        new SwingWorker<List<List<Object>>, Void>() {
            @Override
            protected List<List<Object>> doInBackground() throws Exception {
                return ExcelReader.readPreview(filePath, sheetName, 10);
            }

            @Override
            protected void done() {
                try {
                    List<List<Object>> previewData = get();
                    DefaultTableModel model = isSource ? sourcePreviewModel : targetPreviewModel;

                    if (previewData == null || previewData.isEmpty()) {
                        model.setDataVector(new Vector<>(), new Vector<>());
                        statusLabel.setText("Preview loaded. No data found.");
                        return;
                    }

                    Vector<String> headers = new Vector<>();
                    for (Object header : previewData.get(0)) {
                        headers.add(header != null ? header.toString() : "");
                    }

                    Vector<Vector<Object>> data = new Vector<>();
                    if (previewData.size() > 1) {
                        for (int i = 1; i < previewData.size(); i++) {
                            data.add(new Vector<>(previewData.get(i)));
                        }
                    }

                    model.setDataVector(data, headers);
                    statusLabel.setText("Preview loaded successfully.");

                } catch (Exception e) {
                    statusLabel.setText("Error loading preview.");
                    JOptionPane.showMessageDialog(ComparePanel.this, "Could not load preview: " + e.getMessage(), "Preview Error", JOptionPane.ERROR_MESSAGE);
                }
            }
        }.execute();
    }

    private void openTestCaseGenerator() {
        TestCaseGeneratorDialog dialog = new TestCaseGeneratorDialog((Frame) SwingUtilities.getWindowAncestor(this));
        dialog.setVisible(true);
    }

    private void openProfileManager() {
        ProfileManagerDialog dialog = new ProfileManagerDialog((Frame) SwingUtilities.getWindowAncestor(this), profileService);
        dialog.setVisible(true);
    }

    private void saveProfile() {
        updateProfileFromGui();
        String profileName = JOptionPane.showInputDialog(this, "Enter a name for this profile:", "Save Profile", JOptionPane.PLAIN_MESSAGE);
        if (profileName != null && !profileName.trim().isEmpty()) {
            try {
                profileService.saveProfile(profile, profileName);
                JOptionPane.showMessageDialog(this, "Profile saved successfully.", "Success", JOptionPane.INFORMATION_MESSAGE);
            } catch (IOException e) {
                JOptionPane.showMessageDialog(this, "Error saving profile: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void loadProfile() {
        List<String> profiles = profileService.getAvailableProfiles();
        if (profiles.isEmpty()) {
            JOptionPane.showMessageDialog(this, "No saved profiles found.", "Load Profile", JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        String selectedProfile = (String) JOptionPane.showInputDialog(this, "Select a profile to load:",
                "Load Profile", JOptionPane.QUESTION_MESSAGE, null, profiles.toArray(), profiles.get(0));

        if (selectedProfile != null) {
            try {
                ComparisonProfile loadedProfile = profileService.loadProfile(selectedProfile);
                updateGuiFromProfile(loadedProfile);
                JOptionPane.showMessageDialog(this, "Profile '" + selectedProfile + "' loaded successfully.", "Success", JOptionPane.INFORMATION_MESSAGE);
            } catch (IOException e) {
                JOptionPane.showMessageDialog(this, "Error loading profile: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void updateProfileFromGui() {
        profile.setSourceFilePath(sourceFilePanel.getFilePath());
        profile.setSourceSheetName(sourceFilePanel.getSelectedSheet());
        profile.setTargetFilePath(targetFilePanel.getFilePath());
        profile.setTargetSheetName(targetFilePanel.getSelectedSheet());
        profile.setColumnMappings(columnMappingPanel.getColumnMappings());
        profile.setKeyColumns(columnMappingPanel.getKeyColumns());
        profile.setIgnoredColumns(columnMappingPanel.getIgnoredColumns());
        profile.setSourceHeaderRows(sourceFilePanel.getHeaderRowIndices());
        profile.setTargetHeaderRows(targetFilePanel.getHeaderRowIndices());
        profile.setSourceConcatenationMode(sourceFilePanel.getConcatenationMode());
        profile.setTargetConcatenationMode(targetFilePanel.getConcatenationMode());
        profile.setSourceFilterGroup(sourceFilePanel.getActiveFilterGroup());
        profile.setTargetFilterGroup(targetFilePanel.getActiveFilterGroup());
    }

    private void updateGuiFromProfile(ComparisonProfile loadedProfile) {
        sourceFilePanel.setFilePath(loadedProfile.getSourceFilePath());
        targetFilePanel.setFilePath(loadedProfile.getTargetFilePath());

        SwingUtilities.invokeLater(() -> {
            sourceFilePanel.setSelectedSheet(loadedProfile.getSourceSheetName());
            targetFilePanel.setSelectedSheet(loadedProfile.getTargetSheetName());

            if (loadedProfile.getSourceSheetName() != null) {
                loadHeaders(true);
            }
            if (loadedProfile.getTargetSheetName() != null) {
                loadHeaders(false);
            }

            if (this.sourceHeaders != null && this.targetHeaders != null &&
                loadedProfile.getColumnMappings() != null && loadedProfile.getKeyColumns() != null) {
                columnMappingPanel.setMappings(loadedProfile.getColumnMappings(), loadedProfile.getKeyColumns());
            }
            if (loadedProfile.getIgnoredColumns() != null) {
                columnMappingPanel.setIgnoredColumns(loadedProfile.getIgnoredColumns());
            }
            if(loadedProfile.getSourceHeaderRows() != null) {
                sourceFilePanel.setHeaderRowIndices(loadedProfile.getSourceHeaderRows());
            }
            if(loadedProfile.getTargetHeaderRows() != null) {
                targetFilePanel.setHeaderRowIndices(loadedProfile.getTargetHeaderRows());
            }
            if(loadedProfile.getSourceConcatenationMode() != null) {
                sourceFilePanel.setConcatenationMode(loadedProfile.getSourceConcatenationMode());
            }
            if(loadedProfile.getTargetConcatenationMode() != null) {
                targetFilePanel.setConcatenationMode(loadedProfile.getTargetConcatenationMode());
            }
        });
    }

    private void autoSuggestKeys(boolean isSource) {
        FileConfigPanel panel = isSource ? sourceFilePanel : targetFilePanel;
        String filePath = panel.getFilePath();
        String sheetName = panel.getSelectedSheet();

        if (filePath == null || filePath.trim().isEmpty() || sheetName == null) {
            JOptionPane.showMessageDialog(this, "Please load a file and select a sheet first.", "File Required", JOptionPane.WARNING_MESSAGE);
            return;
        }

        try {
            statusLabel.setText("Analyzing file for key suggestions...");
            List<List<Object>> data = ExcelReader.read(filePath, sheetName, true);
            if (data.size() < 2) {
                statusLabel.setText("Not enough data to suggest keys.");
                return;
            }
            List<Object> headers = data.remove(0);

            List<KeySuggester.KeySuggestion> suggestions = KeySuggester.suggestKeys(data, headers);

            KeySuggestionDialog dialog = new KeySuggestionDialog((Frame) SwingUtilities.getWindowAncestor(this), suggestions);
            dialog.setVisible(true);

            if (dialog.isAccepted()) {
                List<String> selectedKeys = dialog.getSelectedSuggestions().stream()
                        .map(KeySuggester.KeySuggestion::getColumnName)
                        .collect(Collectors.toList());

                if (isSource) {
                    columnMappingPanel.selectKeys(selectedKeys);
                } else {
                    columnMappingPanel.selectKeysFromTarget(selectedKeys);
                }
            }
            statusLabel.setText("Ready.");
        } catch (Exception e) {
            statusLabel.setText("Error during key suggestion.");
            JOptionPane.showMessageDialog(this, "Error suggesting keys: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            e.printStackTrace();
        }
    }

    private void exportResults() {
        if (resultsTableModel.getRowCount() == 0) {
            JOptionPane.showMessageDialog(this, "There are no results to export.", "Export Results", JOptionPane.INFORMATION_MESSAGE);
            return;
        }

        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("Save Exported Results");
        chooser.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("Excel Workbook (*.xlsx)", "xlsx"));

        if (chooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            File fileToSave = chooser.getSelectedFile();
            String filePath = fileToSave.getAbsolutePath();
            if (!filePath.toLowerCase().endsWith(".xlsx")) {
                filePath += ".xlsx";
            }

            final String finalFilePath = filePath;
            statusLabel.setText("Exporting results to Excel...");

            new SwingWorker<Void, Void>() {
                @Override
                protected Void doInBackground() throws Exception {
                    ComparisonResult result = resultsTableModel.getComparisonResult();
                    SimpleExcelWriter.writeComparisonResult(result, finalFilePath);
                    return null;
                }

                @Override
                protected void done() {
                    try {
                        get();
                        statusLabel.setText("Results exported successfully.");
                        JOptionPane.showMessageDialog(ComparePanel.this, "Results exported successfully to:\n" + finalFilePath, "Export Complete", JOptionPane.INFORMATION_MESSAGE);
                    } catch (Exception e) {
                        statusLabel.setText("Error during export.");
                        JOptionPane.showMessageDialog(ComparePanel.this, "Failed to export results: " + e.getMessage(), "Export Error", JOptionPane.ERROR_MESSAGE);
                        e.printStackTrace();
                    }
                }
            }.execute();
        }
    }
}
