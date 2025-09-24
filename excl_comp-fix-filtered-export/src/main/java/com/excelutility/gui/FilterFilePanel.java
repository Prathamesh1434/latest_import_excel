package com.excelutility.gui;

import com.excelutility.core.CanonicalNameBuilder;
import com.excelutility.core.ConcatenationMode;
import com.excelutility.io.ExcelReader;
import net.miginfocom.swing.MigLayout;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A panel for selecting an Excel file and a specific sheet from it.
 * Includes functionality for searching sheets and detecting multi-row headers.
 */
public class FilterFilePanel extends JPanel {

    private final JTextField fileField = new JTextField();
    private final JComboBox<String> sheetCombo = new JComboBox<>();
    private final JButton detectHeaderButton;
    private final JTextField searchField = new JTextField();

    private List<Integer> headerRowIndices = new ArrayList<>();
    private ConcatenationMode concatenationMode = ConcatenationMode.LEAF_ONLY;
    private File selectedFile;
    private final Component parent;
    private List<String> allSheetNames = new ArrayList<>();

    /**
     * Constructs a new file selection panel.
     * @param title  The title to display in the panel's border.
     * @param parent The parent component, used for dialog positioning.
     */
    public FilterFilePanel(String title, Component parent) {
        this.parent = parent;
        setLayout(new MigLayout("fillx", "[][grow][]", ""));
        setBorder(BorderFactory.createTitledBorder(title));

        fileField.setEditable(false);
        JButton openButton = new JButton("Browse...");
        detectHeaderButton = new JButton("Detect Header");

        add(new JLabel("File:"));
        add(fileField, "growx");
        add(openButton, "wrap");
        add(new JLabel("Search Sheet:"));
        add(searchField, "growx, span 2, wrap");
        add(new JLabel("Sheet:"));
        add(sheetCombo, "growx, span 2, wrap, gaptop 5");
        add(detectHeaderButton, "span, growx, gaptop 5");

        openButton.addActionListener(e -> selectFile());
        detectHeaderButton.addActionListener(e -> detectHeader());
        searchField.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) { filterSheets(); }
            @Override
            public void removeUpdate(DocumentEvent e) { filterSheets(); }
            @Override
            public void changedUpdate(DocumentEvent e) { filterSheets(); }
        });
    }

    private void selectFile() {
        JFileChooser chooser = new JFileChooser();
        FileNameExtensionFilter excelFilter = new FileNameExtensionFilter("Excel Files (*.xls, *.xlsx)", "xls", "xlsx");
        chooser.addChoosableFileFilter(excelFilter);
        chooser.setFileFilter(excelFilter);
        if (chooser.showOpenDialog(parent) == JFileChooser.APPROVE_OPTION) {
            this.selectedFile = chooser.getSelectedFile();
            fileField.setText(selectedFile.getAbsolutePath());
            loadAllSheetNames();
            filterSheets();
        }
    }

    private void loadAllSheetNames() {
        if (selectedFile == null) return;
        try {
            this.allSheetNames = ExcelReader.getSheetNames(selectedFile.getAbsolutePath());
        } catch (IOException e) {
            this.allSheetNames = new ArrayList<>();
            JOptionPane.showMessageDialog(parent, "Error reading sheets from file: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void filterSheets() {
        String searchTerm = searchField.getText().toLowerCase();
        List<String> filteredSheets = allSheetNames.stream()
                .filter(sheet -> sheet.toLowerCase().contains(searchTerm))
                .collect(Collectors.toList());

        DefaultComboBoxModel<String> model = new DefaultComboBoxModel<>();
        model.addAll(filteredSheets);
        sheetCombo.setModel(model);

        if (sheetCombo.getItemCount() > 0) {
            sheetCombo.setSelectedIndex(0);
        }
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

    /**
     * Gets the canonical column headers from the selected sheet, respecting multi-row header settings.
     * @return A list of header strings.
     */
    public List<String> getColumnNames() {
        if (selectedFile == null || getSelectedSheet() == null) {
            return new ArrayList<>();
        }
        try (Workbook workbook = WorkbookFactory.create(selectedFile)) {
            Sheet sheet = workbook.getSheet(getSelectedSheet());
            if (sheet == null) return new ArrayList<>();

            List<Integer> finalHeaderRows = headerRowIndices.isEmpty() ? List.of(0) : headerRowIndices;
            return CanonicalNameBuilder.buildCanonicalHeaders(sheet, finalHeaderRows, concatenationMode, " | ");
        } catch (Exception e) {
            JOptionPane.showMessageDialog(parent, "Error reading headers from file: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            return new ArrayList<>();
        }
    }

    public String getFilePath() { return fileField.getText(); }
    public String getSelectedSheet() { return sheetCombo.getSelectedItem() != null ? sheetCombo.getSelectedItem().toString() : null; }
    public List<Integer> getHeaderRowIndices() { return headerRowIndices; }
    public ConcatenationMode getConcatenationMode() { return concatenationMode; }

    public void setFileAndSheet(String filePath, String sheetName) {
        if (filePath == null || filePath.trim().isEmpty()) {
            this.selectedFile = null;
            fileField.setText("");
            allSheetNames.clear();
            filterSheets();
            return;
        }

        this.selectedFile = new File(filePath);
        fileField.setText(filePath);
        loadAllSheetNames();
        filterSheets();

        if (sheetName != null) {
            sheetCombo.setSelectedItem(sheetName);
        }
    }

    public void setHeaderSelection(List<Integer> indices, ConcatenationMode mode) {
        this.headerRowIndices = (indices != null) ? new ArrayList<>(indices) : new ArrayList<>();
        this.concatenationMode = (mode != null) ? mode : ConcatenationMode.LEAF_ONLY;
    }
}
