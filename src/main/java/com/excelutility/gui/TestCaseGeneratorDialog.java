package com.excelutility.gui;

import com.excelutility.test.TestCaseGenerator;
import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.util.List;

public class TestCaseGeneratorDialog extends JDialog {

    private JList<String> scenarioList;
    private JSpinner rowCountSpinner;
    private TestCaseTableModel tableModel;
    private TestCaseGenerator generator = new TestCaseGenerator("target/test-cases");

    public TestCaseGeneratorDialog(Frame owner) {
        super(owner, "Test Case Generator", true);
        setSize(800, 600);
        setLocationRelativeTo(owner);
        initComponents();
    }

    private void initComponents() {
        setLayout(new BorderLayout(10, 10));

        // --- Configuration Panel (Left) ---
        JPanel configPanel = new JPanel(new MigLayout("fillx", "[grow]", "[][grow][]"));
        configPanel.setBorder(BorderFactory.createTitledBorder("Generation Options"));

        // Scenario Selection
        DefaultListModel<String> scenarioListModel = new DefaultListModel<>();
        scenarioListModel.addElement("Exact Match");
        // Add more scenarios here
        scenarioList = new JList<>(scenarioListModel);
        configPanel.add(new JLabel("Select Scenarios to Generate:"), "wrap");
        configPanel.add(new JScrollPane(scenarioList), "grow, wrap");

        // Options
        JPanel optionsGrid = new JPanel(new MigLayout());
        optionsGrid.add(new JLabel("Rows per scenario:"));
        rowCountSpinner = new JSpinner(new SpinnerNumberModel(10, 1, 1000, 1));
        optionsGrid.add(rowCountSpinner, "wrap");
        configPanel.add(optionsGrid, "wrap");

        // Generate Button
        JButton generateButton = new JButton("Generate");
        generateButton.addActionListener(e -> generateTestCases());
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        buttonPanel.add(generateButton);
        configPanel.add(buttonPanel, "growx");

        // --- Results Panel (Right) ---
        JPanel resultsPanel = new JPanel(new MigLayout("fill", "[grow]", "[grow]"));
        resultsPanel.setBorder(BorderFactory.createTitledBorder("Generated Test Cases"));

        tableModel = new TestCaseTableModel();
        JTable generatedTestsTable = new JTable(tableModel);
        resultsPanel.add(new JScrollPane(generatedTestsTable), "grow");

        // --- Split Pane ---
        JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, configPanel, resultsPanel);
        splitPane.setResizeWeight(0.4);

        add(splitPane, BorderLayout.CENTER);
    }

    private void generateTestCases() {
        List<String> selectedScenarios = scenarioList.getSelectedValuesList();
        if (selectedScenarios.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Please select at least one scenario.", "No Scenario Selected", JOptionPane.WARNING_MESSAGE);
            return;
        }
        int rowCount = (int) rowCountSpinner.getValue();

        try {
            tableModel.clear(); // Clear previous results
            for (String scenario : selectedScenarios) {
                // This is a simplified approach. A real implementation would need to handle the metadata.
                generator.generate(scenario, rowCount);
            }
            JOptionPane.showMessageDialog(this, "Test cases generated successfully in 'target/test-cases' directory.", "Success", JOptionPane.INFORMATION_MESSAGE);
            // TODO: Read metadata files and populate the table
        } catch (IOException e) {
            JOptionPane.showMessageDialog(this, "Error generating test cases: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }
}
