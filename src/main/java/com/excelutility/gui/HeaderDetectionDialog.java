package com.excelutility.gui;

import com.excelutility.core.ConcatenationMode;
import com.excelutility.core.HeaderDetector;
import org.apache.poi.ss.usermodel.Sheet;
import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;

public class HeaderDetectionDialog extends JDialog {

    private final List<JCheckBox> rowCheckBoxes = new ArrayList<>();
    private final JComboBox<ConcatenationMode> modeCombo;
    private boolean confirmed = false;

    public HeaderDetectionDialog(Frame owner, Sheet sheet) {
        super(owner, "Header Row Selection", true);
        setSize(800, 600);
        setLocationRelativeTo(owner);
        setLayout(new BorderLayout(10, 10));

        // --- Detection Logic ---
        HeaderDetector detector = new HeaderDetector();
        HeaderDetector.HeaderDetectionResult result = detector.detectHeader(sheet);
        List<Integer> detectedRows = result.getDetectedHeaderRows();

        // --- Main Panel ---
        JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new BoxLayout(mainPanel, BoxLayout.Y_AXIS));
        mainPanel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        int rowsToScan = Math.min(20, sheet.getLastRowNum() + 1);
        for (int i = 0; i < rowsToScan; i++) {
            JPanel rowPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
            JCheckBox checkBox = new JCheckBox("Row " + (i + 1));
            checkBox.setSelected(detectedRows.contains(i));
            rowCheckBoxes.add(checkBox);
            rowPanel.add(checkBox);

            final int currentRowIndex = i;
            result.getConfidenceScores().stream()
                .filter(r -> r.getRowIndex() == currentRowIndex)
                .findFirst()
                .ifPresent(r -> rowPanel.add(new JLabel(String.format("(Confidence: %.2f, Reason: %s)", r.getScore(), r.getReason()))));

            mainPanel.add(rowPanel);
        }

        add(new JScrollPane(mainPanel), BorderLayout.CENTER);

        // --- Top Panel for Mode Selection ---
        JPanel topPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        topPanel.add(new JLabel("Header Concatenation Mode:"));
        modeCombo = new JComboBox<>(ConcatenationMode.values());
        topPanel.add(modeCombo);
        add(topPanel, BorderLayout.NORTH);

        // --- Bottom Panel ---
        JPanel bottomPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton okButton = new JButton("OK");
        JButton cancelButton = new JButton("Cancel");
        bottomPanel.add(okButton);
        bottomPanel.add(cancelButton);
        add(bottomPanel, BorderLayout.SOUTH);

        // --- Listeners ---
        okButton.addActionListener(e -> {
            confirmed = true;
            dispose();
        });
        cancelButton.addActionListener(e -> dispose());
    }

    public boolean isConfirmed() {
        return confirmed;
    }

    public List<Integer> getSelectedHeaderRowIndices() {
        List<Integer> selectedIndices = new ArrayList<>();
        for (int i = 0; i < rowCheckBoxes.size(); i++) {
            if (rowCheckBoxes.get(i).isSelected()) {
                selectedIndices.add(i);
            }
        }
        return selectedIndices;
    }

    public ConcatenationMode getConcatenationMode() {
        return (ConcatenationMode) modeCombo.getSelectedItem();
    }
}
