package com.excelutility.gui;

import com.excelutility.core.ComparisonResult;
import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import java.awt.Font;

/**
 * A panel to display the summary statistics of a comparison run.
 */
public class SummaryPanel extends JPanel {

    private final JLabel totalSourceRowsLabel = createValueLabel();
    private final JLabel totalTargetRowsLabel = createValueLabel();
    private final JLabel identicalRowsLabel = createValueLabel();
    private final JLabel mismatchedRowsLabel = createValueLabel();
    private final JLabel sourceOnlyRowsLabel = createValueLabel();
    private final JLabel targetOnlyRowsLabel = createValueLabel();

    public SummaryPanel() {
        setLayout(new MigLayout("fillx", "[right][left, grow]"));
        setBorder(BorderFactory.createTitledBorder("Comparison Summary"));

        add(new JLabel("Total Rows in Source:"));
        add(totalSourceRowsLabel, "wrap");
        add(new JLabel("Total Rows in Target:"));
        add(totalTargetRowsLabel, "wrap");

        add(new JSeparator(), "span, growx, gaptop 5, gapbottom 5, wrap");

        add(new JLabel("Identical Rows:"));
        add(identicalRowsLabel, "wrap");
        add(new JLabel("Mismatched Rows:"));
        add(mismatchedRowsLabel, "wrap");
        add(new JLabel("Extra Rows in Source:"));
        add(sourceOnlyRowsLabel, "wrap");
        add(new JLabel("Extra Rows in Target:"));
        add(targetOnlyRowsLabel, "wrap");
    }

    public void updateSummary(ComparisonResult.ComparisonStats stats) {
        if (stats == null) {
            clearSummary();
            return;
        }
        totalSourceRowsLabel.setText(String.valueOf(stats.totalSourceRows));
        totalTargetRowsLabel.setText(String.valueOf(stats.totalTargetRows));
        identicalRowsLabel.setText(String.valueOf(stats.matchedIdentical));
        mismatchedRowsLabel.setText(String.valueOf(stats.matchedMismatched));
        sourceOnlyRowsLabel.setText(String.valueOf(stats.sourceOnly));
        targetOnlyRowsLabel.setText(String.valueOf(stats.targetOnly));
    }

    public void clearSummary() {
        totalSourceRowsLabel.setText("-");
        totalTargetRowsLabel.setText("-");
        identicalRowsLabel.setText("-");
        mismatchedRowsLabel.setText("-");
        sourceOnlyRowsLabel.setText("-");
        targetOnlyRowsLabel.setText("-");
    }

    private JLabel createValueLabel() {
        JLabel label = new JLabel("-");
        label.setFont(label.getFont().deriveFont(Font.BOLD));
        return label;
    }
}
