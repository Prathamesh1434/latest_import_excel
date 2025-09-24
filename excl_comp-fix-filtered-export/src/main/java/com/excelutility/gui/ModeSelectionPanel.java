package com.excelutility.gui;

import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;

public class ModeSelectionPanel extends JPanel {

    private final AppContainer appContainer;

    public ModeSelectionPanel(AppContainer appContainer) {
        this.appContainer = appContainer;
        setLayout(new BorderLayout(20, 20));
        setBorder(new EmptyBorder(40, 40, 40, 40));

        // --- Title ---
        JLabel titleLabel = new JLabel("Welcome to Excel Utility", SwingConstants.CENTER);
        titleLabel.setFont(new Font("SansSerif", Font.BOLD, 32));
        add(titleLabel, BorderLayout.NORTH);

        // --- Center Panel for Mode Buttons ---
        JPanel centerPanel = new JPanel(new MigLayout("fillx, align 50% 50%", "[grow, center]20[grow, center]"));

        JPanel comparePanel = createModePanel(
            "Compare Excel Files",
            "Perform a detailed, cell-by-cell comparison of two Excel sheets. Identify differences, and find records that exist in one file but not the other.",
            e -> appContainer.navigateTo("compare")
        );

        JPanel filterPanel = createModePanel(
            "Spec QA Recon",
            "Filter a data sheet using values from another file. Build complex, nested AND/OR logic, view results for each rule, and export the final dataset.",
            e -> appContainer.navigateTo("filter")
        );

        centerPanel.add(comparePanel, "grow");
        centerPanel.add(filterPanel, "grow");
        add(centerPanel, BorderLayout.CENTER);

        // --- Footer ---
        JPanel footerPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        footerPanel.add(new JLabel("Version: 1.1.0"));
        footerPanel.add(new JSeparator(SwingConstants.VERTICAL));
        footerPanel.add(new JLabel("<a>Getting Started</a>"));
        footerPanel.add(new JSeparator(SwingConstants.VERTICAL));
        footerPanel.add(new JLabel("<a>View Logs</a>"));
        add(footerPanel, BorderLayout.SOUTH);
    }

    private JPanel createModePanel(String title, String description, java.awt.event.ActionListener action) {
        JPanel panel = new JPanel(new MigLayout("wrap 1, fill", "[grow]"));
        panel.setBorder(BorderFactory.createEtchedBorder());

        JButton titleButton = new JButton(title);
        titleButton.setFont(new Font("SansSerif", Font.BOLD, 20));
        titleButton.addActionListener(action);
        panel.add(titleButton, "growx, h 60!");

        JTextArea descArea = new JTextArea(description);
        descArea.setWrapStyleWord(true);
        descArea.setLineWrap(true);
        descArea.setEditable(false);
        descArea.setFocusable(false);
        descArea.setFont(new Font("SansSerif", Font.PLAIN, 14));
        descArea.setBorder(new EmptyBorder(10, 10, 10, 10));
        descArea.setBackground(panel.getBackground());
        panel.add(descArea, "grow, h 100!");

        return panel;
    }
}
