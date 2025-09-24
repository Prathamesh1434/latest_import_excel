package com.excelutility.gui;

import javax.swing.*;
import java.awt.*;

public class AppContainer extends JFrame {

    private CardLayout cardLayout;
    private JPanel mainPanel;
    private ModeSelectionPanel modeSelectionPanel;
    private ComparePanel comparePanel;
    private FilterPanel filterPanel;
    private JMenuBar compareMenuBar;
    private JMenuBar filterMenuBar;

    public AppContainer() {
        setTitle("Excel Utility");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1600, 1000);
        setLocationRelativeTo(null);

        cardLayout = new CardLayout();
        mainPanel = new JPanel(cardLayout);

        modeSelectionPanel = new ModeSelectionPanel(this);
        comparePanel = new ComparePanel(this);
        filterPanel = new FilterPanel(this); // Pass container reference

        // Each panel creates its own menu bar
        compareMenuBar = comparePanel.createMenuBar();
        filterMenuBar = filterPanel.createMenuBar();

        mainPanel.add(modeSelectionPanel, "modeSelection");
        mainPanel.add(comparePanel, "compare");
        mainPanel.add(filterPanel, "filter");

        add(mainPanel);
        navigateTo("modeSelection"); // Start at the mode selection screen
    }

    public void navigateTo(String panelName) {
        cardLayout.show(mainPanel, panelName);
        if ("compare".equals(panelName)) {
            setJMenuBar(compareMenuBar);
        } else if ("filter".equals(panelName)) {
            setJMenuBar(filterMenuBar);
        } else {
            setJMenuBar(null); // No menu for the mode selection panel
        }
        revalidate();
        repaint();
    }
}
