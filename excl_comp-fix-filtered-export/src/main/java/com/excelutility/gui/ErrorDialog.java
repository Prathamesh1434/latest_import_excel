package com.excelutility.gui;

import javax.swing.*;
import java.awt.*;
import java.awt.datatransfer.StringSelection;
import java.io.PrintWriter;
import java.io.StringWriter;

public class ErrorDialog extends JDialog {

    public ErrorDialog(Frame owner, String title, String message, Throwable throwable) {
        super(owner, title, true);

        // --- Main Panel ---
        JPanel mainPanel = new JPanel(new BorderLayout(10, 10));
        mainPanel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        // Top part with icon and message
        JPanel topPanel = new JPanel(new BorderLayout(10, 0));
        JLabel iconLabel = new JLabel(UIManager.getIcon("OptionPane.errorIcon"));
        topPanel.add(iconLabel, BorderLayout.WEST);
        topPanel.add(new JLabel("<html>" + message.replace("\n", "<br>") + "</html>"), BorderLayout.CENTER);
        mainPanel.add(topPanel, BorderLayout.NORTH);

        // --- Details Panel (collapsible) ---
        JTextArea detailsTextArea = new JTextArea();
        detailsTextArea.setEditable(false);
        detailsTextArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));

        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        detailsTextArea.setText(sw.toString());
        detailsTextArea.setCaretPosition(0); // Scroll to top

        JScrollPane detailsScrollPane = new JScrollPane(detailsTextArea);
        detailsScrollPane.setPreferredSize(new Dimension(600, 250));
        detailsScrollPane.setVisible(false);

        // --- Button Panel ---
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton detailsButton = new JButton("Details");
        JButton copyButton = new JButton("Copy to Clipboard");
        JButton closeButton = new JButton("Close");

        buttonPanel.add(detailsButton);
        buttonPanel.add(copyButton);
        buttonPanel.add(closeButton);

        // --- Add to main panel ---
        mainPanel.add(buttonPanel, BorderLayout.SOUTH);

        // --- Listeners ---
        detailsButton.addActionListener(e -> {
            detailsScrollPane.setVisible(!detailsScrollPane.isVisible());
            if (detailsScrollPane.isVisible()) {
                mainPanel.add(detailsScrollPane, BorderLayout.CENTER);
            } else {
                mainPanel.remove(detailsScrollPane);
            }
            pack();
        });

        copyButton.addActionListener(e -> {
            StringSelection stringSelection = new StringSelection(detailsTextArea.getText());
            Toolkit.getDefaultToolkit().getSystemClipboard().setContents(stringSelection, null);
            JOptionPane.showMessageDialog(this, "Details copied to clipboard.", "Copied", JOptionPane.INFORMATION_MESSAGE);
        });

        closeButton.addActionListener(e -> dispose());

        setContentPane(mainPanel);
        pack();
        setLocationRelativeTo(owner);
    }
}
