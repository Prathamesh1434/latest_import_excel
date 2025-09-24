package com.excelutility.gui;

import com.excelutility.core.KeySuggester;
import javax.swing.*;
import java.awt.*;
import java.util.List;

public class KeySuggestionDialog extends JDialog {

    private JList<KeySuggester.KeySuggestion> suggestionList;
    private boolean accepted = false;

    public KeySuggestionDialog(Frame owner, List<KeySuggester.KeySuggestion> suggestions) {
        super(owner, "Key Suggestions", true);
        setSize(400, 300);
        setLocationRelativeTo(owner);

        DefaultListModel<KeySuggester.KeySuggestion> listModel = new DefaultListModel<>();
        suggestions.forEach(listModel::addElement);
        suggestionList = new JList<>(listModel);
        suggestionList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);

        add(new JScrollPane(suggestionList), BorderLayout.CENTER);

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton okButton = new JButton("Accept Selected");
        okButton.addActionListener(e -> {
            accepted = true;
            setVisible(false);
        });
        buttonPanel.add(okButton);

        JButton cancelButton = new JButton("Cancel");
        cancelButton.addActionListener(e -> setVisible(false));
        buttonPanel.add(cancelButton);

        add(buttonPanel, BorderLayout.SOUTH);
    }

    public boolean isAccepted() {
        return accepted;
    }

    public List<KeySuggester.KeySuggestion> getSelectedSuggestions() {
        return suggestionList.getSelectedValuesList();
    }
}
