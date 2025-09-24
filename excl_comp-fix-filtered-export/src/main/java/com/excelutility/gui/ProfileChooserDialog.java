package com.excelutility.gui;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.util.List;

public class ProfileChooserDialog extends JDialog {

    private JList<File> profileList;
    private JButton loadButton, deleteButton, cancelButton;
    private File selectedProfile = null;
    private boolean deleteRequested = false;

    public ProfileChooserDialog(Frame owner, List<File> profiles) {
        super(owner, "Load or Delete Profile", true);
        setLayout(new BorderLayout(10, 10));

        DefaultListModel<File> model = new DefaultListModel<>();
        model.addAll(profiles);

        profileList = new JList<>(model);
        profileList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        profileList.setCellRenderer(new FileRenderer());
        if (!profiles.isEmpty()) {
            profileList.setSelectedIndex(0);
        }
        add(new JScrollPane(profileList), BorderLayout.CENTER);

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        loadButton = new JButton("Load");
        deleteButton = new JButton("Delete");
        cancelButton = new JButton("Cancel");

        loadButton.addActionListener(e -> {
            selectedProfile = profileList.getSelectedValue();
            setVisible(false);
        });

        deleteButton.addActionListener(e -> {
            selectedProfile = profileList.getSelectedValue();
            if (selectedProfile != null) {
                deleteRequested = true;
                setVisible(false);
            }
        });

        cancelButton.addActionListener(e -> {
            selectedProfile = null;
            setVisible(false);
        });

        buttonPanel.add(loadButton);
        buttonPanel.add(deleteButton);
        buttonPanel.add(cancelButton);
        add(buttonPanel, BorderLayout.SOUTH);

        pack();
        setSize(400, 300);
        setLocationRelativeTo(owner);
    }

    public File getSelectedProfile() {
        return selectedProfile;
    }

    public boolean isDeleteRequested() {
        return deleteRequested;
    }

    // Custom renderer to show only the file name
    private static class FileRenderer extends DefaultListCellRenderer {
        @Override
        public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
            super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            if (value instanceof File) {
                setText(((File) value).getName());
            }
            return this;
        }
    }
}
