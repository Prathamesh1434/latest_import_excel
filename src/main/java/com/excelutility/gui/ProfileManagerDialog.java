package com.excelutility.gui;

import com.excelutility.io.ProfileService;
import javax.swing.*;
import java.awt.*;

public class ProfileManagerDialog extends JDialog {

    private final ProfileService profileService;
    private DefaultListModel<String> profileListModel;
    private JList<String> profileList;

    public ProfileManagerDialog(Frame owner, ProfileService profileService) {
        super(owner, "Profile Manager", true);
        this.profileService = profileService;
        setSize(400, 300);
        setLocationRelativeTo(owner);

        initComponents();
        loadProfiles();
    }

    private void initComponents() {
        setLayout(new BorderLayout(10, 10));

        profileListModel = new DefaultListModel<>();
        profileList = new JList<>(profileListModel);
        add(new JScrollPane(profileList), BorderLayout.CENTER);

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton deleteButton = new JButton("Delete");
        deleteButton.addActionListener(e -> deleteSelectedProfile());
        buttonPanel.add(deleteButton);

        JButton closeButton = new JButton("Close");
        closeButton.addActionListener(e -> setVisible(false));
        buttonPanel.add(closeButton);

        add(buttonPanel, BorderLayout.SOUTH);
    }

    private void loadProfiles() {
        profileListModel.clear();
        profileService.getAvailableProfiles().forEach(profileListModel::addElement);
    }

    private void deleteSelectedProfile() {
        String selectedProfile = profileList.getSelectedValue();
        if (selectedProfile == null) {
            JOptionPane.showMessageDialog(this, "Please select a profile to delete.", "No Profile Selected", JOptionPane.WARNING_MESSAGE);
            return;
        }

        int confirm = JOptionPane.showConfirmDialog(this, "Are you sure you want to delete the profile '" + selectedProfile + "'?", "Confirm Deletion", JOptionPane.YES_NO_OPTION);
        if (confirm == JOptionPane.YES_OPTION) {
            profileService.deleteProfile(selectedProfile);
            loadProfiles(); // Refresh the list
        }
    }
}
