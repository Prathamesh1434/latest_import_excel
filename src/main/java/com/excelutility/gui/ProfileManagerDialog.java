package com.excelutility.gui;

import com.excelutility.core.ComparisonProfile;
import com.excelutility.io.ProfileService;
import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.io.File;
import java.io.IOException;

public class ProfileManagerDialog extends JDialog {

    private final ProfileService profileService;
    private final FilterPanel filterPanel;
    private DefaultListModel<String> profileListModel;
    private JList<String> profileList;

    public ProfileManagerDialog(Frame owner, ProfileService profileService) {
        this(owner, profileService, null);
    }

    public ProfileManagerDialog(Frame owner, ProfileService profileService, FilterPanel filterPanel) {
        super(owner, "Profile Manager", true);
        this.profileService = profileService;
        this.filterPanel = filterPanel;
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

        JButton loadButton = new JButton("Load");
        loadButton.addActionListener(e -> loadSelectedProfile());
        buttonPanel.add(loadButton);

        JButton importButton = new JButton("Import");
        importButton.addActionListener(e -> importProfile());
        buttonPanel.add(importButton);

        JButton exportButton = new JButton("Export");
        exportButton.addActionListener(e -> exportSelectedProfile());
        buttonPanel.add(exportButton);

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

    private void loadSelectedProfile() {
        String selectedProfile = profileList.getSelectedValue();
        if (selectedProfile == null) {
            JOptionPane.showMessageDialog(this, "Please select a profile to load.", "No Profile Selected", JOptionPane.WARNING_MESSAGE);
            return;
        }

        if (filterPanel != null) {
            filterPanel.loadProfile(selectedProfile);
            setVisible(false);
        }
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

    private void exportSelectedProfile() {
        String selectedProfile = profileList.getSelectedValue();
        if (selectedProfile == null) {
            JOptionPane.showMessageDialog(this, "Please select a profile to export.", "No Profile Selected", JOptionPane.WARNING_MESSAGE);
            return;
        }

        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Export Profile");
        fileChooser.setSelectedFile(new File(selectedProfile + ".json"));
        fileChooser.setFileFilter(new FileNameExtensionFilter("JSON Files", "json"));

        int userSelection = fileChooser.showSaveDialog(this);
        if (userSelection == JFileChooser.APPROVE_OPTION) {
            File fileToSave = fileChooser.getSelectedFile();
            try {
                profileService.exportProfile(selectedProfile, fileToSave);
                JOptionPane.showMessageDialog(this, "Profile exported successfully!", "Export Success", JOptionPane.INFORMATION_MESSAGE);
            } catch (IOException ex) {
                JOptionPane.showMessageDialog(this, "Error exporting profile: " + ex.getMessage(), "Export Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void importProfile() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Import Profile");
        fileChooser.setFileFilter(new FileNameExtensionFilter("JSON Files", "json"));

        int userSelection = fileChooser.showOpenDialog(this);
        if (userSelection == JFileChooser.APPROVE_OPTION) {
            File fileToLoad = fileChooser.getSelectedFile();
            try {
                ComparisonProfile newProfile = profileService.loadProfileFromFile(fileToLoad);
                String newProfileName = JOptionPane.showInputDialog(this, "Enter a name for the new profile:", "Import Profile", JOptionPane.PLAIN_MESSAGE);

                if (newProfileName != null && !newProfileName.trim().isEmpty()) {
                    profileService.saveProfile(newProfile, newProfileName);
                    loadProfiles();
                    JOptionPane.showMessageDialog(this, "Profile imported successfully!", "Import Success", JOptionPane.INFORMATION_MESSAGE);
                }
            } catch (IOException ex) {
                JOptionPane.showMessageDialog(this, "Error importing profile: " + ex.getMessage(), "Import Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }
}
