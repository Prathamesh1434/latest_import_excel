package com.excelutility;

import com.excelutility.gui.AppContainer;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

/**
 * The main entry point for the Excel Utility application.
 */
public class App {
    /**
     * The main method that launches the application.
     *
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        // Set a cross-platform look and feel for consistency
        try {
            // UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception e) {
            // If the native L&F fails, the default (Metal) will be used.
            e.printStackTrace();
        }

        SwingUtilities.invokeLater(() -> {
            AppContainer appContainer = new AppContainer();
            appContainer.setVisible(true);
        });
    }
}
