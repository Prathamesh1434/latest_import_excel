package com.excelutility.gui;

import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;
import java.awt.*;
import java.util.List;
import java.util.Vector;

/**
 * A dialog for displaying a set of results in a read-only table.
 */
public class ResultsViewerDialog extends JDialog {

    /**
     * Constructs a new ResultsViewerDialog.
     *
     * @param owner The parent frame.
     * @param title The title of the dialog window.
     * @param data  The data to display, where the first list is the header row.
     */
    public ResultsViewerDialog(Frame owner, String title, List<List<Object>> data) {
        super(owner, title, true);
        setLayout(new BorderLayout(5, 5));

        if (data == null || data.isEmpty()) {
            add(new JLabel("No data to display."), BorderLayout.CENTER);
        } else {
            Vector<String> headers = new Vector<>();
            for (Object header : data.get(0)) {
                headers.add(header != null ? header.toString() : "");
            }

            Vector<Vector<Object>> dataVector = new Vector<>();
            if (data.size() > 1) {
                for (int i = 1; i < data.size(); i++) {
                    dataVector.add(new Vector<>(data.get(i)));
                }
            }

            DefaultTableModel model = new DefaultTableModel(dataVector, headers) {
                @Override
                public boolean isCellEditable(int row, int column) {
                    return false;
                }
            };

            JTable table = new JTable(model);
            table.setAutoCreateRowSorter(true);
            table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
            table.setFont(new Font("Lucida Sans Unicode", Font.PLAIN, 12));
            adjustColumnWidths(table);

            add(new JScrollPane(table), BorderLayout.CENTER);
        }

        JButton closeButton = new JButton("Close");
        closeButton.addActionListener(e -> setVisible(false));

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        buttonPanel.add(closeButton);
        add(buttonPanel, BorderLayout.SOUTH);

        setSize(800, 600);
        setLocationRelativeTo(owner);
    }

    /**
     * Adjusts the column widths of a table to fit the content.
     * @param table The table whose columns to resize.
     */
    private void adjustColumnWidths(JTable table) {
        table.getColumnModel().getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        for (int column = 0; column < table.getColumnCount(); column++) {
            int width = 150; // Min width
            for (int row = 0; row < table.getRowCount(); row++) {
                TableCellRenderer renderer = table.getCellRenderer(row, column);
                Component comp = table.prepareRenderer(renderer, row, column);
                width = Math.max(comp.getPreferredSize().width + 10, width);
            }
            if (width > 400) {
                width = 400; // Max width
            }
            table.getColumnModel().getColumn(column).setPreferredWidth(width);
        }
    }
}
