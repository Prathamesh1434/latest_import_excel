package com.excelutility.gui;

import net.miginfocom.swing.MigLayout;
import javax.swing.*;
import javax.swing.event.TableModelEvent;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableColumn;
import java.awt.*;
import java.awt.font.TextAttribute;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnMappingPanel extends JPanel {

    private final ColumnMappingTableModel tableModel;
    private final JTable mappingTable;
    private final JList<String> keyList;
    private final DefaultListModel<String> keyListModel;

    public ColumnMappingPanel() {
        setLayout(new MigLayout("fill, insets 5", "[grow, 70%][grow, 30%]", "[grow][]"));
        setBorder(BorderFactory.createTitledBorder("Column Mappings & Row Matching"));

        tableModel = new ColumnMappingTableModel();
        mappingTable = new JTable(tableModel);
        mappingTable.setRowHeight(25);
        mappingTable.getTableHeader().setReorderingAllowed(false);

        // Add custom renderer for ignored rows
        mappingTable.setDefaultRenderer(Object.class, new IgnoredRowRenderer());

        tableModel.addTableModelListener(e -> {
            if (e.getType() == TableModelEvent.UPDATE) {
                updateKeyList();
                mappingTable.repaint(); // Repaint to reflect ignore changes
            }
        });

        add(new JScrollPane(mappingTable), "grow, hmin 150");

        keyListModel = new DefaultListModel<>();
        keyList = new JList<>(keyListModel);
        JPanel keyPanel = new JPanel(new MigLayout("fill", "[grow]", "[grow]"));
        keyPanel.setBorder(BorderFactory.createTitledBorder("Selected Keys"));
        keyPanel.add(new JScrollPane(keyList), "grow");
        add(keyPanel, "grow, wrap");

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton autoMapButton = new JButton("Auto-map");
        JButton clearIgnoresButton = new JButton("Clear All Ignores");
        buttonPanel.add(autoMapButton);
        buttonPanel.add(clearIgnoresButton);
        add(buttonPanel, "growx, span 2");

        clearIgnoresButton.addActionListener(e -> clearAllIgnores());
    }

    public void setColumns(List<String> sourceCols, List<String> targetCols) {
        tableModel.setSourceColumns(sourceCols, targetCols);
        TableColumn targetColumn = mappingTable.getColumnModel().getColumn(1);
        JComboBox<String> comboBox = new JComboBox<>();
        if (targetCols != null) {
            targetCols.forEach(comboBox::addItem);
        }
        targetColumn.setCellEditor(new DefaultCellEditor(comboBox));
        mappingTable.setDefaultRenderer(String.class, new IgnoredRowRenderer());
    }

    public Map<String, String> getColumnMappings() {
        Map<String, String> mappings = new HashMap<>();
        for (Object[] rowData : tableModel.getMappingData()) {
            boolean ignored = (boolean) rowData[3];
            if (!ignored && rowData[1] != null && !rowData[1].toString().isEmpty()) {
                mappings.put(rowData[0].toString(), rowData[1].toString());
            }
        }
        return mappings;
    }

    public List<String> getKeyColumns() {
        return tableModel.getMappingData().stream()
                .filter(rowData -> (boolean) rowData[2])
                .map(rowData -> rowData[0].toString())
                .collect(Collectors.toList());
    }

    public List<String> getIgnoredColumns() {
        return tableModel.getMappingData().stream()
                .filter(rowData -> (boolean) rowData[3])
                .map(rowData -> rowData[0].toString())
                .collect(Collectors.toList());
    }

    public void setIgnoredColumns(List<String> ignoredColumns) {
        if (ignoredColumns == null) return;
        for (int i = 0; i < tableModel.getRowCount(); i++) {
            String sourceColumn = tableModel.getValueAt(i, 0).toString();
            tableModel.setValueAt(ignoredColumns.contains(sourceColumn), i, 3);
        }
    }

    private void updateKeyList() {
        keyListModel.clear();
        getKeyColumns().forEach(keyListModel::addElement);
    }

    public void selectKeys(List<String> keysToSelect) {
        for (int i = 0; i < tableModel.getRowCount(); i++) {
            String sourceColumn = tableModel.getValueAt(i, 0).toString();
            tableModel.setValueAt(keysToSelect.contains(sourceColumn), i, 2);
        }
    }

    private void clearAllIgnores() {
        for (int i = 0; i < tableModel.getRowCount(); i++) {
            tableModel.setValueAt(false, i, 3);
        }
    }

    public void setMappings(Map<String, String> mappings, List<String> keyColumns) {
        tableModel.setMappings(mappings, keyColumns);
        updateKeyList();
    }

    public void selectKeysFromTarget(List<String> targetKeyNames) {
        Map<String, String> targetToSourceMap = getColumnMappings().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        List<String> sourceKeysToSelect = targetKeyNames.stream()
                .map(targetToSourceMap::get)
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toList());
        if (!sourceKeysToSelect.isEmpty()) {
            this.selectKeys(sourceKeysToSelect);
        }
    }

    /**
     * Custom renderer to draw ignored rows with a strikethrough.
     */
    private class IgnoredRowRenderer extends DefaultTableCellRenderer {
        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
            Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            boolean isIgnored = (boolean) table.getModel().getValueAt(row, 3);

            if (isIgnored) {
                c.setForeground(Color.GRAY);
                Map<TextAttribute, Object> attributes = new HashMap<>(getFont().getAttributes());
                attributes.put(TextAttribute.STRIKETHROUGH, TextAttribute.STRIKETHROUGH_ON);
                c.setFont(getFont().deriveFont(attributes));
                setToolTipText("Ignored - excluded from comparison");
            } else {
                c.setForeground(table.getForeground());
                c.setFont(table.getFont());
                setToolTipText(null);
            }

            if (isSelected) {
                c.setBackground(table.getSelectionBackground());
                c.setForeground(table.getSelectionForeground());
            } else {
                c.setBackground(table.getBackground());
            }

            return c;
        }
    }
}
