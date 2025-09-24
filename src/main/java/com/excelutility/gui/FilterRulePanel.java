package com.excelutility.gui;

import com.excelutility.core.FilterRule;
import com.excelutility.core.expression.FilterExpression;
import com.excelutility.core.expression.RuleNode;
import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 * A panel that displays a single filter rule and provides actions for it.
 */
public class FilterRulePanel extends JPanel implements ExpressionNodeComponent {

    private final FilterRule rule;
    private final JTextField ruleNameField;
    private final JLabel recordCountLabel;

    public FilterRulePanel(String name, FilterRule rule, ActionListener deleteListener) {
        this.rule = rule;
        setLayout(new MigLayout("insets 2 5 2 5, fillx", "[grow]rel[]rel[]rel[]"));
        setBorder(BorderFactory.createMatteBorder(0, 0, 1, 0, new Color(224, 224, 224))); // Light gray separator
        setBackground(Color.WHITE);

        ruleNameField = new JTextField(name);
        ruleNameField.setBorder(null);
        add(ruleNameField, "growx, wmin 80");

        JLabel ruleLabel = new JLabel(rule.getDescriptiveName());
        ruleLabel.setForeground(Color.DARK_GRAY);
        add(ruleLabel, "gapleft 10, growx");

        recordCountLabel = new JLabel("(N/A)");
        recordCountLabel.setFont(recordCountLabel.getFont().deriveFont(Font.BOLD));
        add(recordCountLabel, "gapleft 10");

        JButton previewButton = new JButton("Preview");
        previewButton.setToolTipText("Preview matching results for this rule in a new tab");
        // The action listener will be attached in FilterPanel to call the preview logic
        // This is left to the parent container to wire up.
        add(previewButton, "hidemode 3"); // Hide if not used, but we will use it.

        JButton deleteButton = new JButton("X");
        deleteButton.setToolTipText("Delete this filter rule");
        deleteButton.setMargin(new Insets(1, 1, 1, 1));
        deleteButton.addActionListener(e -> deleteListener.actionPerformed(
                new ActionEvent(this, ActionEvent.ACTION_PERFORMED, null)
        ));
        add(deleteButton);
    }

    public String getRuleName() {
        return ruleNameField.getText();
    }

    public FilterRule getRule() {
        return rule;
    }

    public void setRecordCount(int count) {
        recordCountLabel.setText("(" + count + ")");
        if (count == 0) {
            recordCountLabel.setForeground(Color.RED);
        } else {
            recordCountLabel.setForeground(new Color(0, 153, 0)); // Dark Green
        }
    }

    // A way for the parent to get the preview button and attach a listener.
    public JButton getPreviewButton() {
        // Search for the button by its text, as it's a direct child.
        for (Component comp : getComponents()) {
            if (comp instanceof JButton && "Preview".equals(((JButton) comp).getText())) {
                return (JButton) comp;
            }
        }
        return null; // Should not happen
    }

    @Override
    public FilterExpression getExpression() {
        // The RuleNode's descriptive name should come from the rule itself, not the panel's editable name.
        return new RuleNode(this.rule);
    }

    @Override
    public String getName() {
        return getRuleName();
    }

    @Override
    public void setName(String name) {
        ruleNameField.setText(name);
    }
}
