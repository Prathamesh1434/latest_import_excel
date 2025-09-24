package com.excelutility.gui;

import com.excelutility.core.FilteringService;
import com.excelutility.core.expression.FilterExpression;
import com.excelutility.core.expression.GroupNode;
import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A panel that represents a logical grouping of other filter components.
 * This version supports a single logical operator (AND/OR) for the entire group,
 * but visually represents it with infix operators between each component for clarity.
 */
public class LogicalGroupPanel extends JPanel implements ExpressionNodeComponent {

    private final JTextField groupNameField;
    private final JPanel contentPanel;
    private final JButton addRuleButton;
    private final JLabel recordCountLabel;

    /**
     * An independent panel for the AND/OR radio buttons between components.
     * Its state is self-contained.
     */
    public class InfixOperatorPanel extends JPanel {
        private final JRadioButton andButton;
        private final JRadioButton orButton;
        private FilteringService.LogicalOperator operator;

        public InfixOperatorPanel() {
            super(new FlowLayout(FlowLayout.CENTER, 5, 0));
            andButton = new JRadioButton("AND");
            orButton = new JRadioButton("OR");

            ButtonGroup group = new ButtonGroup();
            group.add(andButton);
            group.add(orButton);
            add(andButton);
            add(orButton);

            // Add listeners to update THIS panel's operator
            andButton.addActionListener(e -> setOperator(FilteringService.LogicalOperator.AND));
            orButton.addActionListener(e -> setOperator(FilteringService.LogicalOperator.OR));

            setOperator(FilteringService.LogicalOperator.AND); // Default to AND
        }

        public FilteringService.LogicalOperator getOperator() {
            return operator;
        }

        public void setOperator(FilteringService.LogicalOperator operator) {
            this.operator = operator;
            updateAppearance();
        }

        private void updateAppearance() {
            boolean isAnd = (operator == FilteringService.LogicalOperator.AND);
            andButton.setSelected(isAnd);
            orButton.setSelected(!isAnd);

            Color color = isAnd ? new Color(0x2F8F6D) : new Color(0xF2C94C);
            setBackground(color);
            andButton.setBackground(color);
            orButton.setBackground(color);
        }
    }

    public LogicalGroupPanel(String initialName, ActionListener deleteListener) {
        super(new MigLayout("insets 0, fillx, wrap 1", "[grow]"));
        setBorder(BorderFactory.createTitledBorder(initialName));
        setBackground(Color.WHITE);

        JPanel topBar = new JPanel(new MigLayout("insets 2 5 2 5, fillx", "[grow]push[]"));
        topBar.setBackground(new Color(220, 235, 255));

        groupNameField = new JTextField(initialName);
        groupNameField.setBorder(null);
        groupNameField.setBackground(topBar.getBackground());
        topBar.add(groupNameField, "growx, wmin 100");

        recordCountLabel = new JLabel("(N/A)");
        recordCountLabel.setFont(recordCountLabel.getFont().deriveFont(Font.BOLD));
        topBar.add(recordCountLabel, "gapleft 10");

        addRuleButton = new JButton("Add Rule");
        topBar.add(addRuleButton);

        if (deleteListener != null) {
            JButton deleteGroupButton = new JButton("X");
            deleteGroupButton.setToolTipText("Delete this group");
            deleteGroupButton.setMargin(new Insets(1, 1, 1, 1));
            deleteGroupButton.addActionListener(e -> deleteListener.actionPerformed(new java.awt.event.ActionEvent(this, java.awt.event.ActionEvent.ACTION_PERFORMED, null)));
            topBar.add(deleteGroupButton);
        }

        add(topBar, "growx");

        contentPanel = new JPanel(new MigLayout("insets 5 10 5 10, fillx, wrap 1", "[grow]"));
        add(contentPanel, "growx");
    }

    @Override
    public String getName() {
        return groupNameField.getText();
    }

    @Override
    public void setName(String name) {
        groupNameField.setText(name);
        setBorder(BorderFactory.createTitledBorder(name));
    }

    public void addComponent(Component component) {
        if (!(component instanceof ExpressionNodeComponent)) {
            throw new IllegalArgumentException("Only ExpressionNodeComponents can be added to a LogicalGroupPanel.");
        }
        if (contentPanel.getComponentCount() > 0) {
            contentPanel.add(new InfixOperatorPanel(), "growx, align center, gaptop 5, gapbottom 5");
        }
        contentPanel.add(component, "growx");
        revalidateAndRepaint();
    }

    public void removeComponent(Component component) {
        List<Component> components = new ArrayList<>(Arrays.asList(contentPanel.getComponents()));
        int index = components.indexOf(component);

        if (index != -1) {
            // If we are removing the first component, also remove the operator AFTER it.
            if (index == 0 && components.size() > 1) {
                contentPanel.remove(1); // Remove operator panel
            }
            // If we are removing any other component, remove the operator BEFORE it.
            else if (index > 0) {
                contentPanel.remove(index - 1); // Remove operator panel
            }
            contentPanel.remove(component);
            revalidateAndRepaint();
        }
    }

    private void revalidateAndRepaint() {
        contentPanel.revalidate();
        contentPanel.repaint();
        if (getParent() != null) {
            getParent().revalidate();
            getParent().repaint();
        }
    }

    @Override
    public void removeAll() {
        contentPanel.removeAll();
        revalidateAndRepaint();
    }

    public JPanel getContentPanel() {
        return contentPanel;
    }

    @Override
    public FilterExpression getExpression() {
        List<Component> components = Arrays.asList(contentPanel.getComponents());
        List<ExpressionNodeComponent> rules = new ArrayList<>();
        List<InfixOperatorPanel> operators = new ArrayList<>();

        for (Component comp : components) {
            if (comp instanceof ExpressionNodeComponent) {
                rules.add((ExpressionNodeComponent) comp);
            } else if (comp instanceof InfixOperatorPanel) {
                operators.add((InfixOperatorPanel) comp);
            }
        }

        if (rules.isEmpty()) {
            return new GroupNode(FilteringService.LogicalOperator.AND, getName()); // Empty group
        }

        FilterExpression expressionToWrap;
        if (rules.size() == 1) {
            expressionToWrap = rules.get(0).getExpression();
        } else {
            // Build the expression tree, respecting the order of operations.
        // We'll build it left-associatively: (R1 op1 R2) op2 R3 ...
        FilterExpression left = rules.get(0).getExpression();
        for (int i = 0; i < operators.size(); i++) {
            InfixOperatorPanel opPanel = operators.get(i);
            FilterExpression right = rules.get(i + 1).getExpression();
            GroupNode newGroup = new GroupNode(opPanel.getOperator(), "implicit_group");
            newGroup.addChild(left);
            newGroup.addChild(right);
            left = newGroup;
        }
            expressionToWrap = left;
        }

        // The final expression is the root of our constructed tree.
        // We wrap it in one final GroupNode that has the name of this panel.
        GroupNode finalGroup = new GroupNode(FilteringService.LogicalOperator.AND, getName()); // Operator here is irrelevant
        finalGroup.addChild(expressionToWrap);
        return finalGroup;
    }

    public JButton getAddRuleButton() {
        return addRuleButton;
    }

    public void setRecordCount(int count) {
        if (count < 0) {
            recordCountLabel.setText("(Error)");
            recordCountLabel.setForeground(Color.ORANGE);
        } else {
            recordCountLabel.setText("(" + count + ")");
            recordCountLabel.setForeground(count == 0 ? Color.RED : new Color(0, 153, 0));
        }
    }
}
