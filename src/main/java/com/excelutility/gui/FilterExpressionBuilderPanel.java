package com.excelutility.gui;

import com.excelutility.core.FilterRule;
import net.miginfocom.swing.MigLayout;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionListener;

/**
 * The main container panel for building a nested filter expression.
 * This panel holds the root logical group and orchestrates the creation and deletion
 * of rules and subgroups.
 */
public class FilterExpressionBuilderPanel extends JPanel {

    private final LogicalGroupPanel rootGroup;
    private final FilterPanel panelProvider;

    public FilterExpressionBuilderPanel(FilterPanel panelProvider) {
        this.panelProvider = panelProvider;
        setLayout(new MigLayout("fill, insets 5", "[grow]"));
        setBorder(BorderFactory.createTitledBorder("Filter Logic Builder"));

        // The root group cannot be deleted, so its delete listener is null.
        rootGroup = new LogicalGroupPanel("Root", null);
        add(rootGroup, "growx");
    }

    public void addRuleToGroup(LogicalGroupPanel targetGroup, FilterRule rule) {
        String ruleName = com.excelutility.core.AutoNamingService.suggestRuleName();
        addRuleToGroup(targetGroup, rule, ruleName);
    }

    private void addRuleToGroup(LogicalGroupPanel targetGroup, FilterRule rule, String ruleName) {
        ActionListener deleteListener = e -> {
            FilterRulePanel sourcePanel = (FilterRulePanel) e.getSource();
            targetGroup.removeComponent(sourcePanel);
        };
        FilterRulePanel newRulePanel = new FilterRulePanel(ruleName, rule, deleteListener);

        if (panelProvider != null && newRulePanel.getPreviewButton() != null) {
            newRulePanel.getPreviewButton().addActionListener(e -> panelProvider.previewRule(newRulePanel));
        }

        targetGroup.addComponent(newRulePanel);
    }

    public LogicalGroupPanel getRootGroup() {
        return rootGroup;
    }

    public void rebuildFromState(com.excelutility.core.FilterBuilderState state) {
        rootGroup.removeAll();
        rootGroup.revalidate();
        rootGroup.repaint();
        com.excelutility.core.AutoNamingService.reset();

        if (state == null || state.getGroups() == null || state.getGroups().isEmpty()) {
            return;
        }

        com.excelutility.core.GroupState rootGroupState = state.getGroups().get(0);
        rootGroup.setName(rootGroupState.getName());
        buildGroupPanelFromState(rootGroup, rootGroupState, rootGroupState.getOperator());

        rootGroup.revalidate();
        rootGroup.repaint();
    }

    private void buildGroupPanelFromState(LogicalGroupPanel parentPanel, com.excelutility.core.GroupState groupState, com.excelutility.core.FilteringService.LogicalOperator op) {
        if (groupState.getRules() != null) {
            for (com.excelutility.core.RuleState ruleState : groupState.getRules()) {
                addRuleToGroup(parentPanel, ruleState.toFilterRule(), ruleState.getName());
            }
        }

        // Apply the single operator from the saved state to all infix panels
        for (Component comp : parentPanel.getContentPanel().getComponents()) {
            if (comp instanceof LogicalGroupPanel.InfixOperatorPanel) {
                ((LogicalGroupPanel.InfixOperatorPanel) comp).setOperator(op);
            }
        }

        if (groupState.getGroups() != null) {
            for (com.excelutility.core.GroupState subGroupState : groupState.getGroups()) {
                ActionListener deleteListener = event -> {
                    LogicalGroupPanel sourceGroup = (LogicalGroupPanel) event.getSource();
                    parentPanel.removeComponent(sourceGroup);
                    if (panelProvider != null) {
                        panelProvider.updateFilterResults();
                    }
                };
                LogicalGroupPanel newGroupPanel = new LogicalGroupPanel(subGroupState.getName(), deleteListener);
                buildGroupPanelFromState(newGroupPanel, subGroupState, subGroupState.getOperator());
                parentPanel.addComponent(newGroupPanel);
            }
        }
    }

    public com.excelutility.core.FilterBuilderState getState() {
        com.excelutility.core.GroupState rootGroupState = createGroupStateFromPanel(rootGroup);
        return new com.excelutility.core.FilterBuilderState(java.util.Collections.singletonList(rootGroupState));
    }

    private com.excelutility.core.GroupState createGroupStateFromPanel(LogicalGroupPanel groupPanel) {
        java.util.List<com.excelutility.core.RuleState> ruleStates = new java.util.ArrayList<>();
        java.util.List<com.excelutility.core.GroupState> groupStates = new java.util.ArrayList<>();
        com.excelutility.core.FilteringService.LogicalOperator firstOperator = com.excelutility.core.FilteringService.LogicalOperator.AND;
        boolean operatorFound = false;

        for (Component comp : groupPanel.getContentPanel().getComponents()) {
            if (comp instanceof FilterRulePanel) {
                FilterRulePanel rulePanel = (FilterRulePanel) comp;
                FilterRule rule = rulePanel.getRule();
                ruleStates.add(new com.excelutility.core.RuleState(rulePanel.getName(), rule.getSourceType(), rule.getSourceValue(), rule.getTargetColumn(), rule.isTrimWhitespace()));
            } else if (comp instanceof LogicalGroupPanel) {
                groupStates.add(createGroupStateFromPanel((LogicalGroupPanel) comp));
            } else if (comp instanceof LogicalGroupPanel.InfixOperatorPanel && !operatorFound) {
                firstOperator = ((LogicalGroupPanel.InfixOperatorPanel) comp).getOperator();
                operatorFound = true;
            }
        }
        return new com.excelutility.core.GroupState(groupPanel.getName(), firstOperator, ruleStates, groupStates);
    }
}
