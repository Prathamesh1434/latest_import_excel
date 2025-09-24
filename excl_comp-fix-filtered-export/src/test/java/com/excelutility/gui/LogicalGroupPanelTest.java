package com.excelutility.gui;

import com.excelutility.core.FilterRule;
import com.excelutility.core.FilteringService;
import com.excelutility.core.expression.FilterExpression;
import com.excelutility.core.expression.GroupNode;
import com.excelutility.core.expression.RuleNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

public class LogicalGroupPanelTest {

    private LogicalGroupPanel groupPanel;

    @BeforeEach
    void setUp() {
        // The delete listener can be null for testing purposes
        groupPanel = new LogicalGroupPanel("Test Group", null);
    }

    private FilterRulePanel createRealRulePanel(String ruleName) {
        FilterRule rule = new FilterRule(FilterRule.SourceType.BY_VALUE, ruleName, "ColumnA", false);
        // The delete listener can be null for this test
        return new FilterRulePanel(ruleName, rule, null);
    }

    @Test
    void testGetExpression_singleRule() {
        FilterRulePanel rulePanel1 = createRealRulePanel("Rule1");
        groupPanel.addComponent(rulePanel1);

        FilterExpression expression = groupPanel.getExpression();

        assertTrue(expression instanceof GroupNode, "Expression should be a GroupNode");
        GroupNode rootGroup = (GroupNode) expression;
        assertEquals(1, rootGroup.getChildren().size(), "Root group should contain one child (the rule expression)");
        FilterExpression childExpression = rootGroup.getChildren().get(0);
        assertTrue(childExpression instanceof RuleNode, "Child should be a RuleNode");
        assertEquals("ColumnA = 'Rule1'", childExpression.getDescriptiveName());
    }

    @Test
    void testGetExpression_twoRules_defaultAnd() {
        FilterRulePanel rulePanel1 = createRealRulePanel("Rule1");
        FilterRulePanel rulePanel2 = createRealRulePanel("Rule2");
        groupPanel.addComponent(rulePanel1);
        groupPanel.addComponent(rulePanel2);

        FilterExpression expression = groupPanel.getExpression();

        assertTrue(expression instanceof GroupNode, "Root expression should be a GroupNode");
        GroupNode rootGroup = (GroupNode) expression;
        assertEquals("Test Group", rootGroup.getName());
        assertEquals(1, rootGroup.getChildren().size(), "Root group should have one child (the nested group)");

        assertTrue(rootGroup.getChildren().get(0) instanceof GroupNode, "Child should be a nested GroupNode");
        GroupNode innerGroup = (GroupNode) rootGroup.getChildren().get(0);

        assertEquals(FilteringService.LogicalOperator.AND, innerGroup.getOperator(), "Inner group operator should be AND");
        assertEquals(2, innerGroup.getChildren().size(), "The inner group should have two children");
        assertTrue(innerGroup.getChildren().get(0) instanceof RuleNode);
        assertTrue(innerGroup.getChildren().get(1) instanceof RuleNode);
    }
}
