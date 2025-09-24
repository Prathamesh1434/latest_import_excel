package com.excelutility.core.expression;

import com.excelutility.core.FilterRule;
import com.excelutility.core.FilteringService;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * A leaf node in the filter expression tree that represents a single FilterRule.
 */
public class RuleNode implements FilterExpression {

    private final FilterRule rule;
    private String name;

    @JsonCreator
    public RuleNode(@JsonProperty("rule") FilterRule rule) {
        this.rule = rule;
        this.name = rule.getDescriptiveName(); // Default name
    }

    public FilterRule getRule() {
        return rule;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getDescriptiveName() {
        return this.name;
    }

    @Override
    public boolean evaluate(List<Object> row, List<String> header, FilteringService service) {
        return service.checkRule(row, header, this.rule);
    }
}
