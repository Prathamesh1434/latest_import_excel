package com.excelutility.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a group of filter conditions or other filter groups, connected by an AND or OR logical operator.
 */
public class FilterGroup {

    private FilteringService.LogicalOperator operator = FilteringService.LogicalOperator.AND;
    private final List<FilterCondition> conditions = new ArrayList<>();
    private final List<FilterGroup> groups = new ArrayList<>();

    // Getters and Setters
    public FilteringService.LogicalOperator getOperator() { return operator; }
    public void setOperator(FilteringService.LogicalOperator operator) { this.operator = operator; }
    public List<FilterCondition> getConditions() { return conditions; }
    public List<FilterGroup> getGroups() { return groups; }

    public void addCondition(FilterCondition condition) {
        this.conditions.add(condition);
    }

    public void addGroup(FilterGroup group) {
        this.groups.add(group);
    }
}
