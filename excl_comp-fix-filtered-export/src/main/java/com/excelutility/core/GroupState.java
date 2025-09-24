package com.excelutility.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

/**
 * A serializable representation of a LogicalGroupPanel for saving to a profile.
 */
public class GroupState {

    private final String name;
    private final FilteringService.LogicalOperator operator;
    private final List<RuleState> rules;
    private final List<GroupState> groups;

    @JsonCreator
    public GroupState(
            @JsonProperty("name") String name,
            @JsonProperty("operator") FilteringService.LogicalOperator operator,
            @JsonProperty("rules") List<RuleState> rules,
            @JsonProperty("groups") List<GroupState> groups) {
        this.name = name;
        this.operator = operator;
        this.rules = rules;
        this.groups = groups;
    }

    // Getters
    public String getName() { return name; }
    public FilteringService.LogicalOperator getOperator() { return operator; }
    public List<RuleState> getRules() { return rules; }
    public List<GroupState> getGroups() { return groups; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupState that = (GroupState) o;
        return Objects.equals(name, that.name) &&
                operator == that.operator &&
                Objects.equals(rules, that.rules) &&
                Objects.equals(groups, that.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, operator, rules, groups);
    }
}
