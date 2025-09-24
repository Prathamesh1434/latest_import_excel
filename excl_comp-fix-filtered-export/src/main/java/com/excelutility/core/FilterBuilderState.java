package com.excelutility.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

/**
 * A serializable representation of the entire FilterExpressionBuilderPanel for saving to a profile.
 */
public class FilterBuilderState {

    private final List<GroupState> groups;

    @JsonCreator
    public FilterBuilderState(@JsonProperty("groups") List<GroupState> groups) {
        this.groups = groups;
    }

    // Getter
    public List<GroupState> getGroups() { return groups; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilterBuilderState that = (FilterBuilderState) o;
        return Objects.equals(groups, that.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groups);
    }
}
