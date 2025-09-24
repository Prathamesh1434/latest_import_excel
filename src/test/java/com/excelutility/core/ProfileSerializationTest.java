package com.excelutility.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProfileSerializationTest {

    @Test
    void testProfileSerialization() throws Exception {
        // 1. Create a complex profile object
        RuleState rule1 = new RuleState("Rule 1", FilterRule.SourceType.BY_VALUE, "Value1", "ColumnA", true);
        RuleState rule2 = new RuleState("Rule 2", FilterRule.SourceType.BY_COLUMN, "ColumnB", "ColumnC", false);

        List<RuleState> group1Rules = new ArrayList<>();
        group1Rules.add(rule1);

        GroupState group1 = new GroupState("Group 1", java.util.Arrays.asList(FilteringService.LogicalOperator.AND), group1Rules, new ArrayList<>());

        List<RuleState> group2Rules = new ArrayList<>();
        group2Rules.add(rule2);
        GroupState group2 = new GroupState("Group 2", java.util.Arrays.asList(FilteringService.LogicalOperator.OR), group2Rules, new ArrayList<>());

        List<GroupState> rootGroups = new ArrayList<>();
        rootGroups.add(group1);
        rootGroups.add(group2);

        FilterBuilderState builderState = new FilterBuilderState(rootGroups);

        FilterProfile originalProfile = new FilterProfile(
                "Test Profile",
                "2025-01-01T12:00:00Z",
                "/path/to/data.xlsx",
                "Sheet1",
                "/path/to/filters.xlsx",
                "FilterValues",
                builderState,
                List.of(0),
                ConcatenationMode.LEAF_ONLY,
                List.of(1),
                ConcatenationMode.BREADCRUMB
        );

        // 2. Serialize to JSON
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(originalProfile);

        // 3. Deserialize back from JSON
        FilterProfile deserializedProfile = mapper.readValue(json, FilterProfile.class);

        // 4. Assert that the objects are equal
        assertEquals(originalProfile, deserializedProfile);
    }
}
