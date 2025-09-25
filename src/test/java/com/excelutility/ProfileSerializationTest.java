package com.excelutility;

import com.excelutility.core.*;
import com.excelutility.io.ProfileManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class ProfileSerializationTest {

    @Test
    public void testSerializeAndDeserializeComplexProfile() throws JsonProcessingException {
        // 1. Create a complex ComparisonProfile object
        ComparisonProfile originalProfile = new ComparisonProfile();
        originalProfile.setSourceFilePath("/path/to/source.xlsx"); // Should be ignored
        originalProfile.setTargetFilePath("/path/to/target.xlsx"); // Should be ignored
        originalProfile.setSourceSheetName("SourceSheet"); // Should be ignored
        originalProfile.setTargetSheetName("TargetSheet"); // Should be ignored
        originalProfile.setSourceHeaderRows(Arrays.asList(1, 2));
        originalProfile.setTargetHeaderRows(Collections.singletonList(1));
        originalProfile.setSourceConcatenationMode(ConcatenationMode.BREADCRUMB);
        originalProfile.setMultiRowHeaderSeparator(" - ");
        originalProfile.setColumnMappings(new HashMap<>() {{
            put("Src Col A", "Tgt Col 1");
            put("Src Col B", "Tgt Col 2");
        }});
        originalProfile.setIgnoredColumns(Collections.singletonList("Src Col C"));
        originalProfile.setRowMatchStrategy(RowMatchStrategy.BY_PRIMARY_KEY);
        originalProfile.setKeyColumns(Collections.singletonList("Src Col A"));
        originalProfile.setDuplicatePolicy(DuplicatePolicy.REPORT_ALL);
        originalProfile.setTrimWhitespace(false);
        originalProfile.setIgnoreCase(false);

        // Create a complex filter structure
        FilterGroup rootGroup = new FilterGroup();
        rootGroup.setOperator(FilteringService.LogicalOperator.OR);

        FilterCondition condition1 = new FilterCondition("Name", Operator.EQUALS, "John Doe");
        rootGroup.addCondition(condition1);

        FilterGroup subGroup = new FilterGroup();
        subGroup.setOperator(FilteringService.LogicalOperator.AND);
        FilterCondition condition2 = new FilterCondition("Age", Operator.GREATER_THAN, 30);
        FilterCondition condition3 = new FilterCondition("Department", Operator.IN_LIST, Arrays.asList("Sales", "Marketing"));
        subGroup.addCondition(condition2);
        subGroup.addCondition(condition3);

        rootGroup.addGroup(subGroup);
        originalProfile.setSourceFilterGroup(rootGroup);

        // 2. Serialize the profile using ProfileManager
        ProfileManager profileManager = new ProfileManager();
        String json = profileManager.serializeProfile(originalProfile);

        System.out.println("Serialized JSON:\n" + json);

        // 3. Assert that user-specific fields are NOT in the JSON
        assertFalse(json.contains("sourceFilePath"));
        assertFalse(json.contains("targetFilePath"));
        assertFalse(json.contains("sourceSheetName"));
        assertFalse(json.contains("targetSheetName"));

        // 4. Deserialize the JSON back into a ComparisonProfile object
        ComparisonProfile deserializedProfile = null;
        try {
            deserializedProfile = profileManager.loadProfile(new java.io.File("test.json")); // This is a mock, we need to read from string
        } catch (Exception e) {
             // Correctly load from string
            try {
                deserializedProfile = new com.fasterxml.jackson.databind.ObjectMapper()
                        .setVisibility(com.fasterxml.jackson.annotation.PropertyAccessor.FIELD, com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY)
                        .readValue(json, ComparisonProfile.class);
            } catch (JsonProcessingException ex) {
                fail("Failed to deserialize JSON string", ex);
            }
        }


        assertNotNull(deserializedProfile);

        // 5. Assert that the deserialized profile matches the original (for shared fields)
        assertEquals(originalProfile.getSourceHeaderRows(), deserializedProfile.getSourceHeaderRows());
        assertEquals(originalProfile.getTargetHeaderRows(), deserializedProfile.getTargetHeaderRows());
        assertEquals(originalProfile.getSourceConcatenationMode(), deserializedProfile.getSourceConcatenationMode());
        assertEquals(originalProfile.getMultiRowHeaderSeparator(), deserializedProfile.getMultiRowHeaderSeparator());
        assertEquals(originalProfile.getColumnMappings(), deserializedProfile.getColumnMappings());
        assertEquals(originalProfile.getIgnoredColumns(), deserializedProfile.getIgnoredColumns());
        assertEquals(originalProfile.getRowMatchStrategy(), deserializedProfile.getRowMatchStrategy());
        assertEquals(originalProfile.getKeyColumns(), deserializedProfile.getKeyColumns());
        assertEquals(originalProfile.getDuplicatePolicy(), deserializedProfile.getDuplicatePolicy());
        assertEquals(originalProfile.isTrimWhitespace(), deserializedProfile.isTrimWhitespace());
        assertEquals(originalProfile.isIgnoreCase(), deserializedProfile.isIgnoreCase());

        // Assert filter structure
        assertNotNull(deserializedProfile.getSourceFilterGroup());
        FilterGroup deserializedRoot = deserializedProfile.getSourceFilterGroup();
        assertEquals(FilteringService.LogicalOperator.OR, deserializedRoot.getOperator());
        assertEquals(1, deserializedRoot.getConditions().size());
        assertEquals(1, deserializedRoot.getGroups().size());

        FilterCondition deserializedCondition1 = deserializedRoot.getConditions().get(0);
        assertEquals("Name", deserializedCondition1.getColumnName());
        assertEquals(Operator.EQUALS, deserializedCondition1.getOperator());
        assertEquals("John Doe", deserializedCondition1.getValue());

        FilterGroup deserializedSubGroup = deserializedRoot.getGroups().get(0);
        assertEquals(FilteringService.LogicalOperator.AND, deserializedSubGroup.getOperator());
        assertEquals(2, deserializedSubGroup.getConditions().size());

        FilterCondition cond2 = deserializedSubGroup.getConditions().get(0);
        assertEquals("Age", cond2.getColumnName());
        // Note: Numbers are deserialized as Doubles or Integers by default from JSON
        assertEquals(30, ((Number) cond2.getValue()).intValue());

        FilterCondition cond3 = deserializedSubGroup.getConditions().get(1);
        assertEquals("Department", cond3.getColumnName());
        assertTrue(cond3.getValue() instanceof List);
        assertEquals(Arrays.asList("Sales", "Marketing"), cond3.getValue());
    }
}