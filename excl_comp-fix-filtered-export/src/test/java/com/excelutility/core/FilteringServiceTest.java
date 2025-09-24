package com.excelutility.core;

import com.excelutility.io.SimpleExcelWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.excelutility.core.expression.GroupNode;
import com.excelutility.core.expression.RuleNode;

import static org.junit.jupiter.api.Assertions.*;

public class FilteringServiceTest {

    private FilteringService filteringService;
    private String dataFilePath = "target/test-files/data.xlsx";
    private String filterFilePath = "target/test-files/filters.xlsx";
    private String specialCharsFilePath = "target/test-files/special_chars_test.xlsx";
    private String multiHeaderFilePath = "target/test-files/multi_header_test.xlsx";

    @BeforeEach
    void setUp() throws IOException {
        filteringService = new FilteringService();
        new File(dataFilePath).getParentFile().mkdirs();

        // Create data file with extra whitespace and a row to test column name filtering
        List<List<Object>> data = new ArrayList<>();
        data.add(Arrays.asList("ID", "Name", "City", "Status"));
        data.add(Arrays.asList(1, "Alice", "  New York  ", "Active"));
        data.add(Arrays.asList(2, "Bob", "Los Angeles", "Inactive"));
        data.add(Arrays.asList(3, "Charlie", "New York", "Active"));
        data.add(Arrays.asList(4, "David", "  Chicago", "Active"));
        data.add(Arrays.asList(5, "Eve", "Chicago", "Female")); // To test BY_COLUMN
        data.add(Arrays.asList(6, "Frank", null, "Active")); // Empty cell
        SimpleExcelWriter.write(data, "Sheet1", dataFilePath);

        // Create filter file (used in some old manual tests, can be ignored for these unit tests)
        List<List<Object>> filterData = new ArrayList<>();
        filterData.add(Arrays.asList("Cities", "Names", "Female"));
        filterData.add(Arrays.asList("New York", "Alice", "Yes"));
        filterData.add(Arrays.asList("Chicago", "David", "No"));
        SimpleExcelWriter.write(filterData, "Sheet1", filterFilePath);

        // Create special characters test file
        List<List<Object>> specialData = new ArrayList<>();
        specialData.add(Arrays.asList("ID", "Name", "Code", "Value"));
        specialData.add(Arrays.asList(1, "Test A", "P", 100.0)); // "P" should become ✓
        specialData.add(Arrays.asList(2, "Test B", "OK", 100));   // integer 100
        specialData.add(Arrays.asList(3, "Test C", "FAIL", 100.5)); // double
        SimpleExcelWriter.write(specialData, "Sheet1", specialCharsFilePath);

        // Create multi-header test file
        List<List<Object>> multiHeaderData = new ArrayList<>();
        multiHeaderData.add(Arrays.asList("Group 1", "Group 1", "Group 2"));
        multiHeaderData.add(Arrays.asList("ID", "Name", "Value"));
        multiHeaderData.add(Arrays.asList("A1", "First", 99));
        SimpleExcelWriter.write(multiHeaderData, "Data", multiHeaderFilePath);
    }

    @AfterEach
    void tearDown() {
        new File(dataFilePath).delete();
        new File(filterFilePath).delete();
        new File(specialCharsFilePath).delete();
        new File(multiHeaderFilePath).delete();
    }

    @Test
    void testFilterByValue() throws Exception {
        List<FilterRule> rules = new ArrayList<>();
        rules.add(new FilterRule(FilterRule.SourceType.BY_VALUE, "Active", "Status", false));

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, rules, FilteringService.LogicalOperator.OR);
        assertEquals(5, filteredRows.size()); // Header + 4 rows
    }

    @Test
    void testFilterByValueWithTrim() throws Exception {
        List<FilterRule> rules = new ArrayList<>();
        rules.add(new FilterRule(FilterRule.SourceType.BY_VALUE, "New York", "City", true));

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, rules, FilteringService.LogicalOperator.OR);
        assertEquals(3, filteredRows.size()); // Header + 2 rows
    }

    @Test
    void testFilterByColumnName() throws Exception {
        List<FilterRule> rules = new ArrayList<>();
        // This should filter where Status == "Female"
        rules.add(new FilterRule(FilterRule.SourceType.BY_COLUMN, "Female", "Status", true));

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, rules, FilteringService.LogicalOperator.OR);
        assertEquals(2, filteredRows.size()); // Header + 1 row
        assertEquals("Eve", filteredRows.get(1).get(1));
    }

    @Test
    void testZeroRecordFilter() throws Exception {
        List<FilterRule> rules = new ArrayList<>();
        rules.add(new FilterRule(FilterRule.SourceType.BY_VALUE, "San Francisco", "City", false));

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, rules, FilteringService.LogicalOperator.OR);
        assertEquals(1, filteredRows.size()); // Header only
    }

    @Test
    void testFilterByEmptyCell() throws Exception {
        List<FilterRule> rules = new ArrayList<>();
        rules.add(new FilterRule(FilterRule.SourceType.BY_VALUE, "", "City", false));

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, rules, FilteringService.LogicalOperator.OR);
        assertEquals(2, filteredRows.size()); // Header + 1 row
        assertEquals("Frank", filteredRows.get(1).get(1));
    }

    @Test
    void testFilterWithOrOperator() throws Exception {
        List<FilterRule> rules = new ArrayList<>();
        // City is "Los Angeles" (Bob) OR Name is "David" (David)
        rules.add(new FilterRule(FilterRule.SourceType.BY_VALUE, "Los Angeles", "City", false));
        rules.add(new FilterRule(FilterRule.SourceType.BY_VALUE, "David", "Name", false));

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, rules, FilteringService.LogicalOperator.OR);

        assertEquals(3, filteredRows.size()); // Header + Bob + David

        // Check that the correct rows were returned, regardless of order
        List<String> names = filteredRows.stream().skip(1).map(row -> (String) row.get(1)).collect(Collectors.toList());
        assertTrue(names.contains("Bob"));
        assertTrue(names.contains("David"));
    }

    @Test
    void testFilterWithAndOperator() throws Exception {
        List<FilterRule> rules = new ArrayList<>();
        // City is "New York" (trimmed) AND Status is "Active"
        rules.add(new FilterRule(FilterRule.SourceType.BY_VALUE, "New York", "City", true));
        rules.add(new FilterRule(FilterRule.SourceType.BY_VALUE, "Active", "Status", false));

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, rules, FilteringService.LogicalOperator.AND);

        assertEquals(3, filteredRows.size()); // Header + Alice + Charlie

        List<String> names = filteredRows.stream().skip(1).map(row -> (String) row.get(1)).collect(Collectors.toList());
        assertTrue(names.contains("Alice"));
        assertTrue(names.contains("Charlie"));
    }

    @Test
    void testFilterWithAndOperatorNoResults() throws Exception {
        List<FilterRule> rules = new ArrayList<>();
        // City is "New York" AND Name is "Bob"
        rules.add(new FilterRule(FilterRule.SourceType.BY_VALUE, "New York", "City", false));
        rules.add(new FilterRule(FilterRule.SourceType.BY_VALUE, "Bob", "Name", false));

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, rules, FilteringService.LogicalOperator.AND);

        assertEquals(1, filteredRows.size()); // Header only
    }

    @Test
    void testFilterWithExpressionAnd() throws Exception {
        // (City is "Chicago" AND Status is "Active") -> David
        GroupNode root = new GroupNode(FilteringService.LogicalOperator.AND, "Root");
        root.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "Chicago", "City", true)));
        root.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "Active", "Status", false)));

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, root);
        assertEquals(2, filteredRows.size()); // Header + David
        assertEquals("David", filteredRows.get(1).get(1));
    }

    @Test
    void testFilterWithExpressionOr() throws Exception {
        // (Name is "Bob" OR Name is "Eve") -> Bob, Eve
        GroupNode root = new GroupNode(FilteringService.LogicalOperator.OR, "Root");
        root.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "Bob", "Name", false)));
        root.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "Eve", "Name", false)));

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, root);
        assertEquals(3, filteredRows.size()); // Header + Bob + Eve
        List<String> names = filteredRows.stream().skip(1).map(row -> (String) row.get(1)).collect(Collectors.toList());
        assertTrue(names.contains("Bob"));
        assertTrue(names.contains("Eve"));
    }

    @Test
    void testFilterWithNestedExpression() throws Exception {
        // Status is "Active" AND (City is "Los Angeles" OR City is "Chicago") -> David
        // This should not match Bob (Inactive) or Alice/Charlie (New York)
        GroupNode root = new GroupNode(FilteringService.LogicalOperator.AND, "Root");
        root.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "Active", "Status", false)));

        GroupNode subGroup = new GroupNode(FilteringService.LogicalOperator.OR, "Sub-Group");
        subGroup.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "Los Angeles", "City", true)));
        subGroup.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "Chicago", "City", true)));
        root.addChild(subGroup);

        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, root);
        assertEquals(2, filteredRows.size()); // Header + David
        assertEquals("David", filteredRows.get(1).get(1));
    }

    @Test
    void testFilterWithEmptyGroup() throws Exception {
        // An empty group should evaluate to true and not filter anything out.
        GroupNode root = new GroupNode(FilteringService.LogicalOperator.AND, "Root");
        List<List<Object>> filteredRows = filteringService.filter(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, root);
        assertEquals(7, filteredRows.size()); // Header + 6 data rows
    }

    @Test
    void testTickSymbolNormalization() throws Exception {
        // The Normalizer should convert "P" in the file to "✓" for matching.
        GroupNode root = new GroupNode(FilteringService.LogicalOperator.AND, "Root");
        root.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "✓", "Code", false)));

        List<List<Object>> filteredRows = filteringService.filter(specialCharsFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, root);
        assertEquals(2, filteredRows.size()); // Header + 1 row
        assertEquals("Test A", filteredRows.get(1).get(1));
    }

    @Test
    void testNumericNormalization() throws Exception {
        // The Normalizer should convert 100.0 and 100 to "100" for matching.
        GroupNode root = new GroupNode(FilteringService.LogicalOperator.AND, "Root");
        root.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "100", "Value", false)));

        List<List<Object>> filteredRows = filteringService.filter(specialCharsFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, root);
        assertEquals(3, filteredRows.size()); // Header + 2 rows
        List<Object> names = filteredRows.stream().skip(1).map(row -> row.get(1)).collect(Collectors.toList());
        assertTrue(names.contains("Test A"));
        assertTrue(names.contains("Test B"));
    }

    @Test
    void testMultiHeaderFiltering() throws Exception {
        // Use header rows 0 and 1. The canonical name for the second column should be "Group 1 | Name".
        List<Integer> headerRows = Arrays.asList(0, 1);
        GroupNode root = new GroupNode(FilteringService.LogicalOperator.AND, "Root");
        root.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "First", "Group 1 | Name", false)));

        List<List<Object>> filteredRows = filteringService.filter(multiHeaderFilePath, "Data", headerRows, ConcatenationMode.BREADCRUMB, root);
        assertEquals(2, filteredRows.size()); // Header + 1 row
        assertEquals("A1", filteredRows.get(1).get(0));
    }
    @Test
    void testGetUnifiedFilteredData() throws Exception {
        // Expression: City is "New York"
        GroupNode expression = new GroupNode(FilteringService.LogicalOperator.AND, "New York Users");
        expression.addChild(new RuleNode(new FilterRule(FilterRule.SourceType.BY_VALUE, "New York", "City", true)));

        java.util.Map<String, List<List<Object>>> results = filteringService.getUnifiedFilteredData(dataFilePath, "Sheet1", Collections.singletonList(0), ConcatenationMode.LEAF_ONLY, expression);

        assertNotNull(results);
        assertEquals(3, results.size());
        assertTrue(results.containsKey("matching"));
        assertTrue(results.containsKey("non matching"));
        assertTrue(results.containsKey("unified"));

        // Check matching rows
        List<List<Object>> matchingRows = results.get("matching");
        assertEquals(3, matchingRows.size()); // Header + Alice + Charlie
        List<String> matchingNames = matchingRows.stream().skip(1).map(row -> (String) row.get(1)).collect(Collectors.toList());
        assertTrue(matchingNames.contains("Alice"));
        assertTrue(matchingNames.contains("Charlie"));

        // Check non-matching rows
        List<List<Object>> nonMatchingRows = results.get("non matching");
        assertEquals(5, nonMatchingRows.size()); // Header + Bob + David + Eve + Frank
        List<String> nonMatchingNames = nonMatchingRows.stream().skip(1).map(row -> (String) row.get(1)).collect(Collectors.toList());
        assertTrue(nonMatchingNames.contains("Bob"));
        assertTrue(nonMatchingNames.contains("David"));
        assertTrue(nonMatchingNames.contains("Eve"));
        assertTrue(nonMatchingNames.contains("Frank"));

        // Check unified data
        List<List<Object>> unifiedRows = results.get("unified");
        assertEquals(7, unifiedRows.size()); // Header + 6 original rows
    }
}
