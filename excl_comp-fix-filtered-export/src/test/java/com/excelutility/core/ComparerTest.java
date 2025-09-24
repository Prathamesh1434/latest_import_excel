package com.excelutility.core;

import com.excelutility.core.ComparisonProfile;
import com.excelutility.core.ComparisonService;
import com.excelutility.core.RowComparisonStatus;
import com.excelutility.core.RowResult;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class ComparerTest {

    @Test
    void testMatchRows() {
        ComparisonService service = new ComparisonService();
        ComparisonProfile profile = new ComparisonProfile();

        List<Object> headers = Arrays.asList("ID", "Name", "Value");
        Map<String, String> mappings = new HashMap<>();
        mappings.put("ID", "ID");
        mappings.put("Name", "Name");
        mappings.put("Value", "Value");
        profile.setColumnMappings(mappings);
        profile.setKeyColumns(List.of("ID"));
        profile.setIgnoreCase(true);
        profile.setTrimWhitespace(true);

        List<List<Object>> sourceRows = new ArrayList<>();
        sourceRows.add(Arrays.asList(1, "  John  ", 100)); // Should be identical after trim
        sourceRows.add(Arrays.asList(2, "Jane", 200));   // Mismatched value
        sourceRows.add(Arrays.asList(3, "Mike", 300));   // Source only

        List<List<Object>> targetRows = new ArrayList<>();
        targetRows.add(Arrays.asList(1, "john", 100));   // Should be identical after case change
        targetRows.add(Arrays.asList(2, "Jane", 250));   // Mismatched value
        targetRows.add(Arrays.asList(4, "Sue", 400));    // Target only

        List<RowResult> results = service.matchRows(sourceRows, targetRows, headers, headers, profile);

        assertEquals(4, results.size());

        long identical = results.stream().filter(r -> r.getStatus() == RowComparisonStatus.MATCHED_IDENTICAL).count();
        long mismatched = results.stream().filter(r -> r.getStatus() == RowComparisonStatus.MATCHED_MISMATCHED).count();
        long sourceOnly = results.stream().filter(r -> r.getStatus() == RowComparisonStatus.SOURCE_ONLY).count();
        long targetOnly = results.stream().filter(r -> r.getStatus() == RowComparisonStatus.TARGET_ONLY).count();

        assertEquals(1, identical);
        assertEquals(1, mismatched);
        assertEquals(1, sourceOnly);
        assertEquals(1, targetOnly);

        RowResult mismatchedRow = results.stream().filter(r -> r.getStatus() == RowComparisonStatus.MATCHED_MISMATCHED).findFirst().get();
        assertTrue(mismatchedRow.getDifferences().containsKey(2)); // Value column
    }
}
