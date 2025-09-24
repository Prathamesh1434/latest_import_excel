package com.excelutility;

import com.excelutility.excel.Normalizer;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NormalizerTest {

    @Test
    void testNormalizeHeaders() {
        // Create sample data with a 2-row header
        List<List<Object>> data = new ArrayList<>();
        List<Object> headerRow1 = new ArrayList<>(Arrays.asList("Group A", "", "Group B"));
        List<Object> headerRow2 = new ArrayList<>(Arrays.asList("Name", "ID", "Value"));
        List<Object> dataRow1 = new ArrayList<>(Arrays.asList("John", 1, 100));
        data.add(headerRow1);
        data.add(headerRow2);
        data.add(dataRow1);

        // Normalize the data
        List<List<Object>> normalizedData = Normalizer.normalizeHeaders(data, 2);

        // Check the result
        assertEquals(2, normalizedData.size()); // Header + 1 data row

        // Check combined header
        List<Object> combinedHeader = normalizedData.get(0);
        assertEquals("Group A Name", combinedHeader.get(0));
        assertEquals("Group A ID", combinedHeader.get(1));
        assertEquals("Group B Value", combinedHeader.get(2));

        // Check data row
        assertEquals("John", normalizedData.get(1).get(0));
    }
}
