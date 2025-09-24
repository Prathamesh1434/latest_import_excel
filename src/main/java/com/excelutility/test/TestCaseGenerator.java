package com.excelutility.test;

import com.excelutility.io.SimpleExcelWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCaseGenerator {

    private final String outputDir;

    public TestCaseGenerator(String outputDir) {
        this.outputDir = outputDir;
        try {
            Files.createDirectories(Paths.get(outputDir));
        } catch (IOException e) {
            throw new RuntimeException("Could not create test case directory", e);
        }
    }

    public void generate(String scenario, int rowCount) throws IOException {
        switch (scenario) {
            case "Exact Match":
                generateExactMatch(rowCount);
                break;
            case "Numeric Tolerance Mismatch":
                generateNumericMismatch(rowCount);
                break;
            case "Multi-row Headers":
                generateMultiRowHeader(rowCount);
                break;
            default:
                throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
    }

    private void generateNumericMismatch(int rowCount) throws IOException {
        String testId = "numeric-mismatch-" + System.currentTimeMillis();
        List<List<Object>> sourceData = new ArrayList<>();
        List<List<Object>> targetData = new ArrayList<>();

        sourceData.add(List.of("ID", "Value"));
        targetData.add(List.of("ID", "Value"));

        for (int i = 1; i <= rowCount; i++) {
            sourceData.add(List.of(i, 100.0 + i));
            targetData.add(List.of(i, 100.0 + i + 0.01)); // Slight difference
        }

        saveTestCase(testId, "Numeric mismatch test", sourceData, targetData);
    }

    private void generateMultiRowHeader(int rowCount) throws IOException {
        String testId = "multi-row-header-" + System.currentTimeMillis();
        List<List<Object>> data = new ArrayList<>();

        data.add(List.of("Product Details", "", "Sales"));
        data.add(List.of("ID", "Name", "Amount"));
        for (int i = 1; i <= rowCount; i++) {
            data.add(List.of(i, "Product" + i, 50 * i));
        }

        saveTestCase(testId, "Multi-row header test", data, data); // Use same data for source and target
    }

    private void generateExactMatch(int rowCount) throws IOException {
        String testId = "exact-match-" + System.currentTimeMillis();
        List<List<Object>> data = new ArrayList<>();
        // Header
        data.add(List.of("ID", "FirstName", "LastName", "Email"));
        // Data
        for (int i = 1; i <= rowCount; i++) {
            data.add(List.of(i, "FirstName" + i, "LastName" + i, "user" + i + "@example.com"));
        }

        saveTestCase(testId, "Exact match test with " + rowCount + " rows.", data, data);
    }

    private void saveTestCase(String testId, String description, List<List<Object>> sourceData, List<List<Object>> targetData) throws IOException {
        String sourcePath = new File(outputDir, testId + "-source.xlsx").getAbsolutePath();
        String targetPath = new File(outputDir, testId + "-target.xlsx").getAbsolutePath();

        SimpleExcelWriter.write(sourceData, "Sheet1", sourcePath);
        SimpleExcelWriter.write(targetData, "Sheet1", targetPath);

        // Create metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("id", testId);
        metadata.put("description", description);
        metadata.put("sourceFile", sourcePath);
        metadata.put("targetFile", targetPath);
        // A real implementation would have more detailed expected results
        metadata.put("expectedResult", "Varies");

        saveMetadata(metadata, testId);
    }

    private void saveMetadata(Map<String, Object> metadata, String testId) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(new File(outputDir, testId + "-meta.json"), metadata);
    }
}
