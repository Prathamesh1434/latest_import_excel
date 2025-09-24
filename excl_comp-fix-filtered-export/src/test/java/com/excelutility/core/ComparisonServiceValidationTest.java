package com.excelutility.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ComparisonServiceValidationTest {

    private ComparisonService comparisonService;
    private ComparisonProfile profile;

    @BeforeEach
    void setUp() {
        comparisonService = new ComparisonService();
        profile = new ComparisonProfile();
        // Set up a minimally valid profile
        profile.setSourceFilePath("source.xlsx");
        profile.setTargetFilePath("target.xlsx");
        profile.setSourceSheetName("Sheet1");
        profile.setTargetSheetName("Sheet1");
        profile.setKeyColumns(List.of("ID"));
        profile.setColumnMappings(Map.of("ID", "ID"));
    }

    @Test
    void testValidateProfile_nullProfile() {
        Exception exception = assertThrows(ComparisonException.class, () -> {
            comparisonService.validateProfile(null);
        });
        assertEquals("Comparison profile is missing.", exception.getMessage());
    }

    @Test
    void testValidateProfile_missingSourceFile() {
        profile.setSourceFilePath(null);
        Exception exception = assertThrows(ComparisonException.class, () -> {
            comparisonService.validateProfile(profile);
        });
        assertEquals("Source file path is not set.", exception.getMessage());
    }

    @Test
    void testValidateProfile_missingTargetFile() {
        profile.setTargetFilePath("");
        Exception exception = assertThrows(ComparisonException.class, () -> {
            comparisonService.validateProfile(profile);
        });
        assertEquals("Target file path is not set.", exception.getMessage());
    }

    @Test
    void testValidateProfile_missingSourceSheet() {
        profile.setSourceSheetName(null);
        Exception exception = assertThrows(ComparisonException.class, () -> {
            comparisonService.validateProfile(profile);
        });
        assertEquals("Source sheet name is not set.", exception.getMessage());
    }

    @Test
    void testValidateProfile_missingKeyColumns() {
        profile.setKeyColumns(Collections.emptyList());
        Exception exception = assertThrows(ComparisonException.class, () -> {
            comparisonService.validateProfile(profile);
        });
        assertEquals("At least one key column must be selected for comparison.", exception.getMessage());
    }

    @Test
    void testValidateProfile_keyColumnNotMapped() {
        profile.setKeyColumns(List.of("UnmappedKey"));
        Exception exception = assertThrows(ComparisonException.class, () -> {
            comparisonService.validateProfile(profile);
        });
        assertTrue(exception.getMessage().contains("is not present in the column mappings"));
    }

    @Test
    void testValidateProfile_nullMappingsWithKeys() {
        // This is a likely cause for the original NPE
        profile.setColumnMappings(null);
        Exception exception = assertThrows(ComparisonException.class, () -> {
            comparisonService.validateProfile(profile);
        });
        assertEquals("Column mappings are not set.", exception.getMessage());
    }

    @Test
    void testCompare_NpeRegressionTest() {
        // Reproduce scenario that could cause an NPE if validation is bypassed
        ComparisonProfile incompleteProfile = new ComparisonProfile();
        incompleteProfile.setSourceFilePath("source.xlsx");
        incompleteProfile.setTargetFilePath("target.xlsx");
        incompleteProfile.setSourceSheetName("Sheet1");
        incompleteProfile.setTargetSheetName("Sheet1");
        incompleteProfile.setKeyColumns(List.of("ID"));
        // Mappings are missing, which would cause NPE when building key map
        incompleteProfile.setColumnMappings(new HashMap<>());

        Exception exception = assertThrows(ComparisonException.class, () -> {
            comparisonService.compare(incompleteProfile);
        });

        // We now expect a graceful validation error, not a crash.
        assertTrue(exception.getMessage().contains("Key column 'ID' is not present in the column mappings."));
    }
}
