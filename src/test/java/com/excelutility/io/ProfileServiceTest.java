package com.excelutility.io;

import com.excelutility.core.ComparisonProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class ProfileServiceTest {

    private ProfileService profileService;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() {
        profileService = new ProfileService(tempDir.getAbsolutePath());
    }

    @Test
    void testExportProfile() throws IOException {
        // Given
        ComparisonProfile profile = new ComparisonProfile();
        profile.setSourceFilePath("source.xlsx");
        profile.setTargetFilePath("target.xlsx");
        profile.setKeyColumns(Collections.singletonList("ID"));

        String profileName = "test-profile";
        profileService.saveProfile(profile, profileName);

        File destination = new File(tempDir, "exported-profile.json");

        // When
        profileService.exportProfile(profileName, destination);

        // Then
        assertTrue(destination.exists());
        String content = new String(Files.readAllBytes(destination.toPath()));
        assertTrue(content.contains("source.xlsx"));
        assertTrue(content.contains("target.xlsx"));
        assertTrue(content.contains("ID"));
    }

    @Test
    void testLoadProfileFromFile() throws IOException {
        // Given
        ComparisonProfile originalProfile = new ComparisonProfile();
        originalProfile.setSourceFilePath("source.xlsx");
        originalProfile.setTargetFilePath("target.xlsx");
        originalProfile.setKeyColumns(Collections.singletonList("ID"));

        File profileFile = new File(tempDir, "profile-to-load.json");
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(profileFile, originalProfile);

        // When
        ComparisonProfile loadedProfile = profileService.loadProfileFromFile(profileFile);

        // Then
        assertNotNull(loadedProfile);
        assertEquals("source.xlsx", loadedProfile.getSourceFilePath());
        assertEquals("target.xlsx", loadedProfile.getTargetFilePath());
        assertEquals(Collections.singletonList("ID"), loadedProfile.getKeyColumns());
    }
}