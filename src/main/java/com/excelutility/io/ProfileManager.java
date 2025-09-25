package com.excelutility.io;

import com.excelutility.core.ComparisonProfile;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.File;
import java.io.IOException;

public class ProfileManager {

    private final ObjectMapper objectMapper;

    public ProfileManager() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        // Configure mixin to ignore file paths during serialization
        objectMapper.addMixIn(ComparisonProfile.class, ComparisonProfileMixIn.class);
    }

    /**
     * Serializes the given ComparisonProfile to a JSON string, excluding user-specific fields.
     *
     * @param profile The ComparisonProfile to serialize.
     * @return A JSON string representing the profile.
     * @throws JsonProcessingException if there is an error during serialization.
     */
    public String serializeProfile(ComparisonProfile profile) throws JsonProcessingException {
        return objectMapper.writeValueAsString(profile);
    }

    /**
     * Saves the shared parts of a ComparisonProfile to a file.
     *
     * @param profile  The profile to save.
     * @param file     The file to save to.
     * @throws IOException if there is an error writing to the file.
     */
    public void saveProfile(ComparisonProfile profile, File file) throws IOException {
        objectMapper.writeValue(file, profile);
    }

    /**
     * Loads a ComparisonProfile from a file.
     *
     * @param file The file to load from.
     * @return The loaded ComparisonProfile.
     * @throws IOException if there is an error reading from the file.
     */
    public ComparisonProfile loadProfile(File file) throws IOException {
        return objectMapper.readValue(file, ComparisonProfile.class);
    }

    /**
     * Merges a loaded profile into the existing application profile.
     * It preserves the existing file paths and sheet names while updating all other settings.
     *
     * @param existingProfile The current profile in the application.
     * @param loadedProfile   The profile loaded from a file.
     */
    public void mergeProfiles(ComparisonProfile existingProfile, ComparisonProfile loadedProfile) {
        // Restore all non-file-specific settings from the loaded profile
        existingProfile.setSourceHeaderRows(loadedProfile.getSourceHeaderRows());
        existingProfile.setTargetHeaderRows(loadedProfile.getTargetHeaderRows());
        existingProfile.setSourceConcatenationMode(loadedProfile.getSourceConcatenationMode());
        existingProfile.setTargetConcatenationMode(loadedProfile.getTargetConcatenationMode());
        existingProfile.setMultiRowHeaderSeparator(loadedProfile.getMultiRowHeaderSeparator());
        existingProfile.setColumnMappings(loadedProfile.getColumnMappings());
        existingProfile.setIgnoredColumns(loadedProfile.getIgnoredColumns());
        existingProfile.setRowMatchStrategy(loadedProfile.getRowMatchStrategy());
        existingProfile.setKeyColumns(loadedProfile.getKeyColumns());
        existingProfile.setDuplicatePolicy(loadedProfile.getDuplicatePolicy());
        existingProfile.setTrimWhitespace(loadedProfile.isTrimWhitespace());
        existingProfile.setIgnoreCase(loadedProfile.isIgnoreCase());
        existingProfile.setUseStreaming(loadedProfile.isUseStreaming());
        existingProfile.setSourceFilterGroup(loadedProfile.getSourceFilterGroup());
        existingProfile.setTargetFilterGroup(loadedProfile.getTargetFilterGroup());
    }

    /**
     * Abstract mixin for ComparisonProfile to control serialization.
     * This is used to prevent certain fields from being included in the JSON output.
     */
    private abstract static class ComparisonProfileMixIn {
        // Ignore these fields when writing to JSON
        @com.fasterxml.jackson.annotation.JsonIgnore
        abstract String getSourceFilePath();

        @com.fasterxml.jackson.annotation.JsonIgnore
        abstract String getTargetFilePath();

        @com.fasterxml.jackson.annotation.JsonIgnore
        abstract String getSourceSheetName();

        @com.fasterxml.jackson.annotation.JsonIgnore
        abstract String getTargetSheetName();
    }
}