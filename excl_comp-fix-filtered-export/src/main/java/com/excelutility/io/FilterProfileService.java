package com.excelutility.io;

import com.excelutility.core.FilterProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for saving and loading filter profiles to/from simple JSON files.
 */
public class FilterProfileService {

    private final Path profileDir;
    private final ObjectMapper mapper;

    /**
     * Default constructor, creates a profile directory in the user's home folder.
     */
    public FilterProfileService() {
        this(Paths.get(System.getProperty("user.home"), ".excel-utility", "profiles"));
    }

    /**
     * Constructor for testing purposes, allows specifying a custom profile directory.
     * @param profileDir The directory to save and load profiles from.
     */
    public FilterProfileService(Path profileDir) {
        this.profileDir = profileDir;
        try {
            Files.createDirectories(profileDir);
        } catch (IOException e) {
            throw new RuntimeException("Could not create profile directory: " + profileDir, e);
        }
        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * Saves a filter profile to a JSON file.
     * @param profile The profile object to save.
     * @throws IOException if there is an error writing the file.
     */
    public void saveProfile(FilterProfile profile) throws IOException {
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String safeName = profile.getProfileName().replaceAll("[^a-zA-Z0-9.-]", "_");
        String fileName = String.format("profile_%s_%s.json", timestamp, safeName);

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile(profileDir, "profile-", ".json.tmp");
            mapper.writeValue(tempFile.toFile(), profile);
            Path finalPath = profileDir.resolve(fileName);
            Files.move(tempFile, finalPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            if (tempFile != null) {
                Files.deleteIfExists(tempFile);
            }
            throw e;
        }
    }

    /**
     * Loads a filter profile from a JSON file.
     * @param profileFile The file to load from.
     * @return The deserialized FilterProfile object.
     * @throws IOException if there is an error reading the file.
     */
    public FilterProfile loadProfile(File profileFile) throws IOException {
        return mapper.readValue(profileFile, FilterProfile.class);
    }

    /**
     * Lists all available profile files in the profile directory.
     * @return A list of File objects for each profile.
     */
    public List<File> getAvailableProfiles() {
        File dir = profileDir.toFile();
        if (!dir.exists() || !dir.isDirectory()) {
            return Collections.emptyList();
        }
        File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".json"));
        return files != null ? Arrays.asList(files) : Collections.emptyList();
    }

    public Path getProfileDir() {
        return profileDir;
    }

    /**
     * Deletes a profile file from the filesystem.
     * @param profileFile The file to delete.
     * @throws IOException if there is an error deleting the file.
     */
    public void deleteProfile(File profileFile) throws IOException {
        if (profileFile != null && profileFile.exists()) {
            Files.delete(profileFile.toPath());
        }
    }
}
