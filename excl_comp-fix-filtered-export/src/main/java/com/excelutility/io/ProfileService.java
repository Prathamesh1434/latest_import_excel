package com.excelutility.io;

import com.excelutility.core.ComparisonProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Service for saving, loading, and managing comparison profiles.
 */
public class ProfileService {

    private final ObjectMapper mapper;
    private final String profileDirectory;

    public ProfileService(String profileDirectory) {
        this.profileDirectory = profileDirectory;
        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);

        // Ensure directory exists
        new File(profileDirectory).mkdirs();
    }

    public void saveProfile(ComparisonProfile profile, String profileName) throws IOException {
        File profileFile = new File(profileDirectory, profileName + ".json");
        mapper.writeValue(profileFile, profile);
    }

    public ComparisonProfile loadProfile(String profileName) throws IOException {
        File profileFile = new File(profileDirectory, profileName + ".json");
        return mapper.readValue(profileFile, ComparisonProfile.class);
    }

    public List<String> getAvailableProfiles() {
        List<String> profileNames = new ArrayList<>();
        File dir = new File(profileDirectory);
        File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".json"));
        if (files != null) {
            for (File file : files) {
                profileNames.add(file.getName().replace(".json", ""));
            }
        }
        return profileNames;
    }

    public void deleteProfile(String profileName) {
        File profileFile = new File(profileDirectory, profileName + ".json");
        if (profileFile.exists()) {
            profileFile.delete();
        }
    }
}
