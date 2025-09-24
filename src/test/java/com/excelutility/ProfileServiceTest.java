package com.excelutility;

import com.excelutility.core.ComparisonProfile;
import com.excelutility.io.ProfileService;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class ProfileServiceTest {

    @Test
    void testSaveAndLoadProfile() throws IOException {
        String testDir = "target/test-profiles";
        // Cleanup before test
        File dir = new File(testDir);
        if (dir.exists()) {
            for (File f : dir.listFiles()) {
                f.delete();
            }
        }

        ProfileService service = new ProfileService(testDir);

        ComparisonProfile profile = new ComparisonProfile();
        profile.setSourceFilePath("test.xlsx");
        profile.setIgnoreCase(true);

        String profileName = "my-test-profile";
        service.saveProfile(profile, profileName);

        // Test saving
        File savedFile = new File(testDir, profileName + ".json");
        assertTrue(savedFile.exists());

        // Test loading
        ComparisonProfile loadedProfile = service.loadProfile(profileName);
        assertNotNull(loadedProfile);
        assertEquals("test.xlsx", loadedProfile.getSourceFilePath());
        assertTrue(loadedProfile.isIgnoreCase());

        // Test listing
        List<String> profiles = service.getAvailableProfiles();
        assertTrue(profiles.contains(profileName));
        assertEquals(1, profiles.size());

        // Test deletion
        service.deleteProfile(profileName);
        assertFalse(service.getAvailableProfiles().contains(profileName));
        assertFalse(savedFile.exists());

        dir.delete();
    }
}
