package com.excelutility;

import com.excelutility.core.*;
import com.excelutility.gui.FilterExpressionBuilderPanel;
import com.excelutility.io.FilterProfileService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ProfileEndToEndTest {

    private FilterProfileService profileService;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        profileService = new FilterProfileService(tempDir);
    }

    @Test
    void testSaveAndLoadComplexProfile() throws IOException {
        FilterProfile originalProfile = createComplexTestProfile("End-to-End Test Profile");
        profileService.saveProfile(originalProfile);
        List<File> profiles = profileService.getAvailableProfiles();
        assertEquals(1, profiles.size());
        FilterProfile loadedProfile = profileService.loadProfile(profiles.get(0));
        assertNotNull(loadedProfile);
        assertEquals(originalProfile, loadedProfile);
    }

    @Test
    void testRebuildFromState() {
        FilterExpressionBuilderPanel panel = new FilterExpressionBuilderPanel(null);
        FilterBuilderState originalState = createComplexTestProfile("Test").getFilterBuilder();
        panel.rebuildFromState(originalState);
        FilterBuilderState rebuiltState = panel.getState();
        assertEquals(originalState, rebuiltState);
    }

    private FilterProfile createComplexTestProfile(String profileName) {
        RuleState rule1 = new RuleState("Rule 1", FilterRule.SourceType.BY_VALUE, "Active", "Status", true);
        RuleState rule2 = new RuleState("Rule 2", FilterRule.SourceType.BY_VALUE, "Chicago", "City", false);
        GroupState innerGroup = new GroupState("Location and Status", Arrays.asList(FilteringService.LogicalOperator.AND), Arrays.asList(rule1, rule2), Collections.emptyList());
        RuleState rule3 = new RuleState("Rule 3", FilterRule.SourceType.BY_VALUE, "Bob", "Name", true);
        // The test now reflects that the top-level operator will be AND when read back from the panel
        GroupState rootGroupState = new GroupState("Root", Arrays.asList(FilteringService.LogicalOperator.AND), Collections.singletonList(rule3), Collections.singletonList(innerGroup));
        FilterBuilderState builderState = new FilterBuilderState(Collections.singletonList(rootGroupState));

        return new FilterProfile(
            profileName,
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(new Date()),
            "/path/to/data.xlsx", "Sheet1",
            "/path/to/filters.xlsx", "FilterSheet",
            builderState,
            Collections.singletonList(0), com.excelutility.core.ConcatenationMode.BREADCRUMB,
            Collections.singletonList(1), com.excelutility.core.ConcatenationMode.LEAF_ONLY
        );
    }
}
