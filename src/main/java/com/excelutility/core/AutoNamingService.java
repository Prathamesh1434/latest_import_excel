package com.excelutility.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A service for automatically generating names for UI components like rules and groups.
 */
public class AutoNamingService {

    private static final AtomicInteger groupCounter = new AtomicInteger(1);
    private static final AtomicInteger ruleCounter = new AtomicInteger(1);

    /**
     * Suggests a default name for a new filter group.
     * @return A string like "Group 1", "Group 2", etc.
     */
    public static String suggestGroupName() {
        return "Group " + groupCounter.getAndIncrement();
    }

    /**
     * Suggests a default name for a new filter rule.
     * @return A string like "Rule 1", "Rule 2", etc.
     */
    public static String suggestRuleName() {
        return "Rule " + ruleCounter.getAndIncrement();
    }


    /**
     * Resets all counters. Useful for when clearing all filters.
     */
    public static void reset() {
        groupCounter.set(1);
        ruleCounter.set(1);
    }
}
