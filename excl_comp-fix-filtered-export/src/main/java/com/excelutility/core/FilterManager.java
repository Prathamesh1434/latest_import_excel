package com.excelutility.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A singleton manager to hold saved filter groups for the current session.
 */
public class FilterManager {

    private static final FilterManager INSTANCE = new FilterManager();
    private final Map<String, FilterGroup> savedFilters;

    private FilterManager() {
        savedFilters = new HashMap<>();
    }

    public static FilterManager getInstance() {
        return INSTANCE;
    }

    public void saveFilter(String name, FilterGroup filterGroup) {
        if (name == null || name.trim().isEmpty() || filterGroup == null) {
            return;
        }
        savedFilters.put(name.trim(), filterGroup);
    }

    public FilterGroup getFilter(String name) {
        return savedFilters.get(name);
    }

    public Set<String> getSavedFilterNames() {
        return Collections.unmodifiableSet(savedFilters.keySet());
    }

    public void removeFilter(String name) {
        savedFilters.remove(name);
    }
}
