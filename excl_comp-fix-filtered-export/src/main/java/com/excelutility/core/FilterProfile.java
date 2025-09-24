package com.excelutility.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

/**
 * Represents a saved profile for the Filter tool.
 * This class is designed to be serialized to/from JSON.
 */
public class FilterProfile {

    private final String profileName;
    private final String timestamp;
    private final String dataFilePath;
    private final String dataSheet;
    private final String filterFilePath;
    private final String filterSheet;
    private final FilterBuilderState filterBuilder;
    private final List<Integer> dataHeaderRowIndices;
    private final ConcatenationMode dataConcatenationMode;
    private final List<Integer> filterHeaderRowIndices;
    private final ConcatenationMode filterConcatenationMode;

    @JsonCreator
    public FilterProfile(
            @JsonProperty("profileName") String profileName,
            @JsonProperty("timestamp") String timestamp,
            @JsonProperty("dataFilePath") String dataFilePath,
            @JsonProperty("dataSheet") String dataSheet,
            @JsonProperty("filterFilePath") String filterFilePath,
            @JsonProperty("filterSheet") String filterSheet,
            @JsonProperty("filterBuilder") FilterBuilderState filterBuilder,
            @JsonProperty("dataHeaderRowIndices") List<Integer> dataHeaderRowIndices,
            @JsonProperty("dataConcatenationMode") ConcatenationMode dataConcatenationMode,
            @JsonProperty("filterHeaderRowIndices") List<Integer> filterHeaderRowIndices,
            @JsonProperty("filterConcatenationMode") ConcatenationMode filterConcatenationMode) {
        this.profileName = profileName;
        this.timestamp = timestamp;
        this.dataFilePath = dataFilePath;
        this.dataSheet = dataSheet;
        this.filterFilePath = filterFilePath;
        this.filterSheet = filterSheet;
        this.filterBuilder = filterBuilder;
        this.dataHeaderRowIndices = dataHeaderRowIndices;
        this.dataConcatenationMode = dataConcatenationMode;
        this.filterHeaderRowIndices = filterHeaderRowIndices;
        this.filterConcatenationMode = filterConcatenationMode;
    }

    // Getters
    public String getProfileName() { return profileName; }
    public String getTimestamp() { return timestamp; }
    public String getDataFilePath() { return dataFilePath; }
    public String getDataSheet() { return dataSheet; }
    public String getFilterFilePath() { return filterFilePath; }
    public String getFilterSheet() { return filterSheet; }
    public FilterBuilderState getFilterBuilder() { return filterBuilder; }
    public List<Integer> getDataHeaderRowIndices() { return dataHeaderRowIndices; }
    public ConcatenationMode getDataConcatenationMode() { return dataConcatenationMode; }
    public List<Integer> getFilterHeaderRowIndices() { return filterHeaderRowIndices; }
    public ConcatenationMode getFilterConcatenationMode() { return filterConcatenationMode; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilterProfile that = (FilterProfile) o;
        return Objects.equals(profileName, that.profileName) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(dataFilePath, that.dataFilePath) &&
                Objects.equals(dataSheet, that.dataSheet) &&
                Objects.equals(filterFilePath, that.filterFilePath) &&
                Objects.equals(filterSheet, that.filterSheet) &&
                Objects.equals(filterBuilder, that.filterBuilder) &&
                Objects.equals(dataHeaderRowIndices, that.dataHeaderRowIndices) &&
                dataConcatenationMode == that.dataConcatenationMode &&
                Objects.equals(filterHeaderRowIndices, that.filterHeaderRowIndices) &&
                filterConcatenationMode == that.filterConcatenationMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(profileName, timestamp, dataFilePath, dataSheet, filterFilePath, filterSheet, filterBuilder, dataHeaderRowIndices, dataConcatenationMode, filterHeaderRowIndices, filterConcatenationMode);
    }
}
