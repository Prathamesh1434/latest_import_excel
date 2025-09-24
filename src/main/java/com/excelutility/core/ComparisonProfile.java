package com.excelutility.core;

import java.util.List;
import java.util.Map;

/**
 * A data class that holds all the configuration for a comparison task.
 * This object can be serialized to and from a file (e.g., JSON/YAML) to save and load profiles.
 */
public class ComparisonProfile {

    // File settings
    private String sourceFilePath;
    private String targetFilePath;
    private String sourceSheetName;
    private String targetSheetName;

    // Header settings
    private List<Integer> sourceHeaderRows;
    private List<Integer> targetHeaderRows;
    private ConcatenationMode sourceConcatenationMode = ConcatenationMode.LEAF_ONLY;
    private ConcatenationMode targetConcatenationMode = ConcatenationMode.LEAF_ONLY;
    private String multiRowHeaderSeparator = " | ";

    // Mapping and Matching
    private Map<String, String> columnMappings; // Source Column Name -> Target Column Name
    private List<String> ignoredColumns = new java.util.ArrayList<>();
    private RowMatchStrategy rowMatchStrategy = RowMatchStrategy.BY_PRIMARY_KEY;
    private List<String> keyColumns; // List of source column names to use as keys
    private DuplicatePolicy duplicatePolicy = DuplicatePolicy.REPORT_ALL;

    // Global Normalization/Comparison Rules (to be expanded)
    private boolean trimWhitespace = true;
    private boolean ignoreCase = true;

    // Per-column rules (to be expanded)
    // private Map<String, ColumnProfile> columnProfiles;

    // Filter settings
    private FilterGroup sourceFilterGroup;
    private FilterGroup targetFilterGroup;

    // Performance settings
    private boolean useStreaming = false;


    // Getters and Setters
    public String getSourceFilePath() { return sourceFilePath; }
    public void setSourceFilePath(String sourceFilePath) { this.sourceFilePath = sourceFilePath; }
    public String getTargetFilePath() { return targetFilePath; }
    public void setTargetFilePath(String targetFilePath) { this.targetFilePath = targetFilePath; }
    public String getSourceSheetName() { return sourceSheetName; }
    public void setSourceSheetName(String sourceSheetName) { this.sourceSheetName = sourceSheetName; }
    public String getTargetSheetName() { return targetSheetName; }
    public void setTargetSheetName(String targetSheetName) { this.targetSheetName = targetSheetName; }
    public List<Integer> getSourceHeaderRows() { return sourceHeaderRows; }
    public void setSourceHeaderRows(List<Integer> sourceHeaderRows) { this.sourceHeaderRows = sourceHeaderRows; }
    public List<Integer> getTargetHeaderRows() { return targetHeaderRows; }
    public void setTargetHeaderRows(List<Integer> targetHeaderRows) { this.targetHeaderRows = targetHeaderRows; }
    public ConcatenationMode getSourceConcatenationMode() { return sourceConcatenationMode; }
    public void setSourceConcatenationMode(ConcatenationMode sourceConcatenationMode) { this.sourceConcatenationMode = sourceConcatenationMode; }
    public ConcatenationMode getTargetConcatenationMode() { return targetConcatenationMode; }
    public void setTargetConcatenationMode(ConcatenationMode targetConcatenationMode) { this.targetConcatenationMode = targetConcatenationMode; }
    public String getMultiRowHeaderSeparator() { return multiRowHeaderSeparator; }
    public void setMultiRowHeaderSeparator(String multiRowHeaderSeparator) { this.multiRowHeaderSeparator = multiRowHeaderSeparator; }
    public Map<String, String> getColumnMappings() { return columnMappings; }
    public void setColumnMappings(Map<String, String> columnMappings) { this.columnMappings = columnMappings; }
    public List<String> getIgnoredColumns() { return ignoredColumns; }
    public void setIgnoredColumns(List<String> ignoredColumns) { this.ignoredColumns = ignoredColumns; }
    public RowMatchStrategy getRowMatchStrategy() { return rowMatchStrategy; }
    public void setRowMatchStrategy(RowMatchStrategy rowMatchStrategy) { this.rowMatchStrategy = rowMatchStrategy; }
    public List<String> getKeyColumns() { return keyColumns; }
    public void setKeyColumns(List<String> keyColumns) { this.keyColumns = keyColumns; }
    public DuplicatePolicy getDuplicatePolicy() { return duplicatePolicy; }
    public void setDuplicatePolicy(DuplicatePolicy duplicatePolicy) { this.duplicatePolicy = duplicatePolicy; }
    public boolean isTrimWhitespace() { return trimWhitespace; }
    public void setTrimWhitespace(boolean trimWhitespace) { this.trimWhitespace = trimWhitespace; }
    public boolean isIgnoreCase() { return ignoreCase; }
    public void setIgnoreCase(boolean ignoreCase) { this.ignoreCase = ignoreCase; }
    public boolean isUseStreaming() { return useStreaming; }
    public void setUseStreaming(boolean useStreaming) { this.useStreaming = useStreaming; }
    public FilterGroup getSourceFilterGroup() { return sourceFilterGroup; }
    public void setSourceFilterGroup(FilterGroup sourceFilterGroup) { this.sourceFilterGroup = sourceFilterGroup; }
    public FilterGroup getTargetFilterGroup() { return targetFilterGroup; }
    public void setTargetFilterGroup(FilterGroup targetFilterGroup) { this.targetFilterGroup = targetFilterGroup; }
}
