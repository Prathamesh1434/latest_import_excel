package com.excelutility.core;

import com.excelutility.io.ExcelReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Orchestrates the entire comparison process.
 */
public class ComparisonService {

    public ComparisonResult compare(ComparisonProfile profile) throws ComparisonException {
        validateProfile(profile);

        try {
            // 1. Read all data from files
            List<List<Object>> sourceData = ExcelReader.read(profile.getSourceFilePath(), profile.getSourceSheetName(), profile.isUseStreaming());
            List<List<Object>> targetData = ExcelReader.read(profile.getTargetFilePath(), profile.getTargetSheetName(), profile.isUseStreaming());

            List<String> sourceHeaders;
        List<String> targetHeaders;
        List<List<Object>> sourceRows;
        List<List<Object>> targetRows;

        // 2. Build headers and identify data rows
        try (org.apache.poi.ss.usermodel.Workbook sourceWorkbook = org.apache.poi.ss.usermodel.WorkbookFactory.create(new java.io.File(profile.getSourceFilePath()));
             org.apache.poi.ss.usermodel.Workbook targetWorkbook = org.apache.poi.ss.usermodel.WorkbookFactory.create(new java.io.File(profile.getTargetFilePath()))) {

            org.apache.poi.ss.usermodel.Sheet sourceSheet = sourceWorkbook.getSheet(profile.getSourceSheetName());
            org.apache.poi.ss.usermodel.Sheet targetSheet = targetWorkbook.getSheet(profile.getTargetSheetName());

            sourceHeaders = CanonicalNameBuilder.buildCanonicalHeaders(sourceSheet, profile.getSourceHeaderRows(), profile.getSourceConcatenationMode(), profile.getMultiRowHeaderSeparator());
            targetHeaders = CanonicalNameBuilder.buildCanonicalHeaders(targetSheet, profile.getTargetHeaderRows(), profile.getTargetConcatenationMode(), profile.getMultiRowHeaderSeparator());

            int sourceDataStartRow = profile.getSourceHeaderRows().isEmpty() ? 0 : profile.getSourceHeaderRows().stream().max(Integer::compareTo).get() + 1;
            int targetDataStartRow = profile.getTargetHeaderRows().isEmpty() ? 0 : profile.getTargetHeaderRows().stream().max(Integer::compareTo).get() + 1;

            sourceRows = sourceData.size() > sourceDataStartRow ? sourceData.subList(sourceDataStartRow, sourceData.size()) : new ArrayList<>();
            targetRows = targetData.size() > targetDataStartRow ? targetData.subList(targetDataStartRow, targetData.size()) : new ArrayList<>();
        }

        // 3. Apply filters if they exist
        if (profile.getSourceFilterGroup() != null) {
            sourceRows = applyFilter(sourceRows, sourceHeaders.stream().map(h -> (Object)h).collect(Collectors.toList()), profile.getSourceFilterGroup());
        }
        if (profile.getTargetFilterGroup() != null) {
            targetRows = applyFilter(targetRows, targetHeaders.stream().map(h -> (Object)h).collect(Collectors.toList()), profile.getTargetFilterGroup());
        }

        // 4. Perform row matching
        List<RowResult> rowResults = matchRows(sourceRows, targetRows, sourceHeaders.stream().map(h -> (Object)h).collect(Collectors.toList()), targetHeaders.stream().map(h -> (Object)h).collect(Collectors.toList()), profile);

        // 5. Create final report
        return new ComparisonResult(sourceHeaders, rowResults);
        } catch (Exception e) {
            // Wrap any other unexpected exception
            throw new ComparisonException("An unexpected error occurred during comparison: " + e.getMessage(), e);
        }
    }

    void validateProfile(ComparisonProfile profile) throws ComparisonException {
        if (profile == null) {
            throw new ComparisonException("Comparison profile is missing.");
        }
        if (profile.getSourceFilePath() == null || profile.getSourceFilePath().trim().isEmpty()) {
            throw new ComparisonException("Source file path is not set.");
        }
        if (profile.getTargetFilePath() == null || profile.getTargetFilePath().trim().isEmpty()) {
            throw new ComparisonException("Target file path is not set.");
        }
        if (profile.getSourceSheetName() == null || profile.getSourceSheetName().trim().isEmpty()) {
            throw new ComparisonException("Source sheet name is not set.");
        }
        if (profile.getTargetSheetName() == null || profile.getTargetSheetName().trim().isEmpty()) {
            throw new ComparisonException("Target sheet name is not set.");
        }
        if (profile.getKeyColumns() == null || profile.getKeyColumns().isEmpty()) {
            throw new ComparisonException("At least one key column must be selected for comparison.");
        }
        if (profile.getColumnMappings() == null) {
            throw new ComparisonException("Column mappings are not set.");
        }
        for (String keyColumn : profile.getKeyColumns()) {
            if (!profile.getColumnMappings().containsKey(keyColumn)) {
                throw new ComparisonException("Key column '" + keyColumn + "' is not present in the column mappings.");
            }
        }
    }

    List<RowResult> matchRows(List<List<Object>> sourceRows, List<List<Object>> targetRows, List<Object> sourceHeaders, List<Object> targetHeaders, ComparisonProfile profile) {
        List<RowResult> results = new ArrayList<>();

        if (profile.getRowMatchStrategy() == RowMatchStrategy.BY_PRIMARY_KEY) {
            Map<String, List<Object>> targetMap = buildKeyMap(targetRows, targetHeaders, profile.getColumnMappings(), profile.getKeyColumns());

            for (List<Object> sourceRow : sourceRows) {
                String key = buildKey(sourceRow, sourceHeaders, profile.getKeyColumns());
                if (targetMap.containsKey(key)) {
                    List<Object> targetRow = targetMap.get(key);
                    Map<Integer, CellDifference> diffs = compareRowCells(sourceRow, targetRow, sourceHeaders, targetHeaders, profile);

                    if (diffs.isEmpty()) {
                        results.add(new RowResult(RowComparisonStatus.MATCHED_IDENTICAL, key, sourceRow, targetRow, diffs));
                    } else {
                        results.add(new RowResult(RowComparisonStatus.MATCHED_MISMATCHED, key, sourceRow, targetRow, diffs));
                    }
                    targetMap.remove(key);
                } else {
                    results.add(new RowResult(RowComparisonStatus.SOURCE_ONLY, key, sourceRow, null, null));
                }
            }

            for (Map.Entry<String, List<Object>> entry : targetMap.entrySet()) {
                results.add(new RowResult(RowComparisonStatus.TARGET_ONLY, entry.getKey(), null, entry.getValue(), null));
            }
        } else {
            // TODO: Implement other matching strategies
        }
        return results;
    }

    private Map<Integer, CellDifference> compareRowCells(List<Object> sourceRow, List<Object> targetRow, List<Object> sourceHeaders, List<Object> targetHeaders, ComparisonProfile profile) {
        Map<Integer, CellDifference> differences = new HashMap<>();
        Map<String, String> mappings = profile.getColumnMappings();

        for (Map.Entry<String, String> mapping : mappings.entrySet()) {
            String sourceColName = mapping.getKey();

            // Don't compare key columns or ignored columns
            if (profile.getKeyColumns().contains(sourceColName) || profile.getIgnoredColumns().contains(sourceColName)) {
                continue;
            }

            int sourceColIndex = sourceHeaders.indexOf(sourceColName);
            int targetColIndex = targetHeaders.indexOf(mapping.getValue());

            if (sourceColIndex == -1 || targetColIndex == -1) continue;

            Object sourceVal = sourceColIndex < sourceRow.size() ? sourceRow.get(sourceColIndex) : "";
            Object targetVal = targetColIndex < targetRow.size() ? targetRow.get(targetColIndex) : "";

            boolean sourceIsBlank = (sourceVal == null || sourceVal.toString().trim().isEmpty());
            boolean targetIsBlank = (targetVal == null || targetVal.toString().trim().isEmpty());

            if (sourceIsBlank && targetIsBlank) continue;

            if (sourceIsBlank || targetIsBlank) {
                differences.put(sourceColIndex, new CellDifference(sourceColName, sourceVal, targetVal, MismatchType.BLANK_VS_NON_BLANK));
                continue;
            }

            boolean sourceIsNumeric = isNumeric(sourceVal.toString());
            boolean targetIsNumeric = isNumeric(targetVal.toString());

            if (sourceIsNumeric && targetIsNumeric) {
                double sourceNum = Double.parseDouble(sourceVal.toString());
                double targetNum = Double.parseDouble(targetVal.toString());
                if (Math.abs(sourceNum - targetNum) > 1e-9) { // Tolerance for float comparison
                    differences.put(sourceColIndex, new CellDifference(sourceColName, sourceVal, targetVal, MismatchType.NUMERIC));
                }
            } else if (sourceIsNumeric || targetIsNumeric) {
                differences.put(sourceColIndex, new CellDifference(sourceColName, sourceVal, targetVal, MismatchType.TYPE_MISMATCH));
            } else {
                String normSourceVal = normalize(sourceVal, profile);
                String normTargetVal = normalize(targetVal, profile);
                if (!normSourceVal.equals(normTargetVal)) {
                    differences.put(sourceColIndex, new CellDifference(sourceColName, sourceVal, targetVal, MismatchType.STRING));
                }
            }
        }
        return differences;
    }

    private static boolean isNumeric(String str) {
        if (str == null || str.trim().isEmpty()) {
            return false;
        }
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private String normalize(Object value, ComparisonProfile profile) {
        if (value == null) return "";
        String str = value.toString();
        if (profile.isTrimWhitespace()) {
            str = str.trim();
        }
        if (profile.isIgnoreCase()) {
            str = str.toLowerCase();
        }
        return str;
    }

    private String buildKey(List<Object> row, List<Object> headers, List<String> keyColumns) {
        List<String> keyParts = new ArrayList<>();
        for (String keyColumn : keyColumns) {
            int index = headers.indexOf(keyColumn);
            if (index != -1 && index < row.size()) {
                keyParts.add(Objects.toString(row.get(index), ""));
            }
        }
        return String.join("||", keyParts);
    }

    private Map<String, List<Object>> buildKeyMap(List<List<Object>> rows, List<Object> headers, Map<String, String> columnMappings, List<String> keyColumns) {
        Map<String, List<Object>> map = new HashMap<>();
        // Invert mapping to find target key columns
        Map<String, String> targetToSourceMapping = columnMappings.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        for (List<Object> row : rows) {
            List<String> keyParts = new ArrayList<>();
            for (String sourceKeyColumn : keyColumns) {
                String targetKeyColumn = targetToSourceMapping.get(sourceKeyColumn);
                if (targetKeyColumn != null) {
                    int index = headers.indexOf(targetKeyColumn);
                    if (index != -1 && index < row.size()) {
                        keyParts.add(Objects.toString(row.get(index), ""));
                    }
                }
            }
            map.put(String.join("||", keyParts), row);
        }
        return map;
    }

    private List<List<Object>> applyFilter(List<List<Object>> rows, List<Object> headers, FilterGroup filterGroup) {
        if (filterGroup == null || (filterGroup.getConditions().isEmpty() && filterGroup.getGroups().isEmpty())) {
            return rows;
        }
        return rows.stream()
                .filter(row -> doesRowMatch(row, headers, filterGroup))
                .collect(Collectors.toList());
    }

    private boolean doesRowMatch(List<Object> row, List<Object> headers, FilterGroup group) {
        // Evaluate conditions in the current group
        List<Boolean> conditionResults = group.getConditions().stream()
                .map(condition -> isConditionMet(row, headers, condition))
                .collect(Collectors.toList());

        // Evaluate subgroups recursively
        List<Boolean> groupResults = group.getGroups().stream()
                .map(subgroup -> doesRowMatch(row, headers, subgroup))
                .collect(Collectors.toList());

        List<Boolean> allResults = new ArrayList<>();
        allResults.addAll(conditionResults);
        allResults.addAll(groupResults);

        if (allResults.isEmpty()) {
            return true; // An empty filter group matches everything.
        }

        if (group.getOperator() == FilterGroup.LogicalOperator.AND) {
            return allResults.stream().allMatch(b -> b);
        } else { // OR
            return allResults.stream().anyMatch(b -> b);
        }
    }

    private boolean isConditionMet(List<Object> row, List<Object> headers, FilterCondition condition) {
        int colIndex = headers.indexOf(condition.getColumnName());
        if (colIndex == -1) return false; // Column not found

        Object cellValue = (colIndex < row.size()) ? row.get(colIndex) : null;

        String cellValueStr = (cellValue == null) ? null : cellValue.toString();
        String conditionValueStr = (condition.getValue() == null) ? null : condition.getValue().toString();

        if (condition.isCaseInsensitive() && cellValueStr != null && conditionValueStr != null) {
            cellValueStr = cellValueStr.toLowerCase();
            conditionValueStr = conditionValueStr.toLowerCase();
        }

        switch (condition.getOperator()) {
            case IS_NULL:
                return cellValueStr == null || cellValueStr.trim().isEmpty();
            case IS_NOT_NULL:
                return cellValueStr != null && !cellValueStr.trim().isEmpty();
            case EQUALS:
                return Objects.equals(cellValueStr, conditionValueStr);
            case NOT_EQUALS:
                return !Objects.equals(cellValueStr, conditionValueStr);
            case CONTAINS:
                return cellValueStr != null && conditionValueStr != null && cellValueStr.contains(conditionValueStr);
            case NOT_CONTAINS:
                return cellValueStr == null || conditionValueStr == null || !cellValueStr.contains(conditionValueStr);
            case STARTS_WITH:
                return cellValueStr != null && conditionValueStr != null && cellValueStr.startsWith(conditionValueStr);
            case ENDS_WITH:
                return cellValueStr != null && conditionValueStr != null && cellValueStr.endsWith(conditionValueStr);
            // Basic numeric comparisons
            case GREATER_THAN:
                try {
                    double cellNum = Double.parseDouble(cellValueStr);
                    double conditionNum = Double.parseDouble(conditionValueStr);
                    return cellNum > conditionNum;
                } catch (Exception e) { return false; }
            case LESS_THAN:
                try {
                    double cellNum = Double.parseDouble(cellValueStr);
                    double conditionNum = Double.parseDouble(conditionValueStr);
                    return cellNum < conditionNum;
                } catch (Exception e) { return false; }
            // TODO: Implement other operators like IN_LIST, REGEX, BETWEEN
            default:
                return false;
        }
    }
}
