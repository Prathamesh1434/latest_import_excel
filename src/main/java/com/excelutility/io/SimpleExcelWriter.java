package com.excelutility.io;

import com.excelutility.core.CellDifference;
import com.excelutility.core.ComparisonResult;
import com.excelutility.core.MismatchType;
import com.excelutility.core.RowComparisonStatus;
import com.excelutility.core.RowResult;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleExcelWriter {

    public static void write(List<List<Object>> data, String sheetName, String filePath) throws IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet(sheetName);
            int rowNum = 0;
            for (List<Object> rowData : data) {
                Row row = sheet.createRow(rowNum++);
                int colNum = 0;
                for (Object field : rowData) {
                    Cell cell = row.createCell(colNum++);
                    if (field instanceof String) {
                        cell.setCellValue((String) field);
                    } else if (field instanceof Integer) {
                        cell.setCellValue((Integer) field);
                    } else if (field instanceof Double) {
                        cell.setCellValue((Double) field);
                    } else {
                        cell.setCellValue(field != null ? field.toString() : "");
                    }
                }
            }
            try (FileOutputStream outputStream = new FileOutputStream(filePath)) {
                workbook.write(outputStream);
            }
        }
    }

    public static void writeComparisonResult(ComparisonResult result, String filePath) throws IOException {
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Map<Object, CellStyle> styleCache = createStyles(workbook);

            // Create the two sheets
            writeRecordsSheet(workbook, result, styleCache);
            writeSummarySheet(workbook, result.getStats());

            // Write the workbook to the file
            try (FileOutputStream outputStream = new FileOutputStream(filePath)) {
                workbook.write(outputStream);
            }
        }
    }

    private static void writeRecordsSheet(XSSFWorkbook workbook, ComparisonResult result, Map<Object, CellStyle> styleCache) {
        Sheet sheet = workbook.createSheet("Records");

        // Header
        Row headerRow = sheet.createRow(0);
        List<String> headers = result.getFinalHeaders();
        CellStyle headerStyle = workbook.createCellStyle();
        XSSFFont font = workbook.createFont();
        font.setBold(true);
        headerStyle.setFont(font);

        for (int i = 0; i < headers.size(); i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers.get(i));
            cell.setCellStyle(headerStyle);
        }

        // Data Rows
        int rowNum = 1;
        for (RowResult rowResult : result.getRowResults()) {
            Row row = sheet.createRow(rowNum++);
            List<Object> data = rowResult.getStatus() == RowComparisonStatus.TARGET_ONLY
                                ? rowResult.getTargetRowData()
                                : rowResult.getSourceRowData();

            if (data == null) continue;

            CellStyle baseRowStyle = styleCache.get(rowResult.getStatus());

            for (int i = 0; i < data.size(); i++) {
                Cell cell = row.createCell(i);
                Object value = data.get(i);
                cell.setCellValue(value != null ? value.toString() : "");

                CellStyle finalCellStyle = baseRowStyle;
                if (rowResult.getStatus() == RowComparisonStatus.MATCHED_MISMATCHED) {
                    CellDifference diff = rowResult.getDifferences().get(i);
                    if (diff != null && styleCache.containsKey(diff.getMismatchType())) {
                        finalCellStyle = styleCache.get(diff.getMismatchType());
                    }
                }

                if (finalCellStyle != null) {
                    cell.setCellStyle(finalCellStyle);
                }
            }
        }

        for (int i = 0; i < headers.size(); i++) {
            sheet.autoSizeColumn(i);
        }
    }

    private static void writeSummarySheet(XSSFWorkbook workbook, ComparisonResult.ComparisonStats stats) {
        Sheet sheet = workbook.createSheet("Summary");

        XSSFFont boldFont = workbook.createFont();
        boldFont.setBold(true);
        CellStyle labelStyle = workbook.createCellStyle();
        labelStyle.setFont(boldFont);

        int rowNum = 0;
        createSummaryRow(sheet, rowNum++, "Total Rows in Source:", stats.totalSourceRows, labelStyle);
        createSummaryRow(sheet, rowNum++, "Total Rows in Target:", stats.totalTargetRows, labelStyle);
        rowNum++;
        createSummaryRow(sheet, rowNum++, "Identical Rows:", stats.matchedIdentical, labelStyle);
        createSummaryRow(sheet, rowNum++, "Mismatched Rows:", stats.matchedMismatched, labelStyle);
        createSummaryRow(sheet, rowNum++, "Extra Rows in Source:", stats.sourceOnly, labelStyle);
        createSummaryRow(sheet, rowNum++, "Extra Rows in Target:", stats.targetOnly, labelStyle);

        sheet.autoSizeColumn(0);
        sheet.autoSizeColumn(1);
    }

    private static void createSummaryRow(Sheet sheet, int rowNum, String label, long value, CellStyle labelStyle) {
        Row row = sheet.createRow(rowNum);
        Cell labelCell = row.createCell(0);
        labelCell.setCellValue(label);
        labelCell.setCellStyle(labelStyle);

        Cell valueCell = row.createCell(1);
        valueCell.setCellValue(value);
    }

    private static Map<Object, CellStyle> createStyles(XSSFWorkbook workbook) {
        Map<Object, CellStyle> styles = new HashMap<>();
        styles.put(MismatchType.NUMERIC, createStyleWithColor(workbook, new java.awt.Color(255, 255, 153)));
        styles.put(MismatchType.STRING, createStyleWithColor(workbook, new java.awt.Color(255, 179, 179)));
        styles.put(MismatchType.BLANK_VS_NON_BLANK, createStyleWithColor(workbook, new java.awt.Color(255, 204, 153)));
        styles.put(MismatchType.TYPE_MISMATCH, createStyleWithColor(workbook, new java.awt.Color(221, 179, 255)));
        styles.put(RowComparisonStatus.MATCHED_MISMATCHED, createStyleWithColor(workbook, new java.awt.Color(255, 255, 230)));
        styles.put(RowComparisonStatus.SOURCE_ONLY, createStyleWithColor(workbook, new java.awt.Color(230, 255, 230)));
        styles.put(RowComparisonStatus.TARGET_ONLY, createStyleWithColor(workbook, new java.awt.Color(255, 230, 230)));
        return styles;
    }

    private static XSSFCellStyle createStyleWithColor(XSSFWorkbook workbook, java.awt.Color awtColor) {
        XSSFCellStyle style = workbook.createCellStyle();
        style.setFillForegroundColor(new XSSFColor(awtColor, null));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        return style;
    }

    private static String sanitizeCellValue(Object value) {
        if (value == null) {
            return "";
        }
        String s = value.toString();
        // POI's streaming XML writer will escape characters for us, but we should remove illegal control characters.
        // Allowed characters are tab, newline, and carriage return. All others below 0x20 are illegal.
        return s.replaceAll("[\\p{Cntrl}&&[^\t\n\r]]", "");
    }

    public static String sanitizeSheetName(String name) {
        String sanitized = name.replaceAll("[\\\\/*?\\[\\]:]", "_");
        if (sanitized.length() > 31) {
            sanitized = sanitized.substring(0, 31);
        }
        return sanitized;
    }

    public static void writeFilteredResults(String baseFilePath, Map<String, List<List<Object>>> filteredData, boolean mergeInOneFile, java.awt.Color rowColor) throws IOException {
        // This method now always merges into one file as per the new requirements.
        // The boolean `mergeInOneFile` is kept for signature compatibility but is effectively ignored.

        try (SXSSFWorkbook workbook = new SXSSFWorkbook(100)) { // keep 100 rows in memory
            java.util.Set<List<Object>> unifiedDataRows = new java.util.LinkedHashSet<>();
            List<Object> header = null;

            // First, populate all the group-specific sheets and collect unified data
            for (Map.Entry<String, List<List<Object>>> entry : filteredData.entrySet()) {
                List<List<Object>> sheetData = entry.getValue();
                if (!sheetData.isEmpty()) {
                    if (header == null) {
                        header = sheetData.get(0); // Capture header from the first available sheet
                    }
                    // Add data rows (skip header) to the unified set
                    for (int i = 1; i < sheetData.size(); i++) {
                        unifiedDataRows.add(sheetData.get(i));
                    }
                }
                // Write the individual group sheet
                writeSheet(workbook, sanitizeSheetName(entry.getKey()), sheetData, rowColor);
            }

            // Now, create the "Unified" sheet
            if (header != null) {
                List<List<Object>> unifiedSheetData = new java.util.ArrayList<>();
                unifiedSheetData.add(header);
                unifiedSheetData.addAll(unifiedDataRows);
                writeSheet(workbook, "Unified", unifiedSheetData, null); // No color for unified sheet
            }

            // Write the complete workbook to a file
            try (FileOutputStream outputStream = new FileOutputStream(baseFilePath)) {
                workbook.write(outputStream);
            } catch (Exception e) {
                throw new IOException("Failed to write workbook to file: " + e.getMessage(), e);
            } finally {
                workbook.dispose(); // Important for SXSSFWorkbook
            }
        }
    }

    private static void writeSheet(SXSSFWorkbook workbook, String sheetName, List<List<Object>> data, java.awt.Color highlightColor) {
        Sheet sheet = workbook.createSheet(sheetName);
        if (sheet instanceof org.apache.poi.xssf.streaming.SXSSFSheet) {
            ((org.apache.poi.xssf.streaming.SXSSFSheet) sheet).trackAllColumnsForAutoSizing();
        }

        // Handle empty data case cleanly
        if (data == null || data.isEmpty()) {
            sheet.createRow(0).createCell(0).setCellValue("No data for this sheet.");
            return;
        }

        CellStyle highlightStyle = null;
        if (highlightColor != null) {
            // All workbooks in this context are SXSSF, so this is safe.
            XSSFWorkbook xssfWorkbook = workbook.getXSSFWorkbook();
            highlightStyle = createStyleWithColor(xssfWorkbook, highlightColor);
        }

        // Write header
        Row headerRow = sheet.createRow(0);
        List<Object> headerData = data.get(0);
        for (int i = 0; i < headerData.size(); i++) {
            Cell cell = headerRow.createCell(i);
            setCellValue(cell, headerData.get(i));
        }

        // Write data rows
        if (data.size() > 1) {
            for (int i = 1; i < data.size(); i++) {
                Row row = sheet.createRow(i);
                List<Object> rowData = data.get(i);
                for (int j = 0; j < rowData.size(); j++) {
                    Cell cell = row.createCell(j);
                    Object value = rowData.get(j);
                    // Use a helper to set cell value by type
                    setCellValue(cell, value);
                    if (highlightStyle != null) {
                        cell.setCellStyle(highlightStyle);
                    }
                }
            }
        } else {
            // If there's only a header, it means no data rows matched.
            sheet.createRow(1).createCell(0).setCellValue("No rows matched the filter criteria.");
        }

        // Autosize columns, but only if there is a header
        if (!headerData.isEmpty()) {
            for (int i = 0; i < headerData.size(); i++) {
                sheet.autoSizeColumn(i);
            }
        }
    }

    private static void setCellValue(Cell cell, Object value) {
        if (value == null) {
            cell.setCellValue("");
            return;
        }
        if (value instanceof String) {
            cell.setCellValue(sanitizeCellValue(value));
        } else if (value instanceof Integer) {
            cell.setCellValue((Integer) value);
        } else if (value instanceof Long) {
            cell.setCellValue((Long) value);
        } else if (value instanceof Double) {
            cell.setCellValue((Double) value);
        } else if (value instanceof java.util.Date) {
            cell.setCellValue((java.util.Date) value);
        } else if (value instanceof Boolean) {
            cell.setCellValue((Boolean) value);
        } else {
            cell.setCellValue(sanitizeCellValue(value.toString()));
        }
    }
}
