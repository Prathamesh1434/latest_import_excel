package com.excelutility.core;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CanonicalNameBuilder {

    public static List<String> buildCanonicalHeaders(Sheet sheet, List<Integer> headerRowIndices, ConcatenationMode mode, String separator) {
        if (sheet == null || headerRowIndices == null || headerRowIndices.isEmpty()) {
            return new ArrayList<>();
        }

        int maxCols = 0;
        for (int rowIndex : headerRowIndices) {
            Row row = sheet.getRow(rowIndex);
            if (row != null) {
                maxCols = Math.max(maxCols, row.getLastCellNum());
            }
        }

        List<String> canonicalHeaders = new ArrayList<>();
        for (int i = 0; i < maxCols; i++) {
            List<String> headerParts = new ArrayList<>();
            String lastPart = "";
            for (int rowIndex : headerRowIndices) {
                String cellValue = getCellValue(sheet, rowIndex, i);
                if (cellValue != null && !cellValue.trim().isEmpty()) {
                    lastPart = cellValue.trim();
                }
                headerParts.add(lastPart);
            }

            if (headerParts.isEmpty() || String.join("", headerParts).trim().isEmpty()) {
                canonicalHeaders.add("Column " + (i + 1));
            } else if (mode == ConcatenationMode.LEAF_ONLY) {
                canonicalHeaders.add(headerParts.get(headerParts.size() - 1));
            } else { // BREADCRUMB
                canonicalHeaders.add(String.join(separator, headerParts));
            }
        }
        return canonicalHeaders;
    }

    private static String getCellValue(Sheet sheet, int rowIndex, int colIndex) {
        // First, check if the specified coordinate is part of a merged region.
        for (CellRangeAddress mergedRegion : sheet.getMergedRegions()) {
            if (mergedRegion.isInRange(rowIndex, colIndex)) {
                // If it is, the value is in the top-left cell of the region.
                Row firstRow = sheet.getRow(mergedRegion.getFirstRow());
                if (firstRow != null) {
                    Cell firstCell = firstRow.getCell(mergedRegion.getFirstColumn());
                    if (firstCell != null) {
                        return firstCell.toString();
                    }
                }
            }
        }

        // If not in a merged region, get the cell directly.
        Row row = sheet.getRow(rowIndex);
        if (row != null) {
            Cell cell = row.getCell(colIndex);
            if (cell != null) {
                return cell.toString();
            }
        }

        return null;
    }
}
