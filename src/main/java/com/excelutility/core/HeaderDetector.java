package com.excelutility.core;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * A service to automatically detect header rows from an Excel sheet based on heuristics.
 */
public class HeaderDetector {

    private static final int DEFAULT_ROWS_TO_SCAN = 20;
    private static final int DEFAULT_HEADER_CANDIDATE_COUNT = 3;

    public static class HeaderDetectionResult {
        private final List<Integer> detectedHeaderRows;
        private final List<RowConfidence> confidenceScores;

        public HeaderDetectionResult(List<Integer> detectedHeaderRows, List<RowConfidence> confidenceScores) {
            this.detectedHeaderRows = detectedHeaderRows;
            this.confidenceScores = confidenceScores;
        }

        public List<Integer> getDetectedHeaderRows() {
            return detectedHeaderRows;
        }

        public List<RowConfidence> getConfidenceScores() {
            return confidenceScores;
        }
    }

    public static class RowConfidence {
        private final int rowIndex;
        private final double score;
        private final String reason;

        public RowConfidence(int rowIndex, double score, String reason) {
            this.rowIndex = rowIndex;
            this.score = score;
            this.reason = reason;
        }

        public int getRowIndex() { return rowIndex; }
        public double getScore() { return score; }
        public String getReason() { return reason; }
    }

    public HeaderDetectionResult detectHeader(Sheet sheet) {
        if (sheet == null) {
            return new HeaderDetectionResult(new ArrayList<>(), new ArrayList<>());
        }

        List<RowConfidence> scores = new ArrayList<>();
        int rowsToScan = Math.min(DEFAULT_ROWS_TO_SCAN, sheet.getLastRowNum() + 1);

        for (int i = 0; i < rowsToScan; i++) {
            Row row = sheet.getRow(i);
            if (row == null) {
                scores.add(new RowConfidence(i, 0, "Empty row"));
                continue;
            }
            scores.add(calculateRowConfidence(row));
        }

        // Simple logic for now: pick the top N contiguous rows with the highest scores
        // A more advanced implementation would find the best "block" of rows.
        scores.sort((a, b) -> Double.compare(b.getScore(), a.getScore()));

        List<Integer> bestHeaderRows = new ArrayList<>();
        for (int i = 0; i < Math.min(DEFAULT_HEADER_CANDIDATE_COUNT, scores.size()); i++) {
            if (scores.get(i).getScore() > 0.1) { // Basic threshold
                 bestHeaderRows.add(scores.get(i).getRowIndex());
            }
        }
        bestHeaderRows.sort(Integer::compareTo);

        return new HeaderDetectionResult(bestHeaderRows, scores);
    }

    private RowConfidence calculateRowConfidence(Row row) {
        int cellCount = row.getLastCellNum();
        if (cellCount <= 0) {
            return new RowConfidence(row.getRowNum(), 0, "Empty row");
        }

        int boldCount = 0;
        int stringCount = 0;
        int numericCount = 0;
        int mergedCount = 0;

        // Check for merged regions in this row
        for (CellRangeAddress mergedRegion : row.getSheet().getMergedRegions()) {
            if (mergedRegion.getFirstRow() == row.getRowNum()) {
                mergedCount++;
            }
        }

        for (Cell cell : row) {
            if (cell.getCellStyle().getFontIndex() > 0) {
                Font font = row.getSheet().getWorkbook().getFontAt(cell.getCellStyle().getFontIndex());
                if (font.getBold()) {
                    boldCount++;
                }
            }
            if (cell.getCellType() == CellType.STRING && !cell.getStringCellValue().trim().isEmpty()) {
                stringCount++;
            }
            if (cell.getCellType() == CellType.NUMERIC) {
                numericCount++;
            }
        }

        double boldRatio = (double) boldCount / cellCount;
        double stringRatio = (double) stringCount / cellCount;
        double numericRatio = (double) numericCount / cellCount;
        double mergedRatio = (double) mergedCount / cellCount;

        // Simple weighted score. Merged regions and bold text are strong indicators.
        double score = (boldRatio * 0.4) + (stringRatio * 0.2) - (numericRatio * 0.5) + (mergedRatio * 0.5);
        String reason = String.format("Bold: %.2f, Text: %.2f, Numeric: %.2f, Merged: %.2f", boldRatio, stringRatio, numericRatio, mergedRatio);

        return new RowConfidence(row.getRowNum(), score, reason);
    }
}
