package com.excelutility.core;

import java.util.List;
import java.util.Map;

/**
 * A data class that encapsulates the entire result of a file comparison.
 */
public class ComparisonResult {
    private final List<String> finalHeaders;
    private final List<RowResult> rowResults;
    private final ComparisonStats stats;

    public ComparisonResult(List<String> finalHeaders, List<RowResult> rowResults) {
        this.finalHeaders = finalHeaders;
        this.rowResults = rowResults;
        this.stats = new ComparisonStats(rowResults);
    }

    // Getters
    public List<String> getFinalHeaders() { return finalHeaders; }
    public List<RowResult> getRowResults() { return rowResults; }
    public ComparisonStats getStats() { return stats; }


    /**
     * A nested class to hold summary statistics of the comparison.
     */
    public static class ComparisonStats {
        public final long totalSourceRows;
        public final long totalTargetRows;
        public final long matchedIdentical;
        public final long matchedMismatched;
        public final long sourceOnly;
        public final long targetOnly;
        public final long duplicates;

        public ComparisonStats(List<RowResult> results) {
            this.matchedIdentical = results.stream().filter(r -> r.getStatus() == RowComparisonStatus.MATCHED_IDENTICAL).count();
            this.matchedMismatched = results.stream().filter(r -> r.getStatus() == RowComparisonStatus.MATCHED_MISMATCHED).count();
            this.sourceOnly = results.stream().filter(r -> r.getStatus() == RowComparisonStatus.SOURCE_ONLY).count();
            this.targetOnly = results.stream().filter(r -> r.getStatus() == RowComparisonStatus.TARGET_ONLY).count();
            this.duplicates = results.stream().filter(r -> r.getStatus() == RowComparisonStatus.DUPLICATE).count();

            this.totalSourceRows = matchedIdentical + matchedMismatched + sourceOnly;
            this.totalTargetRows = matchedIdentical + matchedMismatched + targetOnly;
        }
    }
}
