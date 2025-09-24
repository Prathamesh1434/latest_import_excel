package com.excelutility.core;

/**
 * Custom exception for handling errors during the comparison process.
 * This allows for more specific error handling and user-friendly messages.
 */
public class ComparisonException extends Exception {

    public ComparisonException(String message) {
        super(message);
    }

    public ComparisonException(String message, Throwable cause) {
        super(message, cause);
    }
}
