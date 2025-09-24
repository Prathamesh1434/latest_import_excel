# Excel Utility

A production-ready, GUI-only Java Swing desktop application for comparing Excel files and filtering Excel data. This tool provides a comprehensive set of features for normalization, key mapping, detailed comparison, interactive filtering, and reporting, all configurable through an intuitive user interface.

## Application Structure

The application now launches into a **Mode Selection** screen. From here, you can choose your desired workflow. A "File" menu is always available, allowing you to go **Back to Mode Selection** at any time to switch workflows without restarting the application.

## Modes of Operation

### 1. Compare Excel Files (Classic Mode)

This mode allows you to perform a detailed comparison of two Excel files.

**Key Features**:

-   **Advanced Key Column Selection**: Interactively map columns and select a composite key.
-   **Header Normalization**: Supports multi-row and merged-cell headers.
-   **Profile Management**: Save and load complex comparison configurations.
-   **Large File Support**: Includes a streaming mode to handle large `.xlsx` files efficiently.

### 2. Spec QA Recon (New)

This mode allows you to filter one Excel file based on a list of values or entire columns from another Excel file.

**Key Features**:

-   **Advanced Header Detection**: Both the data file and the filter-values file support single-row, multi-row, and merged-cell headers, ensuring accurate data parsing.
-   **Interactive Filter Creation**: Load a "Data File" and a "Filter Values File". Simply double-click any cell (or select multiple cells) in the "Filter Values" preview table to create a filter.
-   **Flexible Filter Sources**: A dialog will ask if you want to filter by the specific **cell value** you clicked, or by all values in that cell's **column**.
-   **Targeted Filtering**: After choosing your filter source, another dialog lets you apply the filter to one or more columns in your Data File. You can also choose to **trim whitespace** from the target column for more robust matching.
-   **Immediate Record Counting**: As soon as a filter is created, the application runs a background check and displays the number of matching records. The count is color-coded: **green for > 0** records, **red for 0**.
-   **Enhanced Previews**: Preview tables now have a grid-like appearance and support column sorting.
-   **Sheet Search**: A new search bar allows you to dynamically filter the list of sheets in a workbook.
-   **Empty Cell Filtering**: The tool now correctly handles filtering by empty or null cells.
-   **Configurable Export**:
    -   Download all filtered results into a **single Excel file** with one sheet per filter, or as **separate files**.
    -   If a filter yields no results, an export file is still created with a "No rows matched" message.
    -   Choose a custom **highlight color** for the filtered rows in the exported files.

## Technical Stack

-   **Language**: Java 11
-   **UI**: Java Swing with MigLayout
-   **Excel Handling**: Apache POI (including streaming APIs)
-   **Serialization**: Jackson (for profiles)
-   **Logging**: SLF4J + Logback
-   **Build Tool**: Maven with the `maven-shade-plugin`

## How to Build and Run

### Build

To compile and package the application into a single runnable "fat JAR", run:
```sh
mvn clean package
```
This creates `excel-utility-1.0.0-all.jar` in the `target` directory.

### Run

Execute the generated JAR file:
```sh
java -jar target/excel-utility-1.0.0-all.jar
```
You will be prompted to choose a mode upon startup.

### Run Tests

To run the full suite of unit tests:
```sh
mvn test
```

## Developer Notes

-   **Project Structure**: The project is organized into `gui`, `core`, `io`, `excel`, and `test` packages. The main UI is managed by `AppContainer.java`, which uses a `CardLayout` to switch between `ModeSelectionPanel`, `ComparePanel`, and `FilterPanel`.
-   **Profiles**: Comparison profiles are saved as `.json` files in a `profiles/` directory created where the application is run.
-   **Test Cases**: Generated test cases are saved to the `target/test-cases/` directory.

## Troubleshooting

If a comparison fails, the application will no longer crash. Instead, it will display a "Comparison Failed" dialog with a user-friendly error message.

If you need to report a bug, please include the full details from this dialog:
1.  Click the **"Details"** button to expand the dialog and show the full technical error message and stack trace.
2.  Click the **"Copy to Clipboard"** button.
3.  Paste the copied details into your bug report. This provides crucial information for debugging the issue.
