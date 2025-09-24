# Excel Utility - User Manual

Welcome to the Excel Utility! This guide will walk you through the features of the application.

## Getting Started: Launching the Application

To start the application, you will need to have Java installed on your computer. You can download Java from the official website if you don't have it. This application is compatible with Java 8 and all newer versions.

Once Java is installed, you can launch the application by simply **double-clicking** on the `excel-utility-1.0.0-all.jar` file.

### Troubleshooting

If the application does not open after double-clicking the JAR file, please try the following:

1.  **Verify Java Installation:** Open a terminal or command prompt and type `java -version`. If you see a message with your Java version, then Java is installed correctly. If not, you will need to install Java.
2.  **Run from Command Line:** If Java is installed but the double-click doesn't work, you can try running the application from the command line. Open a terminal or command prompt, navigate to the directory where the JAR file is located, and run the following command:
    ```
    java -jar excel-utility-1.0.0-all.jar
    ```
    This may show you an error message that can help diagnose the problem.

## 1. Choosing a Mode

When you first start the application, you will be asked to choose a mode. You can switch between modes at any time by using the "File" > "Back to Mode Selection" menu item.

-   **Compare Excel Files**: Use this mode to perform a detailed, cell-by-cell comparison of two Excel files.
-   **Spec QA Recon**: Use this mode to filter a main data file based on values from a second file using a powerful logic builder.

---

## 2. Compare Excel Files Mode

This mode allows you to perform a detailed comparison of two Excel files.

### Basic Workflow

1.  **Load Files**: In the "Source File" and "Target File" panels, click the **"Browse..."** button to select the two Excel files you want to compare.
2.  **Select Sheets**: Once a file is loaded, the "Sheet" dropdown in its panel will be populated. Select the sheet you want to compare from each file.
3.  **(Recommended) Detect Headers**: In each file panel, click the **"Detect Header"** button. This will open a dialog that automatically suggests which rows are part of the header, which is crucial for complex, multi-row headers.
4.  **Configure Mappings**: Go to the "Column Mappings" panel to map columns and select at least one **"Is Key"** column to define how rows should be matched.
5.  **Run Comparison**: Click **"Tools" > "Run Comparison"** from the menu bar.
6.  **View Results**: The results will appear in the results table and the summary panel.
7.  **Export Report**: Go to **"File" > "Export Results..."** to save the results.

*(For more details on advanced features like Profile Management and Test Case Generation, please refer to the README file.)*

---

## 3. Spec QA Recon Mode

This mode allows you to filter one Excel file (the "Data File") using values from another file (the "Filter Values File"). It features a powerful logic builder that allows you to create complex, nested `AND`/`OR` conditions.

### Basic Workflow

1.  **Load Files**:
    -   In the **"Data File"** panel, click "Browse..." to select the main Excel file you want to filter.
    -   In the **"Filter Values File"** panel, click "Browse..." to select the Excel file that contains the values you want to use for filtering.

2.  **Select and Preview Sheets**:
    -   For each file, select the correct sheet from the dropdown.
    -   Click the **"Load & Preview Files"** button to load the data into the preview tables.
    -   Use the **"Preview Search"** box to instantly filter the rows in both preview tables as you type.

3.  **Define Header Rows (If Necessary)**:
    -   If your files have complex headers, click the **"Detect Header"** button for each file to correctly identify the header rows before creating filters.

4.  **Build Your Filter Logic**:
    -   The **"Filter Logic Builder"** is where you will construct your filter. It starts with a single "root" group.
    -   **Adding a Rule**: In the "Filter Values Preview" table, find a cell you want to use and **double-click** it. This will add a new filter rule to the root group. You can also use the **"Add Rule"** button inside any group.
    -   **Adding a Group**: Click the **"Add Group"** button inside any existing group to create a nested group for more complex logic.
    -   **Setting Logic**: Each group has its own **AND / OR dropdown**. Use this to control the logic for all the items directly inside that group.
    -   **Per-Rule Actions**: Each rule has three buttons:
        -   **View**: See the results for *only this rule* in a new window.
        -   **Download**: Export the results for *only this rule* to an Excel file.
        -   **X**: Delete the rule.

5.  **Analyze and Export Results**:
    -   **Calculate Total**: Click this to see the final number of records that match your complete logical expression.
    -   **View Overall Result**: Click this to see the full, combined results in a new window.
    -   **Download Filtered Results**: Click this to save the final, combined results to an Excel file.
    -   **Column Selection**: Before any View or Download action, a dialog will appear, allowing you to select which columns you want to include in the output.

### Understanding the Filter Logic Builder

The builder lets you create a tree of filter conditions.

*   **Groups**: A group is a container for other items (rules or other groups). It has a dropdown to set its logic to `AND` or `OR`, and buttons to add new rules or subgroups.
*   **Rules**: A rule is a single filter condition with its own View, Download, and Delete buttons.

**Example:** To create the filter `(Status is 'Active' AND City is 'Chicago') OR (Name is 'Bob')`, you would:
1.  In the root group, set the logic to `OR`.
2.  Click "Add Group" to create a new nested group.
3.  Inside this new group, set the logic to `AND`.
4.  Click "Add Rule" inside the `AND` group twice to create the "Status" and "City" rules.
5.  Go back to the root `OR` group and click "Add Rule" to create the "Name" rule.

[Image of the Filter Logic Builder with new buttons]

---
*Thank you for using Excel Utility!*
