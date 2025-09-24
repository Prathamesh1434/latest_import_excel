# Manual QA Script for Filter Logic Builder

This script outlines the manual steps to verify the functionality of the revamped Filter Logic Builder UI.

## Prerequisites
- The application is running.
- The "Filter Excel Data" mode is selected.
- A data file and a filter values file are loaded (e.g., the sample files provided with the project).

## Test Steps

1.  **Verify Initial Layout:**
    *   **Expected:** The UI shows "Data Preview" on the left, "Filter Values Preview" in the center, and "Filter Logic Builder" on the right. The "Unified Data View" tabbed pane is below the two preview panels. The "Actions" panel with buttons is in the bottom right.

2.  **Create First Rule:**
    *   In the "Filter Values Preview" table, double-click a cell (e.g., a cell with value 'Nanded' in the 'City' column).
    *   In the dialogs that appear, select the target column to apply the filter to and confirm.
    *   **Expected:**
        *   A new group "Group 1" appears in the Filter Logic Builder.
        *   Inside Group 1, a new rule "Rule 1" is created.
        *   The "Unified" tab immediately updates to show the filtered results (rows where City is 'Nanded').
        *   The "Total Matches" label in the Actions panel updates to the correct count.
        *   A log message appears in the console: `FilterCreate: ...`
        *   A log message appears in the console: `FilterApply: ...`

3.  **Add Second Rule to Same Group:**
    *   In the "Filter Values Preview", double-click another cell to create a second rule.
    *   When prompted for the target, ensure it is added to the existing "Group 1". (Note: current implementation adds to the root group, which is the default behavior. A more advanced implementation might ask.)
    *   **Expected:**
        *   A new "Rule 2" appears in "Group 1".
        *   An "AND / OR" radio button selector appears between Rule 1 and Rule 2.
        *   The "Unified" tab and "Total Matches" count update automatically based on the default "AND" selection.

4.  **Test Rule Preview:**
    *   Click the "Preview" button next to "Rule 1".
    *   **Expected:**
        *   The record count badge next to Rule 1 updates with the correct count and color (green for >0, red for 0).
        *   A new tab appears in the "Unified Data View" area with the title "Rule 1 Preview", showing only the results for that single rule.
        *   The tab is closable using an 'x' button.

5.  **Create Second Group:**
    *   In the "Actions" panel, click the "Add Group" button.
    *   **Expected:**
        *   A new "Group 2" panel appears in the Filter Logic Builder.
        *   An "AND / OR" selector appears between Group 1 and Group 2.
    *   Add a rule to Group 2 by double-clicking a cell in the "Filter Values Preview".
    *   **Expected:**
        *   A new rule appears in Group 2.
        *   The "Unified" tab and "Total Matches" count update based on the combined logic of Group 1 and Group 2.

6.  **Verify Editable Names:**
    *   Click on the name "Group 1" in its header.
    *   **Expected:** The text field is editable. Change the name to "Location Filters" and press Enter. The name should be updated.
    *   Do the same for one of the rules.
    *   **Expected:** The rule name should be editable and update correctly.
