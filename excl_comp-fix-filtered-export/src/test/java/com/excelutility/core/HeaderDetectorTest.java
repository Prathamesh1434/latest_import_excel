package com.excelutility.core;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class HeaderDetectorTest {

    private String testFilePath = "target/test-files/header-test.xlsx";

    @BeforeEach
    void setUp() throws IOException {
        new File(testFilePath).getParentFile().mkdirs();
        createTestFile();
    }

    @AfterEach
    void tearDown() {
        new File(testFilePath).delete();
    }

    private void createTestFile() throws IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("TestSheet");

            // --- Create Styles ---
            CellStyle headerStyle = workbook.createCellStyle();
            Font font = workbook.createFont();
            font.setBold(true);
            headerStyle.setFont(font);
            headerStyle.setAlignment(HorizontalAlignment.CENTER);

            // --- Header Row 0 (Merged) ---
            Row row0 = sheet.createRow(0);
            Cell cell0_0 = row0.createCell(0);
            cell0_0.setCellValue("Main Report Banner");
            cell0_0.setCellStyle(headerStyle);
            sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 3));

            // --- Header Row 1 (Group Headers) ---
            Row row1 = sheet.createRow(1);
            Cell cell1_0 = row1.createCell(0);
            cell1_0.setCellValue("Group A");
            cell1_0.setCellStyle(headerStyle);
            sheet.addMergedRegion(new CellRangeAddress(1, 1, 0, 1));

            Cell cell1_2 = row1.createCell(2);
            cell1_2.setCellValue("Group B");
            cell1_2.setCellStyle(headerStyle);
            sheet.addMergedRegion(new CellRangeAddress(1, 1, 2, 3));

            // --- Header Row 2 (Leaf Headers) ---
            Row row2 = sheet.createRow(2);
            row2.createCell(0).setCellValue("ID");
            row2.createCell(1).setCellValue("Name");
            row2.createCell(2).setCellValue("Value");
            row2.createCell(3).setCellValue("Date");

            // --- Data Row 3 ---
            Row row3 = sheet.createRow(3);
            row3.createCell(0).setCellValue(1);
            row3.createCell(1).setCellValue("Test1");
            row3.createCell(2).setCellValue(100.5);

            // --- Data Row 4 ---
            Row row4 = sheet.createRow(4);
            row4.createCell(0).setCellValue(2);
            row4.createCell(1).setCellValue("Test2");
            row4.createCell(2).setCellValue(200.0);

            try (FileOutputStream fos = new FileOutputStream(testFilePath)) {
                workbook.write(fos);
            }
        }
    }

    @Test
    void testDetectHeader() throws IOException {
        try (Workbook workbook = WorkbookFactory.create(new File(testFilePath))) {
            Sheet sheet = workbook.getSheet("TestSheet");
            HeaderDetector detector = new HeaderDetector();
            HeaderDetector.HeaderDetectionResult result = detector.detectHeader(sheet);

            assertNotNull(result);
            List<Integer> detectedRows = result.getDetectedHeaderRows();

            // The algorithm should strongly prefer the first 3 rows
            assertEquals(3, detectedRows.size());
            assertTrue(detectedRows.contains(0));
            assertTrue(detectedRows.contains(1));
            assertTrue(detectedRows.contains(2));
        }
    }
}
