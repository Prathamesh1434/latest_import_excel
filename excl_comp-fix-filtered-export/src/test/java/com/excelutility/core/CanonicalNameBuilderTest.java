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

public class CanonicalNameBuilderTest {

    private String testFilePath = "target/test-files/canonical-name-test.xlsx";

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
            CellStyle headerStyle = workbook.createCellStyle();
            Font font = workbook.createFont();
            font.setBold(true);
            headerStyle.setFont(font);

            Row row0 = sheet.createRow(0);
            Cell cell0_0 = row0.createCell(0);
            cell0_0.setCellValue("Report");
            cell0_0.setCellStyle(headerStyle);
            sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 3));

            Row row1 = sheet.createRow(1);
            Cell cell1_0 = row1.createCell(0);
            cell1_0.setCellValue("Group A");
            cell1_0.setCellStyle(headerStyle);
            sheet.addMergedRegion(new CellRangeAddress(1, 1, 0, 1));

            Cell cell1_2 = row1.createCell(2);
            cell1_2.setCellValue("Group B");
            cell1_2.setCellStyle(headerStyle);
            sheet.addMergedRegion(new CellRangeAddress(1, 1, 2, 3));

            Row row2 = sheet.createRow(2);
            row2.createCell(0).setCellValue("ID");
            row2.createCell(1).setCellValue("Name");
            row2.createCell(2).setCellValue("Value");
            row2.createCell(3).setCellValue("Date");

            try (FileOutputStream fos = new FileOutputStream(testFilePath)) {
                workbook.write(fos);
            }
        }
    }

    @Test
    void testBuildCanonicalHeaders_BreadcrumbMode() throws IOException {
        try (Workbook workbook = WorkbookFactory.create(new File(testFilePath))) {
            Sheet sheet = workbook.getSheet("TestSheet");
            List<Integer> headerRows = List.of(0, 1, 2);

            List<String> canonicalHeaders = CanonicalNameBuilder.buildCanonicalHeaders(sheet, headerRows, ConcatenationMode.BREADCRUMB, " | ");

            assertEquals(4, canonicalHeaders.size());
            assertEquals("Report | Group A | ID", canonicalHeaders.get(0));
            assertEquals("Report | Group A | Name", canonicalHeaders.get(1));
            assertEquals("Report | Group B | Value", canonicalHeaders.get(2));
            assertEquals("Report | Group B | Date", canonicalHeaders.get(3));
        }
    }

    @Test
    void testBuildCanonicalHeaders_LeafOnlyMode() throws IOException {
        try (Workbook workbook = WorkbookFactory.create(new File(testFilePath))) {
            Sheet sheet = workbook.getSheet("TestSheet");
            List<Integer> headerRows = List.of(0, 1, 2);

            List<String> canonicalHeaders = CanonicalNameBuilder.buildCanonicalHeaders(sheet, headerRows, ConcatenationMode.LEAF_ONLY, " | ");

            assertEquals(4, canonicalHeaders.size());
            assertEquals("ID", canonicalHeaders.get(0));
            assertEquals("Name", canonicalHeaders.get(1));
            assertEquals("Value", canonicalHeaders.get(2));
            assertEquals("Date", canonicalHeaders.get(3));
        }
    }
}
