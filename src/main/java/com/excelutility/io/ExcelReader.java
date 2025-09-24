package com.excelutility.io;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;


import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads data from an Excel file, with support for streaming large XLSX files.
 */
public class ExcelReader {

    // For streaming .xlsx files
    private static class SheetHandler extends DefaultHandler {
        private final SharedStringsTable sst;
        private String lastContents;
        private boolean nextIsString;
        private List<String> currentRow = new ArrayList<>();
        private final List<List<String>> sheetData = new ArrayList<>();

        SheetHandler(SharedStringsTable sst) {
            this.sst = sst;
        }

        public List<List<String>> getSheetData() {
            return sheetData;
        }

        public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {
            if (name.equals("c")) { // cell
                String cellType = attributes.getValue("t");
                if (cellType != null && cellType.equals("s")) {
                    nextIsString = true;
                } else {
                    nextIsString = false;
                }
            }
            lastContents = "";
        }

        public void endElement(String uri, String localName, String name) throws SAXException {
            if (nextIsString) {
                int idx = Integer.parseInt(lastContents);
                lastContents = new XSSFRichTextString(sst.getItemAt(idx).getString()).toString();
                nextIsString = false;
            }

            if (name.equals("v")) { // value
                currentRow.add(lastContents);
            } else if (name.equals("row")) { // end of a row
                sheetData.add(new ArrayList<>(currentRow));
                currentRow.clear();
            }
        }

        public void characters(char[] ch, int start, int length) throws SAXException {
            lastContents += new String(ch, start, length);
        }
    }

    public static List<List<Object>> read(String filePath, String sheetName, boolean useStreaming) throws IOException, InvalidFormatException {
        // The streaming reader has been reported to cause issues on some systems.
        // Forcing the in-memory reader for now as it is more robust.
        return readInMemory(filePath, sheetName);
    }

    private static List<List<Object>> readInMemory(String filePath, String sheetName) throws IOException {
        List<List<Object>> data = new ArrayList<>();
        try (Workbook workbook = WorkbookFactory.create(new File(filePath))) {
            Sheet sheet = workbook.getSheet(sheetName);
            if (sheet == null) {
                throw new IllegalArgumentException("Sheet '" + sheetName + "' not found in the workbook.");
            }
            DataFormatter dataFormatter = new DataFormatter();
            int maxCols = 0;
            for (Row row : sheet) {
                maxCols = Math.max(maxCols, row.getLastCellNum());
            }

            for (Row row : sheet) {
                List<Object> rowData = new ArrayList<>();
                for (int i = 0; i < maxCols; i++) {
                    Cell cell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                    if (cell == null) {
                        rowData.add("");
                    } else {
                        rowData.add(dataFormatter.formatCellValue(cell));
                    }
                }
                data.add(rowData);
            }
        }
        return data;
    }

    private static List<List<Object>> readStream(String filePath, String sheetName) throws Exception {
        try (OPCPackage pkg = OPCPackage.open(filePath)) {
            XSSFReader r = new XSSFReader(pkg);
            SharedStringsTable sst = (SharedStringsTable) r.getSharedStringsTable();
            XMLReader parser = XMLReaderFactory.createXMLReader();
            SheetHandler handler = new SheetHandler(sst);
            parser.setContentHandler(handler);

            XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) r.getSheetsData();
            while (iter.hasNext()) {
                try (InputStream stream = iter.next()) {
                    if (sheetName.equalsIgnoreCase(iter.getSheetName())) {
                        InputSource sheetSource = new InputSource(stream);
                        parser.parse(sheetSource);
                        // The streaming API gives us strings, so we convert to List<List<Object>>
                        List<List<String>> stringData = handler.getSheetData();
                        List<List<Object>> objectData = new ArrayList<>();
                        for (List<String> row : stringData) {
                            objectData.add(new ArrayList<>(row));
                        }
                        return objectData;
                    }
                }
            }
        }
        throw new IllegalArgumentException("Sheet '" + sheetName + "' not found in the workbook.");
    }

    public static List<String> getSheetNames(String filePath) throws IOException {
        List<String> sheetNames = new ArrayList<>();
        try (Workbook workbook = WorkbookFactory.create(new File(filePath))) {
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                sheetNames.add(workbook.getSheetName(i));
            }
        }
        return sheetNames;
    }

    public static List<List<Object>> readPreview(String filePath, String sheetName, int rowLimit) throws IOException {
        List<List<Object>> data = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filePath);
             Workbook workbook = WorkbookFactory.create(fis)) {
            Sheet sheet = workbook.getSheet(sheetName);
            if (sheet == null) {
                throw new IOException("Sheet '" + sheetName + "' not found in the workbook.");
            }

            int lastRow = Math.min(sheet.getLastRowNum(), rowLimit - 1);
            if (lastRow < 0) return data; // Empty sheet

            int maxCols = 0;
            // First pass to find the max number of columns in the preview range
            for (int i = 0; i <= lastRow; i++) {
                Row row = sheet.getRow(i);
                if (row != null) {
                    maxCols = Math.max(maxCols, row.getLastCellNum());
                }
            }

            // Second pass to read data
            for (int i = 0; i <= lastRow; i++) {
                Row row = sheet.getRow(i);
                List<Object> rowData = new ArrayList<>();
                for (int j = 0; j < maxCols; j++) {
                    if (row == null) {
                        rowData.add("");
                        continue;
                    }
                    Cell cell = row.getCell(j, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                    if (cell == null) {
                        rowData.add("");
                        continue;
                    }
                    switch (cell.getCellType()) {
                        case STRING:
                            rowData.add(cell.getStringCellValue());
                            break;
                        case NUMERIC:
                            if (DateUtil.isCellDateFormatted(cell)) {
                                rowData.add(cell.getDateCellValue().toString());
                            } else {
                                rowData.add(new DataFormatter().formatCellValue(cell));
                            }
                            break;
                        case BOOLEAN:
                            rowData.add(cell.getBooleanCellValue());
                            break;
                        case FORMULA:
                             try {
                                rowData.add(new DataFormatter().formatCellValue(cell, workbook.getCreationHelper().createFormulaEvaluator()));
                            } catch (Exception e) {
                                rowData.add("!FORMULA_ERROR!");
                            }
                            break;
                        case BLANK:
                            rowData.add("");
                            break;
                        default:
                            rowData.add("!UNSUPPORTED_TYPE!");
                    }
                }
                data.add(rowData);
            }
        }
        return data;
    }
}
