package com.geosapiens;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.StringReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

class CapacityAndCsvTest {
  @Test void estimatesAdditionalRowsFromConfiguredAverage() { assertEquals(50, CapacityService.estimateAdditionalRecords(9_000, 180)); }
  @Test void parsesQuotedCsvFields() throws Exception {
    CSVRecord row = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(new StringReader("occurred_at,category,amount,source\n2025-01-01T00:00:00Z,food,10.00,\"mobile,partner\"\n")).getRecords().getFirst();
    assertEquals("mobile,partner", row.get("source"));
  }
}
