import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class CsvStats {
    private long rowCount = 0;
    private final Set<String> states = new HashSet<>();
    private final Map<String, Long> customersByState = new HashMap<>();

    void accept(String customerState) {
        rowCount++;
        states.add(customerState);
        customersByState.merge(customerState, 1L, Long::sum);
    }

    long getRowCount() {
        return rowCount;
    }

    int getStateCount() {
        return states.size();
    }

    Map<String, Long> getCustomersByState() {
        return customersByState;
    }
}

public class OlistCsvJavaPractice {
    private static final Path DEFAULT_CUSTOMERS_CSV =
            Path.of("/workspace/data/olist/olist_customers_dataset.csv");

    public static void main(String[] args) {
        Path csvPath = args.length > 0 ? Path.of(args[0]) : DEFAULT_CUSTOMERS_CSV;

        try {
            CsvStats stats = readCustomers(csvPath);
            System.out.println("File: " + csvPath);
            System.out.println("Tong so customer: " + stats.getRowCount());
            System.out.println("So bang/state: " + stats.getStateCount());
            System.out.println("So customer theo state:");

            stats.getCustomersByState().entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(10)
                    .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));
        } catch (IOException e) {
            System.err.println("Khong doc duoc file CSV: " + e.getMessage());
            System.exit(1);
        }
    }

    private static CsvStats readCustomers(Path csvPath) throws IOException {
        CsvStats stats = new CsvStats();

        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("File rong");
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = parseSimpleCsvLine(line);
                if (columns.length < 5) {
                    continue;
                }

                String customerState = clean(columns[4]);
                if (!customerState.isBlank()) {
                    stats.accept(customerState);
                }
            }
        }

        return stats;
    }

    private static String clean(String value) {
        return value.replace("\"", "").trim();
    }

    private static String[] parseSimpleCsvLine(String line) {
        // Dataset Olist customers khong co dau phay ben trong field, nen split don gian la du dung.
        return line.split(",", -1);
    }
}

