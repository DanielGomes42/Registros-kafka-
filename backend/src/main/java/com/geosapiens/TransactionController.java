package com.geosapiens;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.BufferedReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.csv.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class TransactionController {
  private static final String TOPIC = "transactions-import";
  private final JdbcTemplate jdbc;
  private final KafkaTemplate<String, String> kafka;
  private final ObjectMapper json;
  private final CapacityService capacity;
  private final ExecutorService workers = Executors.newSingleThreadExecutor();
  private final Map<String, Job> jobs = new ConcurrentHashMap<>();
  private final Counter readCounter, publishedCounter, persistedCounter, errorCounter;

  public TransactionController(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, ObjectMapper json,
      CapacityService capacity, MeterRegistry meters) {
    this.jdbc = jdbc;
    this.kafka = kafka;
    this.json = json;
    this.capacity = capacity;
    readCounter = meters.counter("geosapiens.records.read");
    publishedCounter = meters.counter("geosapiens.records.published");
    persistedCounter = meters.counter("geosapiens.records.persisted");
    errorCounter = meters.counter("geosapiens.records.errors");
  }

  // recebendo
  @PostMapping("/imports")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public Map<String, String> upload(@RequestParam("file") MultipartFile file) throws Exception {
    if (file.isEmpty() || !Objects.requireNonNullElse(file.getOriginalFilename(), "").toLowerCase().endsWith(".csv"))
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Envie um arquivo CSV.");
    String id = UUID.randomUUID().toString();
    Path temp = Files.createTempFile("geosapiens-", ".csv");
    file.transferTo(temp);
    Job job = new Job(id, file.getOriginalFilename());
    jobs.put(id, job);
    workers.submit(() -> publishFile(temp, job));
    return Map.of("id", id);
  }

  // endpoint que retorna o status do job de importação
  @GetMapping("/imports/{id}")
  public Job status(@PathVariable String id) {
    Job job = jobs.get(id);
    if (job == null)
      throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    return job;
  }

  @GetMapping("/capacity")
  public Map<String, Object> capacity() {
    return capacity.snapshot();
  }

  // endpoint que lista as transações com paginação e filtro por ano e mês
  @GetMapping("/transactions")
  public Map<String, Object> list(@RequestParam(required = false) String cursor,
      @RequestParam(defaultValue = "50") int size, @RequestParam(required = false) Integer year,
      @RequestParam(required = false) Integer month) {
    int limit = Math.min(Math.max(size, 1), 200);
    List<Map<String, Object>> items;
    LocalDate start = null, end = null;
    if (year != null) {
      if (year < 1 || year > 9999 || month != null && (month < 1 || month > 12))
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Filtro de data inválido.");
      start = LocalDate.of(year, month == null ? 1 : month, 1);
      end = month == null ? start.plusYears(1) : start.plusMonths(1);
    } else if (month != null)
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Informe o ano para filtrar o mês.");
    try {
      if (cursor == null || cursor.isBlank())
        items = start == null ? jdbc.queryForList(
            "SELECT id, occurred_at, category, amount, source FROM transactions ORDER BY occurred_at DESC, id DESC LIMIT ?",
            limit + 1)
            : jdbc.queryForList(
                "SELECT id, occurred_at, category, amount, source FROM transactions WHERE occurred_at >= ? AND occurred_at < ? ORDER BY occurred_at DESC, id DESC LIMIT ?",
                start, end, limit + 1);
      else {
        String[] key = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8).split("\\|", 2);
        if (key.length != 2)
          throw new IllegalArgumentException();
        items = start == null ? jdbc.queryForList(
            "SELECT id, occurred_at, category, amount, source FROM transactions WHERE (occurred_at, id) < (?, ?) ORDER BY occurred_at DESC, id DESC LIMIT ?",
            OffsetDateTime.parse(key[0]), Long.parseLong(key[1]), limit + 1)
            : jdbc.queryForList(
                "SELECT id, occurred_at, category, amount, source FROM transactions WHERE occurred_at >= ? AND occurred_at < ? AND (occurred_at, id) < (?, ?) ORDER BY occurred_at DESC, id DESC LIMIT ?",
                start, end, OffsetDateTime.parse(key[0]), Long.parseLong(key[1]), limit + 1);
      }
    } catch (IllegalArgumentException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Cursor inválido.");
    }
    boolean more = items.size() > limit;
    if (more)
      items.removeLast();
    String next = "";
    if (more) {
      Map<String, Object> last = items.getLast();
      next = Base64.getUrlEncoder().withoutPadding()
          .encodeToString((last.get("occurred_at") + "|" + last.get("id")).getBytes(StandardCharsets.UTF_8));
    }
    return Map.of("items", items, "nextCursor", next);
  }

  // endpoint que lista os anos disponíveis para agregação
  @GetMapping("/aggregation-years")
  public List<Integer> aggregationYears() {
    return jdbc.queryForList("SELECT DISTINCT EXTRACT(YEAR FROM occurred_at)::int FROM transactions ORDER BY 1 DESC",
        Integer.class);
  }

  // endpoint que lista as agregações por mês e categoria, com filtro por ano

  @GetMapping({ "/aggregates", "/aggregations" })
  public List<Map<String, Object>> aggregates(@RequestParam(required = false) Integer year) {
    if (year == null)
      return jdbc.queryForList(
          "SELECT to_char(date_trunc('month', occurred_at), 'YYYY-MM') AS month, category, sum(amount) AS total, count(*) AS count FROM transactions GROUP BY 1, 2 ORDER BY 1 DESC, total DESC");
    if (year < 1 || year > 9999)
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Ano inválido.");
    LocalDate start = LocalDate.of(year, 1, 1), end = start.plusYears(1);
    return jdbc.queryForList(
        "SELECT EXTRACT(MONTH FROM occurred_at)::int AS month_number, to_char(date_trunc('month', occurred_at), 'YYYY-MM') AS month, category, sum(amount) AS total, count(*) AS count FROM transactions WHERE occurred_at >= ? AND occurred_at < ? GROUP BY 1, 2, 3 ORDER BY 1, 3",
        start, end);
  }

  // streming com o apache commons csv para ler o arquivo csv e publicar no kafka
  private void publishFile(Path path, Job job) {
    job.status = "PUBLISHING";
    try (BufferedReader reader = Files.newBufferedReader(path);
        CSVParser csv = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setTrim(true).build()
            .parse(reader)) {
      for (CSVRecord row : csv) {
        job.read.incrementAndGet();
        readCounter.increment();
        try {
          String category = row.get("category"), source = row.isMapped("source") ? row.get("source") : null;
          if (category.isBlank() || category.length() > 80 || source != null && source.length() > 120)
            throw new IllegalArgumentException();
          ImportRecord record = new ImportRecord(job.id, OffsetDateTime.parse(row.get("occurred_at")), category,
              new BigDecimal(row.get("amount")), source);
          job.pendingPublishes.incrementAndGet();
          try {
            kafka.send(TOPIC, job.id, json.writeValueAsString(record)).whenComplete((ok, error) -> {
              job.pendingPublishes.decrementAndGet();
              if (error == null) {
                job.published.incrementAndGet();
                publishedCounter.increment();
              } else {
                job.errors.incrementAndGet();
                errorCounter.increment();
              }
              checkCompletion(job);
            });
          } catch (Exception e) {
            job.pendingPublishes.decrementAndGet();
            throw e;
          }
        } catch (Exception e) {
          job.errors.incrementAndGet();
          errorCounter.increment();
        }
      }
      job.publishFinished = true;
      job.status = "CONSUMING";
      checkCompletion(job);
    } catch (Exception e) {
      job.status = "FAILED";
      job.message = e.getMessage();
    } finally {
      try {
        Files.deleteIfExists(path);
      } catch (Exception ignored) {
      }
    }
  }

  // publicar no kafka e consumir em lote para inserir no banco de dados
  @KafkaListener(topics = TOPIC, containerFactory = "kafkaListenerContainerFactory")
  public void consume(List<String> messages) {
    List<Object[]> rows = new ArrayList<>(messages.size());
    List<Job> messageJobs = new ArrayList<>(messages.size());
    for (String message : messages)
      try {
        ImportRecord record = json.readValue(message, ImportRecord.class);
        rows.add(new Object[] { record.occurredAt(), record.category(), record.amount(), record.source() });
        messageJobs.add(jobs.get(record.jobId()));
      } catch (Exception e) {
        errorCounter.increment();
      }
    if (rows.isEmpty())
      return;
    jdbc.batchUpdate("INSERT INTO transactions (occurred_at, category, amount, source) VALUES (?, ?, ?, ?)", rows);
    for (Job job : messageJobs)
      if (job != null) {
        job.persisted.incrementAndGet();
        persistedCounter.increment();
        checkCompletion(job);
      }
  }

  private void checkCompletion(Job job) {
    if (job.publishFinished && job.pendingPublishes.get() == 0 && job.persisted.get() >= job.published.get()
        && job.status.equals("CONSUMING")) {
      job.status = "COMPLETED";
      job.finishedAt = System.currentTimeMillis();
    }
  }

  record ImportRecord(String jobId, OffsetDateTime occurredAt, String category, BigDecimal amount, String source) {
  }

  // classe para representar o status do job de importação, incluindo contadores
  // para registros lidos, publicados, persistidos e erros
  public static class Job {
    public final String id, filename;
    public volatile String status = "QUEUED", message = "";
    public final AtomicLong read = new AtomicLong(), published = new AtomicLong(), persisted = new AtomicLong(),
        errors = new AtomicLong(), pendingPublishes = new AtomicLong();
    public volatile boolean publishFinished;
    public final long startedAt = System.currentTimeMillis();
    public volatile long finishedAt;

    Job(String id, String filename) {
      this.id = id;
      this.filename = filename;
    }

    public String getId() {
      return id;
    }

    public String getFilename() {
      return filename;
    }

    public String getStatus() {
      return status;
    }

    public String getMessage() {
      return message;
    }

    public long getRead() {
      return read.get();
    }

    public long getPublished() {
      return published.get();
    }

    public long getPersisted() {
      return persisted.get();
    }

    public long getErrors() {
      return errors.get();
    }

    public long getPendingPublishes() {
      return pendingPublishes.get();
    }

    public long getDurationMs() {
      return (finishedAt == 0 ? System.currentTimeMillis() : finishedAt) - startedAt;
    }
  }
}
