package com.geosapiens;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class CapacityService {
  private final JdbcTemplate jdbc; private final long limitBytes,rowBytes; private final int alertPercent; private final AtomicLong databaseBytes=new AtomicLong();
  CapacityService(JdbcTemplate jdbc,MeterRegistry meters,@Value("${geosapiens.storage-limit-gb}") long limitGb,@Value("${geosapiens.alert-percent}") int alertPercent,@Value("${geosapiens.avg-row-bytes-estimate}") long rowBytes){this.jdbc=jdbc;this.limitBytes=limitGb*1024L*1024*1024;this.alertPercent=alertPercent;this.rowBytes=rowBytes;Gauge.builder("geosapiens.database.size",databaseBytes,AtomicLong::get).register(meters);Gauge.builder("geosapiens.database.capacity.percent",this,c->c.percent()).register(meters);}
  @Scheduled(fixedDelay=15000) public void refresh(){try{databaseBytes.set(jdbc.queryForObject("SELECT pg_database_size(current_database())",Long.class));}catch(Exception ignored){}}
  private double percent(){return limitBytes==0?0:databaseBytes.get()*100d/limitBytes;}
  static long estimateAdditionalRecords(long remainingBytes,long averageRowBytes){return averageRowBytes<=0?0:Math.max(0,remainingBytes)/averageRowBytes;}
  public Map<String,Object> snapshot(){refresh();long used=databaseBytes.get(),remaining=Math.max(0,limitBytes-used);double pct=percent();String state=pct>=100?"CRITICAL":pct>=alertPercent?"WARNING":"OK";return Map.of("databaseBytes",used,"storageLimitBytes",limitBytes,"usedPercent",pct,"remainingBytes",remaining,"estimatedAdditionalRecords",estimateAdditionalRecords(remaining,rowBytes),"alertPercent",alertPercent,"state",state);}
}
