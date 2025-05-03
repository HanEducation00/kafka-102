### Kafka nın Çalışma Modları
- Standalone mod, Kafka'nın tek bir sunucu üzerinde çalıştırıldığı basit yapılandırmadır:
- Distributed mod, Kafka'nın birden fazla sunucu üzerinde çalıştırıldığı ve bir küme (cluster) oluşturduğu yapıdır:

----------------------------------------------------------------------------------------------------------------------------
### Kafka stondole mod config dosyası
```
ubuntu@ip-172-31-21-179:~/kafka/kafka_2.13-3.8.0/config$ ls

connect-console-sink.properties    connect-file-sink.properties    connect-mirror-maker.properties  kraft                server.properties       zookeeper.properties
connect-console-source.properties  connect-file-source.properties  connect-standalone.properties    log4j.properties     tools-log4j.properties
connect-distributed.properties     connect-log4j.properties        consumer.properties              producer.properties  trogdor.conf
```

#### 1. Server Basics
* broker.id=0
* Bu Kafka broker'ının benzersiz ID’si.
* Her broker’ın ID’si farklı olmalı (cluster içinde).
* Örnek: Eğer 3 broker varsa, broker.id=0, broker.id=1, broker.id=2 gibi.

#### 2. Socket Server Settings
* listeners=PLAINTEXT://:9092 (yorum satırı)
* Broker’ın hangi IP ve port üzerinden dinleme yapacağını belirtir.
* Aktif olsaydı: PLAINTEXT://0.0.0.0:9092 şeklinde tüm arayüzlerden dinleyebilirdi.

* advertised.listeners=PLAINTEXT://your.host.name:9092 (yorum satırı)
* Dışarıya (client'lara) kendisini nasıl tanıtacağı.
* Örneğin dış IP ya da DNS adıyla belirtilir.
* Cloud veya Docker ortamında mutlaka set edilmelidir, yoksa client bağlanamaz.

* listener.security.protocol.map (yorum satırı)
* Listener’lar için hangi güvenlik protokolünün kullanılacağını tanımlar (PLAINTEXT, SSL, SASL).
* Aktif değil ama SSL/SASL kullanacaksan gereklidir.

* num.network.threads=3
* Ağ üzerinden gelen istekleri dinlemek için kaç thread kullanılacak.

* num.io.threads=8
* Disk erişimi gibi I/O işlemleri için kullanılacak thread sayısı.

* Eğer aynı anda:
* 50 farklı microservice Kafka’ya veri gönderiyorsa (producer),
* 20 farklı consumer grup veri okuyorsa,
* Kafka Connect aktifse...
* Bunların hepsi Kafka’ya birer “client” olarak bağlanmış demektir. Bu durumda num.network.threads ve num.io.threads değerlerinin yüksek olması gerekir ki hepsiyle etkili iletişim kurulabilsin.

* Farz edelim Kafka bir restorandır:
* num.network.threads = Garson sayısı (sipariş alır, teslim eder)
* num.io.threads = Mutfaktaki aşçı sayısı (siparişi hazırlar)
* Ne kadar fazla garson ve aşçı varsa, o kadar fazla müşteriye hizmet verebilirsin.

* Çok fazla client aynı anda bağlanıyorsa → network.threads ↑
* Çok sayıda topic/partition varsa veya disk işlemleri yoğunsa → io.threads ↑
* Yük arttıkça bu sayılar artırılabilir.

* socket.send.buffer.bytes=102400
* Socket çıkış buffer’ı (gönderme).
* Network optimizasyonları için önemli olabilir.

* socket.receive.buffer.bytes=102400
* Socket giriş buffer’ı (alma).

* socket.request.max.bytes=104857600
* Kafka’nın kabul edebileceği en büyük istek boyutu (100 MB).
* Büyük mesajlar göndereceksen bu değeri artırmalısın.

#### 3. Log Basics
* log.dirs=/tmp/kafka-logs
* Kafka'nın verilerini yazdığı klasör(ler).
* Çok disk varsa virgülle ayırarak yazılabilir.

* num.partitions=1
* Her yeni topic için varsayılan partition sayısı.
* Daha yüksek değerler paralel okuma/yazma sağlar.

* num.recovery.threads.per.data.dir=1
* Her veri klasörü için başlangıçta kurtarma/temizleme için kaç thread kullanılacağı.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#### 4. Internal Topic Settings
* offsets.topic.replication.factor=1
* __consumer_offsets iç topic'inin replikasyon faktörü.
* Üretimde en az 3 olması önerilir.

* transaction.state.log.replication.factor=1
__transaction_state için replikasyon sayısı.
* transaction.state.log.min.isr=1
* Minimum in-sync replica (ISR) sayısı.
* Replikasyon için güvenlik eşiğidir.

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#### 5. Log Flush Policy (Yorum Satırları)
* fsync (file sync), bir verinin diskte fiziksel olarak yazıldığından emin olmak için kullanılan bir sistem çağrısıdır.
  
* log.flush.interval.messages
- Kaç mesajdan sonra disk'e fsync yapılacağı.

* log.flush.interval.ms
- Belirli bir süre geçtiğinde fsync yapılır.

- log.flush.interval.messages=10000: 10.000 mesajda bir fsync.
- log.flush.interval.ms=1000: 1 saniyede bir fsync.

⚠️ Bunlar devre dışı. Kafka varsayılan olarak fsync işlemini işletim sistemine bırakır. Gerçek zamanlı sistemlerde bu değerleri aktif etmek önemli olabilir.
- Tek broker kullanıyorsan ve veri kaybı kabul edilemezse → fsync önemli.
- Çok broker + replikasyon kullanıyorsan → fsync yerine replica sayısını artırmak daha verimlidir (acks=all ile birlikte).

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#### 6. Log Retention Policy
* log.retention.hours=168
* Log segmentleri 7 gün sonra silinir.

* log.retention.bytes=... (yorum satırı)
* Belirtilirse, log boyutu belli eşiği aşarsa eski segmentler silinir.

* log.segment.bytes=... (yorum satırı)
* Bir log segmenti maksimum bu boyuta ulaşınca yeni segment oluşturulur.

* log.retention.check.interval.ms=300000
* Kafka, eski segmentleri 5 dakikada bir kontrol eder.

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#### 7. Zookeeper Ayarları
* zookeeper.connect=localhost:2181
* Kafka'nın bağlantı kuracağı Zookeeper adresi.

* zookeeper.connection.timeout.ms=18000
* Kafka'nın Zookeeper'a bağlanmak için bekleyeceği maksimum süre.

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#### 8. Group Coordinator Settings
* group.initial.rebalance.delay.ms=0
* Consumer grubu başladığında rebalancing işlemini geciktirme süresi (0 ms).
* Geliştirme için uygundur.
* Üretimde 3000 ms gibi bir değer daha güvenlidir.

