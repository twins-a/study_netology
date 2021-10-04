package netology2.dsw.dsl;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Приложение на Kafka Streams, которое пишет в топик сообщение,
 * каждый раз, когда сумма покупок продукта за минуту было больше, чем MAX_PURCHASES_SUMMA_PER_MINUTE
 */
public class QuantityAlertsAppHomeWork {
    public static final String PURCHASE_TOPIC_NAME = "purchases";
    public static final String PRODUCT_TOPIC_NAME = "products";
    public static final String RESULT_TOPIC = "product_quantity_alerts_dsl_homework";
    private static final double MAX_PURCHASES_SUMMA_PER_MINUTE = 3000D;

    public static void main(String[] args) throws InterruptedException {
        // создаем клиент для общения со schema-registry
        var client = new CachedSchemaRegistryClient("http://localhost:8090", 16);
        var serDeProps = Map.of(
                // указываем сериализатору, что может самостояетльно регистрировать схемы
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8090"
        );

        // строим нашу топологию
        Topology topology = buildTopology(client, serDeProps);

        // если скопировать вывод этой команды вот сюда - https://zz85.github.io/kafka-streams-viz/
        // то можно получить красивую визуализацию топологии прямо в браузере
        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getStreamsConfig());
        // вызов latch.await() будет блокировать текущий поток
        // до тех пор пока latch.countDown() не вызовут 1 раз
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            kafkaStreams.start();
            // будет блокировать поток, пока из другого потока не будет вызван метод countDown()
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    public static Properties getStreamsConfig() {
        Properties props = new Properties();
        // имя этого приложения для кафки
        // приложения с одинаковым именем объединятся в ConsumerGroup и распределят обработку партиций между собой
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "QuantityAlertsAppDSL");
        // адреса брокеров нашей кафки (у нас он 1)
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // если вы захотите обработать записи заново, не забудьте удалить папку со стейтами
        // а лучше воспользуйтесь методом kafkaStreams.cleanUp()
        props.put(StreamsConfig.STATE_DIR_CONFIG, "states");
        return props;
    }

    public static Topology buildTopology(SchemaRegistryClient client, Map<String, String> serDeConfig) {
        var builder = new StreamsBuilder();
        // Создаем класс для сериализации и десериализации наших сообщений
        var avroSerde = new GenericAvroSerde(client);
        avroSerde.configure(serDeConfig, false);
        // Получаем из кафки поток сообщений из топика покупок (purchase = покупка с англ.)
        var purchasesStream = builder.stream(
                PURCHASE_TOPIC_NAME, // указываем имя топика
                Consumed.with(new Serdes.StringSerde(), avroSerde) // указываем тип ключа и тип значения в топике
        );
        // Table - хранит в себе последнюю запись из топика по ключу
        var productsTable1 = builder.globalTable(
                PRODUCT_TOPIC_NAME, // указываем топик, который мы хотим выкачать в таблицу
                Consumed.with(new Serdes.StringSerde(), avroSerde) // указываем тип ключа и тип значения в топике
        );
        var purchaseWithJoinedProduct = purchasesStream.leftJoin(
                productsTable1, // указываем, какую табличку приджоинить
                (key, val) -> val.get("productid").toString(), // указываем, как получить ключ, по которому джоиним
                QuantityAlertsAppHomeWork::joinProductHW // указываем, как джоинить
        );

        Duration oneMinute = Duration.ofMinutes(1);

        purchaseWithJoinedProduct
                // фильтруем только успешные записи
                .filter((key, val) -> val.success)
                // используем именно метод mapValues, потому что он не может вызвать репартиционирования (см 2-ю лекцию)
                .mapValues(val -> val.result)
                .groupBy((key, val) -> val.get("productid").toString(), Grouped.with(new Serdes.StringSerde(), avroSerde))
                .windowedBy(
                        // объединяем записи в рамках минуты
                        TimeWindows.of(oneMinute)
                                // сдвигаем окно всегда на минуту
                                .advanceBy(oneMinute))
                .aggregate(
                        () -> 0D,
                        (key, val, agg) -> agg += (Double) val.get("purchase_summa"),
                        Materialized.with(new Serdes.StringSerde(), new Serdes.DoubleSerde())
                )
                .filter((key, val) -> val > MAX_PURCHASES_SUMMA_PER_MINUTE)
                .toStream()
                .map((key, val) -> {
                    // создаем схему нашего алерта
                    Schema schema = SchemaBuilder.record("QuantityAlert").fields()
                            .name("window_start")
                            // AVRO допускает использование "логических" типов
                            // в данном случае мы показываем, что в данном поле лежит таймстемп
                            // в миллисекундах epoch
                            .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
                            .noDefault()
                            .requiredDouble("purchase_summa")
                            .endRecord();
                    GenericRecord record = new GenericData.Record(schema);
                    // старт окна у нас в миллисекундах
                    record.put("window_start", key.window().start());
                    record.put("purchase_summa", val);
                    return KeyValue.pair(key.key(), record);
                })
                .to(RESULT_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        return builder.build();
    }

    private static JoinResultHW joinProductHW(GenericRecord purchase, GenericRecord product) {
        try {
            // описываем схему нашего сообщения
            Schema schema = SchemaBuilder.record("PurchaseWithProduct").fields()
                    .requiredLong("purchase_id")
                    .requiredLong("productid")
                    .requiredDouble("purchase_summa")
                    .endRecord();
            GenericRecord result = new GenericData.Record(schema);
            // копируем в наше сообщение нужные поля из сообщения о покупке
            result.put("purchase_id", purchase.get("id"));
            result.put("productid", purchase.get("productid"));
            // копируем в наше сообщение нужные поля из сообщения о товаре
            long quantity1 = Long.parseLong(purchase.get("quantity").toString());
            double price1 = Double.parseDouble(product.get("price").toString());
            double summa1 = quantity1 * price1;
            Object Summa_obj = summa1;
            result.put("purchase_summa", Summa_obj);
            return new JoinResultHW(true, result, null);
        } catch (Exception e) {
            return new JoinResultHW(false, purchase, e.getMessage());
        }
    }

    private static record JoinResultHW(boolean success, GenericRecord result, String errorMessage) {}
}