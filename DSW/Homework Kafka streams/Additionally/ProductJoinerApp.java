package netology2.dsw.dsl;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ProductJoinerApp {
    public static final String PRODUCT_TOPIC_NAME = "products";
    public static final String PURCHASE_TOPIC_NAME = "purchases";
    public static final String RESULT_TOPIC = "purchase_with_joined_product-dsl";
    public static final String DLQ_TOPIC = "purchases_product_join_dlq-dsl";

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
        kafkaStreams.setUncaughtExceptionHandler((thread, ex) -> {
            ex.printStackTrace();
            kafkaStreams.close();
            latch.countDown();
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ProductJoinerDSL");
        // адреса брокеров нашей кафки (у нас он 1)
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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
        var productsTable = builder.globalTable(
                PRODUCT_TOPIC_NAME, // указываем топик, который мы хотим выкачать в таблицу
                Consumed.with(new Serdes.StringSerde(), avroSerde) // указываем тип ключа и тип значения в топике
        );
        var purchaseWithJoinedProduct = purchasesStream.leftJoin(
                productsTable, // указываем, какую табличку приджоинить
                (key, val) -> val.get("productid").toString(), // указываем, как получить ключ, по которому джоиним
                ProductJoinerApp::joinProduct // указываем, как джоинить
        );

        purchaseWithJoinedProduct
                // фильтруем только успешные записи
                .filter((key, val) -> val.success)
                // используем именно метод mapValues, потому что он не может вызвать репартиционирования (см 2-ю лекцию)
                .mapValues(val -> val.result)
                // записываем успешные сообщения в результирующий топик
                .to(RESULT_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        purchaseWithJoinedProduct
                // фильтруем только неуспешные записи
                .filter((key, val) -> !val.success)
                // используем именно метод mapValues, потому что он не может вызвать репартиционирования (см 2-ю лекцию)
                .mapValues(val -> val.result)
                // записываем сообщение с ошибкой в dlq топик (dead letter queue) - очередь недоставленных сообщений
                .to(DLQ_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        return builder.build();
    }

    private static JoinResult joinProduct(GenericRecord purchase, GenericRecord product) {
        try {
            // описываем схему нашего сообщения
            Schema schema = SchemaBuilder.record("PurchaseWithProduct").fields()
                    .requiredLong("purchase_id")
                    .requiredLong("purchase_quantity")
                    .requiredLong("productid")
                    .requiredString("product_name")
                    .requiredDouble("product_price")
                    .requiredDouble("purchase_summa")
                    .endRecord();
            GenericRecord result = new GenericData.Record(schema);
            // копируем в наше сообщение нужные поля из сообщения о покупке
            result.put("purchase_id", purchase.get("id"));
            result.put("purchase_quantity", purchase.get("quantity"));
            result.put("productid", purchase.get("productid"));
            // копируем в наше сообщение нужные поля из сообщения о товаре
            result.put("product_name", product.get("name"));
            result.put("product_price", product.get("price"));

            long quantity1 = Long.parseLong(purchase.get("quantity").toString());
            double price1 = Double.parseDouble(product.get("price").toString());
            double summa1 = quantity1 * price1;
            Object Summa_obj = summa1;
            result.put("purchase_summa", Summa_obj);

            return new JoinResult(true, result, null);
        } catch (Exception e) {
            return new JoinResult(false, purchase, e.getMessage());
        }
    }

    private static record JoinResult(boolean success, GenericRecord result, String errorMessage){}
}