package com.kafka.jmeter.plugin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.jmeter.plugin.model.Message;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import static java.lang.String.format;


public class KafkaProducerSampler implements JavaSamplerClient {

    private static final String ENCODING = "UTF-8";

    private static final String PARAMETER_KAFKA_TOPIC = "kafka.topic";

    private static final String PARAMETER_KAFKA_PARTITION = "kafka.partition";

    private static final String PARAMETER_KAFKA_KEY = "kafka.key";

    private static final String PARAMETER_KAFKA_SSL_KEYSTORE = "ssl.keystore";

    private static final String PARAMETER_KAFKA_SSL_KEYSTORE_PASS = "ssl.keystore.password";

    private static final String PARAMETER_KAFKA_SSL_TRUSTSTORE = "ssl.truststore";

    private static final String PARAMETER_KAFKA_SSL_TRUSTSTORE_PASS = "ssl.truststore.password";

    private static final String PARAMETER_KAFKA_USE_SSL = "use.ssl";

    private static final String PARAMETER_BATCH = "batch";

    private static final String PARAMETER_THREAD_NUMBER = "thread.number";

    private static final String PARAMETER_MESSAGE_NUMBER = "message.number";

    private static final ObjectMapper mapper = new ObjectMapper();

    private KafkaProducer<String, String> producer;

    public void setupTest(JavaSamplerContext context) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getParameter(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        props.setProperty(ProducerConfig.ACKS_CONFIG, context.getParameter(ProducerConfig.ACKS_CONFIG));
        props.setProperty(ProducerConfig.RETRIES_CONFIG, context.getParameter(ProducerConfig.RETRIES_CONFIG));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, context.getParameter(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, context.getParameter(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, context.getParameter(ProducerConfig.BATCH_SIZE_CONFIG));
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, context.getParameter(ProducerConfig.LINGER_MS_CONFIG));
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, context.getParameter(ProducerConfig.BUFFER_MEMORY_CONFIG));

        // check if kafka security protocol is SSL or PLAINTEXT (default)
        if ("true".equalsIgnoreCase(context.getParameter(PARAMETER_KAFKA_USE_SSL))) {
            props.put("security.protocol", "SSL");
            props.put("ssl.keystore.location", context.getParameter(PARAMETER_KAFKA_SSL_KEYSTORE));
            props.put("ssl.keystore.password", context.getParameter(PARAMETER_KAFKA_SSL_KEYSTORE_PASS));
            props.put("ssl.truststore.location", context.getParameter(PARAMETER_KAFKA_SSL_TRUSTSTORE));
            props.put("ssl.truststore.password", context.getParameter(PARAMETER_KAFKA_SSL_TRUSTSTORE_PASS));
        }

        final String compressionType = context.getParameter(ProducerConfig.COMPRESSION_TYPE_CONFIG);
        if (compressionType != null) {
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        }

        producer = new KafkaProducer<String, String>(props);
    }

    public void teardownTest(JavaSamplerContext context) {
        producer.close();
    }

    public Arguments getDefaultParameters() {

        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${__P(bootstrap.servers,localhost:9092)}");
        defaultParameters.addArgument(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "${__P(key.serializer,org.apache.kafka.common.serialization.StringSerializer)}");
        defaultParameters.addArgument(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "${__P(value.serializer,org.apache.kafka.common.serialization.StringSerializer)}");
        defaultParameters.addArgument(ProducerConfig.ACKS_CONFIG, "${__P(acks, all)}");
        defaultParameters.addArgument(ProducerConfig.RETRIES_CONFIG, format("${__P(retries, %s)}", Integer.MAX_VALUE));
        defaultParameters.addArgument(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "${__P(max.in.flight.requests.per.connection, 5)}");
        defaultParameters.addArgument(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "${__P(enable.idempotence, true)}");
        defaultParameters.addArgument(ProducerConfig.BATCH_SIZE_CONFIG, "${__P(batch.size, 16645)}");
        defaultParameters.addArgument(ProducerConfig.LINGER_MS_CONFIG, "${__P(linger.ms, 5)}");
        defaultParameters.addArgument(ProducerConfig.BUFFER_MEMORY_CONFIG, "${__P(buffer.memory, 33554432)}");
        defaultParameters.addArgument(ProducerConfig.COMPRESSION_TYPE_CONFIG, "${__P(compression.type, snappy)}");

        defaultParameters.addArgument(PARAMETER_KAFKA_USE_SSL, "${__P(use.ssl, false)}");
        defaultParameters.addArgument(PARAMETER_KAFKA_SSL_KEYSTORE, "${__P(ssl.keystore, )}");
        defaultParameters.addArgument(PARAMETER_KAFKA_SSL_KEYSTORE_PASS, "${__P(ssl.keystore.password, )}");
        defaultParameters.addArgument(PARAMETER_KAFKA_SSL_TRUSTSTORE, "${__P(ssl.truststore, )}");
        defaultParameters.addArgument(PARAMETER_KAFKA_SSL_TRUSTSTORE_PASS, "${__P(ssl.truststore.password, )}");

        defaultParameters.addArgument(PARAMETER_KAFKA_TOPIC, "${__P(kafka.topic, my-jemeter-topicss)}");
        defaultParameters.addArgument(PARAMETER_KAFKA_KEY, "${__P(kafka.key, )}");
        defaultParameters.addArgument(PARAMETER_KAFKA_PARTITION, "${__P(kafka.partition,)}");


        defaultParameters.addArgument(PARAMETER_BATCH, "${PARAMETER_BATCH}");
        defaultParameters.addArgument(PARAMETER_THREAD_NUMBER, "${__threadNum()}");
        defaultParameters.addArgument(PARAMETER_MESSAGE_NUMBER, "${PARAMETER_MESSAGE_NUMBER}");


        return defaultParameters;
    }

    public SampleResult runTest(JavaSamplerContext context) {

        final SampleResult result = newSampleResult();
        final String topic = context.getParameter(PARAMETER_KAFKA_TOPIC);
        final String key = context.getParameter(PARAMETER_KAFKA_KEY);

        final Message message = getMessage(context);

        sampleResultStart(result, message.toString());

        ProducerRecord<String, String> producerRecord;
        final Integer partition = context.getIntParameter(PARAMETER_KAFKA_PARTITION);
        try {
            if (partition != null) {

                producerRecord = new ProducerRecord<>(topic, key, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(message));
            } else {
                producerRecord = new ProducerRecord<>(topic, partition, key, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(message));
            }
            producer.send(producerRecord);
            sampleResultSuccess(result, null);
        }
        catch (JsonProcessingException e) {
            sampleResultFailed(result, "500", e);
            e.printStackTrace();
        }

        return result;
    }

    private Message getMessage(JavaSamplerContext context) {
        final Integer threadNumber = Integer.valueOf(context.getParameter(PARAMETER_THREAD_NUMBER));
        final Integer messageNumber = Integer.valueOf(context.getParameter(PARAMETER_MESSAGE_NUMBER));
        String batch = context.getParameter(PARAMETER_BATCH);
        if(StringUtils.isBlank(batch)){
            batch = "BATCH" + System.currentTimeMillis();
        }
        return new Message(batch, threadNumber, messageNumber, System.currentTimeMillis());
    }

    private SampleResult newSampleResult() {
        final SampleResult result = new SampleResult();
        result.setDataEncoding(ENCODING);
        result.setDataType(SampleResult.TEXT);
        return result;
    }

    private void sampleResultStart(SampleResult result, String data) {
        result.setSamplerData(data);
        result.sampleStart();
    }

    private void sampleResultSuccess(SampleResult result, String response) {
        result.sampleEnd();
        result.setSuccessful(true);
        result.setResponseCodeOK();
        if (response != null) {
            result.setResponseData(response, ENCODING);
        } else {
            result.setResponseData("No response required", ENCODING);
        }
    }

    private void sampleResultFailed(final SampleResult result, final String reason, final Exception exception) {
        sampleResultFailed(result, reason);
        result.setResponseMessage("Exception: " + exception);
        result.setResponseData(getStackTrace(exception), ENCODING);
    }

    private void sampleResultFailed(final SampleResult result, final String reason) {
        result.sampleEnd();
        result.setSuccessful(false);
        result.setResponseCode(reason);
    }

    private String getStackTrace(final Exception exception) {
        final StringWriter stringWriter = new StringWriter();
        exception.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

}
