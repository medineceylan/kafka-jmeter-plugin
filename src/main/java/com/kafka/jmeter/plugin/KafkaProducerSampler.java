package com.kafka.jmeter.plugin;

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


public class KafkaProducerSampler implements JavaSamplerClient {

    private static final String ENCODING = "UTF-8";
    private static final String PARAMETER_KAFKA_BROKERS = "kafka_brokers";
    private static final String PARAMETER_KAFKA_TOPIC = "kafka_topic";
    private static final String PARAMETER_KAFKA_ACKS = "kafka_acks";
    private static final String PARAMETER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max-in-fligh-request-per-connection";
    private static final String PARAMETER_KAFKA_IDEMPOTENCE = "idempotency";

    private static final String PARAMETER_KAFKA_MESSAGE = "kafka_message";

    private static final String PARAMETER_KAFKA_VALUE_SERIALIZER = "kafka_value_serializer";

    private static final String PARAMETER_KAFKA_KEY_SERIALIZER = "kafka_key_serializer";

    private static final String PARAMETER_KAFKA_SSL_KEYSTORE = "kafka_ssl_keystore";

    private static final String PARAMETER_KAFKA_SSL_KEYSTORE_PASSWORD = "kafka_ssl_keystore_password";

    private static final String PARAMETER_KAFKA_SSL_TRUSTSTORE = "kafka_ssl_truststore";

    private static final String PARAMETER_KAFKA_SSL_TRUSTSTORE_PASSWORD = "kafka_ssl_truststore_password";

    private static final String PARAMETER_KAFKA_USE_SSL = "kafka_use_ssl";


    private static final String PARAMETER_KAFKA_COMPRESSION_TYPE = "kafka_compression_type";

    private static final String PARAMETER_KAFKA_PARTITION = "kafka_partition";

    private static final String PARAMETER_KAFKA_KEY = "kafka_key";


    private KafkaProducer<String, String> producer;


    public void setupTest(JavaSamplerContext context) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getParameter(PARAMETER_KAFKA_BROKERS));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, context.getParameter(PARAMETER_KAFKA_KEY_SERIALIZER));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, context.getParameter(PARAMETER_KAFKA_VALUE_SERIALIZER));
        props.setProperty(ProducerConfig.ACKS_CONFIG, context.getParameter(PARAMETER_KAFKA_ACKS));
        props.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, context.getParameter(PARAMETER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, context.getParameter(PARAMETER_KAFKA_IDEMPOTENCE));


        // check if kafka security protocol is SSL or PLAINTEXT (default)
        if (context.getParameter(PARAMETER_KAFKA_USE_SSL).equals("true")) {
            props.put("security.protocol", "SSL");
            props.put("ssl.keystore.location", context.getParameter(PARAMETER_KAFKA_SSL_KEYSTORE));
            props.put("ssl.keystore.password", context.getParameter(PARAMETER_KAFKA_SSL_KEYSTORE_PASSWORD));
            props.put("ssl.truststore.location", context.getParameter(PARAMETER_KAFKA_SSL_TRUSTSTORE));
            props.put("ssl.truststore.password", context.getParameter(PARAMETER_KAFKA_SSL_TRUSTSTORE_PASSWORD));
        }

        String compressionType = context.getParameter(PARAMETER_KAFKA_COMPRESSION_TYPE);
        if (compressionType!=null) {
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        }

        producer = new KafkaProducer<String, String>(props);
    }


    public void teardownTest(JavaSamplerContext context) {
        producer.close();
    }


    public Arguments getDefaultParameters() {

        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(PARAMETER_KAFKA_BROKERS, "${PARAMETER_KAFKA_BROKERS}");
        defaultParameters.addArgument(PARAMETER_KAFKA_KEY, "${PARAMETER_KAFKA_KEY}");
        defaultParameters.addArgument(PARAMETER_KAFKA_PARTITION, "${PARAMETER_KAFKA_PARTITION}");
        defaultParameters.addArgument(PARAMETER_KAFKA_MESSAGE, "${PARAMETER_KAFKA_MESSAGE}");


        defaultParameters.addArgument(PARAMETER_KAFKA_TOPIC, "${PARAMETER_KAFKA_TOPIC}");
        defaultParameters.addArgument(PARAMETER_KAFKA_VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        defaultParameters.addArgument(PARAMETER_KAFKA_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        defaultParameters.addArgument(PARAMETER_KAFKA_COMPRESSION_TYPE, "${PARAMETER_KAFKA_COMPRESSION_TYPE}");
        defaultParameters.addArgument(PARAMETER_KAFKA_ACKS, "${PARAMETER_KAFKA_ACKS}");
        defaultParameters.addArgument(PARAMETER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "${MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}");
        defaultParameters.addArgument(PARAMETER_KAFKA_IDEMPOTENCE, "${PARAMETER_KAFKA_IDEMPOTENCE}");

        defaultParameters.addArgument(PARAMETER_KAFKA_SSL_KEYSTORE, "${PARAMETER_KAFKA_SSL_KEYSTORE}");
        defaultParameters.addArgument(PARAMETER_KAFKA_SSL_KEYSTORE_PASSWORD, "${PARAMETER_KAFKA_SSL_KEYSTORE_PASSWORD}");
        defaultParameters.addArgument(PARAMETER_KAFKA_SSL_TRUSTSTORE, "${PARAMETER_KAFKA_SSL_TRUSTSTORE}");
        defaultParameters.addArgument(PARAMETER_KAFKA_SSL_TRUSTSTORE_PASSWORD, "${PARAMETER_KAFKA_SSL_TRUSTSTORE_PASSWORD}");
        defaultParameters.addArgument(PARAMETER_KAFKA_USE_SSL, "${PARAMETER_KAFKA_USE_SSL}");


        return defaultParameters;
    }


    public SampleResult runTest(JavaSamplerContext context) {

        SampleResult result = newSampleResult();
        String topic = context.getParameter(PARAMETER_KAFKA_TOPIC);
        String key = context.getParameter(PARAMETER_KAFKA_KEY);
        String message = context.getParameter(PARAMETER_KAFKA_MESSAGE);
        sampleResultStart(result, message);

        final ProducerRecord<String, String> producerRecord;
        String partitionString = context.getParameter(PARAMETER_KAFKA_PARTITION);
        if (partitionString!=null) {
            producerRecord = new ProducerRecord<String, String>(topic, key, message);
        } else {
            final int partitionNumber = Integer.parseInt(partitionString);
            producerRecord = new ProducerRecord<String, String>(topic, partitionNumber, key, message);
        }

        try {
            producer.send(producerRecord);
            sampleResultSuccess(result, null);
        } catch (Exception e) {
            sampleResultFailed(result, "500", e);
        }
        return result;
    }


    private SampleResult newSampleResult() {
        SampleResult result = new SampleResult();
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
