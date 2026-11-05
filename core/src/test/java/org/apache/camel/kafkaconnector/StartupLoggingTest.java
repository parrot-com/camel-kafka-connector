/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.kafkaconnector;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.kafkaconnector.utils.CamelKafkaConnectMain;
import org.apache.kafka.connect.connector.Connector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StartupLoggingTest {

    private static final String JAAS_KEY = "camel.idempotency.kafka.sasl.jaas.config";
    private static final String USERNAME = "startup-log-regression-user";
    private static final String PASSWORD = "startup-log-regression-password";
    private static final String JAAS = "org.apache.kafka.common.security.scram.ScramLoginModule required "
        + "username=\"" + USERNAME + "\" password=\"" + PASSWORD + "\";";
    private static final String CUSTOM_VALUE = "startup-log-regression-custom-value";
    private static final String SECRET_KEY = "camel.component.aws2-kinesis.secretKey";
    private static final String SECRET_VALUE = "startup-log-regression-secret-key";

    private final Queue<String> messages = new ConcurrentLinkedQueue<>();
    private final AbstractAppender appender = new AbstractAppender(
            "startup-logging-test", null, PatternLayout.createDefaultLayout(), false) {
        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    };
    private Logger logger;

    @BeforeEach
    public void captureLogs() {
        logger = (Logger) LogManager.getRootLogger();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    public void stopCapturingLogs() {
        logger.removeAppender(appender);
        appender.stop();
    }

    @Test
    public void testSourceConnectorDoesNotLogPropertyValues() {
        assertConnectorLogging(new CamelSourceConnector());
    }

    @Test
    public void testSinkConnectorDoesNotLogPropertyValues() {
        assertConnectorLogging(new CamelSinkConnector());
    }

    @Test
    public void testMainMasksCredentialsAndLogsOtherPropertyValues() throws Exception {
        Map<String, String> props = properties();
        props.put("sasl.jaas.config", JAAS);
        props.put("producer.override.sasl.jaas.config", JAAS);
        props.put("consumer.override.sasl.jaas.config", JAAS);
        props.put(SECRET_KEY, SECRET_VALUE);
        DefaultCamelContext context = new DefaultCamelContext();
        CamelKafkaConnectMain main = CamelKafkaConnectMain.builder("direct:start", "direct:end")
            .withProperties(props)
            .build(context);
        try {
            assertThat(main.getInitialProperties()).containsAllEntriesOf(props);
            assertThat(String.join("\n", messages))
                .contains("Setting initial properties in Camel context:", "custom.setting=" + CUSTOM_VALUE,
                    JAAS_KEY + "=xxxxxxx", "sasl.jaas.config=xxxxxxx",
                    "producer.override.sasl.jaas.config=xxxxxxx", "consumer.override.sasl.jaas.config=xxxxxxx",
                    SECRET_KEY + "=xxxxxxx")
                .doesNotContain(JAAS, USERNAME, PASSWORD, "ScramLoginModule", SECRET_VALUE);
        } finally {
            main.stop();
            context.stop();
        }
    }

    private void assertConnectorLogging(Connector connector) {
        Map<String, String> props = properties();
        connector.start(props);
        try {
            assertThat(connector.taskConfigs(1)).containsExactly(props);
            assertSafeLogs();
        } finally {
            connector.stop();
        }
    }

    private Map<String, String> properties() {
        Map<String, String> props = new HashMap<>();
        props.put(JAAS_KEY, JAAS);
        props.put("custom.setting", CUSTOM_VALUE);
        return props;
    }

    private void assertSafeLogs() {
        assertThat(String.join("\n", messages))
            .contains(JAAS_KEY, "custom.setting")
            .doesNotContain(JAAS, USERNAME, PASSWORD, CUSTOM_VALUE);
    }
}
