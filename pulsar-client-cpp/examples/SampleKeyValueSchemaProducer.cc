/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include <iostream>
#include <thread>
#include <pulsar/Client.h>
#include <lib/LogUtils.h>

DECLARE_LOG_OBJECT()

using namespace pulsar;

int main() {
    Client client("pulsar://localhost:6650");

    std::string jsonSchema =
        "{\"type\":\"record\",\"name\":\"cpx\",\"fields\":[{\"name\":\"re\",\"type\":\"double\"},{\"name\":"
        "\"im\",\"type\":\"double\"}]}";

    SchemaInfo keySchema(JSON, "key-json", jsonSchema);
    SchemaInfo valueSchema(JSON, "value-json", jsonSchema);
    SchemaInfo keyValueSchema(keySchema, valueSchema, SEPARATED);
    std::cout << keyValueSchema.getSchema() << std::endl;

    ProducerConfiguration producerConfiguration;
    producerConfiguration.setSchema(keyValueSchema);

    Producer producer;
    Result result =
        client.createProducer("persistent://public/default/kv-schema", producerConfiguration, producer);
    if (result != ResultOk) {
        LOG_ERROR("Error creating producer: " << result);
        return -1;
    }

    std::string jsonData = "{\"re\":2.1,\"im\":1.23}";

    KeyValue keyValue(jsonData, jsonData, SEPARATED);

    Message msg = MessageBuilder().setContent(keyValue).setProperty("x", "1").build();
    producer.send(msg);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    LOG_ERROR("send message ok");

    client.close();
}