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
#include <pulsar/KeyValue.h>
#include "KeyValueImpl.h"

namespace pulsar {

KeyValue::KeyValue() : impl_() {}

KeyValue::KeyValue(KeyValueImplPtr impl) : impl_(impl) {}

KeyValue::KeyValue(const std::string &key, const std::string &value,
                   const KeyValueEncodingType &keyValueEncodingType)
    : impl_(std::make_shared<KeyValueImpl>(key, value, keyValueEncodingType)) {}

std::string KeyValue::getValue() const { return impl_->getValue(); }

std::string KeyValue::getKey() const { return impl_->getKey(); }

KeyValueEncodingType KeyValue::getEncodingType() const { return impl_->getEncodingType(); }

}  // namespace pulsar
