/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wibidata.appliance.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroUtils {
  private AvroUtils() { }

  /**
   * Standard Avro/JSON encoder.
   *
   * @param record Avro record to encode.
   * @return JSON-encoded value.
   * @throws IOException on error.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static String toAvroJsonString(
      final IndexedRecord record
  ) throws IOException {
    final Schema schema = record.getSchema();
    try {
      final ByteArrayOutputStream jsonOutputStream = new ByteArrayOutputStream();
      final JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, jsonOutputStream);

      final SpecificDatumWriter writer = new SpecificDatumWriter(record.getClass());
      writer.write(record, jsonEncoder);
      jsonEncoder.flush();
      return new String(jsonOutputStream.toByteArray(), "UTF-8");
    } catch (IOException ioe) {
      throw new RuntimeException("Internal error: " + ioe);
    }
  }

  /**
   * Standard Avro JSON decoder.
   *
   * @param json JSON string to decode.
   * @param schema Schema of the value to decode.
   * @return the decoded value.
   * @throws IOException on error.
   */
  public static <T extends IndexedRecord> T fromAvroJsonString(
      final String json,
      final Schema schema
  ) throws IOException {
    final InputStream jsonInput = new ByteArrayInputStream(json.getBytes("UTF-8"));
    final Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonInput);
    final SpecificDatumReader<T> reader = new SpecificDatumReader<T>(schema);
    return reader.read(null, decoder);
  }
}
