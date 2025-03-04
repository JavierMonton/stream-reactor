/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.formats

import io.lenses.streamreactor.connect.aws.s3.formats.bytes.ByteArrayUtils
import io.lenses.streamreactor.connect.aws.s3.formats.bytes.BytesOutputRow
import io.lenses.streamreactor.connect.aws.s3.formats.bytes.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.formats.reader.BytesFormatWithSizesStreamReader
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.model.BytesOutputRowTest
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream

class BytesFormatWithSizesStreamReaderTest extends AnyFlatSpec with MockitoSugar with Matchers {

  import BytesOutputRowTest._

  private val bucketAndPath: S3Location = mock[S3Location]

  private val bytesKeyAndValueWithSizes2: Array[Byte] =
    ByteArrayUtils.longToByteArray(4L) ++ ByteArrayUtils.longToByteArray(3L) ++ "caketea".getBytes

  private val outputKeyAndValueWithSizes2 = BytesOutputRow(Some(4L), Some(3L), "cake".getBytes, "tea".getBytes)

  "next" should "return single record of key and values with sizes" in {

    val target = new BytesFormatWithSizesStreamReader(new ByteArrayInputStream(bytesKeyAndValueWithSizes),
                                                      bytesKeyAndValueWithSizes.length.toLong,
                                                      bucketAndPath,
                                                      bytesWriteMode = BytesWriteMode.KeyAndValueWithSizes,
    )

    checkRecord(target, outputKeyAndValueWithSizes)

    target.hasNext should be(false)

  }

  "next" should "return multiple records of key and values with sizes" in {

    val allElements = bytesKeyAndValueWithSizes ++ bytesKeyAndValueWithSizes2
    val target = new BytesFormatWithSizesStreamReader(new ByteArrayInputStream(allElements),
                                                      allElements.length.toLong,
                                                      bucketAndPath,
                                                      bytesWriteMode = BytesWriteMode.KeyAndValueWithSizes,
    )

    checkRecord(target, outputKeyAndValueWithSizes)

    checkRecord(target, outputKeyAndValueWithSizes2)

    target.hasNext should be(false)

  }

  private def checkRecord(target: BytesFormatWithSizesStreamReader, expectedOutputRow: BytesOutputRow) = {
    target.hasNext should be(true)
    val record = target.next()
    checkEqualsByteArrayValue(record.data, expectedOutputRow)
  }
}
