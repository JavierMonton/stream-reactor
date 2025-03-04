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
package io.lenses.streamreactor.connect.aws.s3.formats.reader

import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.io.text.OptionIteratorAdaptor

class CustomTextFormatStreamReader(
  bucketAndPath: S3Location,
  lineReaderFn:  () => Option[String],
  closeFn:       () => Unit,
) extends S3FormatStreamReader[StringSourceData] {

  private val adaptor = new OptionIteratorAdaptor(() => lineReaderFn())

  override def getBucketAndPath: S3Location = bucketAndPath

  override def getLineNumber: Long = adaptor.getLine()

  override def hasNext: Boolean = adaptor.hasNext

  override def next(): StringSourceData = StringSourceData(adaptor.next(), adaptor.getLine())

  override def close(): Unit = closeFn()
}
