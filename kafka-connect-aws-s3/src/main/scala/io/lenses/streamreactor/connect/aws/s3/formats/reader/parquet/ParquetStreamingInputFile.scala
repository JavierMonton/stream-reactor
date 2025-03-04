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
package io.lenses.streamreactor.connect.aws.s3.formats.reader.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream

class ParquetStreamingInputFile(fileSize: Long, newStreamF: () => SeekableInputStream)
    extends InputFile
    with LazyLogging {

  override def getLength: Long = fileSize

  override def newStream(): SeekableInputStream = {
    val stream = newStreamF()
    logger.debug(s"Creating new stream for file $stream")
    stream
  }

}
