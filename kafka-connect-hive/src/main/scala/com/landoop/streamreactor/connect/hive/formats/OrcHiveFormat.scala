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
package com.landoop.streamreactor.connect.hive.formats

import com.landoop.streamreactor.connect.hive.kerberos.UgiExecute
import com.landoop.streamreactor.connect.hive.orc.OrcSink
import com.landoop.streamreactor.connect.hive.OrcSinkConfig
import com.landoop.streamreactor.connect.hive.OrcSourceConfig
import com.landoop.streamreactor.connect.hive.Serde
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

import scala.util.Try

object OrcHiveFormat extends HiveFormat {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def serde = Serde(
    "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
    "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
    "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
    Map("org.apache.hadoop.hive.ql.io.orc.OrcSerde" -> "1"),
  )

  override def writer(path: Path, schema: Schema)(implicit fs: FileSystem): HiveWriter = new HiveWriter {
    logger.debug(s"Creating orc writer at $path")

    val sink: OrcSink = com.landoop.streamreactor.connect.hive.orc.sink(path, schema, OrcSinkConfig(overwrite = true))
    Try(fs.setPermission(path, FsPermission.valueOf("-rwxrwxrwx")))

    val cretedTimestamp:   Long = System.currentTimeMillis()
    var lastKnownFileSize: Long = if (fs.exists(path)) fs.getFileStatus(path).getLen else 0L
    var readFileSize = false
    var count        = 0L

    override def write(struct: Struct): Long = {
      sink.write(struct)
      count        = count + 1
      readFileSize = true
      count
    }

    override def close(): Unit = {
      logger.debug(s"Closing orc writer at path $path")
      sink.close()
    }
    override def file:         Path = path
    override def currentCount: Long = count
    override def createdTime:  Long = cretedTimestamp
    override def fileSize: Long = {
      if (readFileSize) {
        if (fs.exists(path)) {
          lastKnownFileSize = fs.getFileStatus(path).getLen
          readFileSize      = false
        } else {
          lastKnownFileSize = 0L
        }
      }

      lastKnownFileSize
    }
  }

  override def reader(path: Path, startAt: Int, schema: Schema, ugi: UgiExecute)(implicit fs: FileSystem): HiveReader =
    new HiveReader {

      logger.debug(s"Creating orc reader for $path with offset $startAt")
      val reader = com.landoop.streamreactor.connect.hive.orc.source(path, OrcSourceConfig(), ugi)
      var offset = startAt

      override def iterator: Iterator[Record] = reader.iterator.map { struct =>
        val record = Record(struct, path, offset)
        offset = offset + 1
        record
      }

      override def close(): Unit = reader.close()
    }
}
