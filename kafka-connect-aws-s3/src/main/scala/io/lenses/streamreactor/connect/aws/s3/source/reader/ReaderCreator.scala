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
package io.lenses.streamreactor.connect.aws.s3.source.reader

import cats.implicits.toBifunctorOps
import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.formats.reader.S3FormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.formats.reader.SourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocationWithLine
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.IteratorOps

class ReaderCreator(
  format:      FormatSelection,
  targetTopic: String,
  partitionFn: String => Option[Int],
)(
  implicit
  connectorTaskId:  ConnectorTaskId,
  storageInterface: StorageInterface,
) extends LazyLogging {

  def create(pathWithLine: RemoteS3PathLocationWithLine): Either[Throwable, ResultReader] =
    createInner(pathWithLine).map(new ResultReader(_, targetTopic, partitionFn))

  private def createInner(
    pathWithLine: RemoteS3PathLocationWithLine,
  ): Either[Throwable, S3FormatStreamReader[_ <: SourceData]] = {
    logger.info(s"[${connectorTaskId.show}] Reading next file: ${pathWithLine.file} from line ${pathWithLine.line}")
    val file = pathWithLine.file
    for {
      inputStream <- storageInterface.getBlob(file).leftMap(new IllegalStateException(_))
      fileSize    <- storageInterface.getBlobSize(file).leftMap(new IllegalStateException(_))
      reader = S3FormatStreamReader(inputStream,
                                    fileSize,
                                    format,
                                    file,
                                    () => storageInterface.getBlob(file).leftMap(new IllegalStateException(_)),
      )
      _ <- if (!pathWithLine.isFromStart) IteratorOps.skip(reader, pathWithLine.line) else Right(())
    } yield reader
  }

}
