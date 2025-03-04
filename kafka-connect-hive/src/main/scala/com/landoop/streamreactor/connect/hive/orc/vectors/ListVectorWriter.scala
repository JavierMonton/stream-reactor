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
package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector

class ListVectorWriter[V <: ColumnVector, T](writer: OrcVectorWriter[V, T])
    extends OrcVectorWriter[ListColumnVector, Seq[T]] {

  override def write(vector: ListColumnVector, offset: Int, value: Option[Seq[T]]): Unit = {

    // Each list is composed of a range of elements in the underlying child ColumnVector.
    // The range for list i is offsets[i]..offsets[i]+lengths[i]-1 inclusive.

    // the offset for this value points to the start location in the underlying
    // vector, and so we need to find where the last value finished in the child vector.
    // if the offset is zero then we don't need to do anything as it will be
    // 0 in the underlying vector too
    val start =
      if (offset == 0) 0
      else vector.offsets(offset - 1).toInt + vector.lengths(offset - 1).toInt
    vector.offsets(offset) = start.toLong

    value match {
      case Some(ts) =>
        vector.lengths(offset) = value.size.toLong
        val elementVector = vector.child.asInstanceOf[V]
        ts.zipWithIndex.foreach {
          case (t, k) =>
            writer.write(elementVector, start + k, Option(t))
        }
      case _ =>
        vector.lengths(offset) = 0
    }
  }
}
