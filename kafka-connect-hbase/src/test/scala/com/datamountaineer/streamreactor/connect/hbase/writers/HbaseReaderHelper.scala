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
package com.datamountaineer.streamreactor.connect.hbase.writers

import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
import com.datamountaineer.streamreactor.connect.hbase.HbaseHelper
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName

import scala.jdk.CollectionConverters.IterableHasAsScala

object HbaseReaderHelper {
  def createConnection: Connection =
    ConnectionFactory.createConnection(HBaseConfiguration.create())

  def getAllRecords(tableName: String, columnFamily: String)(implicit connection: Connection): List[HbaseRowData] =
    HbaseHelper.withTable(TableName.valueOf(tableName)) { tbl =>
      val scan = new Scan()
      scan.addFamily(columnFamily.fromString())
      val scanner = tbl.getScanner(scan)
      scanner.asScala.map { rs =>
        val cells = rs.rawCells().map { cell =>
          Bytes.toString(CellUtil.cloneQualifier(cell)) -> CellUtil.cloneValue(cell)
        }.toMap
        HbaseRowData(rs.getRow, cells)
      }.toList
    }

}

case class HbaseRowData(key: Array[Byte], cells: Map[String, Array[Byte]])
