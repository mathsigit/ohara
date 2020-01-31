/*
 * Copyright 2019 is-land
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

package com.island.ohara.shabondi

import java.util.concurrent.CompletableFuture

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.island.ohara.common.data.{Cell, Row}
import spray.json._

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters
import scala.concurrent.Future

private[shabondi] object JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  type RowData = Map[String, JsValue] // column, value

  implicit val rowDataFormat: RootJsonFormat[RowData] = new RootJsonFormat[RowData] {
    override def write(obj: RowData): JsValue = JsObject(obj)
    override def read(json: JsValue): RowData = json.asJsObject.fields
  }

  def toRow(rowData: RowData): Row = toRow(JsObject(rowData))
  def toRowData(row: Row): RowData = toJson(row).fields

  private val TAGS_KEY: String = "tags"

  private def toJson(row: Row): JsObject = JsObject(
    row.cells().asScala.map(cell => cell.name() -> toJson(cell.value())).toMap + (TAGS_KEY -> JsArray(
      row.tags().asScala.map(JsString(_)).toVector
    ))
  )

  private[this] def toJson(value: Any): JsValue = value match {
    //--------[primitive type]--------//
    case b: Boolean     => JsBoolean(b)
    case s: String      => JsString(s)
    case i: Short       => JsNumber(i)
    case i: Int         => JsNumber(i)
    case i: Long        => JsNumber(i)
    case i: Float       => JsNumber(i)
    case i: Double      => JsNumber(i)
    case _: Array[Byte] => JsString("binary data")
    case b: Byte        => JsNumber(b)
    //--------[for scala]--------//
    case i: BigDecimal  => JsNumber(i)
    case s: Iterable[_] => JsArray(s.map(toJson).toVector)
    //--------[ohara data]--------//
    case c: Cell[_] => JsObject(c.name() -> toJson(c.value()))
    case r: Row     => toJson(r)
    //--------[for java]--------//
    case i: java.math.BigDecimal  => JsNumber(i)
    case s: java.lang.Iterable[_] => JsArray(s.asScala.map(toJson).toVector)
    //--------[other]--------//
    case _ => throw new IllegalArgumentException(s"${value.getClass.getName} is unsupported!!!")
  }

  private def toRow(obj: JsObject): Row = Row.of(
    noJsNull(obj.fields)
      .get(TAGS_KEY)
      .map {
        case s: JsArray => s
        case _          => throw DeserializationException(s"$TAGS_KEY must be array type", fieldNames = List(TAGS_KEY))
      }
      .map(_.elements.map(_.convertTo[String]))
      .getOrElse(Seq.empty)
      .asJava,
    noJsNull(obj.fields.filter(_._1 != TAGS_KEY)).map {
      case (name, value) =>
        Cell.of(name, toValue(value))
    }.toSeq: _*
  )

  private def noJsNull(fields: Map[String, JsValue]): Map[String, JsValue] = fields.filter {
    _._2 match {
      case JsNull => false
      case _      => true
    }
  }

  private def toValue(value: JsValue): Any = value match {
    case JsNull       => throw new IllegalArgumentException("null should be eliminated")
    case JsBoolean(b) => b
    case JsNumber(i)  => i
    case JsString(s)  => s
    case JsArray(es) =>
      es.filter {
          case JsNull => false
          case _      => true
        }
        .map(toValue)
        .toList
    case obj: JsObject => toRow(obj)
  }
}

private[shabondi] object ConvertSupport {
  implicit class ScalaFutureConverter[T](completableFuture: java.util.concurrent.Future[T]) {
    def toScala: Future[T] = {
      FutureConverters.toScala(completableFuture.asInstanceOf[CompletableFuture[T]])
    }
  }
}
