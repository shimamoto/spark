/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.linalg

import play.api.libs.json._, Json.{parse => parseJson}

private[ml] object JsonVectorConverter {

  /**
   * Parses the JSON representation of a vector into a [[Vector]].
   */
  def fromJson(json: String): Vector = {
    val jsValue = parseJson(json)
    (jsValue \ "type").as[Int] match {
      case 0 => // sparse
        val size = (jsValue \ "size").as[Int]
        val indices = (jsValue \ "indices").as[Seq[Int]].toArray
        val values = (jsValue \ "values").as[Seq[Double]].toArray
        Vectors.sparse(size, indices, values)
      case 1 => // dense
        val values = (jsValue \ "values").as[Seq[Double]].toArray
        Vectors.dense(values)
      case _ =>
        throw new IllegalArgumentException(s"Cannot parse $json into a vector.")
    }
  }

  /**
   * Coverts the vector to a JSON string.
   */
  def toJson(v: Vector): String = {
    v match {
      case SparseVector(size, indices, values) =>
        val jsValue = Json.obj("type" -> 0,
          "size" -> size,
          "indices" -> indices.toSeq,
          "values" -> values.toSeq)
        jsValue.toString
      case DenseVector(values) =>
        val jsValue = Json.obj("type" -> 1, "values" -> values.toSeq)
        jsValue.toString
    }
  }
}
