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

package org.apache.spark.sql.types

import scala.collection.mutable

import play.api.libs.json._

import org.apache.spark.annotation.InterfaceStability


/**
 * Metadata is a wrapper over Map[String, Any] that limits the value type to simple ones: Boolean,
 * Long, Double, String, Metadata, Array[Boolean], Array[Long], Array[Double], Array[String], and
 * Array[Metadata]. JSON is used for serialization.
 *
 * The default constructor is private. User should use either [[MetadataBuilder]] or
 * `Metadata.fromJson()` to create Metadata instances.
 *
 * @param map an immutable map that stores the data
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
sealed class Metadata private[types] (private[types] val map: Map[String, Any])
  extends Serializable {

  /** No-arg constructor for kryo. */
  protected def this() = this(null)

  /** Tests whether this Metadata contains a binding for a key. */
  def contains(key: String): Boolean = map.contains(key)

  /** Gets a Long. */
  def getLong(key: String): Long = get(key)

  /** Gets a Double. */
  def getDouble(key: String): Double = get(key)

  /** Gets a Boolean. */
  def getBoolean(key: String): Boolean = get(key)

  /** Gets a String. */
  def getString(key: String): String = get(key)

  /** Gets a Metadata. */
  def getMetadata(key: String): Metadata = get(key)

  /** Gets a Long array. */
  def getLongArray(key: String): Array[Long] = get(key)

  /** Gets a Double array. */
  def getDoubleArray(key: String): Array[Double] = get(key)

  /** Gets a Boolean array. */
  def getBooleanArray(key: String): Array[Boolean] = get(key)

  /** Gets a String array. */
  def getStringArray(key: String): Array[String] = get(key)

  /** Gets a Metadata array. */
  def getMetadataArray(key: String): Array[Metadata] = get(key)

  /** Converts to its JSON representation. */
  def json: String = jsonValue.toString

  override def toString: String = json

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: Metadata if map.size == that.map.size =>
        map.keysIterator.forall { key =>
          that.map.get(key) match {
            case Some(otherValue) =>
              val ourValue = map.get(key).get
              (ourValue, otherValue) match {
                case (v0: Array[Long], v1: Array[Long]) => java.util.Arrays.equals(v0, v1)
                case (v0: Array[Double], v1: Array[Double]) => java.util.Arrays.equals(v0, v1)
                case (v0: Array[Boolean], v1: Array[Boolean]) => java.util.Arrays.equals(v0, v1)
                case (v0: Array[AnyRef], v1: Array[AnyRef]) => java.util.Arrays.equals(v0, v1)
                case (v0, v1) => v0 == v1
              }
            case None => false
          }
        }
      case other =>
        false
    }
  }

  private lazy val _hashCode: Int = Metadata.hash(this)
  override def hashCode: Int = _hashCode

  private def get[T](key: String): T = {
    map(key).asInstanceOf[T]
  }

  private[sql] def jsonValue: JsValue = Metadata.toJsonValue(this)
}

/**
 * @since 1.3.0
 */
@InterfaceStability.Stable
object Metadata {

  private[this] val _empty = new Metadata(Map.empty)

  /** Returns an empty Metadata. */
  def empty: Metadata = _empty

  /** Creates a Metadata instance from JSON. */
  def fromJson(json: String): Metadata = {
    fromJObject(Json.parse(json).asInstanceOf[JsObject])
  }

  /** Creates a Metadata instance from JSON AST. */
  private[sql] def fromJObject(jObj: JsObject): Metadata = {
    val builder = new MetadataBuilder
    jObj.fields.foreach {
      case (key, JsNumber(value)) if value.isValidLong =>
        builder.putLong(key, value.longValue)
      case (key, JsNumber(value)) =>
        builder.putDouble(key, value.doubleValue)
      case (key, JsBoolean(value)) =>
        builder.putBoolean(key, value)
      case (key, JsString(value)) =>
        builder.putString(key, value)
      case (key, o: JsObject) =>
        builder.putMetadata(key, fromJObject(o))
      case (key, JsArray(value)) =>
        if (value.isEmpty) {
          // If it is an empty array, we cannot infer its element type. We put an empty Array[Long].
          builder.putLongArray(key, Array.empty)
        } else {
          value.head match {
            case x: JsNumber if x.value.isValidLong =>
              builder.putLongArray(key, value.map(_.as[Long]).toArray)
            case _: JsNumber =>
              builder.putDoubleArray(key, value.map(_.as[Double]).toArray)
            case _: JsBoolean =>
              builder.putBooleanArray(key, value.map(_.as[Boolean]).toArray)
            case _: JsString =>
              builder.putStringArray(key, value.map(_.as[String]).toArray)
            case _: JsObject =>
              builder.putMetadataArray(
                key, value.asInstanceOf[Seq[JsObject]].map(fromJObject).toArray)
            case other =>
              throw new RuntimeException(s"Do not support array of type ${other.getClass}.")
          }
        }
      case (key, JsNull) =>
        builder.putNull(key)
      case (key, other) =>
        throw new RuntimeException(s"Do not support type ${other.getClass}.")
    }
    builder.build()
  }

  /** Converts to JSON AST. */
  private def toJsonValue(obj: Any): JsValue = {
    obj match {
      case map: Map[_, _] =>
        val fields = map.toList.map { case (k, v) => (k.toString, toJsonValue(v)) }
        JsObject(fields)
      case arr: Array[_] =>
        val values = arr.toList.map(toJsonValue)
        JsArray(values)
      case x: Long =>
        JsNumber(x)
      case x: Double =>
        JsNumber(x)
      case x: Boolean =>
        JsBoolean(x)
      case x: String =>
        JsString(x)
      case x: Metadata =>
        toJsonValue(x.map)
      case other =>
        throw new RuntimeException(s"Do not support type ${other.getClass}.")
    }
  }

  /** Computes the hash code for the types we support. */
  private def hash(obj: Any): Int = {
    obj match {
      case map: Map[_, _] =>
        map.mapValues(hash).##
      case arr: Array[_] =>
        // Seq.empty[T] has the same hashCode regardless of T.
        arr.toSeq.map(hash).##
      case x: Long =>
        x.##
      case x: Double =>
        x.##
      case x: Boolean =>
        x.##
      case x: String =>
        x.##
      case x: Metadata =>
        hash(x.map)
      case other =>
        throw new RuntimeException(s"Do not support type ${other.getClass}.")
    }
  }
}

/**
 * Builder for [[Metadata]]. If there is a key collision, the latter will overwrite the former.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
class MetadataBuilder {

  private val map: mutable.Map[String, Any] = mutable.Map.empty

  /** Returns the immutable version of this map.  Used for java interop. */
  protected def getMap = map.toMap

  /** Include the content of an existing [[Metadata]] instance. */
  def withMetadata(metadata: Metadata): this.type = {
    map ++= metadata.map
    this
  }

  /** Puts a null. */
  def putNull(key: String): this.type = put(key, null)

  /** Puts a Long. */
  def putLong(key: String, value: Long): this.type = put(key, value)

  /** Puts a Double. */
  def putDouble(key: String, value: Double): this.type = put(key, value)

  /** Puts a Boolean. */
  def putBoolean(key: String, value: Boolean): this.type = put(key, value)

  /** Puts a String. */
  def putString(key: String, value: String): this.type = put(key, value)

  /** Puts a [[Metadata]]. */
  def putMetadata(key: String, value: Metadata): this.type = put(key, value)

  /** Puts a Long array. */
  def putLongArray(key: String, value: Array[Long]): this.type = put(key, value)

  /** Puts a Double array. */
  def putDoubleArray(key: String, value: Array[Double]): this.type = put(key, value)

  /** Puts a Boolean array. */
  def putBooleanArray(key: String, value: Array[Boolean]): this.type = put(key, value)

  /** Puts a String array. */
  def putStringArray(key: String, value: Array[String]): this.type = put(key, value)

  /** Puts a [[Metadata]] array. */
  def putMetadataArray(key: String, value: Array[Metadata]): this.type = put(key, value)

  /** Builds the [[Metadata]] instance. */
  def build(): Metadata = {
    new Metadata(map.toMap)
  }

  private def put(key: String, value: Any): this.type = {
    map.put(key, value)
    this
  }

  def remove(key: String): this.type = {
    map.remove(key)
    this
  }
}
