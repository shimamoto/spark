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

package org.apache.spark.util

import java.util.{Properties, UUID}

import scala.collection.JavaConverters._
import scala.collection.Map

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import play.api.libs.json._

import org.apache.spark._
import org.apache.spark.executor._
import org.apache.spark.rdd.RDDOperationScope
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage._

/**
 * Serializes SparkListener events to/from JSON.  This protocol provides strong backwards-
 * and forwards-compatibility guarantees: any version of Spark should be able to read JSON output
 * written by any other version, including newer versions.
 *
 * JsonProtocolSuite contains backwards-compatibility tests which check that the current version of
 * JsonProtocol is able to read output written by earlier versions.  We do not currently have tests
 * for reading newer JSON output with older Spark versions.
 *
 * To ensure that we provide these guarantees, follow these rules when modifying these methods:
 *
 *  - Never delete any JSON fields.
 *  - Any new JSON fields should be optional; use `Utils.jsonOption` when reading these fields
 *    in `*FromJson` methods.
 */
private[spark] object JsonProtocol {
  // TODO: Remove this file and put JSON serialization into each individual class.

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /** ------------------------------------------------- *
   * JSON serialization methods for SparkListenerEvents |
   * -------------------------------------------------- */

  def sparkEventToJson(event: SparkListenerEvent): JsValue = {
    event match {
      case stageSubmitted: SparkListenerStageSubmitted =>
        stageSubmittedToJson(stageSubmitted)
      case stageCompleted: SparkListenerStageCompleted =>
        stageCompletedToJson(stageCompleted)
      case taskStart: SparkListenerTaskStart =>
        taskStartToJson(taskStart)
      case taskGettingResult: SparkListenerTaskGettingResult =>
        taskGettingResultToJson(taskGettingResult)
      case taskEnd: SparkListenerTaskEnd =>
        taskEndToJson(taskEnd)
      case jobStart: SparkListenerJobStart =>
        jobStartToJson(jobStart)
      case jobEnd: SparkListenerJobEnd =>
        jobEndToJson(jobEnd)
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        environmentUpdateToJson(environmentUpdate)
      case blockManagerAdded: SparkListenerBlockManagerAdded =>
        blockManagerAddedToJson(blockManagerAdded)
      case blockManagerRemoved: SparkListenerBlockManagerRemoved =>
        blockManagerRemovedToJson(blockManagerRemoved)
      case unpersistRDD: SparkListenerUnpersistRDD =>
        unpersistRDDToJson(unpersistRDD)
      case applicationStart: SparkListenerApplicationStart =>
        applicationStartToJson(applicationStart)
      case applicationEnd: SparkListenerApplicationEnd =>
        applicationEndToJson(applicationEnd)
      case executorAdded: SparkListenerExecutorAdded =>
        executorAddedToJson(executorAdded)
      case executorRemoved: SparkListenerExecutorRemoved =>
        executorRemovedToJson(executorRemoved)
      case logStart: SparkListenerLogStart =>
        logStartToJson(logStart)
      case metricsUpdate: SparkListenerExecutorMetricsUpdate =>
        executorMetricsUpdateToJson(metricsUpdate)
      case blockUpdate: SparkListenerBlockUpdated =>
        blockUpdateToJson(blockUpdate)
      case _ => Json.parse(mapper.writeValueAsString(event))
    }
  }

  def stageSubmittedToJson(stageSubmitted: SparkListenerStageSubmitted): JsValue = {
    val stageInfo = stageInfoToJson(stageSubmitted.stageInfo)
    val properties = propertiesToJson(stageSubmitted.properties)
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.stageSubmitted,
      "Stage Info" -> stageInfo) ++
    (__ \ "Properties").writeNullable[JsValue].writes(properties)
  }

  def stageCompletedToJson(stageCompleted: SparkListenerStageCompleted): JsValue = {
    val stageInfo = stageInfoToJson(stageCompleted.stageInfo)
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.stageCompleted,
      "Stage Info" -> stageInfo)
  }

  def taskStartToJson(taskStart: SparkListenerTaskStart): JsValue = {
    val taskInfo = taskStart.taskInfo
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskStart,
      "Stage ID" -> taskStart.stageId,
      "Stage Attempt ID" -> taskStart.stageAttemptId,
      "Task Info" -> taskInfoToJson(taskInfo))
  }

  def taskGettingResultToJson(taskGettingResult: SparkListenerTaskGettingResult): JsValue = {
    val taskInfo = taskGettingResult.taskInfo
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskGettingResult,
      "Task Info" -> taskInfoToJson(taskInfo))
  }

  def taskEndToJson(taskEnd: SparkListenerTaskEnd): JsValue = {
    val taskEndReason = taskEndReasonToJson(taskEnd.reason)
    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics
    val taskMetricsJson = if (taskMetrics != null) Some(taskMetricsToJson(taskMetrics)) else None
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskEnd,
      "Stage ID" -> taskEnd.stageId,
      "Stage Attempt ID" -> taskEnd.stageAttemptId,
      "Task Type" -> taskEnd.taskType,
      "Task End Reason" -> taskEndReason,
      "Task Info" -> taskInfoToJson(taskInfo)) ++
    (__ \ "Task Metrics").writeNullable[JsValue].writes(taskMetricsJson)
  }

  def jobStartToJson(jobStart: SparkListenerJobStart): JsValue = {
    val properties = propertiesToJson(jobStart.properties)
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.jobStart,
      "Job ID" -> jobStart.jobId,
      "Submission Time" -> jobStart.time,
      "Stage Infos" -> jobStart.stageInfos.map(stageInfoToJson),  // Added in Spark 1.2.0
      "Stage IDs" -> jobStart.stageIds) ++
    (__ \ "Properties").writeNullable[JsValue].writes(properties)
  }

  def jobEndToJson(jobEnd: SparkListenerJobEnd): JsValue = {
    val jobResult = jobResultToJson(jobEnd.jobResult)
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.jobEnd,
      "Job ID" -> jobEnd.jobId,
      "Completion Time" -> jobEnd.time,
      "Job Result" -> jobResult)
  }

  def environmentUpdateToJson(environmentUpdate: SparkListenerEnvironmentUpdate): JsValue = {
    val environmentDetails = environmentUpdate.environmentDetails
    val jvmInformation = mapToJson(environmentDetails("JVM Information").toMap)
    val sparkProperties = mapToJson(environmentDetails("Spark Properties").toMap)
    val systemProperties = mapToJson(environmentDetails("System Properties").toMap)
    val classpathEntries = mapToJson(environmentDetails("Classpath Entries").toMap)
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.environmentUpdate,
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> systemProperties,
      "Classpath Entries" -> classpathEntries)
  }

  def blockManagerAddedToJson(blockManagerAdded: SparkListenerBlockManagerAdded): JsValue = {
    val blockManagerId = blockManagerIdToJson(blockManagerAdded.blockManagerId)
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockManagerAdded,
      "Block Manager ID" -> blockManagerId,
      "Maximum Memory" -> blockManagerAdded.maxMem,
      "Timestamp" -> blockManagerAdded.time) ++
    (__ \ "Maximum Onheap Memory").writeNullable[Long].writes(blockManagerAdded.maxOnHeapMem) ++
    (__ \ "Maximum Offheap Memory").writeNullable[Long].writes(blockManagerAdded.maxOffHeapMem)
  }

  def blockManagerRemovedToJson(blockManagerRemoved: SparkListenerBlockManagerRemoved): JsValue = {
    val blockManagerId = blockManagerIdToJson(blockManagerRemoved.blockManagerId)
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockManagerRemoved,
      "Block Manager ID" -> blockManagerId,
      "Timestamp" -> blockManagerRemoved.time)
  }

  def unpersistRDDToJson(unpersistRDD: SparkListenerUnpersistRDD): JsValue = {
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.unpersistRDD,
      "RDD ID" -> unpersistRDD.rddId)
  }

  def applicationStartToJson(applicationStart: SparkListenerApplicationStart): JsValue = {
    val event = SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.applicationStart
    val appId = applicationStart.appId
    val appAttemptId = applicationStart.appAttemptId
    (__ \ "Event").write[String].writes(event) ++
    (__ \ "App Name").write[String].writes(applicationStart.appName) ++
    (__ \ "App ID").writeNullable[String].writes(appId) ++
    (__ \ "Timestamp").write[Long].writes(applicationStart.time) ++
    (__ \ "User").write[String].writes(applicationStart.sparkUser) ++
    (__ \ "App Attempt ID").writeNullable[String].writes(appAttemptId) ++
    (__ \ "Driver Logs").writeNullable[JsValue].writes(applicationStart.driverLogs.map(mapToJson))
  }

  def applicationEndToJson(applicationEnd: SparkListenerApplicationEnd): JsValue = {
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.applicationEnd,
      "Timestamp" -> applicationEnd.time)
  }

  def executorAddedToJson(executorAdded: SparkListenerExecutorAdded): JsValue = {
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.executorAdded,
      "Timestamp" -> executorAdded.time,
      "Executor ID" -> executorAdded.executorId,
      "Executor Info" -> executorInfoToJson(executorAdded.executorInfo))
  }

  def executorRemovedToJson(executorRemoved: SparkListenerExecutorRemoved): JsValue = {
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.executorRemoved,
      "Timestamp" -> executorRemoved.time,
      "Executor ID" -> executorRemoved.executorId,
      "Removed Reason" -> executorRemoved.reason)
  }

  def logStartToJson(logStart: SparkListenerLogStart): JsValue = {
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.logStart,
      "Spark Version" -> SPARK_VERSION)
  }

  def executorMetricsUpdateToJson(metricsUpdate: SparkListenerExecutorMetricsUpdate): JsValue = {
    val execId = metricsUpdate.execId
    val accumUpdates = metricsUpdate.accumUpdates
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.metricsUpdate,
      "Executor ID" -> execId,
      "Metrics Updated" -> accumUpdates.map { case (taskId, stageId, stageAttemptId, updates) =>
      Json.obj(
        "Task ID" -> taskId,
        "Stage ID" -> stageId,
        "Stage Attempt ID" -> stageAttemptId,
        "Accumulator Updates" -> JsArray(updates.map(accumulableInfoToJson).toList)
      )}
    )
  }

  def blockUpdateToJson(blockUpdate: SparkListenerBlockUpdated): JsValue = {
    val blockUpdatedInfo = blockUpdatedInfoToJson(blockUpdate.blockUpdatedInfo)
    Json.obj(
      "Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockUpdate,
      "Block Updated Info" -> blockUpdatedInfo)
  }

  /** ------------------------------------------------------------------- *
   * JSON serialization methods for classes SparkListenerEvents depend on |
   * -------------------------------------------------------------------- */

  def stageInfoToJson(stageInfo: StageInfo): JsValue = {
    val rddInfo = JsArray(stageInfo.rddInfos.map(rddInfoToJson).toList)
    val parentIds = JsArray(stageInfo.parentIds.map(JsNumber(_)).toList)
    val submissionTime = stageInfo.submissionTime
    val completionTime = stageInfo.completionTime
    val failureReason = stageInfo.failureReason
    Json.obj(
      "Stage ID" -> stageInfo.stageId,
      "Stage Attempt ID" -> stageInfo.attemptId,
      "Stage Name" -> stageInfo.name,
      "Number of Tasks" -> stageInfo.numTasks,
      "RDD Info" -> rddInfo,
      "Parent IDs" -> parentIds,
      "Details" -> stageInfo.details) ++
    (__ \ "Submission Time").writeNullable[Long].writes(submissionTime) ++
    (__ \ "Completion Time").writeNullable[Long].writes(completionTime) ++
    (__ \ "Failure Reason").writeNullable[String].writes(failureReason) ++
    (__ \ "Accumulables").write[JsValue].writes(accumulablesToJson(stageInfo.accumulables.values))
  }

  def taskInfoToJson(taskInfo: TaskInfo): JsValue = {
    Json.obj(
      "Task ID" -> taskInfo.taskId,
      "Index" -> taskInfo.index,
      "Attempt" -> taskInfo.attemptNumber,
      "Launch Time" -> taskInfo.launchTime,
      "Executor ID" -> taskInfo.executorId,
      "Host" -> taskInfo.host,
      "Locality" -> taskInfo.taskLocality.toString,
      "Speculative" -> taskInfo.speculative,
      "Getting Result Time" -> taskInfo.gettingResultTime,
      "Finish Time" -> taskInfo.finishTime,
      "Failed" -> taskInfo.failed,
      "Killed" -> taskInfo.killed,
      "Accumulables" -> accumulablesToJson(taskInfo.accumulables))
  }

  private lazy val accumulableBlacklist = Set("internal.metrics.updatedBlockStatuses")

  def accumulablesToJson(accumulables: Traversable[AccumulableInfo]): JsArray = {
    JsArray(accumulables
        .filterNot(_.name.exists(accumulableBlacklist.contains))
        .toList.map(accumulableInfoToJson))
  }

  def accumulableInfoToJson(accumulableInfo: AccumulableInfo): JsValue = {
    val name = accumulableInfo.name
    val update = accumulableInfo.update.map { v => accumValueToJson(name, v) }
    val value = accumulableInfo.value.map { v => accumValueToJson(name, v) }
    (__ \ "ID").write[Long].writes(accumulableInfo.id) ++
    (__ \ "Name").writeNullable[String].writes(name) ++
    (__ \ "Update").writeNullable[JsValue].writes(update) ++
    (__ \ "Value").writeNullable[JsValue].writes(value) ++
    (__ \ "Internal").write[Boolean].writes(accumulableInfo.internal) ++
    (__ \ "Count Failed Values").write[Boolean].writes(accumulableInfo.countFailedValues) ++
    (__ \ "Metadata").writeNullable[String].writes(accumulableInfo.metadata)
  }

  /**
   * Serialize the value of an accumulator to JSON.
   *
   * For accumulators representing internal task metrics, this looks up the relevant
   * [[AccumulatorParam]] to serialize the value accordingly. For all other accumulators,
   * this will simply serialize the value as a string.
   *
   * The behavior here must match that of [[accumValueFromJson]]. Exposed for testing.
   */
  private[util] def accumValueToJson(name: Option[String], value: Any): JsValue = {
    if (name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))) {
      value match {
        case v: Int => JsNumber(v)
        case v: Long => JsNumber(v)
        // We only have 3 kind of internal accumulator types, so if it's not int or long, it must be
        // the blocks accumulator, whose type is `java.util.List[(BlockId, BlockStatus)]`
        case v =>
          JsArray(v.asInstanceOf[java.util.List[(BlockId, BlockStatus)]].asScala.toList.map {
            case (id, status) =>
              Json.obj(
                "Block ID" -> id.toString,
                "Status" -> blockStatusToJson(status))
          })
      }
    } else {
      // For all external accumulators, just use strings
      JsString(value.toString)
    }
  }

  def taskMetricsToJson(taskMetrics: TaskMetrics): JsValue = {
    val shuffleReadMetrics: JsValue =
      Json.obj(
        "Remote Blocks Fetched" -> taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
        "Local Blocks Fetched" -> taskMetrics.shuffleReadMetrics.localBlocksFetched,
        "Fetch Wait Time" -> taskMetrics.shuffleReadMetrics.fetchWaitTime,
        "Remote Bytes Read" -> taskMetrics.shuffleReadMetrics.remoteBytesRead,
        "Remote Bytes Read To Disk" -> taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
        "Local Bytes Read" -> taskMetrics.shuffleReadMetrics.localBytesRead,
        "Total Records Read" -> taskMetrics.shuffleReadMetrics.recordsRead)
    val shuffleWriteMetrics: JsValue =
      Json.obj(
        "Shuffle Bytes Written" -> taskMetrics.shuffleWriteMetrics.bytesWritten,
        "Shuffle Write Time" -> taskMetrics.shuffleWriteMetrics.writeTime,
        "Shuffle Records Written" -> taskMetrics.shuffleWriteMetrics.recordsWritten)
    val inputMetrics: JsValue =
      Json.obj(
        "Bytes Read" -> taskMetrics.inputMetrics.bytesRead,
        "Records Read" -> taskMetrics.inputMetrics.recordsRead)
    val outputMetrics: JsValue =
      Json.obj(
        "Bytes Written" -> taskMetrics.outputMetrics.bytesWritten,
        "Records Written" -> taskMetrics.outputMetrics.recordsWritten)
    val updatedBlocks =
      JsArray(taskMetrics.updatedBlockStatuses.toList.map { case (id, status) =>
        Json.obj(
          "Block ID" -> id.toString,
          "Status" -> blockStatusToJson(status))
      })
    Json.obj(
      "Executor Deserialize Time" -> taskMetrics.executorDeserializeTime,
      "Executor Deserialize CPU Time" -> taskMetrics.executorDeserializeCpuTime,
      "Executor Run Time" -> taskMetrics.executorRunTime,
      "Executor CPU Time" -> taskMetrics.executorCpuTime,
      "Result Size" -> taskMetrics.resultSize,
      "JVM GC Time" -> taskMetrics.jvmGCTime,
      "Result Serialization Time" -> taskMetrics.resultSerializationTime,
      "Memory Bytes Spilled" -> taskMetrics.memoryBytesSpilled,
      "Disk Bytes Spilled" -> taskMetrics.diskBytesSpilled,
      "Shuffle Read Metrics" -> shuffleReadMetrics,
      "Shuffle Write Metrics" -> shuffleWriteMetrics,
      "Input Metrics" -> inputMetrics,
      "Output Metrics" -> outputMetrics,
      "Updated Blocks" -> updatedBlocks)
  }

  def taskEndReasonToJson(taskEndReason: TaskEndReason): JsValue = {
    val reason = Utils.getFormattedClassName(taskEndReason)
    val json: JsObject = taskEndReason match {
      case fetchFailed: FetchFailed =>
        val blockManagerAddress = Option(fetchFailed.bmAddress).map(blockManagerIdToJson)
        (__ \ "Block Manager Address").writeNullable[JsValue].writes(blockManagerAddress) ++
        Json.obj(
          "Shuffle ID" -> fetchFailed.shuffleId,
          "Map ID" -> fetchFailed.mapId,
          "Reduce ID" -> fetchFailed.reduceId,
          "Message" -> fetchFailed.message)
      case exceptionFailure: ExceptionFailure =>
        val stackTrace = stackTraceToJson(exceptionFailure.stackTrace)
        val accumUpdates = accumulablesToJson(exceptionFailure.accumUpdates)
        Json.obj(
          "Class Name" -> exceptionFailure.className,
          "Description" -> exceptionFailure.description,
          "Stack Trace" -> stackTrace,
          "Full Stack Trace" -> exceptionFailure.fullStackTrace,
          "Accumulator Updates" -> accumUpdates)
      case taskCommitDenied: TaskCommitDenied =>
        Json.obj(
          "Job ID" -> taskCommitDenied.jobID,
          "Partition ID" -> taskCommitDenied.partitionID,
          "Attempt Number" -> taskCommitDenied.attemptNumber)
      case ExecutorLostFailure(executorId, exitCausedByApp, reason) =>
        Json.obj(
          "Executor ID" -> executorId,
          "Exit Caused By App" -> exitCausedByApp) ++
        (__ \ "Loss Reason").writeNullable[String].writes(reason)
      case taskKilled: TaskKilled =>
        Json.obj("Kill Reason" -> taskKilled.reason)
      case _ => Utils.emptyJson
    }
    Json.obj("Reason" -> reason) ++ json
  }

  def blockManagerIdToJson(blockManagerId: BlockManagerId): JsValue = {
    Json.obj(
      "Executor ID" -> blockManagerId.executorId,
      "Host" -> blockManagerId.host,
      "Port" -> blockManagerId.port)
  }

  def jobResultToJson(jobResult: JobResult): JsValue = {
    val result = Utils.getFormattedClassName(jobResult)
    val json = jobResult match {
      case JobSucceeded => Utils.emptyJson
      case jobFailed: JobFailed =>
        Json.obj("Exception" -> exceptionToJson(jobFailed.exception))
    }
    Json.obj("Result" -> result) ++ json
  }

  def rddInfoToJson(rddInfo: RDDInfo): JsValue = {
    val storageLevel = storageLevelToJson(rddInfo.storageLevel)
    val parentIds = JsArray(rddInfo.parentIds.map(JsNumber(_)).toList)
    Json.obj(
      "RDD ID" -> rddInfo.id,
      "Name" -> rddInfo.name) ++
    (__ \ "Scope").writeNullable[String].writes(rddInfo.scope.map(_.toJson)) ++
    Json.obj(
      "Callsite" -> rddInfo.callSite,
      "Parent IDs" -> parentIds,
      "Storage Level" -> storageLevel,
      "Number of Partitions" -> rddInfo.numPartitions,
      "Number of Cached Partitions" -> rddInfo.numCachedPartitions,
      "Memory Size" -> rddInfo.memSize,
      "Disk Size" -> rddInfo.diskSize)
  }

  def storageLevelToJson(storageLevel: StorageLevel): JsValue = {
    Json.obj(
      "Use Disk" -> storageLevel.useDisk,
      "Use Memory" -> storageLevel.useMemory,
      "Deserialized" -> storageLevel.deserialized,
      "Replication" -> storageLevel.replication)
  }

  def blockStatusToJson(blockStatus: BlockStatus): JsValue = {
    val storageLevel = storageLevelToJson(blockStatus.storageLevel)
    Json.obj(
      "Storage Level" -> storageLevel,
      "Memory Size" -> blockStatus.memSize,
      "Disk Size" -> blockStatus.diskSize)
  }

  def executorInfoToJson(executorInfo: ExecutorInfo): JsValue = {
    Json.obj(
      "Host" -> executorInfo.executorHost,
      "Total Cores" -> executorInfo.totalCores,
      "Log Urls" -> mapToJson(executorInfo.logUrlMap))
  }

  def blockUpdatedInfoToJson(blockUpdatedInfo: BlockUpdatedInfo): JsValue = {
    Json.obj(
      "Block Manager ID" -> blockManagerIdToJson(blockUpdatedInfo.blockManagerId),
      "Block ID" -> blockUpdatedInfo.blockId.toString,
      "Storage Level" -> storageLevelToJson(blockUpdatedInfo.storageLevel),
      "Memory Size" -> blockUpdatedInfo.memSize,
      "Disk Size" -> blockUpdatedInfo.diskSize)
  }

  /** ------------------------------ *
   * Util JSON serialization methods |
   * ------------------------------- */

  def mapToJson(m: Map[String, String]): JsValue = {
    val jsonFields = m.map { case (k, v) => k -> JsString(v) }
    JsObject(jsonFields.toList)
  }

  def propertiesToJson(properties: Properties): Option[JsValue] = {
    Option(properties).map { p =>
      mapToJson(p.asScala)
    }
  }

  def UUIDToJson(id: UUID): JsValue = {
    Json.obj(
      "Least Significant Bits" -> id.getLeastSignificantBits,
      "Most Significant Bits" -> id.getMostSignificantBits)
  }

  def stackTraceToJson(stackTrace: Array[StackTraceElement]): JsValue = {
    JsArray(stackTrace.map { case line =>
      Json.obj(
        "Declaring Class" -> line.getClassName,
        "Method Name" -> line.getMethodName,
        "File Name" -> line.getFileName,
        "Line Number" -> line.getLineNumber)
    }.toList)
  }

  def exceptionToJson(exception: Exception): JsValue = {
    Json.obj(
      "Message" -> exception.getMessage,
      "Stack Trace" -> stackTraceToJson(exception.getStackTrace))
  }


  /** --------------------------------------------------- *
   * JSON deserialization methods for SparkListenerEvents |
   * ---------------------------------------------------- */

  private object SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES {
    val stageSubmitted = Utils.getFormattedClassName(SparkListenerStageSubmitted)
    val stageCompleted = Utils.getFormattedClassName(SparkListenerStageCompleted)
    val taskStart = Utils.getFormattedClassName(SparkListenerTaskStart)
    val taskGettingResult = Utils.getFormattedClassName(SparkListenerTaskGettingResult)
    val taskEnd = Utils.getFormattedClassName(SparkListenerTaskEnd)
    val jobStart = Utils.getFormattedClassName(SparkListenerJobStart)
    val jobEnd = Utils.getFormattedClassName(SparkListenerJobEnd)
    val environmentUpdate = Utils.getFormattedClassName(SparkListenerEnvironmentUpdate)
    val blockManagerAdded = Utils.getFormattedClassName(SparkListenerBlockManagerAdded)
    val blockManagerRemoved = Utils.getFormattedClassName(SparkListenerBlockManagerRemoved)
    val unpersistRDD = Utils.getFormattedClassName(SparkListenerUnpersistRDD)
    val applicationStart = Utils.getFormattedClassName(SparkListenerApplicationStart)
    val applicationEnd = Utils.getFormattedClassName(SparkListenerApplicationEnd)
    val executorAdded = Utils.getFormattedClassName(SparkListenerExecutorAdded)
    val executorRemoved = Utils.getFormattedClassName(SparkListenerExecutorRemoved)
    val logStart = Utils.getFormattedClassName(SparkListenerLogStart)
    val metricsUpdate = Utils.getFormattedClassName(SparkListenerExecutorMetricsUpdate)
    val blockUpdate = Utils.getFormattedClassName(SparkListenerBlockUpdated)
  }

  def sparkEventFromJson(json: JsValue): SparkListenerEvent = {
    import SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES._

    (json \ "Event").as[String] match {
      case `stageSubmitted` => stageSubmittedFromJson(json)
      case `stageCompleted` => stageCompletedFromJson(json)
      case `taskStart` => taskStartFromJson(json)
      case `taskGettingResult` => taskGettingResultFromJson(json)
      case `taskEnd` => taskEndFromJson(json)
      case `jobStart` => jobStartFromJson(json)
      case `jobEnd` => jobEndFromJson(json)
      case `environmentUpdate` => environmentUpdateFromJson(json)
      case `blockManagerAdded` => blockManagerAddedFromJson(json)
      case `blockManagerRemoved` => blockManagerRemovedFromJson(json)
      case `unpersistRDD` => unpersistRDDFromJson(json)
      case `applicationStart` => applicationStartFromJson(json)
      case `applicationEnd` => applicationEndFromJson(json)
      case `executorAdded` => executorAddedFromJson(json)
      case `executorRemoved` => executorRemovedFromJson(json)
      case `logStart` => logStartFromJson(json)
      case `metricsUpdate` => executorMetricsUpdateFromJson(json)
      case `blockUpdate` => blockUpdateFromJson(json)
      case other => mapper.readValue(json.toString, Utils.classForName(other))
        .asInstanceOf[SparkListenerEvent]
    }
  }

  def stageSubmittedFromJson(json: JsValue): SparkListenerStageSubmitted = {
    val stageInfo = stageInfoFromJson(json \ "Stage Info")
    val properties = propertiesFromJson(json \ "Properties")
    SparkListenerStageSubmitted(stageInfo, properties)
  }

  def stageCompletedFromJson(json: JsValue): SparkListenerStageCompleted = {
    val stageInfo = stageInfoFromJson(json \ "Stage Info")
    SparkListenerStageCompleted(stageInfo)
  }

  def taskStartFromJson(json: JsValue): SparkListenerTaskStart = {
    val stageId = (json \ "Stage ID").as[Int]
    val stageAttemptId =
      Utils.jsonOption(json \ "Stage Attempt ID").map(_.as[Int]).getOrElse(0)
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    SparkListenerTaskStart(stageId, stageAttemptId, taskInfo)
  }

  def taskGettingResultFromJson(json: JsValue): SparkListenerTaskGettingResult = {
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    SparkListenerTaskGettingResult(taskInfo)
  }

  def taskEndFromJson(json: JsValue): SparkListenerTaskEnd = {
    val stageId = (json \ "Stage ID").as[Int]
    val stageAttemptId =
      Utils.jsonOption(json \ "Stage Attempt ID").map(_.as[Int]).getOrElse(0)
    val taskType = (json \ "Task Type").as[String]
    val taskEndReason = taskEndReasonFromJson(json \ "Task End Reason")
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    val taskMetrics = taskMetricsFromJson(json \ "Task Metrics")
    SparkListenerTaskEnd(stageId, stageAttemptId, taskType, taskEndReason, taskInfo, taskMetrics)
  }

  def jobStartFromJson(json: JsValue): SparkListenerJobStart = {
    val jobId = (json \ "Job ID").as[Int]
    val submissionTime =
      Utils.jsonOption(json \ "Submission Time").map(_.as[Long]).getOrElse(-1L)
    val stageIds = (json \ "Stage IDs").as[List[JsValue]].map(_.as[Int])
    val properties = propertiesFromJson(json \ "Properties")
    // The "Stage Infos" field was added in Spark 1.2.0
    val stageInfos = Utils.jsonOption(json \ "Stage Infos")
      .map(_.as[Seq[JsValue]].map(v => stageInfoFromJson(JsDefined(v)))).getOrElse {
        stageIds.map { id =>
          new StageInfo(id, 0, "unknown", 0, Seq.empty, Seq.empty, "unknown")
        }
      }
    SparkListenerJobStart(jobId, submissionTime, stageInfos, properties)
  }

  def jobEndFromJson(json: JsValue): SparkListenerJobEnd = {
    val jobId = (json \ "Job ID").as[Int]
    val completionTime =
      Utils.jsonOption(json \ "Completion Time").map(_.as[Long]).getOrElse(-1L)
    val jobResult = jobResultFromJson(json \ "Job Result")
    SparkListenerJobEnd(jobId, completionTime, jobResult)
  }

  def environmentUpdateFromJson(json: JsValue): SparkListenerEnvironmentUpdate = {
    val environmentDetails = Map[String, Seq[(String, String)]](
      "JVM Information" -> mapFromJson(json \ "JVM Information").toSeq,
      "Spark Properties" -> mapFromJson(json \ "Spark Properties").toSeq,
      "System Properties" -> mapFromJson(json \ "System Properties").toSeq,
      "Classpath Entries" -> mapFromJson(json \ "Classpath Entries").toSeq)
    SparkListenerEnvironmentUpdate(environmentDetails)
  }

  def blockManagerAddedFromJson(json: JsValue): SparkListenerBlockManagerAdded = {
    val blockManagerId = blockManagerIdFromJson(json \ "Block Manager ID")
    val maxMem = (json \ "Maximum Memory").as[Long]
    val time = Utils.jsonOption(json \ "Timestamp").map(_.as[Long]).getOrElse(-1L)
    val maxOnHeapMem = Utils.jsonOption(json \ "Maximum Onheap Memory").map(_.as[Long])
    val maxOffHeapMem = Utils.jsonOption(json \ "Maximum Offheap Memory").map(_.as[Long])
    SparkListenerBlockManagerAdded(time, blockManagerId, maxMem, maxOnHeapMem, maxOffHeapMem)
  }

  def blockManagerRemovedFromJson(json: JsValue): SparkListenerBlockManagerRemoved = {
    val blockManagerId = blockManagerIdFromJson(json \ "Block Manager ID")
    val time = Utils.jsonOption(json \ "Timestamp").map(_.as[Long]).getOrElse(-1L)
    SparkListenerBlockManagerRemoved(time, blockManagerId)
  }

  def unpersistRDDFromJson(json: JsValue): SparkListenerUnpersistRDD = {
    SparkListenerUnpersistRDD((json \ "RDD ID").as[Int])
  }

  def applicationStartFromJson(json: JsValue): SparkListenerApplicationStart = {
    val appName = (json \ "App Name").as[String]
    val appId = Utils.jsonOption(json \ "App ID").map(_.as[String])
    val time = (json \ "Timestamp").as[Long]
    val sparkUser = (json \ "User").as[String]
    val appAttemptId = Utils.jsonOption(json \ "App Attempt ID").map(_.as[String])
    val driverLogs = Utils.jsonOption(json \ "Driver Logs").map(v => mapFromJson(JsDefined(v)))
    SparkListenerApplicationStart(appName, appId, time, sparkUser, appAttemptId, driverLogs)
  }

  def applicationEndFromJson(json: JsValue): SparkListenerApplicationEnd = {
    SparkListenerApplicationEnd((json \ "Timestamp").as[Long])
  }

  def executorAddedFromJson(json: JsValue): SparkListenerExecutorAdded = {
    val time = (json \ "Timestamp").as[Long]
    val executorId = (json \ "Executor ID").as[String]
    val executorInfo = executorInfoFromJson(json \ "Executor Info")
    SparkListenerExecutorAdded(time, executorId, executorInfo)
  }

  def executorRemovedFromJson(json: JsValue): SparkListenerExecutorRemoved = {
    val time = (json \ "Timestamp").as[Long]
    val executorId = (json \ "Executor ID").as[String]
    val reason = (json \ "Removed Reason").as[String]
    SparkListenerExecutorRemoved(time, executorId, reason)
  }

  def logStartFromJson(json: JsValue): SparkListenerLogStart = {
    val sparkVersion = (json \ "Spark Version").as[String]
    SparkListenerLogStart(sparkVersion)
  }

  def executorMetricsUpdateFromJson(json: JsValue): SparkListenerExecutorMetricsUpdate = {
    val execInfo = (json \ "Executor ID").as[String]
    val accumUpdates = (json \ "Metrics Updated").as[List[JsValue]].map { json =>
      val taskId = (json \ "Task ID").as[Long]
      val stageId = (json \ "Stage ID").as[Int]
      val stageAttemptId = (json \ "Stage Attempt ID").as[Int]
      val updates =
        (json \ "Accumulator Updates").as[List[JsValue]].map(accumulableInfoFromJson)
      (taskId, stageId, stageAttemptId, updates)
    }
    SparkListenerExecutorMetricsUpdate(execInfo, accumUpdates)
  }

  def blockUpdateFromJson(json: JsValue): SparkListenerBlockUpdated = {
    val blockUpdatedInfo = blockUpdatedInfoFromJson(json \ "Block Updated Info")
    SparkListenerBlockUpdated(blockUpdatedInfo)
  }

  /** --------------------------------------------------------------------- *
   * JSON deserialization methods for classes SparkListenerEvents depend on |
   * ---------------------------------------------------------------------- */

  def stageInfoFromJson(json: JsLookup): StageInfo = {
    val stageId = (json \ "Stage ID").as[Int]
    val attemptId = Utils.jsonOption(json \ "Stage Attempt ID").map(_.as[Int]).getOrElse(0)
    val stageName = (json \ "Stage Name").as[String]
    val numTasks = (json \ "Number of Tasks").as[Int]
    val rddInfos = (json \ "RDD Info").as[List[JsValue]].map(rddInfoFromJson)
    val parentIds = Utils.jsonOption(json \ "Parent IDs")
      .map { l => l.as[List[JsValue]].map(_.as[Int]) }
      .getOrElse(Seq.empty)
    val details = Utils.jsonOption(json \ "Details").map(_.as[String]).getOrElse("")
    val submissionTime = Utils.jsonOption(json \ "Submission Time").map(_.as[Long])
    val completionTime = Utils.jsonOption(json \ "Completion Time").map(_.as[Long])
    val failureReason = Utils.jsonOption(json \ "Failure Reason").map(_.as[String])
    val accumulatedValues = {
      Utils.jsonOption(json \ "Accumulables").map(_.as[List[JsValue]]) match {
        case Some(values) => values.map(accumulableInfoFromJson)
        case None => Seq.empty[AccumulableInfo]
      }
    }

    val stageInfo = new StageInfo(
      stageId, attemptId, stageName, numTasks, rddInfos, parentIds, details)
    stageInfo.submissionTime = submissionTime
    stageInfo.completionTime = completionTime
    stageInfo.failureReason = failureReason
    for (accInfo <- accumulatedValues) {
      stageInfo.accumulables(accInfo.id) = accInfo
    }
    stageInfo
  }

  def taskInfoFromJson(json: JsLookup): TaskInfo = {
    val taskId = (json \ "Task ID").as[Long]
    val index = (json \ "Index").as[Int]
    val attempt = Utils.jsonOption(json \ "Attempt").map(_.as[Int]).getOrElse(1)
    val launchTime = (json \ "Launch Time").as[Long]
    val executorId = (json \ "Executor ID").as[String].intern()
    val host = (json \ "Host").as[String].intern()
    val taskLocality = TaskLocality.withName((json \ "Locality").as[String])
    val speculative = Utils.jsonOption(json \ "Speculative").exists(_.as[Boolean])
    val gettingResultTime = (json \ "Getting Result Time").as[Long]
    val finishTime = (json \ "Finish Time").as[Long]
    val failed = (json \ "Failed").as[Boolean]
    val killed = Utils.jsonOption(json \ "Killed").exists(_.as[Boolean])
    val accumulables = Utils.jsonOption(json \ "Accumulables").map(_.as[Seq[JsValue]]) match {
      case Some(values) => values.map(accumulableInfoFromJson)
      case None => Seq.empty[AccumulableInfo]
    }

    val taskInfo =
      new TaskInfo(taskId, index, attempt, launchTime, executorId, host, taskLocality, speculative)
    taskInfo.gettingResultTime = gettingResultTime
    taskInfo.finishTime = finishTime
    taskInfo.failed = failed
    taskInfo.killed = killed
    taskInfo.setAccumulables(accumulables)
    taskInfo
  }

  def accumulableInfoFromJson(json: JsValue): AccumulableInfo = {
    val id = (json \ "ID").as[Long]
    val name = Utils.jsonOption(json \ "Name").map(_.as[String])
    val update = Utils.jsonOption(json \ "Update").map { v => accumValueFromJson(name, v) }
    val value = Utils.jsonOption(json \ "Value").map { v => accumValueFromJson(name, v) }
    val internal = Utils.jsonOption(json \ "Internal").exists(_.as[Boolean])
    val countFailedValues =
      Utils.jsonOption(json \ "Count Failed Values").exists(_.as[Boolean])
    val metadata = Utils.jsonOption(json \ "Metadata").map(_.as[String])
    new AccumulableInfo(id, name, update, value, internal, countFailedValues, metadata)
  }

  /**
   * Deserialize the value of an accumulator from JSON.
   *
   * For accumulators representing internal task metrics, this looks up the relevant
   * [[AccumulatorParam]] to deserialize the value accordingly. For all other
   * accumulators, this will simply deserialize the value as a string.
   *
   * The behavior here must match that of [[accumValueToJson]]. Exposed for testing.
   */
  private[util] def accumValueFromJson(name: Option[String], value: JsValue): Any = {
    if (name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))) {
      value match {
        case JsNumber(v) => v.toLong
        case JsArray(v) =>
          v.map { blockJson =>
            val id = BlockId((blockJson \ "Block ID").as[String])
            val status = blockStatusFromJson(blockJson \ "Status")
            (id, status)
          }.asJava
        case _ => throw new IllegalArgumentException(s"unexpected json value $value for " +
          "accumulator " + name.get)
      }
    } else {
      value.as[String]
    }
  }

  def taskMetricsFromJson(json: JsLookup): TaskMetrics = {
    val metrics = TaskMetrics.empty
    if (Utils.jsonOption(json).isEmpty) {
      return metrics
    }
    metrics.setExecutorDeserializeTime((json \ "Executor Deserialize Time").as[Long])
    metrics.setExecutorDeserializeCpuTime(Utils.jsonOption(json \ "Executor Deserialize CPU Time")
      .map(_.as[Long]).getOrElse(0)
    )
    metrics.setExecutorRunTime((json \ "Executor Run Time").as[Long])
    metrics.setExecutorCpuTime(Utils.jsonOption(json \ "Executor CPU Time")
      .map(_.as[Long]).getOrElse(0)
    )
    metrics.setResultSize((json \ "Result Size").as[Long])
    metrics.setJvmGCTime((json \ "JVM GC Time").as[Long])
    metrics.setResultSerializationTime((json \ "Result Serialization Time").as[Long])
    metrics.incMemoryBytesSpilled((json \ "Memory Bytes Spilled").as[Long])
    metrics.incDiskBytesSpilled((json \ "Disk Bytes Spilled").as[Long])

    // Shuffle read metrics
    Utils.jsonOption(json \ "Shuffle Read Metrics").foreach { readJson =>
      val readMetrics = metrics.createTempShuffleReadMetrics()
      readMetrics.incRemoteBlocksFetched((readJson \ "Remote Blocks Fetched").as[Int])
      readMetrics.incLocalBlocksFetched((readJson \ "Local Blocks Fetched").as[Int])
      readMetrics.incRemoteBytesRead((readJson \ "Remote Bytes Read").as[Long])
      Utils.jsonOption(readJson \ "Remote Bytes Read To Disk")
        .foreach { v => readMetrics.incRemoteBytesReadToDisk(v.as[Long])}
      readMetrics.incLocalBytesRead(
        Utils.jsonOption(readJson \ "Local Bytes Read").map(_.as[Long]).getOrElse(0L))
      readMetrics.incFetchWaitTime((readJson \ "Fetch Wait Time").as[Long])
      readMetrics.incRecordsRead(
        Utils.jsonOption(readJson \ "Total Records Read").map(_.as[Long]).getOrElse(0L))
      metrics.mergeShuffleReadMetrics()
    }

    // Shuffle write metrics
    // TODO: Drop the redundant "Shuffle" since it's inconsistent with related classes.
    Utils.jsonOption(json \ "Shuffle Write Metrics").foreach { writeJson =>
      val writeMetrics = metrics.shuffleWriteMetrics
      writeMetrics.incBytesWritten((writeJson \ "Shuffle Bytes Written").as[Long])
      writeMetrics.incRecordsWritten(
        Utils.jsonOption(writeJson \ "Shuffle Records Written").map(_.as[Long]).getOrElse(0L))
      writeMetrics.incWriteTime((writeJson \ "Shuffle Write Time").as[Long])
    }

    // Output metrics
    Utils.jsonOption(json \ "Output Metrics").foreach { outJson =>
      val outputMetrics = metrics.outputMetrics
      outputMetrics.setBytesWritten((outJson \ "Bytes Written").as[Long])
      outputMetrics.setRecordsWritten(
        Utils.jsonOption(outJson \ "Records Written").map(_.as[Long]).getOrElse(0L))
    }

    // Input metrics
    Utils.jsonOption(json \ "Input Metrics").foreach { inJson =>
      val inputMetrics = metrics.inputMetrics
      inputMetrics.incBytesRead((inJson \ "Bytes Read").as[Long])
      inputMetrics.incRecordsRead(
        Utils.jsonOption(inJson \ "Records Read").map(_.as[Long]).getOrElse(0L))
    }

    // Updated blocks
    Utils.jsonOption(json \ "Updated Blocks").foreach { blocksJson =>
      metrics.setUpdatedBlockStatuses(blocksJson.as[List[JsValue]].map { blockJson =>
        val id = BlockId((blockJson \ "Block ID").as[String])
        val status = blockStatusFromJson(blockJson \ "Status")
        (id, status)
      })
    }

    metrics
  }

  private object TASK_END_REASON_FORMATTED_CLASS_NAMES {
    val success = Utils.getFormattedClassName(Success)
    val resubmitted = Utils.getFormattedClassName(Resubmitted)
    val fetchFailed = Utils.getFormattedClassName(FetchFailed)
    val exceptionFailure = Utils.getFormattedClassName(ExceptionFailure)
    val taskResultLost = Utils.getFormattedClassName(TaskResultLost)
    val taskKilled = Utils.getFormattedClassName(TaskKilled)
    val taskCommitDenied = Utils.getFormattedClassName(TaskCommitDenied)
    val executorLostFailure = Utils.getFormattedClassName(ExecutorLostFailure)
    val unknownReason = Utils.getFormattedClassName(UnknownReason)
  }

  def taskEndReasonFromJson(json: JsLookup): TaskEndReason = {
    import TASK_END_REASON_FORMATTED_CLASS_NAMES._

    (json \ "Reason").as[String] match {
      case `success` => Success
      case `resubmitted` => Resubmitted
      case `fetchFailed` =>
        val blockManagerAddress = blockManagerIdFromJson(json \ "Block Manager Address")
        val shuffleId = (json \ "Shuffle ID").as[Int]
        val mapId = (json \ "Map ID").as[Int]
        val reduceId = (json \ "Reduce ID").as[Int]
        val message = Utils.jsonOption(json \ "Message").map(_.as[String])
        new FetchFailed(blockManagerAddress, shuffleId, mapId, reduceId,
          message.getOrElse("Unknown reason"))
      case `exceptionFailure` =>
        val className = (json \ "Class Name").as[String]
        val description = (json \ "Description").as[String]
        val stackTrace = stackTraceFromJson(json \ "Stack Trace")
        val fullStackTrace =
          Utils.jsonOption(json \ "Full Stack Trace").map(_.as[String]).orNull
        // Fallback on getting accumulator updates from TaskMetrics, which was logged in Spark 1.x
        val accumUpdates = Utils.jsonOption(json \ "Accumulator Updates")
          .map(_.as[List[JsValue]].map(accumulableInfoFromJson))
          .getOrElse(taskMetricsFromJson(json \ "Metrics").accumulators().map(acc => {
            acc.toInfo(Some(acc.value), None)
          }))
        ExceptionFailure(className, description, stackTrace, fullStackTrace, None, accumUpdates)
      case `taskResultLost` => TaskResultLost
      case `taskKilled` =>
        val killReason = Utils.jsonOption(json \ "Kill Reason")
          .map(_.as[String]).getOrElse("unknown reason")
        TaskKilled(killReason)
      case `taskCommitDenied` =>
        // Unfortunately, the `TaskCommitDenied` message was introduced in 1.3.0 but the JSON
        // de/serialization logic was not added until 1.5.1. To provide backward compatibility
        // for reading those logs, we need to provide default values for all the fields.
        val jobId = Utils.jsonOption(json \ "Job ID").map(_.as[Int]).getOrElse(-1)
        val partitionId = Utils.jsonOption(json \ "Partition ID").map(_.as[Int]).getOrElse(-1)
        val attemptNo = Utils.jsonOption(json \ "Attempt Number").map(_.as[Int]).getOrElse(-1)
        TaskCommitDenied(jobId, partitionId, attemptNo)
      case `executorLostFailure` =>
        val exitCausedByApp = Utils.jsonOption(json \ "Exit Caused By App").map(_.as[Boolean])
        val executorId = Utils.jsonOption(json \ "Executor ID").map(_.as[String])
        val reason = Utils.jsonOption(json \ "Loss Reason").map(_.as[String])
        ExecutorLostFailure(
          executorId.getOrElse("Unknown"),
          exitCausedByApp.getOrElse(true),
          reason)
      case `unknownReason` => UnknownReason
    }
  }

  def blockManagerIdFromJson(json: JsLookup): BlockManagerId = {
    // On metadata fetch fail, block manager ID can be null (SPARK-4471)
    Utils.jsonOption(json).map { json =>
      val executorId = (json \ "Executor ID").as[String].intern()
      val host = (json \ "Host").as[String].intern()
      val port = (json \ "Port").as[Int]
      BlockManagerId(executorId, host, port)
    }.orNull
  }

  private object JOB_RESULT_FORMATTED_CLASS_NAMES {
    val jobSucceeded = Utils.getFormattedClassName(JobSucceeded)
    val jobFailed = Utils.getFormattedClassName(JobFailed)
  }

  def jobResultFromJson(json: JsLookup): JobResult = {
    import JOB_RESULT_FORMATTED_CLASS_NAMES._

    (json \ "Result").as[String] match {
      case `jobSucceeded` => JobSucceeded
      case `jobFailed` =>
        val exception = exceptionFromJson(json \ "Exception")
        new JobFailed(exception)
    }
  }

  def rddInfoFromJson(json: JsValue): RDDInfo = {
    val rddId = (json \ "RDD ID").as[Int]
    val name = (json \ "Name").as[String]
    val scope = Utils.jsonOption(json \ "Scope")
      .map(_.as[String])
      .map(RDDOperationScope.fromJson)
    val callsite = Utils.jsonOption(json \ "Callsite").map(_.as[String]).getOrElse("")
    val parentIds = Utils.jsonOption(json \ "Parent IDs")
      .map { l => l.as[List[JsValue]].map(_.as[Int]) }
      .getOrElse(Seq.empty)
    val storageLevel = storageLevelFromJson(json \ "Storage Level")
    val numPartitions = (json \ "Number of Partitions").as[Int]
    val numCachedPartitions = (json \ "Number of Cached Partitions").as[Int]
    val memSize = (json \ "Memory Size").as[Long]
    val diskSize = (json \ "Disk Size").as[Long]

    val rddInfo = new RDDInfo(rddId, name, numPartitions, storageLevel, parentIds, callsite, scope)
    rddInfo.numCachedPartitions = numCachedPartitions
    rddInfo.memSize = memSize
    rddInfo.diskSize = diskSize
    rddInfo
  }

  def storageLevelFromJson(json: JsLookup): StorageLevel = {
    val useDisk = (json \ "Use Disk").as[Boolean]
    val useMemory = (json \ "Use Memory").as[Boolean]
    val deserialized = (json \ "Deserialized").as[Boolean]
    val replication = (json \ "Replication").as[Int]
    StorageLevel(useDisk, useMemory, deserialized, replication)
  }

  def blockStatusFromJson(json: JsLookup): BlockStatus = {
    val storageLevel = storageLevelFromJson(json \ "Storage Level")
    val memorySize = (json \ "Memory Size").as[Long]
    val diskSize = (json \ "Disk Size").as[Long]
    BlockStatus(storageLevel, memorySize, diskSize)
  }

  def executorInfoFromJson(json: JsLookup): ExecutorInfo = {
    val executorHost = (json \ "Host").as[String]
    val totalCores = (json \ "Total Cores").as[Int]
    val logUrls = mapFromJson(json \ "Log Urls").toMap
    new ExecutorInfo(executorHost, totalCores, logUrls)
  }

  def blockUpdatedInfoFromJson(json: JsLookup): BlockUpdatedInfo = {
    val blockManagerId = blockManagerIdFromJson(json \ "Block Manager ID")
    val blockId = BlockId((json \ "Block ID").as[String])
    val storageLevel = storageLevelFromJson(json \ "Storage Level")
    val memorySize = (json \ "Memory Size").as[Long]
    val diskSize = (json \ "Disk Size").as[Long]
    BlockUpdatedInfo(blockManagerId, blockId, storageLevel, memorySize, diskSize)
  }

  /** -------------------------------- *
   * Util JSON deserialization methods |
   * --------------------------------- */

  def mapFromJson(json: JsLookup): Map[String, String] = {
    Utils.jsonOption(json).collect {
      case jsObject: JsObject => jsObject.value.map {
        case (k, JsString(v)) => (k, v)
        case (k, v) => (k, v.toString)
      }
    }.getOrElse(Map.empty)
  }

  def propertiesFromJson(json: JsLookup): Properties = {
    Utils.jsonOption(json).collect {
      case jsObject: JsObject =>
        val properties = new Properties
        jsObject.value.collect { case (k, JsString(v)) => properties.setProperty(k, v) }
        properties
    }.orNull
  }

  def UUIDFromJson(json: JsLookup): UUID = {
    val leastSignificantBits = (json \ "Least Significant Bits").as[Long]
    val mostSignificantBits = (json \ "Most Significant Bits").as[Long]
    new UUID(leastSignificantBits, mostSignificantBits)
  }

  def stackTraceFromJson(json: JsLookup): Array[StackTraceElement] = {
    json.result.as[List[JsValue]].map { line =>
      val declaringClass = (line \ "Declaring Class").as[String]
      val methodName = (line \ "Method Name").as[String]
      val fileName = (line \ "File Name").as[String]
      val lineNumber = (line \ "Line Number").as[Int]
      new StackTraceElement(declaringClass, methodName, fileName, lineNumber)
    }.toArray
  }

  def exceptionFromJson(json: JsLookup): Exception = {
    val e = new Exception((json \ "Message").as[String])
    e.setStackTrace(stackTraceFromJson(json \ "Stack Trace"))
    e
  }

}
