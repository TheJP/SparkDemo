import org.apache.spark.scheduler._

final class SparkInfoLogger extends SparkListener {

  var taskCount = 0

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit =
    println(s"Executor ${executorAdded.executorId} added: ${executorAdded.executorInfo.executorHost} ${executorAdded.executorInfo.totalCores} cores")

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit =
    println(s"Executor ${executorMetricsUpdate.execId} update: ${executorMetricsUpdate.accumUpdates match {
      case Seq() => "(no further details)"
      case updates => updates.map({
        case (taskId, stageId, _, _) => s"task=$taskId stage=$stageId"
      }).reduce(_ + _)
    }}")

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit =
    println(s"Executor ${executorRemoved.executorId} removed: ${executorRemoved.reason}")

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = println(s"Job ${jobStart.jobId} starting: ${jobStart.stageInfos.size}")

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = println(s"Job ${jobEnd.jobId} ended: ${jobEnd.jobResult.toString}")

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit =
    println(s"> Stage ${stageSubmitted.stageInfo.stageId} started: function ${stageSubmitted.stageInfo.name} (${stageSubmitted.stageInfo.numTasks} tasks)")

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
    println(s"< Stage ${stageCompleted.stageInfo.stageId} completed: function ${stageCompleted.stageInfo.name}")

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit =
    println(s"[+] Task ${taskStart.taskInfo.taskId} of stage ${taskStart.stageId} started")

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
    println(s"[-] Task ${taskEnd.taskInfo.taskId} of stage ${taskEnd.stageId} ended")

//  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = println(s"Block ${blockManagerAdded.blockManagerId} added")
//
//  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = println(s"Block ${blockManagerRemoved.blockManagerId} removed")
//
//  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit =
//    println(s"Block ${blockUpdated.blockUpdatedInfo.blockManagerId}/${blockUpdated.blockUpdatedInfo.blockId} update: ${blockUpdated.blockUpdatedInfo.storageLevel}")

}
