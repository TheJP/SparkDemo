import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded}

/**
  * Logs info of added executors.
  */
final class ExecutorLogger extends SparkListener {

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit =
    println(s"\rExecutor ${executorAdded.executorId} added: ${executorAdded.executorInfo.executorHost} ${executorAdded.executorInfo.totalCores} cores")

}
