import org.apache.log4j.{Level, Logger}
import org.apache.spark.scheduler._

/**
  * Displays a progressbar for each stage.
  */
final class ProgressbarLogger extends SparkListener {

  Logger.getLogger("org").setLevel(Level.ERROR)

  private val LetterCount = 30
  private var tasks = 0
  private var taskStarted = 0
  private var taskEnded = 0
  private var taskInfo: Option[SparkListenerTaskStart] = None
  private var stageInfo: String = ""

  private def show(): Unit = {
    val countEnded = (taskEnded / tasks.toFloat * LetterCount).toInt
    val countStarted = ((taskStarted - taskEnded) / tasks.toFloat * LetterCount).toInt
    print(s"\rProgress: |${"=" * countEnded}>${"." * countStarted}${" " * (LetterCount - countStarted - countEnded)}|" +
      taskInfo.map(task => s" Task ${task.taskInfo.taskId} / Stage ${task.stageId} (function ${stageInfo})").getOrElse(""))
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    taskInfo = Some(taskStart)
    taskStarted += 1
    show()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    taskEnded += 1
    show()
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    tasks = stageSubmitted.stageInfo.numTasks
    stageInfo = stageSubmitted.stageInfo.name
    taskStarted = 0
    taskEnded = 0
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = println()

}
