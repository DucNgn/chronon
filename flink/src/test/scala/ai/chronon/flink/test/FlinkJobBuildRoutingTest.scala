package ai.chronon.flink.test

import ai.chronon.api.Builders
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.{Accuracy, Operation, TimeUnit, Window}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Tests for FlinkJob.buildFlinkJob routing logic.
  *
  * buildFlinkJob is private, so we test the condition it uses:
  * `servingInfo.groupBy.streamingSource.exists(_.isSetJoinSource)`
  *
  * streamingSource is derived from the first source with a non-null topic
  * (see Extensions.GroupByOps.streamingSource). When no source has a topic,
  * streamingSource is None. FlinkJob must handle this gracefully when
  * --topic-override is provided at deploy time.
  */
class FlinkJobBuildRoutingTest extends AnyFlatSpec with Matchers {

  private def makeGroupByWithTopic(topic: String): ai.chronon.api.GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "test_db.test_table",
          topic = topic,
          query = Builders.Query(
            selects = Map("id" -> "id", "value" -> "value"),
            timeColumn = "ts"
          )
        )
      ),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.COUNT,
          inputColumn = "id",
          windows = Seq(new Window(7, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = "test.group_by.v1"),
      accuracy = Accuracy.TEMPORAL
    )

  private def makeGroupByWithoutTopic(): ai.chronon.api.GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "test_db.test_table",
          query = Builders.Query(
            selects = Map("id" -> "id", "value" -> "value"),
            timeColumn = "ts"
          )
        )
      ),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.COUNT,
          inputColumn = "id",
          windows = Seq(new Window(7, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = "test.group_by.v1"),
      accuracy = Accuracy.TEMPORAL
    )

  "streamingSource" should "be Some when EventSource has a topic" in {
    val groupBy = makeGroupByWithTopic("kafka://test-topic")
    groupBy.streamingSource shouldBe defined
    groupBy.streamingSource.get.topic shouldBe "kafka://test-topic"
  }

  it should "be None when EventSource has no topic" in {
    val groupBy = makeGroupByWithoutTopic()
    groupBy.streamingSource shouldBe None
  }

  "buildFlinkJob routing condition" should "return false (not JoinSource) when streamingSource is defined without JoinSource" in {
    val groupBy = makeGroupByWithTopic("kafka://test-topic")
    val isJoinSource = groupBy.streamingSource.exists(_.isSetJoinSource)
    isJoinSource shouldBe false
  }

  it should "return false (not JoinSource) when streamingSource is None" in {
    val groupBy = makeGroupByWithoutTopic()
    val isJoinSource = groupBy.streamingSource.exists(_.isSetJoinSource)
    isJoinSource shouldBe false
  }

  it should "not throw when streamingSource is None (the old .get would crash)" in {
    val groupBy = makeGroupByWithoutTopic()
    // This is the exact condition from the fix. The old code did .get.isSetJoinSource
    // which would throw NoSuchElementException: None.get
    noException should be thrownBy {
      groupBy.streamingSource.exists(_.isSetJoinSource)
    }
  }
}
