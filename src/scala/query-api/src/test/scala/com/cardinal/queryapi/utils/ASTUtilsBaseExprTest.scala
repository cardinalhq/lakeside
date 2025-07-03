package com.cardinal.queryapi.utils

import com.cardinal.logs.LogCommons.STRING_TYPE
import com.cardinal.model.query.common.TagDataType
import com.cardinal.utils.ast.{ASTUtils, BaseExpr}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ASTUtilsBaseExprTest {

  @Test
  def testTagApiShouldNotDoASelectStar(): Unit = {
    val tagPayload =
      """
        |{
        |  "baseExpressions": {
        |    "A": {
        |      "dataset": "logs",
        |      "limit": 1000,
        |      "order": "DESC",
        |      "filter": {
        |        "q1": {
        |          "k": "resource.container.name",
        |          "v": [
        |            "agent"
        |          ],
        |          "op": "eq",
        |          "dataType": "string",
        |          "extracted": false,
        |          "computed": false
        |        },
        |        "q2": {
        |          "k": "_cardinalhq.message",
        |          "v": [
        |            "compressed"
        |          ],
        |          "op": "contains",
        |          "dataType": "string",
        |          "extracted": false,
        |          "computed": false
        |        },
        |        "op": "and"
        |      }
        |    }
        |  }
        |}
        |""".stripMargin

    val astInput = ASTUtils.toASTInput(payload = tagPayload)
    val baseExprAOpt = astInput.baseExpressions.get("A")
    assert(baseExprAOpt.isDefined, "baseExprA is not defined")
    val baseExprA = baseExprAOpt.get
    val sql = BaseExpr.generateSql(baseExpr = baseExprA, 1, 1, globalAgg = Some("sum"), isTagQuery = true,
      tagDataType = Some(TagDataType("resource.container.name", STRING_TYPE)), nonExistentFields = Set.empty)
    val expectedSql = """SELECT "resource.container.name" as "resource.container.name", COUNT(*) AS count FROM {tableName} WHERE ("resource.container.name" = 'agent' and regexp_matches("_cardinalhq.message", '.*compressed.*','i')) AND "_cardinalhq.timestamp" >= 1 AND "_cardinalhq.timestamp" < 1 GROUP BY "resource.container.name""""
    assert(sql == expectedSql)
  }



  @Test
  def testQueryApiPayloadWithExtract(): Unit = {
    val queryPayload =
      """
        |{
        |  "baseExpressions": {
        |    "A": {
        |      "dataset": "logs",
        |      "limit": 1000,
        |      "order": "DESC",
        |      "filter": {
        |        "q1": {
        |          "k": "resource.container.name",
        |          "v": [
        |            "agent"
        |          ],
        |          "op": "eq",
        |          "dataType": "string",
        |          "extracted": false,
        |          "computed": false
        |        },
        |        "q2": {
        |          "k": "_cardinalhq.message",
        |          "v": [
        |            "compressed"
        |          ],
        |          "op": "contains",
        |          "dataType": "string",
        |          "extracted": false,
        |          "computed": false
        |        },
        |        "q3": {
        |          "k": "raw",
        |          "v": [
        |            ""
        |          ],
        |          "op": "has",
        |          "dataType": "number",
        |          "extracted": true,
        |          "computed": false
        |        },
        |        "q4": {
        |          "k": "compressed",
        |          "v": [
        |            ""
        |          ],
        |          "op": "has",
        |          "dataType": "number",
        |          "extracted": true,
        |          "computed": false
        |        },
        |        "op": "and"
        |      },
        |      "extract": {
        |        "regex": "([A-Za-z]+) \\| ([A-Za-z]+) \\| ([A-Za-z]+) \\| \\(([^)]*)\\) \\| ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+), ([A-Za-z]+) \\(([^)]*)\\)([A-Za-z0-9-_.:]+) ([0-9]+)/([0-9]+) ([A-Za-z0-9-_.:]+)",
        |        "fields": [
        |          {
        |            "name": "var_1",
        |            "type": "string"
        |          },
        |          {
        |            "name": "var_2",
        |            "type": "string"
        |          },
        |          {
        |            "name": "var_3",
        |            "type": "string"
        |          },
        |          {
        |            "name": "var_4",
        |            "type": "string"
        |          },
        |          {
        |            "name": "var_5",
        |            "type": "string"
        |          },
        |          {
        |            "name": "var_6",
        |            "type": "string"
        |          },
        |          {
        |            "name": "var_7",
        |            "type": "string"
        |          },
        |          {
        |            "name": "var_8",
        |            "type": "string"
        |          },
        |          {
        |            "name": "var_9",
        |            "type": "string"
        |          },
        |          {
        |            "name": "var_10",
        |            "type": "string"
        |          },
        |          {
        |            "name": "raw",
        |            "type": "number"
        |          },
        |          {
        |            "name": "compressed",
        |            "type": "number"
        |          },
        |          {
        |            "name": "var_13",
        |            "type": "string"
        |          }
        |        ]
        |      },
        |      "chart": {
        |        "aggregation": "sum",
        |        "rollup": "sum",
        |        "groupBys": [
        |          "_cardinalhq.level"
        |        ],
        |        "type": "count"
        |      }
        |    }
        |  }
        |}
        |""".stripMargin

    val astInput = ASTUtils.toASTInput(payload = queryPayload)
    val baseExprAOpt = astInput.baseExpressions.get("A")
    assert(baseExprAOpt.isDefined, "baseExprA is not defined")
    val baseExprA = baseExprAOpt.get
    //    assert(baseExprA.filter == expectedFilter)
    val ts: Long = 1694635527646L
    val chartSql = BaseExpr.generateSql(baseExpr = baseExprA, ts, ts, globalAgg = Some("sum"), nonExistentFields = Set.empty)
    val expectedChartSql =
      """|SELECT ("_cardinalhq.timestamp" - ("_cardinalhq.timestamp" % 10000.0)) as step_ts, sum("_cardinalhq.value"), "_cardinalhq.name" as name , "_cardinalhq.level" FROM (SELECT nlp_struct['raw'] as raw, nlp_struct['compressed'] as compressed, * FROM (SELECT  regexp_extract(replace("_cardinalhq.message", '''', ''), '([A-Za-z]+) \| ([A-Za-z]+) \| ([A-Za-z]+) \| \(([^)]*)\) \| ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+), ([A-Za-z]+) \(([^)]*)\)([A-Za-z0-9-_.:]+) ([0-9]+)/([0-9]+) ([A-Za-z0-9-_.:]+)', ['var_1', 'var_2', 'var_3', 'var_4', 'var_5', 'var_6', 'var_7', 'var_8', 'var_9', 'var_10', 'raw', 'compressed', 'var_13']) as nlp_struct, * FROM (SELECT * FROM {tableName} WHERE "_cardinalhq.timestamp" >= 1694635527646 AND "_cardinalhq.timestamp" < 1694635527646) WHERE  regexp_matches(replace("_cardinalhq.message", '''', ''), '([A-Za-z]+) \| ([A-Za-z]+) \| ([A-Za-z]+) \| \(([^)]*)\) \| ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+), ([A-Za-z]+) \(([^)]*)\)([A-Za-z0-9-_.:]+) ([0-9]+)/([0-9]+) ([A-Za-z0-9-_.:]+)')))  WHERE true AND ((("resource.container.name" = 'agent' and regexp_matches("_cardinalhq.message", '.*compressed.*','i')) and raw IS NOT NULL) and compressed IS NOT NULL) GROUP BY step_ts , "_cardinalhq.level", name ORDER BY step_ts ASC""".stripMargin
    assertEquals(chartSql, expectedChartSql)
    val noChartBaseExpr = baseExprA.copy(chartOpts = None)
    val exemplarSql = BaseExpr.generateSql(noChartBaseExpr, ts, ts, globalAgg = Some("sum"), nonExistentFields = Set.empty)
    val expectedExemplarSql = """SELECT "_cardinalhq.timestamp", "_cardinalhq.value", "_cardinalhq.name", "_cardinalhq.message", * FROM (SELECT nlp_struct['raw'] as raw, nlp_struct['compressed'] as compressed, * FROM (SELECT  regexp_extract(replace("_cardinalhq.message", '''', ''), '([A-Za-z]+) \| ([A-Za-z]+) \| ([A-Za-z]+) \| \(([^)]*)\) \| ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+), ([A-Za-z]+) \(([^)]*)\)([A-Za-z0-9-_.:]+) ([0-9]+)/([0-9]+) ([A-Za-z0-9-_.:]+)', ['var_1', 'var_2', 'var_3', 'var_4', 'var_5', 'var_6', 'var_7', 'var_8', 'var_9', 'var_10', 'raw', 'compressed', 'var_13']) as nlp_struct, * FROM (SELECT * FROM {tableName} WHERE "_cardinalhq.timestamp" >= 1694635527646 AND "_cardinalhq.timestamp" < 1694635527646) WHERE  regexp_matches(replace("_cardinalhq.message", '''', ''), '([A-Za-z]+) \| ([A-Za-z]+) \| ([A-Za-z]+) \| \(([^)]*)\) \| ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+), ([A-Za-z]+) \(([^)]*)\)([A-Za-z0-9-_.:]+) ([0-9]+)/([0-9]+) ([A-Za-z0-9-_.:]+)'))) WHERE ((("resource.container.name" = 'agent' and regexp_matches("_cardinalhq.message", '.*compressed.*','i')) and raw IS NOT NULL) and compressed IS NOT NULL) ORDER BY "_cardinalhq.timestamp" DESC"""
    assertEquals(exemplarSql, expectedExemplarSql)
  }
}
