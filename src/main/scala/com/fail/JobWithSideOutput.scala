package com.fail

import com.spotify.scio._
import com.spotify.scio.values.SideOutput

/*
sbt "runMain [PACKAGE].WordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object JobWithSideOutput {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val p1 = sc.parallelize(Seq("a", "b", "c"))
    val p2 = SideOutput[String]()
    val (main, side) = p1.withSideOutputs(p2).flatMap { (x, s) =>
      s.output(p2, x + "2x").output(p2, x + "2y")
      Seq(x + "1x", x + "1y")
    }

    sc.close()
  }
}
