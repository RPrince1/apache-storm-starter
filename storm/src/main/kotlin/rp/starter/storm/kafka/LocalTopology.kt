package rp.starter.storm.kafka

import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.LocalDRPC
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.testing.TestWordSpout
import org.apache.storm.topology.ConfigurableTopology
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values

@Throws(Exception::class)
fun main(args: Array<String>) {
    LocalTopology().runDatShit()
}

class LocalTopology {

    fun runDatShit() {
        val builder = TopologyBuilder()

        builder.setSpout("word", TestWordSpout(), 10)
        builder.setBolt("exclaim1", ExclamationBolt(), 3).shuffleGrouping("word")
        builder.setBolt("exclaim2", ExclamationBolt(), 2).shuffleGrouping("exclaim1")

        val topologyName = "test"

        val conf = Config()

        conf.setDebug(true)
        conf.setNumWorkers(3)

        try {
            val drpc = LocalDRPC()
            val cluster = LocalCluster()
            val topology =  cluster.submitTopology("demo", conf, builder.createTopology(drpc))

            println(drpc.execute("fnName", "fnArgs"))
        } catch (e: Exception) {

        }

    }

    companion object class ExclamationBolt : BaseRichBolt() {

        lateinit var outputCollector: OutputCollector

        override fun prepare(topoConf: MutableMap<String, Any>?, context: TopologyContext?, collector: OutputCollector?) {
            if (collector != null) {
                this.outputCollector = collector
            }
        }

        override fun execute(input: Tuple?) {
            outputCollector.emit(input, Values(input?.getString(0) + "!"))
            outputCollector.ack(input)
        }

        override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
            declarer?.declare(Fields("word"))
        }
    }
}