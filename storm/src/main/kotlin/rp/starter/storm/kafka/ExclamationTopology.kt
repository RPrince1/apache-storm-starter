package rp.starter.storm.kafka

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
    ConfigurableTopology.start(ExclamationTopology(), args)
}

class ExclamationTopology : ConfigurableTopology() {

    override fun run(args: Array<out String>?): Int {
        val builder = TopologyBuilder()

        builder.setSpout("word", TestWordSpout(), 10)
        builder.setBolt("exclaim1", ExclamationBolt(), 3).shuffleGrouping("word")
        builder.setBolt("exclaim2", ExclamationBolt(), 2).shuffleGrouping("exclaim1")

        conf.setDebug(true)

        val topologyName = "test"

        conf.setNumWorkers(3)

        return submit(topologyName, conf, builder)
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