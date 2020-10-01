package rp.starter.storm

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class StormApplication

fun main(args: Array<String>) {
	runApplication<StormApplication>(*args)
}
