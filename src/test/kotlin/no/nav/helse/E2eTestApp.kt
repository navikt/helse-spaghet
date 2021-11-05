package no.nav.helse

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import no.nav.helse.TestData.toJson
import no.nav.helse.TestUtil.failOnExceptions
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.slf4j.LoggerFactory
import org.testcontainers.containers.PostgreSQLContainer
import javax.sql.DataSource

class E2eTestApp(
    var rapid: TestRapid = TestRapid(),
    var listAppender: ListAppender<ILoggingEvent> = ListAppender(),
    private val embeddedPostgres: PostgreSQLContainer<Nothing> = embeddedPostgres(),
    val dataSource: DataSource = setupDataSourceMedFlyway(embeddedPostgres)
) {

    private fun start() {
        mockLog()
        rapid.setupRiver(dataSource)
        rapid.failOnExceptions()
        rapid.start()
    }

    fun Annullering.sendTilRapid() {
        rapid.sendTestMessage(toJson())
    }

    fun TestData.VedtaksperiodeEndret.sendTilRapid() {
        rapid.sendTestMessage(toJson())
    }

    private fun mockLog() {
        val logger = LoggerFactory.getLogger("spaghet") as Logger
        listAppender.start()
        logger.addAppender(listAppender)
    }

    private fun reset() {
        dataSource.resetDatabase()
        rapid = TestRapid()
        listAppender = ListAppender()
    }


    companion object {
        private val testEnv by lazy { E2eTestApp() }
        fun e2eTest(f: E2eTestApp.() -> Unit) {
            try {
                testEnv.start()
                f(testEnv)
            } finally {
                testEnv.reset()
            }
        }
    }
}
