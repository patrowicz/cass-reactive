package org.example

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.*
import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.core.uuid.Uuids
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.random.Random

fun main(args: Array<String>) {
    val keyspace = CqlIdentifier.fromCql("test")
    CqlSession.builder().withKeyspace(keyspace).build().use { session ->
        val tStmt = SchemaBuilder
            .createTable("study")
            .ifNotExists()
            .withPartitionKey("year",DataTypes.INT)
            .withClusteringColumn("month",DataTypes.INT)
            .withClusteringColumn("id",DataTypes.TIMEUUID)
            .withColumn("description",DataTypes.TEXT)
            .build()
        val iStmt = SimpleStatement.newInstance("""
            CREATE CUSTOM INDEX IF NOT EXISTS study_idx ON study ()
            USING 'com.stratio.cassandra.lucene.Index'
            WITH OPTIONS = {
               'refresh_seconds': '1',
               'schema': '{
                  fields: {
                     description: {type: "text", analyzer: "english"}
                  }
               }'
            };
        """.trimIndent())

        session.execute(tStmt)
        session.execute(iStmt)

        inserts(session).blockLast()

        Flux.fromIterable(2000..2009).flatMap { y ->
            Flux
                .from(session
                    .executeReactive(SimpleStatement.newInstance("select description from study where year=?",y))
                )
                .map { it.getString(0)?:"" }
                .take(1000000)
        }
            .count()
//            .flatMap { Flux.fromIterable(it.split(" ")) }
//            .groupBy( { it },Int.MAX_VALUE)
//            .flatMap { it.count().map { c -> it.key() to c  } }
//            .filter { it.second>10 }
            .doOnNext {
                println(it)
            }
            .block()
//            .blockLast()
//        System.out.println(row?.getString("release_version"))

    }
}

private fun inserts(session: CqlSession): Flux<ReactiveRow> {
    val insert = session.prepare("insert into study (year,month,id,description) values (?,?,?,?)")
    val years = Flux.fromIterable(2000..2009)
    val inserts = years.concatMap { y ->
        println("Start y $y")
        Flux.fromIterable(1..12).concatMap { m ->
            println("Start m $y.$m")
            Flux.fromIterable(1..10000)
                .flatMap( { s ->
                    Flux
                        .from(
                            session
                                .executeReactive(
                                    insert.bind(y, m, Uuids.timeBased(), rtext())
                                )
                        )
                        .doOnError { it.printStackTrace() }
                        .onErrorResume { Mono.empty() }
                }, 10)


        }
    }
    return inserts
}

fun rtext() = (1..4).map {

    ('a'..'z').map { it }.shuffled().subList(0, 3).joinToString("")

}.joinToString(" ")

