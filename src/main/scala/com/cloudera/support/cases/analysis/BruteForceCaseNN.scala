package com.cloudera.support.cases.analysis

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Get, Result, Scan}
import org.apache.hadoop.hbase.filter.{BinaryComparator, CompareFilter, RowFilter, SubstringComparator}
import org.apache.hadoop.hbase.spark.HBaseContext

/**
  * Created by wchevreuil on 06/04/2018.
  */
object BruteForceCaseNN {

  def main(args: Array[String]): Unit = {

    val caseRowKey = args(0);

    val sparkConf = new SparkConf().setAppName("BruteForceCaseNN")

    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()

    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))

    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    var scan = new Scan()

//    scan.setCaching(10)

    scan.addFamily(Bytes.toBytes("msgs"))

    scan.setFilter(new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new
        BinaryComparator(Bytes.toBytes("1450543590-122205"))))

    val hbaseContext = new HBaseContext(sc, conf)

    val scanRdd = hbaseContext.hbaseRDD( TableName.valueOf("cases"), scan)

    //The first scan resulted on a RDD with a single tuple, where first
    // element was the row key, and second element was the set of cells.
    // We want to combine individual messages with the row key, so this next
    // map will result in an RDD where each cell is paired in a separate
    // tuple with the row key.
    // For example, say case_1 had 2 messages, msg1 and msg2. The previous
    // scan result would be an RDD as: [case_1, (msg1, msg2)]. Now this next
    // scan will result in the following: [case_1, msg1], [case_1, msg2]
    val msgsByCaseRdd = scanRdd.map( tuple => (tuple._1, tuple._2.rawCells().map(cell => cell.getValue)))

    //Now what we want is a distinct list of words for the case. So we need
    // to go over each word from each message, distinct it and pair with the
    // case id. Resulting RDD will be: [ case_1, (w1, w2, w3, ...) ]
    val caseWordsRdd = msgsByCaseRdd.map( tuple => (tuple._1, tuple._2
      .flatMap( word => Bytes.toString(word).split("\\W+").map( w => (w) ))
      .distinct))

    //We had the TFs for each case already calculated and stored on cases-tf
    // table. Now we need to generate the rowkey for each of this case words
    // and qyery that table.
    val caseTFs = caseWordsRdd.flatMap(key => key._2.map(w=> key._1 +
      " -> " + w)).map( w => Bytes.toBytes(w))

    //bulk gets all TFs for this case. It still keeps the case row key so
    // that ti can later insert the calculated tfidfs on  its own table.
    val getCaseTFsRdd = hbaseContext.bulkGet[Array[Byte], String](
      TableName.valueOf("cases-tf"), 2, caseTFs, record => { new Get(record) },
      (result : Result) => {
        val it = result.list().iterator()
        val b = new StringBuilder
        b.append(Bytes.toString(result.getRow()) + ":")
        while (it.hasNext()) {
          val kv = it.next()
          b.append(Bytes.toInt(kv.getValue))
          }
        b.toString()
      })

    // Will return the case message words itself as unique values
    val wordsRdd = caseWordsRdd.flatMap( key => key._2.map( w => Bytes
      .toBytes(w)))

    // bulk gets the document frequency for each of these words.
    val getTermsDFsRdd = hbaseContext.bulkGet[Array[Byte], String](
       TableName.valueOf("words-df"), 2, wordsRdd,
      record => { new Get(record)},
       (result : Result) => {
         val it = result.list().iterator()
         val b = new StringBuilder
         b.append(Bytes.toString(result.getRow()) + ":")
         while (it.hasNext()){
           val kv = it.next()
           b.append(Bytes.toInt(kv.getValue))
         }
         b.toString()
       })

    val kvTermsDFsRdd = getTermsDFsRdd.map( kv => (kv.split(":")(0), kv.split(":")(1)))

    val tupleCaseTFsRdd = getCaseTFsRdd.map ( kv => (kv.split(":")(0).split(" -> ")(1),
      (kv.split(":")(1), kv.split(" -> ")(0))))

    //joins the two RDDs by word
    val join = kvTermsDFsRdd.join(tupleCaseTFsRdd)

    //Calculates tf*idf for each word, for each case.
    val tfidfRdd = join.map( tuple => (tuple._1,
      (BigDecimal.apply(tuple._2._2._1)/BigDecimal.apply(tuple._2._1), tuple
        ._2._2._2)))





  }



}
