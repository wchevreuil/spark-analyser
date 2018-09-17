package com.cloudera.support.cases.analysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.rdd.RDD

object CasesCorpusWordProcessor {

  def main(args: Array[String]) {

    if (args.length == 0) {

      System.out.println("CasesCorpusWordProcessor {tableName}")

      return ;

    }

    val tableName = args(0);

    val sparkConf = new SparkConf().setAppName("CasesCorpusWordProcessor ")

    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()

    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))

    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    var scan = new Scan()

    scan.setCaching(10)

    scan.addFamily(Bytes.toBytes("msgs"))

    if(args.length>=2)
      scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(args(1))))
//      scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("64879315Case#")))

    val hbaseContext = new HBaseContext(sc, conf);

    val scanRdd = hbaseContext.hbaseRDD( TableName.valueOf(tableName), scan)

//  We need to count words per case, as this will be the Term Frequency for each case.
//  In our table, each case is a row. Each case message will be an additional column under
//  column family "msgs". This leads to the following result for each entry on
//  scanRdd: (ImmutableBytesWritable, Result)
//  Then, 1st thing is to get the value for each message on each case. Each message must
//  stay grouped by case id, so resulting RDDs must stay as tuples.
//  Below line will get cell values in array of byte[].
    // Resulting RDD entries will be: (ImmutableBytesWritable, Array(byte[]))
    val msgsByCaseRdd = scanRdd.map( tuple => (tuple._1, tuple._2.rawCells().map(cell => cell.getValue)))

//  Now it will go for each byte[] on the previous rdd and do a flatMap, so that it can
//    have several values on the second element of the tuple, a pair of (word,1).
//    Since the previous rdd had a an array as the second element, this will still
//    result on an array as the type of second element.
//    Resulting RDD entries will be: (ImmutableBytesWritable, Array((word,1)))
    val wordsByCaseRdd = msgsByCaseRdd.map( tuple => (tuple._1, tuple._2.flatMap( word =>
      Bytes.toString(word).split("\\W+").map( w => ( w,1) ))))

//  Time to reduce, but we need to pair every word with the case id first
//    because we are calculating TF per case.
//    So, we will do flatMap call on the previous RDD, since resulting RDD should
//    have more elements (every item on the Array((word,1)) should be paired with
//    related case id.
//    Resulting RDD entries now should be: (key -> word, word_count)
    val tempRdd = wordsByCaseRdd.flatMap(tuple => tuple._2.map(kv => (Bytes.toString(tuple._1.get()) +
      " -> " + kv._1, kv._2)))

    val caseTfRdd = tempRdd.reduceByKey((x,y) => x+y)
//
//    caseTfRdd.collect.foreach( wordCount =>  println( wordCount._1 + ":" + wordCount._2) )

    hbaseContext.bulkPut[(String, Int)](caseTfRdd,
      TableName.valueOf("cases-tf"),
      (putRecord) => {
        val put = new Put(Bytes.toBytes(putRecord._1))
        put.addColumn(Bytes.toBytes("count"), Bytes.toBytes("1"), Bytes.toBytes(putRecord._2))
//        putRecord._2.foreach((value) => put.add(Bytes.toBytes("count"), Bytes.toBytes("1"), Bytes.toBytes(value)))
        put
      });

    //Need to distinct words occurrence on each case. Example, if case #1 has "hdfs" word 7 times, it
    //will only count as 1 for case #1
    //Because the input is (ImmutableBytesWritable, Array((word,1))), we need to first do a distinct
    //on the Array of words from each case, to just count each word once, per case.
    // Resulting RDD will be: ( ImmutableBytesWritable, Array(word)) where each case has only a single
    // occurrence of each word (no duplicate word).
    val distinctWordsCaseRdd = wordsByCaseRdd.map( tuple => (tuple._1, (tuple._2.map(w => w._1)).distinct))

    //From previous output will flatMap over each case array of words, generating tuples of (word, 1)
    // and ignoring case id, so that partial resulting RDD become: (word, 1).
    // The reduceByKey call then results in the final document frequency for each word.
    val wordsByCase = distinctWordsCaseRdd.flatMap( tuple => tuple._2.map( w=> (w,1))).
      reduceByKey( (x,y) => x+y)

    val filtered = wordsByCase.filter{ case (k,v) => !k.equals("")}

    hbaseContext.bulkPut[(String, Int)](filtered,
      TableName.valueOf("words-df"),
      (putRecord) => {
        println(">>>>" + putRecord._1 + " - " + putRecord._2)
        val put = new Put(Bytes.toBytes(putRecord._1))
        put.addColumn(Bytes.toBytes("count"), Bytes.toBytes("1"), Bytes.toBytes(putRecord._2))
        put
      });
  }
}