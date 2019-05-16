package com.cloudera.scala

import org.apache.spark.sql.SparkSession

  class App {
    val appName = "ice-spark-250268"

    val simId = 250268

    val tgtDb = "qa_ice_simulation_data"

    val srcTable = "svdriverdetail_spark"

    val tgtTable = "svdriverlist"


    val spark = SparkSession

      .builder()

      .appName(appName)

      .enableHiveSupport()

      .getOrCreate()


    //SQL to get seed data

    val df_driverlist_seed = spark.sql(
      s"""

SELECT

  *

  ,0 AS cnt

  ,ROW_NUMBER() OVER (PARTITION BY SimID, ESN, FleetID, IterNum, EventDate ORDER BY DriverName) AS sequence

FROM

  $tgtDb.$srcTable

WHERE

  SimID = $simId

""");



    //Register the dataframe as a temp table to be used in next step for iteration.

    df_driverlist_seed.createOrReplaceTempView("vt_seed0")


    //Hold value of the number of rows in the new dataset

    var df_cnt: Int = 1


    //Iteration Counter

    var cnt: Int = 1


    //Use the following while loop to generate a new dataframe for each run.

    //We have generated a new dataframe with sequence. At each step, the previous dataframe is used to retrieve new resultset.

    //If the DataFrame does not have any rows, then the loop is terminated.

    //Same query from "Iteration" Statement is used here too

    //Also only register a temporary table if the dataframe has rows in it, hence the "IF" condition is present in WHILE loop.


    while (df_cnt != 0) {

      var tblnm = "vt_seed".concat((cnt - 1).toString);

      var tblnm1 = "vt_seed".concat((cnt).toString);

      val df_driverlist_rec = spark.sql(
        s"""

    SELECT

      concat_ws(',',tb2.drivername,tb1.drivername) AS drivername

      ,tb1.sequence

      ,tb1.simid

      ,tb1.esn

      ,tb1.fleetid

      ,tb1.iternum

      ,tb1.eventdate

      ,$cnt AS cnt

    FROM

      vt_seed0 tb1

      ,$tblnm tb2

    WHERE

      tb2.sequence+1=tb1.sequence

      AND tb2.esn = tb1.esn

      AND tb2.fleetid = tb1.fleetid

      AND tb2.iternum = tb1.iternum

      AND tb2.eventdate = tb1.eventdate""");

      df_cnt = df_driverlist_rec.count.toInt;

      if (df_cnt != 0) {

        df_driverlist_rec.createOrReplaceTempView(s"$tblnm1");

      }

      cnt = cnt + 1;

    }


    //Union all of the tables together.

    var union_query = "";

    for (a <- 0 to (cnt - 2)) {

      if (a == 0) {

        union_query = union_query.concat("SELECT drivername, sequence, simid, esn, fleetid, iternum, eventdate, cnt FROM vt_seed").concat(a.toString());

      }

      else {

        union_query = union_query.concat(" UNION SELECT drivername, sequence, simid, esn, fleetid, iternum, eventdate, cnt FROM vt_seed").concat(a.toString());

      }

    }


    //Create the DataFrame with the SQL

    val df_union = spark.sql(s"$union_query");

    df_union.createOrReplaceTempView("vt_union")


    //Execute the analytical query to extract only the max iteration based on partition

    //Insert Overwrite the target partitions, NOTE I added a distribute by on SIMID that forces spark to a single reducer for each SIM.

    //Prior it was creating tons of files.

    //Finally we only want the max sequence from each set in the last result


    //set non-strict mode because of partitioning


    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    val df_maxrecord = spark.sql(
      s"""

  SELECT

    esn

    , fleetid

    , iternum

    , eventdate

    , drivername

    , sequence

    , simid

  FROM

  (

    SELECT

      esn

      , fleetid

      , iternum

      , eventdate

      , drivername

      , sequence

      , simid

      , max(sequence) over (partition by simid, esn, fleetid, iternum, eventdate) max_sequence

    FROM

    (

      SELECT

        drivername

        , sequence

        , simid

        , esn

        , fleetid

        , iternum

        , eventdate

        , cnt

        , max(cnt) over (partition by sequence, simid, esn, fleetid, iternum, eventdate) max_cnt

      FROM vt_union

    )

    WHERE

      cnt = max_cnt

  )

  WHERE

    sequence = max_sequence

  DISTRIBUTE BY simid

""")

    df_maxrecord.createOrReplaceTempView("vt_maxrecord")


    val df_materialize = spark.sql(
      s"""

  INSERT OVERWRITE TABLE $tgtDb.$tgtTable PARTITION ( simid )

  SELECT * FROM vt_maxrecord

""")
  }
