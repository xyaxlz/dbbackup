#!/bin/bash

awk \
-v "OUTDIR=${OUTDIR:-out}" \
-v "PREFIX=${PREFIX:-}" \
-v "SUFFIX=${SUFFIX:-.sql}" \
-v "PRINT=${PRINT:-N}" \
-v "EXECUTE=${EXECUTE:-Y}" \
'
BEGIN {
  HEADER_PREFIX="-- MySQL dump "
  DATABASE_DEFINE_PREFIX="-- Current Database: "
  TABLE_DEFINE_PREFIX="-- Table structure for table "
  TABLE_DATA_PREFIX="-- Dumping data for table "
  VIEW_TEMPTABLE_PREFIX="-- Temporary table structure for view "
  VIEW_DEFINE_PREFIX="-- Final view structure for view "
  ROUTINE_DEFINE_PREFIX="-- Dumping routines for database "
  HEADER_PREFIX_LEN=length(HEADER_PREFIX)
  DATABASE_DEFINE_PREFIX_LEN=length(DATABASE_DEFINE_PREFIX)
  TABLE_DEFINE_PREFIX_LEN=length(TABLE_DEFINE_PREFIX)
  TABLE_DATA_PREFIX_LEN=length(TABLE_DATA_PREFIX)
  VIEW_TEMPTABLE_PREFIX_LEN=length(VIEW_TEMPTABLE_PREFIX)
  VIEW_DEFINE_PREFIX_LEN=length(VIEW_DEFINE_PREFIX)
  ROUTINE_DEFINE_PREFIX_LEN=length(ROUTINE_DEFINE_PREFIX)
  TRAILER_LINE="/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;"
  TRIGGER_DEFINE_LINE="/*!50003 SET @saved_cs_client      = @@character_set_client */ ;"
  system("mkdir -p " OUTDIR)
}

function norm(s,c,a,b)
{
  a=1
  b=length(s)
  c=substr(s,b,1)
  if ( c=="`" || c=="\x27" ){
    b-=1
  }
  
  c=substr(s,a,1)
  if ( c=="`"||c=="\x27" ){
    a+=1; b-=1
  }
  
  return substr(s,a,b) 
}

function print_use()
{
  if (PRINT=="Y"){
    print "use " DB ";"
  }
  if (EXECUTE=="Y"){
    print "use " DB ";" >> OUTDIR "/" PREFIX FILE SUFFIX
  }
}

{
  if ( substr($0,1,HEADER_PREFIX_LEN) == HEADER_PREFIX ){
    FLAG="header"
    FILE="header"
  }else if ( substr($0,1,DATABASE_DEFINE_PREFIX_LEN) == DATABASE_DEFINE_PREFIX ){
    DB=$NF
    DBNAME=norm(DB)
    FILE="database_define." DBNAME
  }else if ( substr($0,1,TABLE_DEFINE_PREFIX_LEN) == TABLE_DEFINE_PREFIX ){
    TBL=$NF
    TBLNAME=norm(TBL)
    FILE="table_define." DBNAME "." TBLNAME

    if (PRINT=="Y"){
      print DB "." TBL "(table_order)"
    }
    if (EXECUTE=="Y"){
      print DB "." TBL >> OUTDIR "/" PREFIX "table_order.txt"
    }
  } else if ( substr($0,1,TABLE_DATA_PREFIX_LEN) == TABLE_DATA_PREFIX ){
    FLAG="table_data"
    FILE="table_data." DBNAME "." TBLNAME
  }else if ( substr($0,1,VIEW_TEMPTABLE_PREFIX_LEN) == VIEW_TEMPTABLE_PREFIX ){
    FILE="alldb_ddl"
    print_use()
  }else if ( substr($0,1,VIEW_DEFINE_PREFIX_LEN) == VIEW_DEFINE_PREFIX ){
    FILE="alldb_ddl"
    print_use()
  } else if ( substr($0,1,ROUTINE_DEFINE_PREFIX_LEN) == ROUTINE_DEFINE_PREFIX ){
    DB=$NF
    FILE="alldb_ddl"
    print_use()
  } else if ( $0 == TRAILER_LINE ){
    if (FLAG!="header"){
      FILE="trailer"
    }
  } else if ( $0 == TRIGGER_DEFINE_LINE ){
    if (FLAG=="table_data") {
      FILE="alldb_ddl"
      print_use()
    }
  }
  if (PRINT=="Y"){
    print
  }
  if (EXECUTE=="Y"){
    print >> OUTDIR "/" PREFIX FILE SUFFIX
  }
}
' $1

