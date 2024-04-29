from snowflake.snowpark import Session
from snowflake.snowpark import Window
from snowflake.snowpark.functions import col,min,rank,max,avg,sum
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

connection_parameters = {"account":"wkb48950.us-east-1",
"user":"jeanboscokpowadan",
"password": 'Tohono123&le',
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()
emp_stg_tbl=session.table("EMPLY")
emp_dpt_tbl=session.table("DEPARTMENT")
emp_dpt_join=emp_stg_tbl.join(emp_dpt_tbl,emp_stg_tbl["DEPTCODE"]==emp_dpt_tbl["DEPTCODE"],"inner")
emp_dpt_select=emp_dpt_join.select("EMPFNAME","EMPLNAME",emp_stg_tbl.DEPTCODE.alias("DEPTCODE"),"DEPTNAME","LOCATION").orderBy(col('EMPFNAME').asc())
emp_dpt_select.show()

total_sal_group_by=emp_stg_tbl.group_by("EMPFNAME").agg(sum(col("COMMISSION")+col("SALARY")).alias("TOTAL_SALARY"))
total_sal_group_by.show()
emp_max_salary= emp_stg_tbl.select("salary",rank().over(Window.order_by(col("salary").desc())).as_("RANK"))\
.where(col("RANK")<=2).distinct().select("salary").show()



total_sal_ana20=emp_stg_tbl.where((col("JOB")=='SOFTWARE ENGINEER') & (col("DEPTCODE")==20)).group_by("EmpCode","EMPFNAME").agg(sum(col("SALARY")+col("COMMISSION")).alias("TOTALSALARY")).select("EMPCODE","EMPFNAME","TOTALSALARY").show()

total_sal=emp_stg_tbl.where(col("JOB")=='ANALYST').agg(avg("SALARY"),max("SALARY"),min("SALARY"))
total_sal.show()