package com.swt.test.flink.utils

import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.descriptors.Schema

object CommonUtil {

    def main(args: Array[String]): Unit = {
        println("Hello Scala.")
    }

    def getFlinkSqlSchema(schemaStr: String) = {
        val schema = new Schema()
        if (StringUtils.isNotBlank(schemaStr)) {
            val fieldArray = schemaStr.split(",", -1)

            fieldArray.foreach(field => {
                val nameAndType = field.split(":")
                if ("map".equals(nameAndType(1))) {
                    schema.field(nameAndType(0), DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                }
                else {
                    schema.field(nameAndType(0), nameAndType(1))
                }
            })
        }
        schema
    }
}
