package com.keedio.flink.utils

import com.keedio.flink.entities.LogEntry
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

/**
  * Created by luislazaro on 17/4/17.
  * lalazaro@keedio.com
  * Keedio
  */

/**
  * Implements a Serialization-Schema and DeserializationSchema for kafka data sinks and sources.
  */
class LogEntrySchema(val parseBody: Boolean) extends DeserializationSchema[LogEntry] with SerializationSchema[LogEntry]{

  override def serialize(logEntry: LogEntry): Array[Byte] = logEntry.toString.getBytes

  override def deserialize(message: Array[Byte]): LogEntry = LogEntry(new String(message), parseBody)

  override def isEndOfStream(nextElement: LogEntry): Boolean = false

  override def getProducedType: TypeInformation[LogEntry] = TypeExtractor.getForClass(classOf[LogEntry])


}
