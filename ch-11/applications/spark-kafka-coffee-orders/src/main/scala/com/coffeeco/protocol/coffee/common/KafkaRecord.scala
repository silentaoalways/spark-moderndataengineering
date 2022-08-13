// Generated by the Scala Plugin for the Protocol Buffer Compiler.
// Do not edit!
//
// Protofile syntax: PROTO3

package com.coffeeco.protocol.coffee.common

@SerialVersionUID(0L)
final case class KafkaRecord(
    key: _root_.com.google.protobuf.ByteString = _root_.com.google.protobuf.ByteString.EMPTY,
    value: _root_.com.google.protobuf.ByteString = _root_.com.google.protobuf.ByteString.EMPTY,
    topic: _root_.scala.Predef.String = "",
    unknownFields: _root_.scalapb.UnknownFieldSet = _root_.scalapb.UnknownFieldSet.empty
    ) extends scalapb.GeneratedMessage with scalapb.lenses.Updatable[KafkaRecord] {
    @transient
    private[this] var __serializedSizeCachedValue: _root_.scala.Int = 0
    private[this] def __computeSerializedValue(): _root_.scala.Int = {
      var __size = 0
      
      {
        val __value = key
        if (!__value.isEmpty) {
          __size += _root_.com.google.protobuf.CodedOutputStream.computeBytesSize(1, __value)
        }
      };
      
      {
        val __value = value
        if (!__value.isEmpty) {
          __size += _root_.com.google.protobuf.CodedOutputStream.computeBytesSize(2, __value)
        }
      };
      
      {
        val __value = topic
        if (!__value.isEmpty) {
          __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(3, __value)
        }
      };
      __size += unknownFields.serializedSize
      __size
    }
    override def serializedSize: _root_.scala.Int = {
      var read = __serializedSizeCachedValue
      if (read == 0) {
        read = __computeSerializedValue()
        __serializedSizeCachedValue = read
      }
      read
    }
    def writeTo(`_output__`: _root_.com.google.protobuf.CodedOutputStream): _root_.scala.Unit = {
      {
        val __v = key
        if (!__v.isEmpty) {
          _output__.writeBytes(1, __v)
        }
      };
      {
        val __v = value
        if (!__v.isEmpty) {
          _output__.writeBytes(2, __v)
        }
      };
      {
        val __v = topic
        if (!__v.isEmpty) {
          _output__.writeString(3, __v)
        }
      };
      unknownFields.writeTo(_output__)
    }
    def withKey(__v: _root_.com.google.protobuf.ByteString): KafkaRecord = copy(key = __v)
    def withValue(__v: _root_.com.google.protobuf.ByteString): KafkaRecord = copy(value = __v)
    def withTopic(__v: _root_.scala.Predef.String): KafkaRecord = copy(topic = __v)
    def withUnknownFields(__v: _root_.scalapb.UnknownFieldSet) = copy(unknownFields = __v)
    def discardUnknownFields = copy(unknownFields = _root_.scalapb.UnknownFieldSet.empty)
    def getFieldByNumber(__fieldNumber: _root_.scala.Int): _root_.scala.Any = {
      (__fieldNumber: @_root_.scala.unchecked) match {
        case 1 => {
          val __t = key
          if (__t != _root_.com.google.protobuf.ByteString.EMPTY) __t else null
        }
        case 2 => {
          val __t = value
          if (__t != _root_.com.google.protobuf.ByteString.EMPTY) __t else null
        }
        case 3 => {
          val __t = topic
          if (__t != "") __t else null
        }
      }
    }
    def getField(__field: _root_.scalapb.descriptors.FieldDescriptor): _root_.scalapb.descriptors.PValue = {
      _root_.scala.Predef.require(__field.containingMessage eq companion.scalaDescriptor)
      (__field.number: @_root_.scala.unchecked) match {
        case 1 => _root_.scalapb.descriptors.PByteString(key)
        case 2 => _root_.scalapb.descriptors.PByteString(value)
        case 3 => _root_.scalapb.descriptors.PString(topic)
      }
    }
    def toProtoString: _root_.scala.Predef.String = _root_.scalapb.TextFormat.printToUnicodeString(this)
    def companion = com.coffeeco.protocol.coffee.common.KafkaRecord
    // @@protoc_insertion_point(GeneratedMessage[KafkaRecord])
}

object KafkaRecord extends scalapb.GeneratedMessageCompanion[com.coffeeco.protocol.coffee.common.KafkaRecord] {
  implicit def messageCompanion: scalapb.GeneratedMessageCompanion[com.coffeeco.protocol.coffee.common.KafkaRecord] = this
  def parseFrom(`_input__`: _root_.com.google.protobuf.CodedInputStream): com.coffeeco.protocol.coffee.common.KafkaRecord = {
    var __key: _root_.com.google.protobuf.ByteString = _root_.com.google.protobuf.ByteString.EMPTY
    var __value: _root_.com.google.protobuf.ByteString = _root_.com.google.protobuf.ByteString.EMPTY
    var __topic: _root_.scala.Predef.String = ""
    var `_unknownFields__`: _root_.scalapb.UnknownFieldSet.Builder = null
    var _done__ = false
    while (!_done__) {
      val _tag__ = _input__.readTag()
      _tag__ match {
        case 0 => _done__ = true
        case 10 =>
          __key = _input__.readBytes()
        case 18 =>
          __value = _input__.readBytes()
        case 26 =>
          __topic = _input__.readStringRequireUtf8()
        case tag =>
          if (_unknownFields__ == null) {
            _unknownFields__ = new _root_.scalapb.UnknownFieldSet.Builder()
          }
          _unknownFields__.parseField(tag, _input__)
      }
    }
    com.coffeeco.protocol.coffee.common.KafkaRecord(
        key = __key,
        value = __value,
        topic = __topic,
        unknownFields = if (_unknownFields__ == null) _root_.scalapb.UnknownFieldSet.empty else _unknownFields__.result()
    )
  }
  implicit def messageReads: _root_.scalapb.descriptors.Reads[com.coffeeco.protocol.coffee.common.KafkaRecord] = _root_.scalapb.descriptors.Reads{
    case _root_.scalapb.descriptors.PMessage(__fieldsMap) =>
      _root_.scala.Predef.require(__fieldsMap.keys.forall(_.containingMessage eq scalaDescriptor), "FieldDescriptor does not match message type.")
      com.coffeeco.protocol.coffee.common.KafkaRecord(
        key = __fieldsMap.get(scalaDescriptor.findFieldByNumber(1).get).map(_.as[_root_.com.google.protobuf.ByteString]).getOrElse(_root_.com.google.protobuf.ByteString.EMPTY),
        value = __fieldsMap.get(scalaDescriptor.findFieldByNumber(2).get).map(_.as[_root_.com.google.protobuf.ByteString]).getOrElse(_root_.com.google.protobuf.ByteString.EMPTY),
        topic = __fieldsMap.get(scalaDescriptor.findFieldByNumber(3).get).map(_.as[_root_.scala.Predef.String]).getOrElse("")
      )
    case _ => throw new RuntimeException("Expected PMessage")
  }
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.Descriptor = CoffeeCommonProto.javaDescriptor.getMessageTypes().get(1)
  def scalaDescriptor: _root_.scalapb.descriptors.Descriptor = CoffeeCommonProto.scalaDescriptor.messages(1)
  def messageCompanionForFieldNumber(__number: _root_.scala.Int): _root_.scalapb.GeneratedMessageCompanion[_] = throw new MatchError(__number)
  lazy val nestedMessagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] = Seq.empty
  def enumCompanionForFieldNumber(__fieldNumber: _root_.scala.Int): _root_.scalapb.GeneratedEnumCompanion[_] = throw new MatchError(__fieldNumber)
  lazy val defaultInstance = com.coffeeco.protocol.coffee.common.KafkaRecord(
    key = _root_.com.google.protobuf.ByteString.EMPTY,
    value = _root_.com.google.protobuf.ByteString.EMPTY,
    topic = ""
  )
  implicit class KafkaRecordLens[UpperPB](_l: _root_.scalapb.lenses.Lens[UpperPB, com.coffeeco.protocol.coffee.common.KafkaRecord]) extends _root_.scalapb.lenses.ObjectLens[UpperPB, com.coffeeco.protocol.coffee.common.KafkaRecord](_l) {
    def key: _root_.scalapb.lenses.Lens[UpperPB, _root_.com.google.protobuf.ByteString] = field(_.key)((c_, f_) => c_.copy(key = f_))
    def value: _root_.scalapb.lenses.Lens[UpperPB, _root_.com.google.protobuf.ByteString] = field(_.value)((c_, f_) => c_.copy(value = f_))
    def topic: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Predef.String] = field(_.topic)((c_, f_) => c_.copy(topic = f_))
  }
  final val KEY_FIELD_NUMBER = 1
  final val VALUE_FIELD_NUMBER = 2
  final val TOPIC_FIELD_NUMBER = 3
  def of(
    key: _root_.com.google.protobuf.ByteString,
    value: _root_.com.google.protobuf.ByteString,
    topic: _root_.scala.Predef.String
  ): _root_.com.coffeeco.protocol.coffee.common.KafkaRecord = _root_.com.coffeeco.protocol.coffee.common.KafkaRecord(
    key,
    value,
    topic
  )
  // @@protoc_insertion_point(GeneratedMessageCompanion[KafkaRecord])
}
