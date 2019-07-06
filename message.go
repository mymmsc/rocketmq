package rocketmq

import (
	"bytes"
	//"compress/zlib"
	"github.com/klauspost/compress/zlib"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

const (
	CompressedFlag          = (0x1 << 0)
	MultiTagsFlag           = (0x1 << 1)
	TransactionNotType      = (0x0 << 2)
	TransactionPreparedType = (0x1 << 2)
	TransactionCommitType   = (0x2 << 2)
	TransactionRollbackType = (0x3 << 2)
)

const (
	NameValueSeparator = 1 + iota
	PropertySeparator
)

const (
	CharacterMaxLength = 255
)

type Message struct {
	Topic      string
	Flag       int32
	Properties map[string]string
	Body       []byte
}

func NewMessage(topic string, body []byte) *Message {
	return &Message{
		Topic:      topic,
		Body:       body,
		Properties: make(map[string]string),
	}
}

type MessageExt struct {
	Message
	QueueId       int32
	StoreSize     int32
	QueueOffset   int64
	SysFlag       int32
	BornTimestamp int64
	// bornHost
	StoreTimestamp int64
	// storeHost
	MsgId                     string
	CommitLogOffset           int64
	BodyCRC                   int32
	ReconsumeTimes            int32
	PreparedTransactionOffset int64
}

func decodeMessage(data []byte) []*MessageExt {
	buf := bytes.NewBuffer(data)
	var storeSize, magicCode, bodyCRC, queueId, flag, sysFlag, reconsumeTimes, bodyLength, bornPort, storePort int32
	var queueOffset, physicOffset, preparedTransactionOffset, bornTimeStamp, storeTimestamp int64
	var topicLen byte
	var topic, body, properties, bornHost, storeHost []byte
	var propertiesLength int16

	var propertiesMap map[string]string

	msgs := make([]*MessageExt, 0, 32)
	for buf.Len() > 0 {
		msg := new(MessageExt)
		binary.Read(buf, binary.BigEndian, &storeSize)
		binary.Read(buf, binary.BigEndian, &magicCode)
		binary.Read(buf, binary.BigEndian, &bodyCRC)
		binary.Read(buf, binary.BigEndian, &queueId)
		binary.Read(buf, binary.BigEndian, &flag)
		binary.Read(buf, binary.BigEndian, &queueOffset)
		binary.Read(buf, binary.BigEndian, &physicOffset)
		binary.Read(buf, binary.BigEndian, &sysFlag)
		binary.Read(buf, binary.BigEndian, &bornTimeStamp)
		bornHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &bornHost)
		binary.Read(buf, binary.BigEndian, &bornPort)
		binary.Read(buf, binary.BigEndian, &storeTimestamp)
		storeHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &storeHost)
		binary.Read(buf, binary.BigEndian, &storePort)
		binary.Read(buf, binary.BigEndian, &reconsumeTimes)
		binary.Read(buf, binary.BigEndian, &preparedTransactionOffset)
		binary.Read(buf, binary.BigEndian, &bodyLength)
		if bodyLength > 0 {
			body = make([]byte, bodyLength)
			binary.Read(buf, binary.BigEndian, body)

			if (sysFlag & CompressedFlag) == CompressedFlag {
				b := bytes.NewReader(body)
				z, err := zlib.NewReader(b)
				if err != nil {
					fmt.Println(err)
					return nil
				}
				defer z.Close()
				body, err = ioutil.ReadAll(z)
				if err != nil {
					fmt.Println(err)
					return nil
				}
			}
		}
		binary.Read(buf, binary.BigEndian, &topicLen)
		topic = make([]byte, 0)
		binary.Read(buf, binary.BigEndian, &topic)
		binary.Read(buf, binary.BigEndian, &propertiesLength)
		if propertiesLength > 0 {
			properties = make([]byte, propertiesLength)
			binary.Read(buf, binary.BigEndian, &properties)
			propertiesMap = make(map[string]string)
			json.Unmarshal(properties, &propertiesMap)
		}

		/*if magicCode != -626843481 {
			fmt.Printf("magic code is error %d", magicCode)
			return nil
		}*/

		msg.Topic = string(topic)
		msg.QueueId = queueId
		msg.SysFlag = sysFlag
		msg.QueueOffset = queueOffset
		msg.BodyCRC = bodyCRC
		msg.StoreSize = storeSize
		msg.BornTimestamp = bornTimeStamp
		msg.ReconsumeTimes = reconsumeTimes
		msg.Flag = flag
		//msg.commitLogOffset=physicOffset
		msg.StoreTimestamp = storeTimestamp
		msg.PreparedTransactionOffset = preparedTransactionOffset
		msg.Body = body
		msg.Properties = propertiesMap

		msgs = append(msgs, msg)
	}

	return msgs
}

//NAME_VALUE_SEPARATOR char 1 and 2 from java code
var NAME_VALUE_SEPARATOR = string(rune(NameValueSeparator))

//PROPERTY_SEPARATOR property separator
var PROPERTY_SEPARATOR = string(rune(PropertySeparator))

var (
	NameValueSep    = byte(1)
	PropertySep     = byte(2)
	nameValueSepStr = string([]byte{NameValueSep})
	propertySepStr  = string([]byte{PropertySep})
)

// Properties2Bytes converts properties to byte array
func Properties2Bytes(properties map[string]string) []byte {
	if len(properties) == 0 {
		return nil
	}
	//fmt.Printf("properties langth=%d\n", propertiesLength(properties))
	bs, n := make([]byte, propertiesLength(properties)), 0
	for k, v := range properties {
		n += copy(bs[n:], k)
		bs[n] = NameValueSep
		n++

		n += copy(bs[n:], v)
		bs[n] = PropertySep
		n++
	}

	return bs
}

func propertiesLength(properties map[string]string) (size int) {
	for k, v := range properties {
		size += len(k) + 1 + len(v) + 1
		//fmt.Printf("pp: k=%s, v=%s\n", k, v)
	}
	return
}

// Properties2String converts properties to string
func messageProperties2String(properties map[string]string) string {
	return string(Properties2Bytes(properties))
}

// String2Properties converts string to map
func String2Properties(properties string) map[string]string {
	ret := make(map[string]string, 32)
	for _, p := range strings.Split(properties, propertySepStr) {
		nv := strings.Split(p, nameValueSepStr)
		if len(nv) == 2 {
			ret[nv[0]] = nv[1]
		}
	}

	return ret
}

func messageProperties2String_too_long(properties map[string]string) string {
	stringBuilder := bytes.NewBuffer([]byte{})
	if properties != nil && len(properties) != 0 {
		for k, v := range properties {
			stringBuilder.WriteString(k);
			stringBuilder.WriteString(NAME_VALUE_SEPARATOR)
			stringBuilder.WriteString(v);
			stringBuilder.WriteString(PROPERTY_SEPARATOR)
		}
	}
	return stringBuilder.String()
}

func origin_messageProperties2String(properties map[string]string) string {
	StringBuilder := bytes.NewBuffer([]byte{})
	if properties != nil && len(properties) != 0 {
		for k, v := range properties {
			binary.Write(StringBuilder, binary.BigEndian, k)                  // 4
			binary.Write(StringBuilder, binary.BigEndian, NameValueSeparator) // 4
			binary.Write(StringBuilder, binary.BigEndian, v)                  // 4
			binary.Write(StringBuilder, binary.BigEndian, PropertySeparator)  // 4
		}
	}
	return StringBuilder.String()
}

func (msg Message) checkMessage(producer *DefaultProducer) (err error) {
	if err = checkTopic(msg.Topic); err != nil {
		if len(msg.Body) == 0 {
			err = errors.New("ResponseCode:" + strconv.Itoa(MsgIllegal) + ", the message body is null")
		} else if len(msg.Body) > producer.maxMessageSize {
			err = errors.New("ResponseCode:" + strconv.Itoa(MsgIllegal) + ", the message body size over max value, MAX:" + strconv.Itoa(producer.maxMessageSize))
		}
	}
	return
}

func checkTopic(topic string) (err error) {
	if topic == "" {
		err = errors.New("the specified topic is blank")
	}
	if len(topic) > CharacterMaxLength {
		err = errors.New("the specified topic is longer than topic max length 255")
	}
	if topic == DefaultTopic {
		err = errors.New("the topic[" + topic + "] is conflict with default topic")
	}
	return
}
