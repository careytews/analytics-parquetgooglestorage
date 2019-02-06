package main

// Code for converting a cyberprobe Event object into a FlatEvent which can be
// parquet-serialised.

// FIXME: The set of selected HTTP headers to serialise is mainly arbitrary

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	dt "github.com/trustnetworks/analytics-common/datatypes"
)

// A flattener takes Event objects and outputs FlatEvent objects.  This
// object makes the flattener configurable.
type Flattener struct {
	WritePayloads bool
}

// FlatEvent, similar to the cyberprobe Event, but no structure, useful for
// columnar storage.
type FlatEvent struct {

	// Common fields
	Id     string `parquet:"name=id, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Action string `parquet:"name=action, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Device string `parquet:"name=device, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Time   string `parquet:"name=time, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Origin string `parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// Time in minutes since 1970, and microseconds since 1970.
	TimeMins int32 `parquet:"name=time_mins, type=INT32"`

	// Would like to use TIMESTAMP_MICROS but Spark isn't happy about that.
	TimeMicros int64 `parquet:"name=time_micros, type=INT64"`

	Network string  `parquet:"name=network, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Url     string  `parquet:"name=url, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Risk    float64 `parquet:"name=risk, type=DOUBLE"`

	// Addresses
	SrcIpv4  string `parquet:"name=src_ipv4_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	SrcIpv6  string `parquet:"name=src_ipv6_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	SrcTcp   int32  `parquet:"name=src_tcp_0, type=INT32"`
	SrcUdp   int32  `parquet:"name=src_udp_0, type=INT32"`
	DestIpv4 string `parquet:"name=dest_ipv4_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DestIpv6 string `parquet:"name=dest_ipv6_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DestTcp  int32  `parquet:"name=dest_tcp_0, type=INT32"`
	DestUdp  int32  `parquet:"name=dest_udp_0, type=INT32"`

	// DNS
	DnsMessageAnswerName0    string `parquet:"name=dns_message_answer_name_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerName1    string `parquet:"name=dns_message_answer_name_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerName2    string `parquet:"name=dns_message_answer_name_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerName3    string `parquet:"name=dns_message_answer_name_3, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerName4    string `parquet:"name=dns_message_answer_name_4, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerAddress0 string `parquet:"name=dns_message_answer_address_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerAddress1 string `parquet:"name=dns_message_answer_address_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerAddress2 string `parquet:"name=dns_message_answer_address_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerAddress3 string `parquet:"name=dns_message_answer_address_3, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerAddress4 string `parquet:"name=dns_message_answer_address_4, type=UTF8, encoding=PLAIN_DICTIONARY"`

	DnsMessageQueryName0  string `parquet:"name=dns_message_query_name_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageQueryType0  string `parquet:"name=dns_message_query_type_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageQueryClass0 string `parquet:"name=dns_message_query_class_0, type=UTF8, encoding=PLAIN_DICTIONARY"`

	DnsMessageType string `parquet:"name=dns_message_type, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// HTTP header
	HttpHeader_Accept                    string `parquet:"name=http_header_Accept, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Accept_Encoding           string `parquet:"name=http_header_Accept_Encoding, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Accept_Language           string `parquet:"name=http_header_Accept_Language, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Cache_Control             string `parquet:"name=http_header_Cache_Control, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Connection                string `parquet:"name=http_header_Connection, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Host                      string `parquet:"name=http_header_Host, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Metadata_Flavor           string `parquet:"name=http_header_Metadata_Flavor, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Pragma                    string `parquet:"name=http_header_Pragma, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Referer                   string `parquet:"name=http_header_Referer, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Upgrade_Insecure_Requests string `parquet:"name=http_header_Upgrade_Insecure_Requests, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_User_Agent                string `parquet:"name=http_header_User_Agent, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Content_Length            string `parquet:"name=http_header_Content_Length, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Content_Type              string `parquet:"name=http_header_Content_Type, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Date                      string `parquet:"name=http_header_Date, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_ETag                      string `parquet:"name=http_header_ETag, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Server                    string `parquet:"name=http_header_Server, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_X_Frame_Options           string `parquet:"name=http_header_X_Frame_Options, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_X_XSS_Protection          string `parquet:"name=http_header_X_XSS_Protection, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// HTTP request
	HttpRequestMethod string `parquet:"name=http_request_method, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// HTTP response
	HttpResponseStatus string `parquet:"name=http_response_status, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpResponseCode   int32  `parquet:"name=http_response_code, type=INT32"`

	// Request or response body
	HttpBody string `parquet:"name=http_body, type=BYTE_ARRAY"`

	// ICMP
	IcmpCode    int32  `parquet:"name=icmp_code, type=INT32"`
	IcmpPayload string `parquet:"name=icmp_payload, type=BYTE_ARRAY"`
	IcmpType    int32  `parquet:"name=icmp_type, type=INT32"`

	// Location
	LocationDestAccuracy    int32   `parquet:"name=location_dest_accuracy, type=INT32"`
	LocationDestAsnum       int32   `parquet:"name=location_dest_asnum, type=INT32"`
	LocationDestAsorg       string  `parquet:"name=location_dest_asorg, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationDestCity        string  `parquet:"name=location_dest_city, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationDestCountry     string  `parquet:"name=location_dest_country, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationDestIso         string  `parquet:"name=location_dest_iso, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationDestPositionLat float64 `parquet:"name=location_dest_position_lat, type=DOUBLE"`
	LocationDestPositionLon float64 `parquet:"name=location_dest_position_lon, type=DOUBLE"`
	LocationDestPostCode    string  `parquet:"name=location_dest_postcode, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcAccuracy     int32   `parquet:"name=location_src_accuracy, type=INT32"`
	LocationSrcAsnum        int32   `parquet:"name=location_src_asnum, type=INT32"`
	LocationSrcAsorg        string  `parquet:"name=location_src_asorg, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcCity         string  `parquet:"name=location_src_city, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcCountry      string  `parquet:"name=location_src_country, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcIso          string  `parquet:"name=location_src_iso, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcPositionLat  float64 `parquet:"name=location_src_position_lat, type=DOUBLE"`
	LocationSrcPositionLon  float64 `parquet:"name=location_src_position_lon, type=DOUBLE"`
	LocationSrcPostCode     string  `parquet:"name=location_src_postcode, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// NTP
	NtpTimestampMode    int32 `parquet:"name=ntp_timestamp_mode, type=INT32"`
	NtpTimestampVersion int32 `parquet:"name=ntp_timestamp_version, type=INT32"`

	// Unrecognised datagram
	UnrecognisedDatagramPayload       string `parquet:"name=unrecognised_datagram_payload, type=BYTE_ARRAY"`
	UnrecognisedDatagramPayloadLength int64  `parquet:"name=unrecognised_datagram_payload_length, type=INT64"`
	UnrecognisedDatagramPayloadSha1   string `parquet:"name=unrecognised_datagram_payload_sha1, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// Unrecognised stream
	UnrecognisedStreamPayload       string `parquet:"name=unrecognised_stream_payload, type=BYTE_ARRAY"`
	UnrecognisedStreamPayloadLength int64  `parquet:"name=unrecognised_stream_payload_length, type=INT64"`
	UnrecognisedStreamPayloadSha1   string `parquet:"name=unrecognised_stream_payload_sha1, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// Indicator
	IndicatorId0          string `parquet:"name=indicator_id_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorType0        string `parquet:"name=indicator_type_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorValue0       string `parquet:"name=indicator_value_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorDescription0 string `parquet:"name=indicator_description_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorCategory0    string `parquet:"name=indicator_category_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorAuthor0      string `parquet:"name=indicator_author_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorSource0      string `parquet:"name=indicator_source_0, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// Indicator
	IndicatorId1          string `parquet:"name=indicator_id_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorType1        string `parquet:"name=indicator_type_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorValue1       string `parquet:"name=indicator_value_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorDescription1 string `parquet:"name=indicator_description_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorCategory1    string `parquet:"name=indicator_category_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorAuthor1      string `parquet:"name=indicator_author_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorSource1      string `parquet:"name=indicator_source_1, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// Indicator
	IndicatorId2          string `parquet:"name=indicator_id_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorType2        string `parquet:"name=indicator_type_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorValue2       string `parquet:"name=indicator_value_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorDescription2 string `parquet:"name=indicator_description_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorCategory2    string `parquet:"name=indicator_category_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorAuthor2      string `parquet:"name=indicator_author_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorSource2      string `parquet:"name=indicator_source_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
}

// Decode Base64 string to a string
func Debase64(in string) string {
	enc := base64.StdEncoding
	p, err := enc.DecodeString(in)
	if err == nil {
		return string(p)
	} else {
		return ""
	}

}

// Flatten the source addresses
func (f *Flattener) FlattenSrc(e *dt.Event, oe *FlatEvent) {

	for _, f := range e.Src {
		if strings.HasPrefix(f, "ipv4:") {
			oe.SrcIpv4 = f[5:]
		}
		if strings.HasPrefix(f, "ipv6:") {
			oe.SrcIpv6 = f[5:]
		}
		if strings.HasPrefix(f, "tcp:") {
			port, _ := strconv.Atoi(f[4:])
			oe.SrcTcp = int32(port)
		}
		if strings.HasPrefix(f, "udp:") {
			port, _ := strconv.Atoi(f[4:])
			oe.SrcUdp = int32(port)
		}
	}
}

// Flatten the destination addresses
func (f *Flattener) FlattenDest(e *dt.Event, oe *FlatEvent) {

	for _, f := range e.Dest {
		if strings.HasPrefix(f, "ipv4:") {
			oe.DestIpv4 = f[5:]
		}
		if strings.HasPrefix(f, "ipv6:") {
			oe.DestIpv6 = f[5:]
		}
		if strings.HasPrefix(f, "tcp:") {
			port, _ := strconv.Atoi(f[4:])
			oe.DestTcp = int32(port)
		}
		if strings.HasPrefix(f, "udp:") {
			port, _ := strconv.Atoi(f[4:])
			oe.DestUdp = int32(port)
		}
	}
}

// Flatten DNS information
func (f *Flattener) FlattenDnsMessage(e *dt.Event, oe *FlatEvent) {
	oe.DnsMessageType = e.DnsMessage.Type
	if e.DnsMessage.Query != nil {
		if len(e.DnsMessage.Query) >= 1 {
			oe.DnsMessageQueryName0 =
				e.DnsMessage.Query[0].Name
			oe.DnsMessageQueryType0 =
				e.DnsMessage.Query[0].Type
			oe.DnsMessageQueryClass0 =
				e.DnsMessage.Query[0].Class
		}
	}
	if e.DnsMessage.Answer != nil {
		if len(e.DnsMessage.Answer) >= 1 {
			oe.DnsMessageAnswerName0 =
				e.DnsMessage.Answer[0].Name
			oe.DnsMessageAnswerAddress0 =
				e.DnsMessage.Answer[0].Address
		}
		if len(e.DnsMessage.Answer) >= 2 {
			oe.DnsMessageAnswerName1 =
				e.DnsMessage.Answer[1].Name
			oe.DnsMessageAnswerAddress1 =
				e.DnsMessage.Answer[1].Address
		}
		if len(e.DnsMessage.Answer) >= 3 {
			oe.DnsMessageAnswerName2 =
				e.DnsMessage.Answer[2].Name
			oe.DnsMessageAnswerAddress2 =
				e.DnsMessage.Answer[2].Address
		}
		if len(e.DnsMessage.Answer) >= 4 {
			oe.DnsMessageAnswerName3 =
				e.DnsMessage.Answer[3].Name
			oe.DnsMessageAnswerAddress3 =
				e.DnsMessage.Answer[3].Address
		}
		if len(e.DnsMessage.Answer) >= 5 {
			oe.DnsMessageAnswerName4 =
				e.DnsMessage.Answer[4].Name
			oe.DnsMessageAnswerAddress4 =
				e.DnsMessage.Answer[4].Address
		}
	}
}

// Flatten HTTP information
func (f *Flattener) FlattenHttpHeader(header map[string]string, oe *FlatEvent) {
	if v, ok := header["Accept"]; ok {
		oe.HttpHeader_Accept = v
	}
	if v, ok := header["Accept-Encoding"]; ok {
		oe.HttpHeader_Accept_Encoding = v
	}
	if v, ok := header["Accept-Language"]; ok {
		oe.HttpHeader_Accept_Language = v
	}
	if v, ok := header["Accept-Cache-Control"]; ok {
		oe.HttpHeader_Cache_Control = v
	}
	if v, ok := header["Connection"]; ok {
		oe.HttpHeader_Connection = v
	}
	if v, ok := header["Host"]; ok {
		oe.HttpHeader_Host = v
	}
	if v, ok := header["Metadata-Flavor"]; ok {
		oe.HttpHeader_Metadata_Flavor = v
	}
	if v, ok := header["Pragma"]; ok {
		oe.HttpHeader_Pragma = v
	}
	if v, ok := header["Referer"]; ok {
		oe.HttpHeader_Referer = v
	}
	if v, ok := header["Upgrade-Insecure-Requests"]; ok {
		oe.HttpHeader_Upgrade_Insecure_Requests = v
	}
	if v, ok := header["User-Agent"]; ok {
		oe.HttpHeader_User_Agent = v
	}
	if v, ok := header["Content-Length"]; ok {
		oe.HttpHeader_Content_Length = v
	}
	if v, ok := header["Content-Type"]; ok {
		oe.HttpHeader_Content_Type = v
	}
	if v, ok := header["Date"]; ok {
		oe.HttpHeader_Date = v
	}
	if v, ok := header["ETag"]; ok {
		oe.HttpHeader_ETag = v
	}
	if v, ok := header["Server"]; ok {
		oe.HttpHeader_Server = v
	}
	if v, ok := header["X-Frame-Options"]; ok {
		oe.HttpHeader_X_Frame_Options = v
	}
	if v, ok := header["X-XSS-Protection"]; ok {
		oe.HttpHeader_X_XSS_Protection = v
	}
}

// Flatten an HTTP request
func (f *Flattener) FlattenHttpRequest(e *dt.Event, oe *FlatEvent) {
	oe.HttpRequestMethod = e.HttpRequest.Method
	if f.WritePayloads {
		oe.HttpBody = Debase64(e.HttpRequest.Body)
	}
	f.FlattenHttpHeader(e.HttpRequest.Header, oe)
}

// Flatten an HTTP response
func (f *Flattener) FlattenHttpResponse(e *dt.Event, oe *FlatEvent) {
	oe.HttpResponseStatus = e.HttpResponse.Status
	oe.HttpResponseCode = int32(e.HttpResponse.Code)
	if f.WritePayloads {
		oe.HttpBody = Debase64(e.HttpRequest.Body)
	}
	f.FlattenHttpHeader(e.HttpResponse.Header, oe)
}

// Flatten ICMP information
func (f *Flattener) FlattenIcmp(e *dt.Event, oe *FlatEvent) {
	oe.IcmpCode = int32(e.Icmp.Code)
	oe.IcmpType = int32(e.Icmp.Type)
	if f.WritePayloads {
		oe.IcmpPayload = Debase64(e.Icmp.Payload)
	}
}

// Flatten location information
func (f *Flattener) FlattenLocation(e *dt.Event, oe *FlatEvent) {
	if e.Location.Src != nil {
		oe.LocationSrcAccuracy = int32(e.Location.Src.AccuracyRadius)
		oe.LocationSrcAsnum = int32(e.Location.Src.ASNum)
		oe.LocationSrcAsorg = e.Location.Src.ASOrg
		oe.LocationSrcCity = e.Location.Src.City
		oe.LocationSrcCountry = e.Location.Src.Country
		oe.LocationSrcIso = e.Location.Src.IsoCode
		if e.Location.Src.Position != nil {
			oe.LocationSrcPositionLat =
				e.Location.Src.Position.Latitude
			oe.LocationSrcPositionLon =
				e.Location.Src.Position.Longitude
		}
		oe.LocationSrcPostCode = e.Location.Src.PostCode
	}
	if e.Location.Dest != nil {
		oe.LocationDestAccuracy = int32(e.Location.Dest.AccuracyRadius)
		oe.LocationDestAsnum = int32(e.Location.Dest.ASNum)
		oe.LocationDestAsorg = e.Location.Dest.ASOrg
		oe.LocationDestCity = e.Location.Dest.City
		oe.LocationDestCountry = e.Location.Dest.Country
		oe.LocationDestIso = e.Location.Dest.IsoCode
		if e.Location.Dest.Position != nil {
			oe.LocationDestPositionLat =
				e.Location.Dest.Position.Latitude
			oe.LocationDestPositionLon =
				e.Location.Dest.Position.Longitude
		}
		oe.LocationDestPostCode = e.Location.Dest.PostCode
	}
}

// Flatten NTP information
func (f *Flattener) FlattenNtpTimestamp(e *dt.Event, oe *FlatEvent) {
	oe.NtpTimestampMode = int32(e.NtpTimestamp.Mode)
	oe.NtpTimestampVersion = int32(e.NtpTimestamp.Version)
}

// Flatten unrecognised datagram information
func (f *Flattener) FlattenUnrecognisedDatagram(e *dt.Event, oe *FlatEvent) {

	if f.WritePayloads {
		oe.UnrecognisedDatagramPayload =
			Debase64(e.UnrecognisedDatagram.Payload)
	}
	oe.UnrecognisedDatagramPayloadLength =
		int64(e.UnrecognisedDatagram.PayloadLength)
	oe.UnrecognisedDatagramPayloadSha1 = e.UnrecognisedDatagram.PayloadHash

}

// Flatten an unrecognised stream
func (f *Flattener) FlattenUnrecognisedStream(e *dt.Event, oe *FlatEvent) {

	if f.WritePayloads {
		oe.UnrecognisedStreamPayload =
			Debase64(e.UnrecognisedStream.Payload)
	}
	oe.UnrecognisedStreamPayloadLength =
		int64(e.UnrecognisedStream.PayloadLength)
	oe.UnrecognisedStreamPayloadSha1 = e.UnrecognisedStream.PayloadHash

}

// Flatten DNS information
func (f *Flattener) FlattenIndicators(e *dt.Event, oe *FlatEvent) {
	if e.Indicators != nil {
		if len(*e.Indicators) >= 1 {
			ind := (*e.Indicators)[0]
			oe.IndicatorId0 = ind.Id
			oe.IndicatorType0 = ind.Type
			oe.IndicatorValue0 = ind.Value
			oe.IndicatorDescription0 = ind.Description
			oe.IndicatorCategory0 = ind.Category
			oe.IndicatorAuthor0 = ind.Author
			oe.IndicatorSource0 = ind.Source
		}
		if len(*e.Indicators) >= 2 {
			ind := (*e.Indicators)[1]
			oe.IndicatorId1 = ind.Id
			oe.IndicatorType1 = ind.Type
			oe.IndicatorValue1 = ind.Value
			oe.IndicatorDescription1 = ind.Description
			oe.IndicatorCategory1 = ind.Category
			oe.IndicatorAuthor1 = ind.Author
			oe.IndicatorSource1 = ind.Source
		}
		if len(*e.Indicators) >= 3 {
			ind := (*e.Indicators)[2]
			oe.IndicatorId2 = ind.Id
			oe.IndicatorType2 = ind.Type
			oe.IndicatorValue2 = ind.Value
			oe.IndicatorDescription2 = ind.Description
			oe.IndicatorCategory2 = ind.Category
			oe.IndicatorAuthor2 = ind.Author
			oe.IndicatorSource2 = ind.Source
		}
	}
}

// Flatten an Event, returns a FlatEvent.
func (f *Flattener) FlattenEvent(e *dt.Event) *FlatEvent {

	oe := &FlatEvent{
		Id:      e.Id,
		Action:  e.Action,
		Device:  e.Device,
		Time:    e.Time,
		Network: e.Network,
		Url:     e.Url,
		Risk:    e.Risk,
		Origin:  e.Origin,
	}

	tm, _ := time.Parse("2006-01-02T15:04:05.000Z", e.Time)
	nanos := tm.UnixNano()
	oe.TimeMicros = nanos / 1000
	oe.TimeMins = int32(nanos / 1000000000 / 60)

	f.FlattenSrc(e, oe)
	f.FlattenDest(e, oe)
	if e.DnsMessage != nil {
		f.FlattenDnsMessage(e, oe)
	}

	if e.HttpRequest != nil {
		f.FlattenHttpRequest(e, oe)
	}

	if e.HttpResponse != nil {
		f.FlattenHttpResponse(e, oe)
	}

	if e.Icmp != nil {
		f.FlattenIcmp(e, oe)
	}

	if e.Location != nil {
		f.FlattenLocation(e, oe)
	}

	if e.NtpTimestamp != nil {
		f.FlattenNtpTimestamp(e, oe)
	}

	if e.UnrecognisedDatagram != nil {
		f.FlattenUnrecognisedDatagram(e, oe)
	}

	if e.UnrecognisedStream != nil {
		f.FlattenUnrecognisedStream(e, oe)
	}

	if e.Indicators != nil {
		f.FlattenIndicators(e, oe)
	}

	return oe

}
