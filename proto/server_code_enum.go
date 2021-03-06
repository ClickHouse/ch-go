// Code generated by "enumer -type ServerCode -trimprefix ServerCode -output server_code_enum.go"; DO NOT EDIT.

package proto

import (
	"fmt"
	"strings"
)

const _ServerCodeName = "HelloDataExceptionProgressPongEndOfStreamProfileTotalsExtremesTablesStatusLogTableColumnsServerPartUUIDsServerReadTaskRequestServerProfileEvents"

var _ServerCodeIndex = [...]uint8{0, 5, 9, 18, 26, 30, 41, 48, 54, 62, 74, 77, 89, 104, 125, 144}

const _ServerCodeLowerName = "hellodataexceptionprogresspongendofstreamprofiletotalsextremestablesstatuslogtablecolumnsserverpartuuidsserverreadtaskrequestserverprofileevents"

func (i ServerCode) String() string {
	if i >= ServerCode(len(_ServerCodeIndex)-1) {
		return fmt.Sprintf("ServerCode(%d)", i)
	}
	return _ServerCodeName[_ServerCodeIndex[i]:_ServerCodeIndex[i+1]]
}

// An "invalid array index" compiler error signifies that the constant values have changed.
// Re-run the stringer command to generate them again.
func _ServerCodeNoOp() {
	var x [1]struct{}
	_ = x[ServerCodeHello-(0)]
	_ = x[ServerCodeData-(1)]
	_ = x[ServerCodeException-(2)]
	_ = x[ServerCodeProgress-(3)]
	_ = x[ServerCodePong-(4)]
	_ = x[ServerCodeEndOfStream-(5)]
	_ = x[ServerCodeProfile-(6)]
	_ = x[ServerCodeTotals-(7)]
	_ = x[ServerCodeExtremes-(8)]
	_ = x[ServerCodeTablesStatus-(9)]
	_ = x[ServerCodeLog-(10)]
	_ = x[ServerCodeTableColumns-(11)]
	_ = x[ServerPartUUIDs-(12)]
	_ = x[ServerReadTaskRequest-(13)]
	_ = x[ServerProfileEvents-(14)]
}

var _ServerCodeValues = []ServerCode{ServerCodeHello, ServerCodeData, ServerCodeException, ServerCodeProgress, ServerCodePong, ServerCodeEndOfStream, ServerCodeProfile, ServerCodeTotals, ServerCodeExtremes, ServerCodeTablesStatus, ServerCodeLog, ServerCodeTableColumns, ServerPartUUIDs, ServerReadTaskRequest, ServerProfileEvents}

var _ServerCodeNameToValueMap = map[string]ServerCode{
	_ServerCodeName[0:5]:          ServerCodeHello,
	_ServerCodeLowerName[0:5]:     ServerCodeHello,
	_ServerCodeName[5:9]:          ServerCodeData,
	_ServerCodeLowerName[5:9]:     ServerCodeData,
	_ServerCodeName[9:18]:         ServerCodeException,
	_ServerCodeLowerName[9:18]:    ServerCodeException,
	_ServerCodeName[18:26]:        ServerCodeProgress,
	_ServerCodeLowerName[18:26]:   ServerCodeProgress,
	_ServerCodeName[26:30]:        ServerCodePong,
	_ServerCodeLowerName[26:30]:   ServerCodePong,
	_ServerCodeName[30:41]:        ServerCodeEndOfStream,
	_ServerCodeLowerName[30:41]:   ServerCodeEndOfStream,
	_ServerCodeName[41:48]:        ServerCodeProfile,
	_ServerCodeLowerName[41:48]:   ServerCodeProfile,
	_ServerCodeName[48:54]:        ServerCodeTotals,
	_ServerCodeLowerName[48:54]:   ServerCodeTotals,
	_ServerCodeName[54:62]:        ServerCodeExtremes,
	_ServerCodeLowerName[54:62]:   ServerCodeExtremes,
	_ServerCodeName[62:74]:        ServerCodeTablesStatus,
	_ServerCodeLowerName[62:74]:   ServerCodeTablesStatus,
	_ServerCodeName[74:77]:        ServerCodeLog,
	_ServerCodeLowerName[74:77]:   ServerCodeLog,
	_ServerCodeName[77:89]:        ServerCodeTableColumns,
	_ServerCodeLowerName[77:89]:   ServerCodeTableColumns,
	_ServerCodeName[89:104]:       ServerPartUUIDs,
	_ServerCodeLowerName[89:104]:  ServerPartUUIDs,
	_ServerCodeName[104:125]:      ServerReadTaskRequest,
	_ServerCodeLowerName[104:125]: ServerReadTaskRequest,
	_ServerCodeName[125:144]:      ServerProfileEvents,
	_ServerCodeLowerName[125:144]: ServerProfileEvents,
}

var _ServerCodeNames = []string{
	_ServerCodeName[0:5],
	_ServerCodeName[5:9],
	_ServerCodeName[9:18],
	_ServerCodeName[18:26],
	_ServerCodeName[26:30],
	_ServerCodeName[30:41],
	_ServerCodeName[41:48],
	_ServerCodeName[48:54],
	_ServerCodeName[54:62],
	_ServerCodeName[62:74],
	_ServerCodeName[74:77],
	_ServerCodeName[77:89],
	_ServerCodeName[89:104],
	_ServerCodeName[104:125],
	_ServerCodeName[125:144],
}

// ServerCodeString retrieves an enum value from the enum constants string name.
// Throws an error if the param is not part of the enum.
func ServerCodeString(s string) (ServerCode, error) {
	if val, ok := _ServerCodeNameToValueMap[s]; ok {
		return val, nil
	}

	if val, ok := _ServerCodeNameToValueMap[strings.ToLower(s)]; ok {
		return val, nil
	}
	return 0, fmt.Errorf("%s does not belong to ServerCode values", s)
}

// ServerCodeValues returns all values of the enum
func ServerCodeValues() []ServerCode {
	return _ServerCodeValues
}

// ServerCodeStrings returns a slice of all String values of the enum
func ServerCodeStrings() []string {
	strs := make([]string, len(_ServerCodeNames))
	copy(strs, _ServerCodeNames)
	return strs
}

// IsAServerCode returns "true" if the value is listed in the enum definition. "false" otherwise
func (i ServerCode) IsAServerCode() bool {
	for _, v := range _ServerCodeValues {
		if i == v {
			return true
		}
	}
	return false
}
