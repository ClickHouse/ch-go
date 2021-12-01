// Code generated by "enumer -type Feature -trimprefix Feature -output feature_gen.go"; DO NOT EDIT.

package proto

import (
	"fmt"
	"strings"
)

const _FeatureName = "TempTablesTimezoneQuotaKeyInClientInfoDisplayNameVersionPatchServerLogsColumnDefaultsMetadataClientWriteInfoSettingsSerializedAsStringsInterServerSecretOpenTelemetryQueryStartTime"
const _FeatureLowerName = "temptablestimezonequotakeyinclientinfodisplaynameversionpatchserverlogscolumndefaultsmetadataclientwriteinfosettingsserializedasstringsinterserversecretopentelemetryquerystarttime"

var _FeatureMap = map[Feature]string{
	50264: _FeatureName[0:10],
	54058: _FeatureName[10:18],
	54060: _FeatureName[18:38],
	54372: _FeatureName[38:49],
	54401: _FeatureName[49:61],
	54406: _FeatureName[61:71],
	54410: _FeatureName[71:93],
	54420: _FeatureName[93:108],
	54429: _FeatureName[108:135],
	54441: _FeatureName[135:152],
	54442: _FeatureName[152:165],
	54449: _FeatureName[165:179],
}

func (i Feature) String() string {
	if str, ok := _FeatureMap[i]; ok {
		return str
	}
	return fmt.Sprintf("Feature(%d)", i)
}

// An "invalid array index" compiler error signifies that the constant values have changed.
// Re-run the stringer command to generate them again.
func _FeatureNoOp() {
	var x [1]struct{}
	_ = x[FeatureTempTables-(50264)]
	_ = x[FeatureTimezone-(54058)]
	_ = x[FeatureQuotaKeyInClientInfo-(54060)]
	_ = x[FeatureDisplayName-(54372)]
	_ = x[FeatureVersionPatch-(54401)]
	_ = x[FeatureServerLogs-(54406)]
	_ = x[FeatureColumnDefaultsMetadata-(54410)]
	_ = x[FeatureClientWriteInfo-(54420)]
	_ = x[FeatureSettingsSerializedAsStrings-(54429)]
	_ = x[FeatureInterServerSecret-(54441)]
	_ = x[FeatureOpenTelemetry-(54442)]
	_ = x[FeatureQueryStartTime-(54449)]
}

var _FeatureValues = []Feature{FeatureTempTables, FeatureTimezone, FeatureQuotaKeyInClientInfo, FeatureDisplayName, FeatureVersionPatch, FeatureServerLogs, FeatureColumnDefaultsMetadata, FeatureClientWriteInfo, FeatureSettingsSerializedAsStrings, FeatureInterServerSecret, FeatureOpenTelemetry, FeatureQueryStartTime}

var _FeatureNameToValueMap = map[string]Feature{
	_FeatureName[0:10]:         FeatureTempTables,
	_FeatureLowerName[0:10]:    FeatureTempTables,
	_FeatureName[10:18]:        FeatureTimezone,
	_FeatureLowerName[10:18]:   FeatureTimezone,
	_FeatureName[18:38]:        FeatureQuotaKeyInClientInfo,
	_FeatureLowerName[18:38]:   FeatureQuotaKeyInClientInfo,
	_FeatureName[38:49]:        FeatureDisplayName,
	_FeatureLowerName[38:49]:   FeatureDisplayName,
	_FeatureName[49:61]:        FeatureVersionPatch,
	_FeatureLowerName[49:61]:   FeatureVersionPatch,
	_FeatureName[61:71]:        FeatureServerLogs,
	_FeatureLowerName[61:71]:   FeatureServerLogs,
	_FeatureName[71:93]:        FeatureColumnDefaultsMetadata,
	_FeatureLowerName[71:93]:   FeatureColumnDefaultsMetadata,
	_FeatureName[93:108]:       FeatureClientWriteInfo,
	_FeatureLowerName[93:108]:  FeatureClientWriteInfo,
	_FeatureName[108:135]:      FeatureSettingsSerializedAsStrings,
	_FeatureLowerName[108:135]: FeatureSettingsSerializedAsStrings,
	_FeatureName[135:152]:      FeatureInterServerSecret,
	_FeatureLowerName[135:152]: FeatureInterServerSecret,
	_FeatureName[152:165]:      FeatureOpenTelemetry,
	_FeatureLowerName[152:165]: FeatureOpenTelemetry,
	_FeatureName[165:179]:      FeatureQueryStartTime,
	_FeatureLowerName[165:179]: FeatureQueryStartTime,
}

var _FeatureNames = []string{
	_FeatureName[0:10],
	_FeatureName[10:18],
	_FeatureName[18:38],
	_FeatureName[38:49],
	_FeatureName[49:61],
	_FeatureName[61:71],
	_FeatureName[71:93],
	_FeatureName[93:108],
	_FeatureName[108:135],
	_FeatureName[135:152],
	_FeatureName[152:165],
	_FeatureName[165:179],
}

// FeatureString retrieves an enum value from the enum constants string name.
// Throws an error if the param is not part of the enum.
func FeatureString(s string) (Feature, error) {
	if val, ok := _FeatureNameToValueMap[s]; ok {
		return val, nil
	}

	if val, ok := _FeatureNameToValueMap[strings.ToLower(s)]; ok {
		return val, nil
	}
	return 0, fmt.Errorf("%s does not belong to Feature values", s)
}

// FeatureValues returns all values of the enum
func FeatureValues() []Feature {
	return _FeatureValues
}

// FeatureStrings returns a slice of all String values of the enum
func FeatureStrings() []string {
	strs := make([]string, len(_FeatureNames))
	copy(strs, _FeatureNames)
	return strs
}

// IsAFeature returns "true" if the value is listed in the enum definition. "false" otherwise
func (i Feature) IsAFeature() bool {
	_, ok := _FeatureMap[i]
	return ok
}
