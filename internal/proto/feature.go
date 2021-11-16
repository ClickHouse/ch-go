package proto

//go:generate go run github.com/dmarkham/enumer -type Feature -trimprefix Feature -output feature_gen.go

// Feature represents server side feature.
type Feature byte

// Possible features.
const (
	FeatureTimezone Feature = iota
	FeatureQuotaKeyInClientInfo
	FeatureDisplayName
	FeatureVersionPatch
	FeatureServerLogs
	FeatureColumnDefaultsMetadata
	FeatureClientWriteInfo
	FeatureSettingsSerializedAsStrings
	FeatureInterServerSecret
	FeatureOpenTelemetry
)

var featureRevision = map[Feature]int{
	FeatureTimezone:                    54058,
	FeatureDisplayName:                 54372,
	FeatureQuotaKeyInClientInfo:        54060,
	FeatureVersionPatch:                54401,
	FeatureServerLogs:                  54406,
	FeatureColumnDefaultsMetadata:      54410,
	FeatureClientWriteInfo:             54420,
	FeatureSettingsSerializedAsStrings: 54429,
	FeatureInterServerSecret:           54441,
	FeatureOpenTelemetry:               54442,
}

// Revision reports starting server revision since feature is supported.
func (f Feature) Revision() int {
	return featureRevision[f]
}

// In reports whether feature is implemented in provided revision.
func (f Feature) In(revision int) bool {
	return revision >= f.Revision()
}
