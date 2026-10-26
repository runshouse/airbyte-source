package airbyte_source

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/planetscale/airbyte-source/cmd/internal"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
)

// This tests that when starting_gtids are passed AND a state file is passed,
// the state file takes precedence.
func TestRead_StartingGtidsAndState(t *testing.T) {
	psc := internal.PlanetScaleSource{
		Host:          "aws.connect.psdb.cloud",
		Database:      "sharded",
		Username:      "user",
		Password:      "pscale_password",
		StartingGtids: "{\"sharded\": {\"-80\": \"MySQL56/MyGTID:1-3\"}}",
	}

	streams := []internal.ConfiguredStream{
		{
			Stream: internal.Stream{
				Name: "table1",
				Schema: internal.StreamSchema{
					Type: "object",
					Properties: map[string]internal.PropertyType{
						"id": {
							Type:        []string{"number"},
							AirbyteType: "integer",
						},
					},
				},
				SupportedSyncModes: []string{
					"full_refresh",
					"incremental",
				},
				Namespace: "sharded",
				PrimaryKeys: [][]string{
					{"id"},
				},
				SourceDefinedCursor: true,
				DefaultCursorFields: []string{
					"id",
				},
			},
			SyncMode: "incremental",
		},
		{
			Stream: internal.Stream{
				Name: "table2",
				Schema: internal.StreamSchema{
					Type: "object",
					Properties: map[string]internal.PropertyType{
						"id": {
							Type:        []string{"number"},
							AirbyteType: "integer",
						},
					},
				},
				SupportedSyncModes: []string{
					"full_refresh",
					"incremental",
				},
				Namespace: "sharded",
				PrimaryKeys: [][]string{
					{"id"},
				},
				SourceDefinedCursor: true,
				DefaultCursorFields: []string{
					"id",
				},
			},
			SyncMode: "incremental",
		},
	}
	shards := []string{"-80", "80-"}

	firstCursor, err := internal.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "-80",
		Keyspace: "sharded",
		Position: "MySQL56/MyOtherGTID:1-3",
	})
	assert.NoError(t, err)
	secondCursor, err := internal.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "80-",
		Keyspace: "sharded",
		Position: "MySQL56/MyOtherGTID:1-3",
	})
	assert.NoError(t, err)
	state := fmt.Sprintf("{\"streams\":{\"sharded:table1\":{\"shards\":{\"-80\":{\"cursor\":\"%s\"},\"80-\":{\"cursor\":\"%s\"}}},\"sharded:table2\":{\"shards\":{\"-80\":{\"cursor\":\"%s\"},\"80-\":{\"cursor\":\"%s\"}}}}}", firstCursor.Cursor, secondCursor.Cursor, firstCursor.Cursor, secondCursor.Cursor)

	expectedStates := internal.SyncState{
		Streams: map[string]internal.ShardStates{
			"sharded:table1": {
				Shards: map[string]*internal.SerializedCursor{
					"-80": firstCursor,
					"80-": secondCursor,
				},
			},
			"sharded:table2": {
				Shards: map[string]*internal.SerializedCursor{
					"-80": firstCursor,
					"80-": secondCursor,
				},
			},
		},
	}
	syncStates, err := readState(state, psc, streams, shards, internal.NewLogger(os.Stdout))
	assert.NoError(t, err)
	assert.Equal(t, expectedStates, syncStates)
}

func TestReadState_GlobalStateType(t *testing.T) {
	psc := internal.PlanetScaleSource{Database: "sharded", StateType: "GLOBAL"}
	streams := []internal.ConfiguredStream{{
		Stream: internal.Stream{Name: "table1", Namespace: "sharded"}, SyncMode: "incremental",
	}}
	shards := []string{"-80", "80-"}
	expected := internal.SyncState{Streams: map[string]internal.ShardStates{}}
	positions := map[string]*internal.SerializedCursor{}
	for _, shard := range shards {
		cursor, err := internal.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard: shard, Keyspace: "sharded", Position: "MySQL56/MyGTID:1-3",
		})
		if !assert.NoError(t, err) {
			return
		}
		positions[shard] = cursor
	}
	expected.Streams["sharded:table1"] = internal.ShardStates{Shards: positions}

	for _, stateType := range []string{"GLOBAL", "STREAM"} {
		t.Run(stateType, func(t *testing.T) {
			var output bytes.Buffer
			logger := internal.NewLogger(&output)
			if stateType == "GLOBAL" {
				logger.GlobalState(nil, expected.Streams)
			} else {
				logger.StreamState("table1", "sharded", expected.Streams["sharded:table1"])
			}
			var envelope internal.AirbyteMessage
			if !assert.NoError(t, json.Unmarshal(output.Bytes(), &envelope)) {
				return
			}
			assert.Equal(t, internal.STATE, envelope.Type)
			if !assert.NotNil(t, envelope.State) {
				return
			}
			// Airbyte supplies inner state messages, not the outer stdout envelope.
			for _, payload := range []interface{}{envelope.State, []*internal.AirbyteState{envelope.State}} {
				encoded, err := json.Marshal(payload)
				if !assert.NoError(t, err) {
					return
				}
				got, err := readState(string(encoded), psc, streams, shards, internal.NewLogger(os.Stdout))
				if assert.NoError(t, err) {
					assert.Equal(t, expected, got)
				}
			}
			_, err := readState(output.String(), psc, streams, shards, internal.NewLogger(os.Stdout))
			assert.ErrorContains(t, err, "unknown state type: STATE")
		})
	}
	legacy, err := json.Marshal(expected)
	if !assert.NoError(t, err) {
		return
	}
	got, err := readState(string(legacy), psc, streams, shards, internal.NewLogger(os.Stdout))
	if assert.NoError(t, err) {
		assert.Equal(t, expected, got)
	}
}

func TestReadState_RequiresConfiguredShardCoverage(t *testing.T) {
	for _, tc := range []struct {
		name       string
		discovered []string
		configured string
		saved      []string
		wantError  bool
	}{
		{"all shards", []string{"-80", "80-"}, "", []string{"-80", "80-"}, false},
		{"missing saved shard", []string{"-80", "80-"}, "", []string{"-80"}, true},
		{"extra saved shard", []string{"-"}, "", []string{"-", "80-"}, true},
		{"changed topology", []string{"-80", "80-"}, "", []string{"-40", "40-"}, true},
		{"configured subset", []string{"-80", "80-"}, "-80", []string{"-80"}, false},
		{"changed configured subset", []string{"-80", "80-"}, "80-", []string{"-80"}, true},
		{"empty saved coverage", []string{"-80", "80-"}, "", nil, true},
		{"empty source coverage", nil, "", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saved := internal.SyncState{Streams: map[string]internal.ShardStates{
				"sharded:table1": {Shards: map[string]*internal.SerializedCursor{}},
			}}
			for _, shard := range tc.saved {
				cursor, err := internal.TableCursorToSerializedCursor(&psdbconnect.TableCursor{
					Shard: shard, Keyspace: "sharded", Position: "MySQL56/MyGTID:1-3",
				})
				if !assert.NoError(t, err) {
					return
				}
				saved.Streams["sharded:table1"].Shards[shard] = cursor
			}
			encoded, err := json.Marshal(saved)
			if !assert.NoError(t, err) {
				return
			}
			psc := internal.PlanetScaleSource{Database: "sharded", Shards: tc.configured}
			streams := []internal.ConfiguredStream{{Stream: internal.Stream{Name: "table1", Namespace: "sharded"}, SyncMode: "incremental"}}
			got, err := readState(string(encoded), psc, streams, tc.discovered, internal.NewLogger(os.Stdout))
			if tc.wantError {
				assert.ErrorContains(t, err, "shard coverage changed")
			} else if assert.NoError(t, err) {
				assert.Equal(t, saved, got)
			}
		})
	}
}

func TestReadState_FullRefreshClearsSavedAndStartingPositions(t *testing.T) {
	psc := internal.PlanetScaleSource{
		Database: "sharded", StartingGtids: `{"sharded":{"-80":"MySQL56/old:1-9","80-":"MySQL56/old:1-9"}}`,
	}
	streams := []internal.ConfiguredStream{{Stream: internal.Stream{Name: "table1", Namespace: "sharded"}, SyncMode: "full_refresh"}}
	shards := []string{"-80", "80-"}
	state := `{"streams":{"sharded:table1":{"shards":{"old-shard":{"cursor":"old-opaque-checkpoint"}}}}}`
	got, err := readState(state, psc, streams, shards, internal.NewLogger(os.Stdout))
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, got.Streams["sharded:table1"].Shards, 2)
	for shard, cursor := range got.Streams["sharded:table1"].Shards {
		decoded, err := cursor.SerializedCursorToTableCursor(streams[0])
		if assert.NoError(t, err) {
			assert.Equal(t, shard, decoded.Shard)
			assert.Equal(t, "", decoded.Position)
			assert.Nil(t, decoded.LastKnownPk)
		}
	}
}

func TestReadState_RejectsMissingGlobalCheckpoints(t *testing.T) {
	for _, state := range []string{
		`[{"type":"GLOBAL","global":{"streamStates":[]}}]`,
		`[{"type":"GLOBAL","global":{"stream_states":[{"stream_descriptor":{"name":"table1","namespace":"sharded"},"stream_state":null}]}}]`,
	} {
		_, err := readState(state, internal.PlanetScaleSource{Database: "sharded"}, nil, nil, internal.NewLogger(os.Stdout))
		assert.Error(t, err)
	}
}

// TestGetStateType tests the GetStateType method returns correct defaults and values
func TestGetStateType(t *testing.T) {
	// Test default (empty string should return STREAM)
	psc1 := internal.PlanetScaleSource{}
	assert.Equal(t, internal.STATE_TYPE_STREAM, psc1.GetStateType())

	// Test explicit STREAM
	psc2 := internal.PlanetScaleSource{StateType: "STREAM"}
	assert.Equal(t, internal.STATE_TYPE_STREAM, psc2.GetStateType())

	// Test explicit GLOBAL
	psc3 := internal.PlanetScaleSource{StateType: "GLOBAL"}
	assert.Equal(t, internal.STATE_TYPE_GLOBAL, psc3.GetStateType())

	// Test invalid value (should default to STREAM)
	psc4 := internal.PlanetScaleSource{StateType: "INVALID"}
	assert.Equal(t, internal.STATE_TYPE_STREAM, psc4.GetStateType())
}
