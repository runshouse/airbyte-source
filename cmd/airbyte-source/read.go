package airbyte_source

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/planetscale/airbyte-source/cmd/internal"
	psdbconnectv1alpha1 "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/spf13/cobra"
)

var (
	readSourceConfigFilePath string
	readSourceCatalogPath    string
	stateFilePath            string
)

func init() {
	rootCmd.AddCommand(ReadCommand(DefaultHelper(os.Stdout)))
}

func ReadCommand(ch *Helper) *cobra.Command {
	readCmd := &cobra.Command{
		Use:   "read",
		Short: "Converts rows from a PlanetScale database into AirbyteRecordMessages",
		Run: func(cmd *cobra.Command, args []string) {
			ch.Logger = internal.NewLogger(cmd.OutOrStdout())
			defer ch.Logger.Flush()
			if readSourceConfigFilePath == "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "Please pass path to a valid source config file via the [%v] argument", "config")
				os.Exit(1)
			}

			if readSourceCatalogPath == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Please pass path to a valid source catalog file via the [%v] argument", "config")
				os.Exit(1)
			}

			ch.Logger.Log(internal.LOGLEVEL_INFO, "Checking connection")

			psc, err := parseSource(ch.FileReader, readSourceConfigFilePath)
			if err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Please provide path to a valid configuration file")
				return
			}

			if err := ch.EnsureDB(psc); err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Unable to connect to PlanetScale Database")
				return
			}

			defer func() {
				if err := ch.Database.Close(); err != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "Unable to close connection to PlanetScale Database, failed with %v", err)
				}
			}()

			cs, err := checkConnectionStatus(ch.Database, psc)
			if err != nil {
				ch.Logger.ConnectionStatus(cs)
				return
			}

			catalog, err := readCatalog(readSourceCatalogPath)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read catalog: %+v", err))
				os.Exit(1)
			}

			if len(catalog.Streams) == 0 {
				ch.Logger.Log(internal.LOGLEVEL_ERROR, "Catalog has no streams")
				return
			}

			state := ""
			if stateFilePath != "" {
				ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("State file detected, parsing provided file %s", stateFilePath))
				b, err := os.ReadFile(stateFilePath)
				if err != nil {
					ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
					os.Exit(1)
				}
				state = string(b)
				ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("State value: %s", state))
			}
			shards, err := ch.Database.ListShards(context.Background(), psc)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to list shards : %v", err))
				os.Exit(1)
			}

			syncState, err := readState(state, psc, catalog.Streams, shards, ch.Logger)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
				os.Exit(1)
			}

			for _, table := range catalog.Streams {
				keyspaceOrDatabase := table.Stream.Namespace
				if keyspaceOrDatabase == "" {
					keyspaceOrDatabase = psc.Database
				}
				streamStateKey := keyspaceOrDatabase + ":" + table.Stream.Name
				streamState, ok := syncState.Streams[streamStateKey]
				if !ok {
					ch.Logger.Error(fmt.Sprintf("Unable to read state for stream %v", streamStateKey))
					os.Exit(1)
				}

				ch.Logger.StreamTrace(table.Stream.Name, keyspaceOrDatabase, internal.STREAM_STATUS_STARTED)

				for shardName, shardState := range streamState.Shards {
					var tc *psdbconnectv1alpha1.TableCursor

					tc, err = shardState.SerializedCursorToTableCursor(table)
					ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Using serialized cursor for stream %s", streamStateKey))
					if err != nil {
						ch.Logger.Error(fmt.Sprintf("Invalid serialized cursor for stream %v, failed with [%v]", streamStateKey, err))
						os.Exit(1)
					}

					// Emit RUNNING trace message before processing
					ch.Logger.StreamTrace(table.Stream.Name, keyspaceOrDatabase, internal.STREAM_STATUS_RUNNING)

					sc, err := ch.Database.Read(context.Background(), cmd.OutOrStdout(), psc, table, tc)
					if err != nil {
						ch.Logger.Error(err.Error())
						os.Exit(1)
					}

					if sc != nil {
						// if we get any new state, we assign it here.
						// otherwise, the older state is round-tripped back to Airbyte.
						syncState.Streams[streamStateKey].Shards[shardName] = sc
					}
				}

				ch.Logger.StreamTrace(table.Stream.Name, keyspaceOrDatabase, internal.STREAM_STATUS_COMPLETE)

				stateType := psc.GetStateType()
				if stateType == internal.STATE_TYPE_STREAM {
					ch.Logger.StreamState(table.Stream.Name, keyspaceOrDatabase, syncState.Streams[streamStateKey])
				}
			}

			stateType := psc.GetStateType()
			if stateType == internal.STATE_TYPE_GLOBAL {
				ch.Logger.GlobalState(nil, syncState.Streams) // No shared state for this connector
			}
		},
	}
	readCmd.Flags().StringVar(&readSourceCatalogPath, "catalog", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&readSourceConfigFilePath, "config", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&stateFilePath, "state", "", "Path to the PlanetScale state information")
	return readCmd
}

type State struct {
	Shards map[string]map[string]interface{} `json:"shards"`
}

func readState(state string, psc internal.PlanetScaleSource, streams []internal.ConfiguredStream, shards []string, logger internal.AirbyteLogger) (internal.SyncState, error) {
	syncState := internal.SyncState{
		Streams: map[string]internal.ShardStates{},
	}

	if state != "" {
		logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Parsing state file with length: %d", len(state)))

		// Parse based on state type
		err := parseStateByType(state, &syncState, logger)
		if err != nil {
			return syncState, fmt.Errorf("failed to parse state: %v", err)
		}
	}

	for _, s := range streams {
		keyspaceOrDatabase := s.Stream.Namespace
		if keyspaceOrDatabase == "" {
			keyspaceOrDatabase = psc.Database
		}
		stateKey := keyspaceOrDatabase + ":" + s.Stream.Name
		logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Syncing stream %s with sync mode %s", s.Stream.Name, s.SyncMode))
		logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Looking for state with key: %s", stateKey))
		ignoreCurrentCursor := !s.IncrementalSyncRequested()

		// if no table cursor was found in the state, or we want to ignore the current cursor,
		// Send along an empty cursor for each shard.
		if _, ok := syncState.Streams[stateKey]; !ok || ignoreCurrentCursor {
			if !ok {
				logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("No cursor found for key %s (state exists: %t)", stateKey, ok))
			}
			if ignoreCurrentCursor {
				logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Ignoring cursor for key %s because incremental sync not requested", stateKey))
			}
			logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Ignoring current cursor since incremental sync is disabled, or no cursor was found for key %s", stateKey))
			initialSource := psc
			if s.SyncMode == internal.SYNC_MODE_FULL_REFRESH {
				initialSource.StartingGtids = ""
			}
			initialState, err := initialSource.GetInitialState(keyspaceOrDatabase, shards)
			if err != nil {
				return syncState, err
			}
			syncState.Streams[stateKey] = initialState
		} else {
			logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Using existing state for key %s", stateKey))
		}
	}

	return syncState, nil
}

// parseStateByType detects the state type and parses accordingly
func parseStateByType(state string, syncState *internal.SyncState, logger internal.AirbyteLogger) error {
	// First, try to detect if it's an array (STREAM type states)
	if strings.TrimSpace(state)[0] == '[' {
		logger.Log(internal.LOGLEVEL_INFO, "Detected array format, parsing as STREAM type states")
		return parseStreamTypeStates(state, syncState)
	}

	// Otherwise, try to parse as a single object and check its type
	var stateObj map[string]interface{}
	err := json.Unmarshal([]byte(state), &stateObj)
	if err != nil {
		return fmt.Errorf("failed to parse state as JSON object: %v", err)
	}

	stateType, exists := stateObj["type"]
	if !exists {
		// No type field, try legacy format parsing
		logger.Log(internal.LOGLEVEL_INFO, "No type field found, trying legacy format parsing")
		return parseLegacyStateFormat(state, syncState)
	}

	stateTypeStr, ok := stateType.(string)
	if !ok {
		return fmt.Errorf("state type is not a string: %v", stateType)
	}

	switch stateTypeStr {
	case "GLOBAL":
		logger.Log(internal.LOGLEVEL_INFO, "Detected GLOBAL state type")
		return parseGlobalTypeState(state, syncState)
	case "STREAM":
		logger.Log(internal.LOGLEVEL_INFO, "Detected single STREAM state type")
		return parseSingleStreamTypeState(state, syncState)
	default:
		return fmt.Errorf("unknown state type: %s", stateTypeStr)
	}
}

// parseGlobalTypeState parses a GLOBAL type state
func parseGlobalTypeState(state string, syncState *internal.SyncState) error {
	type GlobalState struct {
		Type   string `json:"type"`
		Global struct {
			SharedState  map[string]interface{} `json:"sharedState,omitempty"`
			StreamStates []struct {
				StreamState struct {
					Shards map[string]internal.SerializedCursor `json:"shards"`
				} `json:"streamState"`
				StreamDescriptor struct {
					Name      string  `json:"name"`
					Namespace *string `json:"namespace,omitempty"`
				} `json:"streamDescriptor"`
			} `json:"streamStates"`
		} `json:"global"`
	}

	var globalState GlobalState
	err := json.Unmarshal([]byte(state), &globalState)
	if err != nil {
		return fmt.Errorf("failed to parse GLOBAL state: %v", err)
	}

	// Convert to our internal format
	for _, streamState := range globalState.Global.StreamStates {
		namespace := ""
		if streamState.StreamDescriptor.Namespace != nil {
			namespace = *streamState.StreamDescriptor.Namespace
		}

		stateKey := namespace + ":" + streamState.StreamDescriptor.Name

		// Convert shards to proper format
		shards := make(map[string]*internal.SerializedCursor)
		for shardName, cursor := range streamState.StreamState.Shards {
			shards[shardName] = &cursor
		}

		syncState.Streams[stateKey] = internal.ShardStates{
			Shards: shards,
		}
	}

	return nil
}

// parseStreamTypeStates parses an array of STREAM type states
func parseStreamTypeStates(state string, syncState *internal.SyncState) error {
	type StreamStateMessage struct {
		Type   string `json:"type"`
		Stream struct {
			StreamDescriptor struct {
				Name      string  `json:"name"`
				Namespace *string `json:"namespace,omitempty"`
			} `json:"stream_descriptor"`
			StreamState struct {
				Shards map[string]internal.SerializedCursor `json:"shards"`
			} `json:"stream_state"`
		} `json:"stream"`
	}

	var messages []StreamStateMessage
	err := json.Unmarshal([]byte(state), &messages)
	if err != nil {
		return fmt.Errorf("failed to parse STREAM states array: %v", err)
	}

	// Convert to our internal format
	for _, message := range messages {
		if message.Type == "STREAM" {
			namespace := ""
			if message.Stream.StreamDescriptor.Namespace != nil {
				namespace = *message.Stream.StreamDescriptor.Namespace
			}

			stateKey := namespace + ":" + message.Stream.StreamDescriptor.Name

			// Convert shards to proper format
			shards := make(map[string]*internal.SerializedCursor)
			for shardName, cursor := range message.Stream.StreamState.Shards {
				shards[shardName] = &cursor
			}

			syncState.Streams[stateKey] = internal.ShardStates{
				Shards: shards,
			}
		}
	}

	return nil
}

// parseSingleStreamTypeState parses a single STREAM type state
func parseSingleStreamTypeState(state string, syncState *internal.SyncState) error {
	type SingleStreamState struct {
		Type   string `json:"type"`
		Stream struct {
			StreamDescriptor struct {
				Name      string  `json:"name"`
				Namespace *string `json:"namespace,omitempty"`
			} `json:"stream_descriptor"`
			StreamState struct {
				Shards map[string]internal.SerializedCursor `json:"shards"`
			} `json:"stream_state"`
		} `json:"stream"`
	}

	var streamState SingleStreamState
	err := json.Unmarshal([]byte(state), &streamState)
	if err != nil {
		return fmt.Errorf("failed to parse single STREAM state: %v", err)
	}

	if streamState.Type == "STREAM" {
		namespace := ""
		if streamState.Stream.StreamDescriptor.Namespace != nil {
			namespace = *streamState.Stream.StreamDescriptor.Namespace
		}

		stateKey := namespace + ":" + streamState.Stream.StreamDescriptor.Name

		// Convert shards to proper format
		shards := make(map[string]*internal.SerializedCursor)
		for shardName, cursor := range streamState.Stream.StreamState.Shards {
			shards[shardName] = &cursor
		}

		syncState.Streams[stateKey] = internal.ShardStates{
			Shards: shards,
		}
	}

	return nil
}

// parseLegacyStateFormat parses legacy state formats for backward compatibility
func parseLegacyStateFormat(state string, syncState *internal.SyncState) error {
	// Try to parse the legacy direct format first
	err := json.Unmarshal([]byte(state), syncState)
	if err == nil {
		return nil
	}

	// Fall back to the old parseStreamStateMessages logic for line-based messages
	lines := strings.Split(strings.TrimSpace(state), "\n")

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var message internal.AirbyteMessage
		err := json.Unmarshal([]byte(line), &message)
		if err != nil {
			continue // Skip invalid JSON lines
		}

		if message.Type == internal.STATE && message.State != nil {
			switch message.State.Type {
			case internal.STATE_TYPE_STREAM:
				// Handle STREAM state message
				if message.State.Stream != nil {
					streamDesc := message.State.Stream.StreamDescriptor
					namespace := ""
					if streamDesc.Namespace != nil {
						namespace = *streamDesc.Namespace
					}

					stateKey := namespace + ":" + streamDesc.Name
					if message.State.Stream.StreamState != nil {
						syncState.Streams[stateKey] = *message.State.Stream.StreamState
					}
				}
			case internal.STATE_TYPE_GLOBAL:
				// Handle GLOBAL state message
				if message.State.Global != nil {
					// Process each stream state in the global state
					for _, streamState := range message.State.Global.StreamStates {
						streamDesc := streamState.StreamDescriptor
						namespace := ""
						if streamDesc.Namespace != nil {
							namespace = *streamDesc.Namespace
						}

						stateKey := namespace + ":" + streamDesc.Name
						if streamState.StreamState != nil {
							syncState.Streams[stateKey] = *streamState.StreamState
						}
					}
				}
			case "":
				// Legacy state format (backwards compatibility)
				// This handles old state messages that don't have state_type set
				// Treat as stream state if it has stream field
				if message.State.Stream != nil {
					streamDesc := message.State.Stream.StreamDescriptor
					namespace := ""
					if streamDesc.Namespace != nil {
						namespace = *streamDesc.Namespace
					}

					stateKey := namespace + ":" + streamDesc.Name
					if message.State.Stream.StreamState != nil {
						syncState.Streams[stateKey] = *message.State.Stream.StreamState
					}
				}
			}
		}
	}

	return nil
}

func readCatalog(path string) (c internal.ConfiguredCatalog, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
