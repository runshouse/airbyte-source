package internal

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/auth"
	grpcclient "github.com/planetscale/psdb/core/pool"
	clientoptions "github.com/planetscale/psdb/core/pool/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	vtmysql "vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

// PlanetScaleDatabase is a general purpose interface
// that defines all the data access methods needed for the PlanetScale Airbyte source to function.
type PlanetScaleDatabase interface {
	CanConnect(ctx context.Context, ps PlanetScaleSource) error
	DiscoverSchema(ctx context.Context, ps PlanetScaleSource) (Catalog, error)
	ListShards(ctx context.Context, ps PlanetScaleSource) ([]string, error)
	Read(ctx context.Context, w io.Writer, ps PlanetScaleSource, s ConfiguredStream, tc *psdbconnect.TableCursor) (*SerializedCursor, error)
	Close() error
}

// PlanetScaleEdgeDatabase is an implementation of the PlanetScaleDatabase interface defined above.
// It uses the mysql interface provided by PlanetScale for all schema/shard/tablet discovery and
// the grpc API for incrementally syncing rows from PlanetScale.
type PlanetScaleEdgeDatabase struct {
	Logger         AirbyteLogger
	Mysql          PlanetScaleEdgeMysqlAccess
	vtgateClientFn func(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, error)
}

func (p PlanetScaleEdgeDatabase) CanConnect(ctx context.Context, psc PlanetScaleSource) error {
	if err := p.checkEdgePassword(ctx, psc); err != nil {
		return errors.Wrap(err, "Unable to initialize Connect Session")
	}

	return p.Mysql.PingContext(ctx, psc)
}

func (p PlanetScaleEdgeDatabase) checkEdgePassword(ctx context.Context, psc PlanetScaleSource) error {
	if !strings.HasSuffix(psc.Host, ".connect.psdb.cloud") {
		return errors.New("This password is not connect-enabled, please ensure that your organization is enrolled in the Connect beta.")
	}
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, fmt.Sprintf("https://%v", psc.Host), nil)
	if err != nil {
		return err
	}

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		return errors.New(fmt.Sprintf("The database %q, hosted at %q, is inaccessible from this process", psc.Database, psc.Host))
	}

	return nil
}

func (p PlanetScaleEdgeDatabase) DiscoverSchema(ctx context.Context, psc PlanetScaleSource) (Catalog, error) {
	var c Catalog

	tables, err := p.Mysql.GetTableNames(ctx, psc)
	if err != nil {
		return c, errors.Wrap(err, "Unable to query database for schema")
	}

	for _, tableName := range tables {
		stream, err := p.getStreamForTable(ctx, psc, tableName)
		if err != nil {
			return c, errors.Wrapf(err, "unable to get stream for table %v", tableName)
		}
		c.Streams = append(c.Streams, stream)
	}
	return c, nil
}

func (p PlanetScaleEdgeDatabase) getStreamForTable(ctx context.Context, psc PlanetScaleSource, tableName string) (Stream, error) {
	schema := StreamSchema{
		Type:       "object",
		Properties: map[string]PropertyType{},
	}
	stream := Stream{
		Name:               tableName,
		Schema:             schema,
		SupportedSyncModes: []string{"full_refresh", "incremental"},
		Namespace:          psc.Database,
	}

	var err error
	stream.Schema.Properties, err = p.Mysql.GetTableSchema(ctx, psc, tableName)
	if err != nil {
		return stream, errors.Wrapf(err, "Unable to get column names & types for table %v", tableName)
	}
	stream.Schema.Properties[CDCCursorField] = PropertyType{Type: []string{"number"}, AirbyteType: "integer"}
	stream.Schema.Properties[CDCDeletedAtField] = PropertyType{Type: []string{"null", "string"}, CustomFormat: "date-time", AirbyteType: "timestamp_with_timezone"}

	// need this otherwise Airbyte will fail schema discovery for views
	// without primary keys.
	stream.PrimaryKeys = [][]string{}

	primaryKeys, err := p.Mysql.GetTablePrimaryKeys(ctx, psc, tableName)
	if err != nil {
		return stream, errors.Wrapf(err, "unable to iterate primary keys for table %s", tableName)
	}
	for _, key := range primaryKeys {
		stream.PrimaryKeys = append(stream.PrimaryKeys, []string{key})
	}

	// Dedup orders versions of a row by the cursor. A primary key is identical across versions,
	// so use the emission-ordered CDC cursor instead.
	stream.DefaultCursorFields = []string{CDCCursorField}
	stream.SourceDefinedCursor = true
	return stream, nil
}

// Convert columnType to Airbyte type.
func getJsonSchemaType(mysqlType string, treatTinyIntAsBoolean bool, nullable string) PropertyType {
	// Support custom airbyte types documented here :
	// https://docs.airbyte.com/understanding-airbyte/supported-data-types/#the-types
	var (
		jsonSchemaType string
		customFormat   string
		airbyteType    string
	)

	switch {
	case strings.HasPrefix(mysqlType, "tinyint(1)"):
		if treatTinyIntAsBoolean {
			jsonSchemaType = "boolean"
		} else {
			jsonSchemaType = "number"
			airbyteType = "integer"
		}
	case strings.HasPrefix(mysqlType, "int"), strings.HasPrefix(mysqlType, "smallint"), strings.HasPrefix(mysqlType, "mediumint"), strings.HasPrefix(mysqlType, "bigint"), strings.HasPrefix(mysqlType, "tinyint"):
		jsonSchemaType = "number"
		airbyteType = "integer"
	case strings.HasPrefix(mysqlType, "decimal"), strings.HasPrefix(mysqlType, "double"), strings.HasPrefix(mysqlType, "float"):
		jsonSchemaType = "number"
	case strings.HasPrefix(mysqlType, "datetime"):
		jsonSchemaType = "string"
		customFormat = "date-time"
		airbyteType = "timestamp_without_timezone"
	case strings.HasPrefix(mysqlType, "timestamp"):
		jsonSchemaType = "string"
		customFormat = "date-time"
		airbyteType = "timestamp_with_timezone"
	case strings.HasPrefix(mysqlType, "date"):
		jsonSchemaType = "string"
		customFormat = "date"
		airbyteType = "date"
	case strings.HasPrefix(mysqlType, "time"):
		jsonSchemaType = "string"
		customFormat = "time"
		airbyteType = "time_with_timezone"
	default:
		jsonSchemaType = "string"
	}

	propertyType := PropertyType{
		Type:         []string{jsonSchemaType},
		CustomFormat: customFormat,
		AirbyteType:  airbyteType,
	}

	if strings.ToLower(nullable) == "yes" {
		propertyType.Type = []string{"null", propertyType.Type[0]}
	}

	return propertyType
}

func (p PlanetScaleEdgeDatabase) Close() error {
	return p.Mysql.Close()
}

func (p PlanetScaleEdgeDatabase) ListShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	return p.Mysql.GetVitessShards(ctx, psc)
}

// Read streams rows from a table given a starting cursor.
// 1. We will get the latest vgtid for a given table in a shard when a sync session starts.
// 2. This latest vgtid is now the stopping point for this sync session.
// 3. Ask vstream to stream from the last known vgtid
// 4. When we reach the stopping point, read all rows available at this vgtid
// 5. Complete only after COPY_COMPLETED or the target VGTID; exhausted timeouts fail.
func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, w io.Writer, ps PlanetScaleSource, s ConfiguredStream, lastKnownPosition *psdbconnect.TableCursor) (*SerializedCursor, error) {
	var (
		sErr                    error
		currentSerializedCursor *SerializedCursor
		syncMode                string
	)

	tabletType := psdbconnect.TabletType_primary
	if ps.UseRdonly {
		tabletType = psdbconnect.TabletType_batch
	} else if ps.UseReplica {
		tabletType = psdbconnect.TabletType_replica
	}

	currentPosition := lastKnownPosition
	if currentPosition.Position == "" || currentPosition.LastKnownPk != nil {
		syncMode = "full"
	} else {
		syncMode = "incremental"
	}

	table := s.Stream
	timeout := 5 * time.Minute
	if timeoutSeconds := ps.TimeoutSeconds; timeoutSeconds != nil {
		timeout = time.Duration(*timeoutSeconds) * time.Second
	}
	maxRetries := ps.MaxRetries

	preamble := fmt.Sprintf("[%v:%v:%v shard : %v] ", table.Namespace, TabletTypeToString(tabletType), table.Name, currentPosition.Shard)

	p.Logger.Log(LOGLEVEL_INFO, preamble+"Peeking PRIMARY for the target GTID")
	stopPosition, lcErr := p.getStopCursorPosition(ctx, currentPosition.Shard, currentPosition.Keyspace, table, ps, psdbconnect.TabletType_primary)
	if lcErr != nil {
		p.Logger.Log(LOGLEVEL_ERROR, preamble+fmt.Sprintf("Error fetching latest cursor position: %+v", lcErr))
		return currentSerializedCursor, errors.Wrap(lcErr, "Unable to get latest cursor position")
	}
	if stopPosition == "" {
		p.Logger.Log(LOGLEVEL_ERROR, preamble+"Error fetching latest cursor position, was empty string")
		return currentSerializedCursor, errors.New("Unable to get latest cursor position: empty position")
	}

	// the last synced VGTID is not after the current VGTID.
	// Only skip in incremental mode: an unfinished COPY still has rows to read at the same GTID.
	if syncMode == "incremental" && positionAtLeast(currentPosition.Position, stopPosition) {
		p.Logger.Log(LOGLEVEL_INFO, preamble+"No new GTIDs found, exiting")
		return TableCursorToSerializedCursor(currentPosition)
	}
	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf(preamble+"New GTIDs found, syncing for %v", timeout))

	copyCompleted := false
	var syncCount uint = 0
	totalRecordCount := 0

	for {
		syncCount += 1
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sStarting sync #%v", preamble, syncCount))
		newPosition, recordCount, err := p.sync(ctx, syncMode, currentPosition, stopPosition, table, ps, tabletType, timeout, &copyCompleted)
		totalRecordCount += recordCount
		// sync may return a new cursor (e.g. after a LASTPK event), so checkpoint what it returned
		currentPosition = newPosition
		currentSerializedCursor, sErr = TableCursorToSerializedCursor(currentPosition)
		if sErr != nil {
			// if we failed to serialize here, we should bail.
			return currentSerializedCursor, errors.Wrap(sErr, "unable to serialize current position")
		}
		if err != nil {
			if s, ok := status.FromError(err); ok {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%v%v records synced after %v syncs. Got error [%v], returning with cursor [%v] after gRPC error", preamble, totalRecordCount, syncCount, s.Code(), currentPosition))
				if syncCount >= maxRetries {
					return currentSerializedCursor, errors.Wrapf(err, "giving up after %v syncs", syncCount)
				}
			} else if errors.Is(err, io.EOF) {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%vFinished reading %v records after %v syncs for table [%v]", preamble, totalRecordCount, syncCount, table.Name))
				return currentSerializedCursor, nil
			} else {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%v%v records synced after %v syncs. Got error [%v], returning with cursor [%v] after server timeout", preamble, totalRecordCount, syncCount, err, currentPosition))
				return currentSerializedCursor, err
			}
		}
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%vContinuing to next sync #%v. Set next sync start position to [%+v].", preamble, syncCount+1, currentPosition))
	}
}

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, syncMode string, tc *psdbconnect.TableCursor, stopPosition string, s Stream, ps PlanetScaleSource, tabletType psdbconnect.TabletType, timeout time.Duration, copyCompleted *bool) (_ *psdbconnect.TableCursor, _ int, err error) {
	preamble := fmt.Sprintf("[%v:%v:%v shard : %v] ", s.Namespace, TabletTypeToString(tabletType), s.Name, tc.Shard)

	// records that failed to write must not be checkpointed past
	defer func() {
		if flushErr := p.Logger.Flush(); flushErr != nil {
			err = errors.Wrap(flushErr, "unable to write records")
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		vtgateClient vtgateservice.VitessClient
		fields       []*query.Field
	)

	vtgateClient, conn, err := p.initializeVTGateClient(ctx, ps)
	if err != nil {
		return tc, 0, err
	}
	if conn != nil {
		defer conn.Close()
	}

	isFullSync := syncMode == "full"
	if isFullSync && !*copyCompleted && tc.LastKnownPk == nil || tc.LastKnownPk != nil && !ps.UseGTIDWithTablePKs {
		tc.Position = ""
	}
	vtgateReq := buildVStreamRequest(tabletType, s.Name, tc.Shard, tc.Keyspace, tc.Position, tc.LastKnownPk)
	p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sRequesting VStream with %+v", preamble, vtgateReq))

	if isFullSync {
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sWill stop after COPY_COMPLETED and the PRIMARY target are covered.", preamble))
	} else {
		p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sWill stop once stop position [%+v] is found.", preamble, stopPosition))
	}

	c, err := vtgateClient.VStream(ctx, vtgateReq)
	if err != nil {
		p.Logger.Log(LOGLEVEL_ERROR, fmt.Sprintf("%sExiting sync due to client sync error: %+v", preamble, err))
		return tc, 0, err
	}

	keyspaceOrDatabase := s.Namespace
	if keyspaceOrDatabase == "" {
		keyspaceOrDatabase = ps.Database
	}

	// COPY completion survives retries; the captured PRIMARY target never moves.
	canFinishSync := false
	resultCount := 0

	for {
		res, err := c.Recv()
		if err != nil {
			if s, ok := status.FromError(err); ok && s.Code() == codes.DeadlineExceeded {
				// No next VGTID found
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting sync and flushing records because no new VGTID found after last position or deadline exceeded %+v", preamble, tc))
				return tc, resultCount, err
			} else if errors.Is(err, io.EOF) {
				if isFullSync && !*copyCompleted {
					return tc, resultCount, errors.New("VStream ended before COPY_COMPLETED")
				}
				return tc, resultCount, errors.New("VStream ended before target VGTID")
			} else {
				p.Logger.Log(LOGLEVEL_ERROR, fmt.Sprintf("%sExiting sync and flushing records due to error: %+v", preamble, err))
				return tc, resultCount, err
			}
		}

		var rows []struct {
			result    *sqltypes.Result
			deletedAt *string
		}
		for _, event := range res.Events {
			switch event.Type {
			case binlogdata.VEventType_VGTID:
				positions := event.GetVgtid().GetShardGtids()
				if len(positions) != 1 || positions[0] == nil || positions[0].Shard != tc.Shard || positions[0].Keyspace != tc.Keyspace {
					return tc, resultCount, errors.New("unexpected VStream shard identity; reconcile source checkpoints")
				}
				vgtid := positions[0]
				tc.Position = vgtid.Gtid
				if len(vgtid.TablePKs) > 0 && vgtid.TablePKs[0] != nil {
					tc.LastKnownPk = vgtid.TablePKs[0].Lastpk
				} else {
					tc.LastKnownPk = nil
				}
			case binlogdata.VEventType_LASTPK:
				if event.LastPKEvent.TableLastPK != nil {
					// Only update last PK because we're in a COPY phase
					tc = &psdbconnect.TableCursor{
						Shard:       tc.Shard,
						Keyspace:    tc.Keyspace,
						LastKnownPk: event.LastPKEvent.TableLastPK.Lastpk,
						Position:    tc.Position,
					}
					p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sLASTPK event found, setting last PK to %+v", preamble, tc))
				}
			case binlogdata.VEventType_FIELD:
				// Save fields for processing
				fields = event.FieldEvent.Fields
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sFIELD event found, setting fields to %+v", preamble, fields))
			case binlogdata.VEventType_ROW:
				for _, change := range event.RowEvent.RowChanges {
					row := change.After
					var deletedAt *string
					if row == nil {
						row = change.Before
						if row == nil || event.Timestamp <= 0 || len(s.PrimaryKeys) == 0 {
							return tc, resultCount, errors.New("delete requires a before-image, binlog timestamp and declared primary key")
						}
						value := time.Unix(event.Timestamp, 0).UTC().Format(time.RFC3339)
						deletedAt = &value
					}
					result := sqltypes.Proto3ToResult(&query.QueryResult{Fields: fields, Rows: []*query.Row{row}})
					if deletedAt != nil {
						for _, key := range s.PrimaryKeys {
							found := false
							if len(key) == 1 {
								for i, field := range result.Fields {
									if field.Name == key[0] && i < len(result.Rows[0]) && !result.Rows[0][i].IsNull() {
										found = true
									}
								}
							}
							if !found {
								return tc, resultCount, errors.New("delete before-image is missing a supported non-null primary key")
							}
						}
					}
					rows = append(rows, struct {
						result    *sqltypes.Result
						deletedAt *string
					}{result, deletedAt})
				}
			case binlogdata.VEventType_COPY_COMPLETED:
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sCOPY_COMPLETED event found, copy phase finished", preamble))
				*copyCompleted = true
			case binlogdata.VEventType_JOURNAL:
				return tc, resultCount, errors.New("reshard or migration journal received; reconcile source checkpoints")
			case binlogdata.VEventType_BEGIN, binlogdata.VEventType_COMMIT,
				binlogdata.VEventType_DDL, binlogdata.VEventType_DELETE,
				binlogdata.VEventType_GTID, binlogdata.VEventType_HEARTBEAT,
				binlogdata.VEventType_INSERT,
				binlogdata.VEventType_OTHER, binlogdata.VEventType_REPLACE,
				binlogdata.VEventType_ROLLBACK, binlogdata.VEventType_SAVEPOINT,
				binlogdata.VEventType_SET, binlogdata.VEventType_UNKNOWN,
				binlogdata.VEventType_UPDATE, binlogdata.VEventType_VERSION:
				// No special handling.
			default:
				panic(fmt.Sprintf("unexpected binlogdata.VEventType: %#v", event.Type))
			}
		}

		if isFullSync && *copyCompleted {
			tc.LastKnownPk = nil
		}
		if (!isFullSync || *copyCompleted) && positionAtLeast(tc.Position, stopPosition) {
			p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sReady to finish sync and flush since PRIMARY target [%+v] is covered", preamble, stopPosition))
			canFinishSync = true
		}

		if len(rows) > 0 {
			for _, result := range rows {
				qr := result.result
				for _, row := range qr.Rows {
					resultCount += 1
					sqlResult := &sqltypes.Result{
						Fields: qr.Fields,
					}
					sqlResult.Rows = append(sqlResult.Rows, row)
					// Results queued to Airbyte here, and flushed at the end of sync()
					p.printQueryResult(sqlResult, keyspaceOrDatabase, s.Name, &ps, result.deletedAt)
				}
			}
		}

		// Exit sync and flush records once the VGTID position is at or past the desired stop position, and we're no longer waiting for COPY phase to complete
		if canFinishSync {
			if isFullSync {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting full sync after COPY_COMPLETED and PRIMARY target coverage, current position is %+v, stop position is %+v", preamble, tc.Position, stopPosition))
			} else {
				p.Logger.Log(LOGLEVEL_INFO, fmt.Sprintf("%sExiting incremental sync and flushing records because current position %+v has reached or passed stop position %+v", preamble, tc.Position, stopPosition))
			}
			return tc, resultCount, io.EOF
		}
	}
}

func (p PlanetScaleEdgeDatabase) getStopCursorPosition(ctx context.Context, shard, keyspace string, s Stream, ps PlanetScaleSource, tabletType psdbconnect.TabletType) (string, error) {
	timeout := 45 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var (
		err          error
		vtgateClient vtgateservice.VitessClient
	)

	vtgateClient, conn, err := p.initializeVTGateClient(ctx, ps)
	if err != nil {
		return "", err
	}
	if conn != nil {
		defer conn.Close()
	}

	vtgateReq := buildVStreamRequest(tabletType, s.Name, shard, keyspace, "current", nil)
	vtgateCursor, vtgateErr := vtgateClient.VStream(ctx, vtgateReq)

	if vtgateErr != nil {
		return "", vtgateErr
	}

	// keep reading until a VGTID arrives; the timeout above bounds the wait
	for {
		res, err := vtgateCursor.Recv()
		if err != nil {
			return "", errors.Wrap(err, "unable to find VEvent of VGTID type to use as stop cursor")
		}

		target := ""
		for _, event := range res.Events {
			if event.Type == binlogdata.VEventType_JOURNAL {
				return "", errors.New("reshard or migration journal received while capturing PRIMARY target")
			}
			if event.Type == binlogdata.VEventType_VGTID {
				positions := event.GetVgtid().GetShardGtids()
				if len(positions) != 1 || positions[0] == nil || positions[0].Shard != shard || positions[0].Keyspace != keyspace {
					return "", errors.New("unexpected PRIMARY target shard identity; reconcile source checkpoints")
				}
				if target == "" {
					target = positions[0].Gtid
				}
			}
		}
		if target != "" {
			return target, nil
		}
	}
}

func toTopoTabletType(tabletType psdbconnect.TabletType) topodata.TabletType {
	if tabletType == psdbconnect.TabletType_replica {
		return topodata.TabletType_REPLICA
	} else if tabletType == psdbconnect.TabletType_batch {
		return topodata.TabletType_RDONLY
	} else if tabletType == psdbconnect.TabletType_primary {
		return topodata.TabletType_PRIMARY
	}

	// Fall back to replica
	return topodata.TabletType_REPLICA
}

func (p PlanetScaleEdgeDatabase) initializeVTGateClient(ctx context.Context, ps PlanetScaleSource) (vtgateservice.VitessClient, grpcclient.ConnPool, error) {
	if p.vtgateClientFn == nil {
		conn, err := grpcclient.Dial(ctx, ps.Host,
			clientoptions.WithDefaultTLSConfig(),
			clientoptions.WithCompression(true),
			clientoptions.WithConnectionPool(1),
			clientoptions.WithExtraCallOption(
				auth.NewBasicAuth(ps.Username, ps.Password).CallOption(),
			),
		)
		if err != nil {
			return nil, nil, err
		}
		return vtgateservice.NewVitessClient(conn), conn, err
	} else {
		vtgateClient, err := p.vtgateClientFn(ctx, ps)
		if err != nil {
			return nil, nil, err
		}
		return vtgateClient, nil, nil
	}
}

// printQueryResult will pretty-print an AirbyteRecordMessage to the logger.
// Copied from vtctl/query.go
func (p PlanetScaleEdgeDatabase) printQueryResult(qr *sqltypes.Result, tableNamespace, tableName string, ps *PlanetScaleSource, deletedAt *string) {
	data := QueryResultToRecords(qr, ps)

	for _, record := range data {
		record[CDCCursorField] = nextCDCCursor()
		record[CDCDeletedAtField] = deletedAt
		p.Logger.Record(tableNamespace, tableName, record)
	}
}

// CDCCursorField orders versions of the same row for destination dedup, like Airbyte's own CDC sources:
// https://docs.airbyte.com/platform/understanding-airbyte/cdc
const CDCCursorField = "_ab_cdc_cursor"
const CDCDeletedAtField = "_ab_cdc_deleted_at"

var lastCDCCursor int64

// nextCDCCursor returns a value strictly greater than any previous one in this process.
// Rows are emitted in binlog order, so a later version of a row always gets a larger cursor,
// even when both share the same millisecond emitted_at. Values track wall-clock nanoseconds
// so later syncs sort after earlier ones.
func nextCDCCursor() int64 {
	lastCDCCursor = max(lastCDCCursor+1, time.Now().UnixNano())
	return lastCDCCursor
}

func buildVStreamRequest(tabletType psdbconnect.TabletType, table string, shard string, keyspace string, gtid string, lastKnownPk *query.QueryResult) *vtgate.VStreamRequest {
	req := &vtgate.VStreamRequest{
		TabletType: toTopoTabletType(tabletType),
		Vgtid: &binlogdata.VGtid{
			ShardGtids: []*binlogdata.ShardGtid{
				{
					Shard:    shard,
					Keyspace: keyspace,
					Gtid:     gtid,
				},
			},
		},
		Flags: &vtgate.VStreamFlags{
			MinimizeSkew:  true,
			StopOnReshard: true,
			Cells:         "planetscale_operator_default",
		},
		Filter: &binlogdata.Filter{
			Rules: []*binlogdata.Rule{{
				Match:  table,
				Filter: "SELECT * FROM " + sqlescape.EscapeID(table),
			}},
		},
	}

	if lastKnownPk != nil {
		req.Vgtid.ShardGtids[0].TablePKs = []*binlogdata.TableLastPK{
			{
				TableName: table,
				Lastpk:    lastKnownPk,
			},
		}
	}

	return req
}

// positionAtLeast returns true if position `a` is equal to or after position `b`
func positionAtLeast(a string, b string) bool {
	if a == "" || b == "" {
		return false
	}

	parsedA, err := vtmysql.DecodePosition(a)
	if err != nil {
		return false
	}

	parsedB, err := vtmysql.DecodePosition(b)
	if err != nil {
		return false
	}

	return parsedA.AtLeast(parsedB)
}
