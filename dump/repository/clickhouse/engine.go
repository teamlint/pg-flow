package clickhouse

import "time"

const (
	defaultInactivityMergeTimeout = time.Minute
	publicSchema                  = "public"
	defaultClickHousePort         = 9000
	defaultClickHouseHost         = "127.0.0.1"
	defaultPostgresPort           = 5432
	defaultPostgresHost           = "127.0.0.1"
	defaultRowIdColumn            = "row_id"
	defaultMaxBufferLength        = 1000
	defaultSignColumn             = "sign"
	defaultVerColumn              = "ver"
	defaultIsDeletedColumn        = "is_deleted"
)

const (
	MergeTree                    = "MergeTree"
	CollapsingMergeTree          = "CollapsingMergeTree"
	VersionedCollapsingMergeTree = "VersionedCollapsingMergeTree"
	ReplacingMergeTree           = "ReplacingMergeTree"
	GraphiteMergeTree            = "GraphiteMergeTree"
	AggregatingMergeTree         = "AggregatingMergeTree"
	SummingMergeTree             = "SummingMergeTree"
)
