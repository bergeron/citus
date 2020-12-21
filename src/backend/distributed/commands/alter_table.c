/*-------------------------------------------------------------------------
 *
 * alter_table.c
 *	  Routines relation to the altering of tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "access/hash.h"
#include "catalog/dependency.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/distribution_column.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/worker_protocol.h"
#include "executor/spi.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/* Table Conversion Types */
#define UNDISTRIBUTE_TABLE 'u'
#define ALTER_DISTRIBUTED_TABLE 'a'
#define ALTER_TABLE_SET_ACCESS_METHOD 'm'

struct TableConversion;

typedef void (*TableConversionFunction)(struct TableConversion *);

typedef struct TableConversion
{
	/*
	 * Determines type of conversion: UNDISTRIBUTE_TABLE,
	 * ALTER_DISTRIBUTED_TABLE, ALTER_TABLE_SET_ACCESS_METHOD.
	 *
	 *
	 */
	char conversionType;

	/* Oid of the table to do conversion on */
	Oid relationId;

	/*
	 * Options to do conversions on the table
	 * distributionColumn is the name of the new distribution column,
	 * shardCountIsNull is if the shardCount variable is not given
	 * shardCount is the new shard count,
	 * colocateWith is the name of the table to colocate with, 'none', or
	 * 'default'
	 * accessMethod is the name of the new accessMethod for the table
	 */
	char *distributionColumn;
	bool shardCountIsNull;
	int shardCount;
	char *colocateWith;
	char *accessMethod;

	/*
	 * cascadeToColocatedIsNull is if the cascateToColocated
	 * variable is given
	 * cascateToColocated determines whether to cascated
	 * shardCount and colocateWith will be cascaded to the
	 * currently colocated tables
	 */
	bool cascadeToColocatedIsNull;
	bool cascadeToColocated;

	/* schema of the table */
	char *schemaName;
	Oid schemaId;

	/* name of the table */
	char *relationName;

	/* new relation oid after the conversion */
	Oid newRelationId;

	/* temporary name for intermediate table */
	char *tempName;

	/*hash that is appended to the name to create tempName */
	uint32 hashOfName;

	/* shard count of the table before conversion */
	int originalShardCount;

	/* list of the table oids of tables colocated with the table before conversion */
	List *colocatedTableList;

	/* new distribution key, if distributionColumn variable is given */
	Var *distributionKey;

	/* distribution key of the table before conversion */
	Var *originalDistributionKey;

	/*
	 * The function that will be used for the conversion
	 * Must comply with conversionType
	 * UNDISTRIBUTE_TABLE -> UndistributeTable
	 * ALTER_DISTRIBUTED_TABLE -> AlterDistributedTable
	 * ALTER_TABLE_SET_ACCESS_METHOD -> AlterTableSetAccessMethod
	 */
	TableConversionFunction function;
} TableConversion;


static void UndistributeTable(TableConversion *con);
static void AlterDistributedTable(TableConversion *con);
static void AlterTableSetAccessMethod(TableConversion *con);
static void ConvertTable(TableConversion *con);
static void EnsureTableNotReferencing(Oid relationId);
static void EnsureTableNotReferenced(Oid relationId);
static void EnsureTableNotForeign(Oid relationId);
static void EnsureTableNotPartition(Oid relationId);
static TableConversion * CreateTableConversion(char conversionType, Oid relationId,
											   char *distributionColumn,
											   bool shardCountIsNull, int shardCount,
											   char *colocateWith, char *accessMethod,
											   bool cascadeToColocatedIsNull,
											   bool cascadeToColocated);
static void CreateDistributedTableLike(TableConversion *con);
static void CreateCitusTableLike(TableConversion *con);
static List * GetViewCreationCommandsOfTable(Oid relationId);
static void ReplaceTable(Oid sourceId, Oid targetId, List *justBeforeDropCommands);
static void AlterDistributedTableMessages(TableConversion *con);

PG_FUNCTION_INFO_V1(undistribute_table);
PG_FUNCTION_INFO_V1(alter_distributed_table);
PG_FUNCTION_INFO_V1(alter_table_set_access_method);


/*
 * undistribute_table gets a distributed table name and
 * udistributes it.
 */
Datum
undistribute_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	CheckCitusVersion(ERROR);
	EnsureCoordinator();
	EnsureRelationExists(relationId);
	EnsureTableOwner(relationId);

	TableConversion *con = CreateTableConversion(UNDISTRIBUTE_TABLE, relationId, NULL,
												 true, 0, NULL, NULL, true, false);

	UndistributeTable(con);

	PG_RETURN_VOID();
}


/*
 * alter_distributed_table gets a distributed table and some other
 * parameters and alters some properties of the table according to
 * the parameters.
 */
Datum
alter_distributed_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	char *distributionColumn = NULL;
	if (!PG_ARGISNULL(1))
	{
		text *distributionColumnText = PG_GETARG_TEXT_P(1);
		distributionColumn = text_to_cstring(distributionColumnText);
	}

	int shardCount = 0;
	bool shardCountIsNull = true;
	if (!PG_ARGISNULL(2))
	{
		shardCount = PG_GETARG_INT32(2);
		shardCountIsNull = false;
	}

	char *colocateWith = NULL;
	if (!PG_ARGISNULL(3))
	{
		text *colocateWithText = PG_GETARG_TEXT_P(3);
		colocateWith = text_to_cstring(colocateWithText);
	}

	bool cascadeToColocated = false;
	bool cascadeToColocatedIsNull = true;
	if (!PG_ARGISNULL(4))
	{
		cascadeToColocated = PG_GETARG_BOOL(4);
		cascadeToColocatedIsNull = false;
	}

	CheckCitusVersion(ERROR);
	EnsureCoordinator();
	EnsureRelationExists(relationId);
	EnsureTableOwner(relationId);

	TableConversion *con = CreateTableConversion(ALTER_DISTRIBUTED_TABLE, relationId,
												 distributionColumn, shardCountIsNull,
												 shardCount, colocateWith, NULL,
												 cascadeToColocatedIsNull,
												 cascadeToColocated);

	AlterDistributedTableMessages(con);

	AlterDistributedTable(con);

	PG_RETURN_VOID();
}


/*
 * alter_table_set_access_method gets a distributed table and an access
 * method and changes table's access method into that.
 */
Datum
alter_table_set_access_method(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	text *accessMethodText = PG_GETARG_TEXT_P(1);
	char *accessMethod = text_to_cstring(accessMethodText);

	CheckCitusVersion(ERROR);
	EnsureRelationExists(relationId);
	EnsureTableOwner(relationId);

	if (IsCitusTable(relationId))
	{
		EnsureCoordinator();
	}

	TableConversion *con = CreateTableConversion(ALTER_TABLE_SET_ACCESS_METHOD,
												 relationId, NULL, true, 0, NULL,
												 accessMethod, true, false);

	AlterTableSetAccessMethod(con);

	PG_RETURN_VOID();
}


/*
 * UndistributeTable undistributes the given table. It uses ConvertTable function to
 * create a new local table and move everything to that table.
 *
 * The local tables, tables with references, partition tables and foreign tables are
 * not supported. The function gives errors in these cases.
 */
void
UndistributeTable(TableConversion *con)
{
	Relation relation = try_relation_open(con->relationId, ExclusiveLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("cannot undistribute table "
							   "because no such distributed table exists")));
	}

	relation_close(relation, NoLock);

	if (!IsCitusTable(con->relationId))
	{
		ereport(ERROR, (errmsg("cannot undistribute table "
							   "because the table is not distributed")));
	}

	EnsureTableNotReferencing(con->relationId);
	EnsureTableNotReferenced(con->relationId);
	EnsureTableNotForeign(con->relationId);
	EnsureTableNotPartition(con->relationId);

	ConvertTable(con);
}


/*
 * AlterDistributedTable changes some properties of the given table. It uses
 * ConvertTable function to create a new local table and move everything to that table.
 *
 * The local and reference tables, tables with references, partition tables and foreign
 * tables are not supported. The function gives errors in these cases.
 */
void
AlterDistributedTable(TableConversion *con)
{
	Relation relation = try_relation_open(con->relationId, ExclusiveLock);

	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("cannot alter table because no such "
							   "distributed table exists")));
	}
	relation_close(relation, NoLock);

	if (!IsCitusTableType(con->relationId, DISTRIBUTED_TABLE))
	{
		ereport(ERROR, (errmsg("cannot alter table because the table "
							   "is not distributed")));
	}

	EnsureTableNotReferencing(con->relationId);
	EnsureTableNotReferenced(con->relationId);
	EnsureTableNotForeign(con->relationId);
	EnsureTableNotPartition(con->relationId);
	if (con->colocateWith != NULL && strcmp(con->colocateWith, "default") != 0 &&
		strcmp(con->colocateWith, "none") != 0)
	{
		text *colocateWithText = cstring_to_text(con->colocateWith);
		Oid colocateWithTableOid = ResolveRelationId(colocateWithText, false);
		CitusTableCacheEntry *colocateWithTableCacheEntry =
			GetCitusTableCacheEntry(colocateWithTableOid);
		int colocateWithTableShardCount =
			colocateWithTableCacheEntry->shardIntervalArrayLength;

		if (!con->shardCountIsNull && con->shardCount != colocateWithTableShardCount)
		{
			ereport(ERROR, (errmsg("shard_count cannot be different than the shard "
								   "count of the table in colocate_with"),
							errhint("if no shard_count is specified shard count "
									"will be same with colocate_with table's")));
		}

		/*shardCount is either 0 or already same with colocateWith table's*/
		con->shardCount = colocateWithTableShardCount;
		con->shardCountIsNull = false;

		Var *colocateWithPartKey = DistPartitionKey(colocateWithTableOid);

		if (con->distributionColumn &&
			colocateWithPartKey->vartype != con->distributionKey->vartype)
		{
			ereport(ERROR, (errmsg("cannot colocate with %s and change distribution "
								   "column to %s because data type of column %s is "
								   "different then the distribution column of the %s",
								   con->colocateWith, con->distributionColumn,
								   con->distributionColumn, con->colocateWith)));
		}
		else if (!con->distributionColumn &&
				 colocateWithPartKey->vartype != con->originalDistributionKey->vartype)
		{
			ereport(ERROR, (errmsg("cannot colocate with %s because data type of its "
								   "distribution column is different than %s",
								   con->colocateWith, con->relationName)));
		}
	}
	ConvertTable(con);
}


/*
 * AlterTableSetAccessMethod changes the access method of the given table. It uses
 * ConvertTable function to create a new table with the access method and move everything
 * to that table.
 *
 * The local and references tables, tables with references, partition tables and foreign
 * tables are not supported. The function gives errors in these cases.
 */
void
AlterTableSetAccessMethod(TableConversion *con)
{
	Relation relation = try_relation_open(con->relationId, ExclusiveLock);

	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("cannot alter table because no such "
							   "distributed table exists")));
	}
	relation_close(relation, NoLock);

	EnsureTableNotReferencing(con->relationId);
	EnsureTableNotReferenced(con->relationId);
	EnsureTableNotForeign(con->relationId);

	if (PartitionedTable(con->relationId))
	{
		ereport(ERROR, (errmsg("you cannot alter access method of a partitioned table")));
	}

	ConvertTable(con);
}


/*
 * ConvertTable is used for converting a table into a new table with different properties.
 * The conversion is done by creating a new table, moving everything to the new table and
 * dropping the old one. So the oid of the table is not preserved.
 *
 * The new table will have the same name, columns and rows. It will also have partitions,
 * views, sequences of the old table. Finally it will have everything created by
 * GetPostLoadTableCreationCommands function, which include indexes. These will be
 * re-created during conversion, so their oids are not preserved either (except for
 * sequences). However, their names are preserved.
 *
 * The dropping of old table is done with CASCADE. Anything not mentioned here will
 * be dropped.
 */
void
ConvertTable(TableConversion *con)
{
	if (con->shardCountIsNull)
	{
		con->shardCount = con->originalShardCount;
		con->shardCountIsNull = false;
	}
	List *preLoadCommands = GetPreLoadTableCreationCommands(con->relationId, true,
															con->accessMethod);
	List *postLoadCommands = GetPostLoadTableCreationCommands(con->relationId);
	List *justBeforeDropCommands = NIL;

	postLoadCommands = list_concat(postLoadCommands,
								   GetViewCreationCommandsOfTable(con->relationId));

	int spiResult = SPI_connect();
	if (spiResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	bool isPartitionTable = false;
	char *attachToParentCommand = NULL;
	if (PartitionTable(con->relationId))
	{
		isPartitionTable = true;
		char *detachFromParentCommand = GenerateDetachPartitionCommand(con->relationId);
		attachToParentCommand = GenerateAlterTableAttachPartitionCommand(con->relationId);


		justBeforeDropCommands = lappend(justBeforeDropCommands, detachFromParentCommand);
	}

	if (PartitionedTable(con->relationId))
	{
		ereport(NOTICE, (errmsg("undistributing the partitions of %s",
								quote_qualified_identifier(con->schemaName,
														   con->relationName))));
		List *partitionList = PartitionList(con->relationId);
		Oid partitionRelationId = InvalidOid;
		foreach_oid(partitionRelationId, partitionList)
		{
			char *detachPartitionCommand = GenerateDetachPartitionCommand(
				partitionRelationId);
			char *attachPartitionCommand = GenerateAlterTableAttachPartitionCommand(
				partitionRelationId);

			/*
			 * We first detach the partitions to be able to undistribute them separately.
			 */
			spiResult = SPI_execute(detachPartitionCommand, false, 0);
			if (spiResult != SPI_OK_UTILITY)
			{
				ereport(ERROR, (errmsg("could not run SPI query")));
			}
			preLoadCommands = lappend(preLoadCommands,
									  makeTableDDLCommandString(attachPartitionCommand));


			TableConversion *partitionConfig = CreateTableConversion(con->conversionType,
																	 partitionRelationId,
																	 NULL, false,
																	 con->shardCount,
																	 NULL, NULL, true,
																	 false);

			con->function(partitionConfig);
		}
	}

	ereport(NOTICE, (errmsg("creating a new table for %s",
							quote_qualified_identifier(con->schemaName,
													   con->relationName))));

	TableDDLCommand *tableCreationCommand = NULL;
	foreach_ptr(tableCreationCommand, preLoadCommands)
	{
		Assert(CitusIsA(tableCreationCommand, TableDDLCommand));

		char *tableCreationSql = GetTableDDLCommand(tableCreationCommand);
		Node *parseTree = ParseTreeNode(tableCreationSql);

		RelayEventExtendNames(parseTree, con->schemaName, con->hashOfName);
		CitusProcessUtility(parseTree, tableCreationSql, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}

	con->newRelationId = get_relname_relid(con->tempName, con->schemaId);

	if (con->conversionType == ALTER_DISTRIBUTED_TABLE)
	{
		CreateDistributedTableLike(con);
	}
	else if (con->conversionType == ALTER_TABLE_SET_ACCESS_METHOD)
	{
		CreateCitusTableLike(con);
	}

	ReplaceTable(con->relationId, con->newRelationId, justBeforeDropCommands);

	TableDDLCommand *tableConstructionCommand = NULL;
	foreach_ptr(tableConstructionCommand, postLoadCommands)
	{
		Assert(CitusIsA(tableConstructionCommand, TableDDLCommand));
		char *tableConstructionSQL = GetTableDDLCommand(tableConstructionCommand);
		spiResult = SPI_execute(tableConstructionSQL, false, 0);
		if (spiResult != SPI_OK_UTILITY)
		{
			ereport(ERROR, (errmsg("could not run SPI query")));
		}
	}

	if (isPartitionTable)
	{
		spiResult = SPI_execute(attachToParentCommand, false, 0);
		if (spiResult != SPI_OK_UTILITY)
		{
			ereport(ERROR, (errmsg("could not run SPI query")));
		}
	}

	spiResult = SPI_finish();
	if (spiResult != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not finish SPI connection")));
	}

	if (con->cascadeToColocated)
	{
		Oid colocatedTableId = InvalidOid;

		/* For now we only support cascade to colocation for alter_distributed_table UDF */
		Assert(con->conversionType == ALTER_DISTRIBUTED_TABLE);
		foreach_oid(colocatedTableId, con->colocatedTableList)
		{
			if (colocatedTableId == con->relationId)
			{
				continue;
			}
			char *qualifiedRelationName = quote_qualified_identifier(con->schemaName,
																	 con->relationName);

			TableConversion *cascadeConfig =
				CreateTableConversion(con->conversionType, colocatedTableId, NULL,
									  con->shardCountIsNull, con->shardCount,
									  qualifiedRelationName, NULL, false, false);
			con->function(cascadeConfig);
		}
	}
}


/*
 * EnsureTableNotReferencing checks if the table has a reference to another
 * table and errors if it is.
 */
void
EnsureTableNotReferencing(Oid relationId)
{
	if (TableReferencing(relationId))
	{
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because table has a foreign key")));
	}
}


/*
 * EnsureTableNotReferenced checks if the table is referenced by another
 * table and errors if it is.
 */
void
EnsureTableNotReferenced(Oid relationId)
{
	if (TableReferenced(relationId))
	{
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because a foreign key references to table")));
	}
}


/*
 * EnsureTableNotForeign checks if the table is a foreign table and errors
 * if it is.
 */
void
EnsureTableNotForeign(Oid relationId)
{
	char relationKind = get_rel_relkind(relationId);
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because it is a foreign table")));
	}
}


/*
 * EnsureTableNotPartition checks if the table is a partition of another
 * table and errors if it is.
 */
void
EnsureTableNotPartition(Oid relationId)
{
	if (PartitionTable(relationId))
	{
		Oid parentRelationId = PartitionParentOid(relationId);
		char *parentRelationName = get_rel_name(parentRelationId);
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because table is a partition"),
						errhint("the parent table is \"%s\"",
								parentRelationName)));
	}
}


TableConversion *
CreateTableConversion(char conversionType, Oid relationId, char *distributionColumn,
					  bool shardCountIsNull, int shardCount, char *colocateWith,
					  char *accessMethod, bool cascadeToColocatedIsNull,
					  bool cascadeToColocated)
{
	TableConversion *con = malloc(sizeof(TableConversion));

	con->conversionType = conversionType;
	con->relationId = relationId;
	con->distributionColumn = distributionColumn;
	con->shardCountIsNull = shardCountIsNull;
	con->shardCount = shardCount;
	con->colocateWith = colocateWith;
	con->accessMethod = accessMethod;
	con->cascadeToColocatedIsNull = cascadeToColocatedIsNull;
	con->cascadeToColocated = cascadeToColocated;


	/* calculate original shard count */
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
	con->originalShardCount = cacheEntry->shardIntervalArrayLength;

	/* find relation and schema names */
	con->relationName = get_rel_name(relationId);
	con->schemaId = get_rel_namespace(relationId);
	con->schemaName = get_namespace_name(con->schemaId);

	/* calculate a temp name for the new table */
	con->tempName = pstrdup(con->relationName);
	con->hashOfName = hash_any((unsigned char *) con->tempName, strlen(con->tempName));
	AppendShardIdToName(&con->tempName, con->hashOfName);

	con->colocatedTableList = ColocatedTableList(con->relationId);


	Relation relation = try_relation_open(con->relationId, ExclusiveLock);
	relation_close(relation, NoLock);
	con->distributionKey =
		BuildDistributionKeyFromColumnName(relation, con->distributionColumn);

	con->originalDistributionKey = DistPartitionKey(con->relationId);

	if (conversionType == UNDISTRIBUTE_TABLE)
	{
		con->function = &UndistributeTable;
	}
	if (conversionType == ALTER_DISTRIBUTED_TABLE)
	{
		con->function = &AlterDistributedTable;
	}
	if (conversionType == ALTER_TABLE_SET_ACCESS_METHOD)
	{
		con->function = &AlterTableSetAccessMethod;
	}

	return con;
}


void
CreateDistributedTableLike(TableConversion *con)
{
	Var *newDistributionKey =
		con->distributionColumn ? con->distributionKey : con->originalDistributionKey;

	char *newColocateWith = con->colocateWith;
	if (con->colocateWith == NULL)
	{
		if (con->originalDistributionKey->vartype == newDistributionKey->vartype &&
			con->shardCountIsNull)
		{
			newColocateWith =
				quote_qualified_identifier(con->schemaName, con->relationName);
		}
		else
		{
			newColocateWith = "default";
		}
	}
	char partitionMethod = PartitionMethod(con->relationId);
	CreateDistributedTable(con->newRelationId, newDistributionKey, partitionMethod,
						   con->shardCount, newColocateWith, false);
}


void
CreateCitusTableLike(TableConversion *con)
{
	if (IsCitusTableType(con->relationId, DISTRIBUTED_TABLE))
	{
		CreateDistributedTableLike(con);
	}
	else if (IsCitusTableType(con->relationId, REFERENCE_TABLE))
	{
		CreateDistributedTable(con->newRelationId, NULL, DISTRIBUTE_BY_NONE, 0,
							   NULL, false);
	}
	else if (IsCitusTableType(con->relationId, CITUS_LOCAL_TABLE))
	{
		CreateCitusLocalTable(con->newRelationId);
	}
}


/*
 * GetViewCreationCommandsOfTable takes a table oid generates the CREATE VIEW
 * commands for views that depend to the given table. This includes the views
 * that recursively depend on the table too.
 */
List *
GetViewCreationCommandsOfTable(Oid relationId)
{
	List *views = GetDependingViews(relationId);
	List *commands = NIL;

	Oid viewOid = InvalidOid;
	foreach_oid(viewOid, views)
	{
		Datum viewDefinitionDatum = DirectFunctionCall1(pg_get_viewdef,
														ObjectIdGetDatum(viewOid));
		char *viewDefinition = TextDatumGetCString(viewDefinitionDatum);
		StringInfo query = makeStringInfo();
		char *viewName = get_rel_name(viewOid);
		char *schemaName = get_namespace_name(get_rel_namespace(viewOid));
		char *qualifiedViewName = quote_qualified_identifier(schemaName, viewName);
		appendStringInfo(query,
						 "CREATE VIEW %s AS %s",
						 qualifiedViewName,
						 viewDefinition);
		commands = lappend(commands, makeTableDDLCommandString(query->data));
	}
	return commands;
}


/*
 * ReplaceTable replaces the source table with the target table.
 * It moves all the rows of the source table to target table with INSERT SELECT.
 * Changes the dependencies of the sequences owned by source table to target table.
 * Then drops the source table and renames the target table to source tables name.
 *
 * Source and target tables need to be in the same schema and have the same columns.
 */
void
ReplaceTable(Oid sourceId, Oid targetId, List *justBeforeDropCommands)
{
	char *sourceName = get_rel_name(sourceId);
	char *targetName = get_rel_name(targetId);
	Oid schemaId = get_rel_namespace(sourceId);
	char *schemaName = get_namespace_name(schemaId);

	StringInfo query = makeStringInfo();

	ereport(NOTICE, (errmsg("Moving the data of %s",
							quote_qualified_identifier(schemaName, sourceName))));

	appendStringInfo(query, "INSERT INTO %s SELECT * FROM %s",
					 quote_qualified_identifier(schemaName, targetName),
					 quote_qualified_identifier(schemaName, sourceName));
	int spiResult = SPI_execute(query->data, false, 0);
	if (spiResult != SPI_OK_INSERT)
	{
		ereport(ERROR, (errmsg("could not run SPI query")));
	}

#if PG_VERSION_NUM >= PG_VERSION_13
	List *ownedSequences = getOwnedSequences(sourceId);
#else
	List *ownedSequences = getOwnedSequences(sourceId, InvalidAttrNumber);
#endif
	Oid sequenceOid = InvalidOid;
	foreach_oid(sequenceOid, ownedSequences)
	{
		changeDependencyFor(RelationRelationId, sequenceOid,
							RelationRelationId, sourceId, targetId);
	}

	char *justBeforeDropCommand = NULL;
	foreach_ptr(justBeforeDropCommand, justBeforeDropCommands)
	{
		spiResult = SPI_execute(justBeforeDropCommand, false, 0);
		if (spiResult != SPI_OK_UTILITY)
		{
			ereport(ERROR, (errmsg("could not run SPI query")));
		}
	}

	ereport(NOTICE, (errmsg("Dropping the old %s",
							quote_qualified_identifier(schemaName, sourceName))));

	resetStringInfo(query);
	appendStringInfo(query, "DROP TABLE %s CASCADE",
					 quote_qualified_identifier(schemaName, sourceName));
	spiResult = SPI_execute(query->data, false, 0);
	if (spiResult != SPI_OK_UTILITY)
	{
		ereport(ERROR, (errmsg("could not run SPI query")));
	}

	ereport(NOTICE, (errmsg("Renaming the new table to %s",
							quote_qualified_identifier(schemaName, sourceName))));

	resetStringInfo(query);
	appendStringInfo(query, "ALTER TABLE %s RENAME TO %s",
					 quote_qualified_identifier(schemaName, targetName),
					 quote_identifier(sourceName));
	spiResult = SPI_execute(query->data, false, 0);
}


/*
 * AlterDistributedTableMessages errors for the cases where
 * alter_distributed_table UDF wouldn't work.
 */
void
AlterDistributedTableMessages(TableConversion *con)
{
	/* Changing nothing is not allowed */
	if (con->distributionColumn == NULL && con->shardCountIsNull &&
		con->colocateWith == NULL &&
		(con->cascadeToColocatedIsNull || con->cascadeToColocated == false))
	{
		ereport(ERROR, (errmsg("you have to specify at least one of the "
							   "distribution_column, shard_count or "
							   "colocate_with parameters")));
	}

	/*Error for no operation UDF calls. First, check distribution column. */
	if (con->distributionColumn != NULL)
	{
		if (equal(con->distributionKey, con->originalDistributionKey))
		{
			ereport(ERROR, (errmsg("table is already distributed by %s",
								   con->distributionColumn)));
		}
	}

	/* Second, check for no-op shard count UDF calls. */
	if (!con->shardCountIsNull)
	{
		if (con->originalShardCount == con->shardCount)
		{
			ereport(ERROR, (errmsg("shard count of the table is already %d",
								   con->shardCount)));
		}
	}

	/* Third, check for no-op colocate with UDF calls. */
	if (con->colocateWith != NULL && strcmp(con->colocateWith, "default") != 0 &&
		strcmp(con->colocateWith, "none") != 0)
	{
		Oid colocatedTableOid = InvalidOid;
		text *colocateWithText = cstring_to_text(con->colocateWith);
		Oid colocateWithTableOid = ResolveRelationId(colocateWithText, false);
		foreach_oid(colocatedTableOid, con->colocatedTableList)
		{
			if (colocateWithTableOid == colocatedTableOid)
			{
				ereport(ERROR, (errmsg("table is already colocated with %s",
									   con->colocateWith)));
				break;
			}
		}
	}

	/* shard_count:=0 is not allowed */
	if (!con->shardCountIsNull && con->shardCount == 0)
	{
		ereport(ERROR, (errmsg("shard_count cannot be 0"),
						errhint("if you no longer want this to be a "
								"distributed table you can try "
								"undistribute_table() function")));
	}

	if (con->cascadeToColocated == true && con->distributionColumn != NULL)
	{
		ereport(ERROR, (errmsg("distribution_column changes cannot be "
							   "cascaded to colocated tables")));
	}
	if (con->cascadeToColocated == true && con->shardCountIsNull &&
		con->colocateWith == NULL)
	{
		ereport(ERROR, (errmsg("shard_count or colocate_with is necessary "
							   "for cascading to colocated tables")));
	}
	if (con->cascadeToColocated == true && con->colocateWith != NULL &&
		strcmp(con->colocateWith, "none") == 0)
	{
		ereport(ERROR, (errmsg("colocate_with := 'none' cannot be "
							   "cascaded to colocated tables")));
	}

	int colocatedTableCount = list_length(con->colocatedTableList) - 1;
	if (!con->shardCountIsNull && con->cascadeToColocatedIsNull &&
		colocatedTableCount > 0)
	{
		ereport(ERROR, (errmsg("cascade_to_colocated parameter is necessary"),
						errdetail("this table is colocated with some other tables"),
						errhint("cascade_to_colocated := false will break the "
								"current colocation, cascade_to_colocated := true "
								"will change the shard count of colocated tables "
								"too.")));
	}
}
