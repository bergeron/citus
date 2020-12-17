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

static void UndistributeTable(Oid relationId);
static void AlterDistributedTable(Oid relationId, char *distributionColumn, int shardCount, char *colocateWith, bool cascadeToColocated);
static void AlterTableSetAccessMethod(Oid relationId, char *accessMethod);
static void ConvertTable(char conversionType, Oid relationId, char *distributionColumn, int shardCount, char *colocateWith, char *accessMethod, bool cascadeToColocated);
static void EnsureTableNotReferencing(Oid relationId);
static void EnsureTableNotReferenced(Oid relationId);
static void EnsureTableNotForeign(Oid relationId);
static void EnsureTableNotPartition(Oid relationId);
static List * GetViewCreationCommandsOfTable(Oid relationId);
static void ReplaceTable(Oid sourceId, Oid targetId);
static void AlterDistributedTableMessages(Oid relationId, char *distributionColumn, bool shardCountIsNull, int shardCount, char *colocateWith, bool cascadeToColocatedIsNull, bool cascadeToColocated);

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

	UndistributeTable(relationId);

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
	Oid relationId = InvalidOid;
	relationId = PG_GETARG_OID(0);
	
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

	AlterDistributedTableMessages(relationId, distributionColumn, shardCountIsNull, shardCount, colocateWith, cascadeToColocatedIsNull, cascadeToColocated);

	AlterDistributedTable(relationId, distributionColumn, shardCount, colocateWith, cascadeToColocated);

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
	EnsureCoordinator();
	EnsureRelationExists(relationId);
	EnsureTableOwner(relationId);

	AlterTableSetAccessMethod(relationId, accessMethod);

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
UndistributeTable(Oid relationId)
{
	Relation relation = try_relation_open(relationId, ExclusiveLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("cannot undistribute table "
							   "because no such distributed table exists")));
	}

	relation_close(relation, NoLock);

	if (!IsCitusTable(relationId))
	{
		ereport(ERROR, (errmsg("cannot undistribute table "
							   "because the table is not distributed")));
	}

	EnsureTableNotReferencing(relationId);
	EnsureTableNotReferenced(relationId);
	EnsureTableNotForeign(relationId);
	EnsureTableNotPartition(relationId);

	ConvertTable(UNDISTRIBUTE_TABLE, relationId, NULL, 0, NULL, NULL, false);
}


/*
 * AlterDistributedTable changes some properties of the given table. It uses
 * ConvertTable function to create a new local table and move everything to that table.
 * 
 * The local and reference tables, tables with references, partition tables and foreign
 * tables are not supported. The function gives errors in these cases.
 */
void
AlterDistributedTable(Oid relationId, char *distributionColumn, int shardCount, char *colocateWith, bool cascadeToColocated)
{
	Relation relation = try_relation_open(relationId, ExclusiveLock);

	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("cannot undistribute table "
							   "because no such distributed table exists")));
	}
	relation_close(relation, NoLock);

	if (!IsCitusTableType(relationId, DISTRIBUTED_TABLE))
	{
		ereport(ERROR, (errmsg("cannot undistribute table "
							   "because the table is not distributed")));
	}

	EnsureTableNotReferencing(relationId);
	EnsureTableNotReferenced(relationId);
	EnsureTableNotForeign(relationId);
	EnsureTableNotPartition(relationId);
	if (colocateWith != NULL && strcmp(colocateWith, "default") != 0 && strcmp(colocateWith, "none") != 0)
	{	
		text *colocateWithText = cstring_to_text(colocateWith);
		Oid colocateWithTableOid = ResolveRelationId(colocateWithText, false);
		CitusTableCacheEntry *colocateWithTableCacheEntry = GetCitusTableCacheEntry(colocateWithTableOid);
		int colocateWithTableShardCount = colocateWithTableCacheEntry -> shardIntervalArrayLength;

		if (shardCount != 0 && shardCount != colocateWithTableShardCount)
		{
			ereport(ERROR, (errmsg("shard_count cannot be different than the shard "
								   "count of the table in colocate_with"),
							errhint("if no shard_count is specified shard count "
									"will be same with colocate_with table's")));
		}

		/*shardCount is either 0 or already same with colocateWith table's*/
		shardCount = colocateWithTableShardCount;
	}

	ConvertTable(ALTER_DISTRIBUTED_TABLE, relationId, distributionColumn, shardCount, colocateWith, NULL, cascadeToColocated);
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
AlterTableSetAccessMethod(Oid relationId, char *accessMethod)
{
	Relation relation = try_relation_open(relationId, ExclusiveLock);

	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("cannot undistribute table "
							   "because no such distributed table exists")));
	}
	relation_close(relation, NoLock);

	EnsureTableNotReferencing(relationId);
	EnsureTableNotReferenced(relationId);
	EnsureTableNotForeign(relationId);

	ConvertTable(ALTER_TABLE_SET_ACCESS_METHOD, relationId, NULL, 0, NULL, accessMethod, false);
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
ConvertTable(char conversionType, Oid relationId, char *distributionColumn, int shardCount, char *colocateWith, char *accessMethod, bool cascadeToColocated)
{
	List *colocatedTableList = NIL;
	if (cascadeToColocated)
	{
		colocatedTableList = ColocatedTableList(relationId);
	}

	bool shardCountIsNull = false;
	if (shardCount == 0)
	{
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		shardCount = cacheEntry -> shardIntervalArrayLength;
		shardCountIsNull = true;
	}
	List *preLoadCommands = GetPreLoadTableCreationCommands(relationId, true, accessMethod);
	List *postLoadCommands = GetPostLoadTableCreationCommands(relationId);

	postLoadCommands = list_concat(postLoadCommands,
								   GetViewCreationCommandsOfTable(relationId));

	char *relationName = get_rel_name(relationId);
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);

	int spiResult = SPI_connect();
	if (spiResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	bool isPartitionTable = false;
	char *attachToParentCommand = NULL;
	if (PartitionTable(relationId))
	{
		isPartitionTable = true;
		char *detachFromParentCommand = GenerateDetachPartitionCommand(relationId);
		attachToParentCommand = GenerateAlterTableAttachPartitionCommand(relationId);

		spiResult = SPI_execute(detachFromParentCommand, false, 0);
		if (spiResult != SPI_OK_UTILITY)
		{
			ereport(ERROR, (errmsg("could not run SPI query")));
		}
	}

	if (PartitionedTable(relationId))
	{
		ereport(NOTICE, (errmsg("undistributing the partitions of %s",
								quote_qualified_identifier(schemaName, relationName))));
		List *partitionList = PartitionList(relationId);
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
			
			if (conversionType == UNDISTRIBUTE_TABLE)
			{
				UndistributeTable(partitionRelationId);
			}
			else if (conversionType == ALTER_DISTRIBUTED_TABLE)
			{
				AlterDistributedTable(partitionRelationId, NULL, shardCount, NULL, false);
			}
		}
	}

	char *tempName = pstrdup(relationName);
	uint32 hashOfName = hash_any((unsigned char *) tempName, strlen(tempName));
	AppendShardIdToName(&tempName, hashOfName);


	ereport(NOTICE, (errmsg("creating a new table for %s",
							quote_qualified_identifier(schemaName, relationName))));

	TableDDLCommand *tableCreationCommand = NULL;
	foreach_ptr(tableCreationCommand, preLoadCommands)
	{
		Assert(CitusIsA(tableCreationCommand, TableDDLCommand));

		char *tableCreationSql = GetTableDDLCommand(tableCreationCommand);
		Node *parseTree = ParseTreeNode(tableCreationSql);

		RelayEventExtendNames(parseTree, schemaName, hashOfName);
		CitusProcessUtility(parseTree, tableCreationSql, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
	}

	if (conversionType == ALTER_DISTRIBUTED_TABLE || (conversionType == ALTER_TABLE_SET_ACCESS_METHOD && IsCitusTableType(relationId, DISTRIBUTED_TABLE)))
	{
		Var *distributionKey = NULL;

		if (distributionColumn)
		{
			Relation relation = try_relation_open(relationId, ExclusiveLock);
			relation_close(relation, NoLock);
			distributionKey = BuildDistributionKeyFromColumnName(relation,
																 distributionColumn);
		}
		else
		{
			distributionKey = DistPartitionKey(relationId);
		}

		if (colocateWith == NULL)
		{
			Var *originalDistributionKey = DistPartitionKey(relationId);
			if ((distributionColumn == NULL || originalDistributionKey->vartype == distributionKey->vartype) && shardCountIsNull)
			{
				colocateWith = quote_qualified_identifier(schemaName, relationName);
			}
			else
			{
				colocateWith = "default";
			}
		}
		char partitionMethod = PartitionMethod(relationId);
		CreateDistributedTable(get_relname_relid(tempName, schemaId), distributionKey, partitionMethod, shardCount, colocateWith, false);
	}
	else if (conversionType == ALTER_TABLE_SET_ACCESS_METHOD)
	{
		if (IsCitusTableType(relationId, REFERENCE_TABLE))
		{
			CreateDistributedTable(get_relname_relid(tempName, schemaId), NULL, DISTRIBUTE_BY_NONE, ShardCount, NULL, false);
		}
		else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
		{
			CreateCitusLocalTable(get_relname_relid(tempName, schemaId));
		}
	}

	ReplaceTable(relationId, get_relname_relid(tempName, schemaId));

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
	
	if (cascadeToColocated)
	{
		Oid colocatedTableId = InvalidOid;
		// For now we only support cascade to colocation for alter_distributed_table UDF
		Assert(conversionType == ALTER_DISTRIBUTED_TABLE);
		foreach_oid(colocatedTableId, colocatedTableList)
		{
			if (colocatedTableId == relationId)
			{
				continue;
			}
			if (conversionType == ALTER_DISTRIBUTED_TABLE)
			{
				StringInfo qualifiedRelationName = makeStringInfo();
				appendStringInfo(qualifiedRelationName, "%s.%s", schemaName, relationName);
				AlterDistributedTable(colocatedTableId, NULL, shardCount, qualifiedRelationName->data, false);
			}
		}
	}
}


/*
 * EnsureTableNotReferencing checks if the table has a reference to another
 * table and errors if it is.
 */
void EnsureTableNotReferencing(Oid relationId)
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
void EnsureTableNotReferenced(Oid relationId)
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
void EnsureTableNotForeign(Oid relationId)
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
void EnsureTableNotPartition(Oid relationId)
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
ReplaceTable(Oid sourceId, Oid targetId)
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
void AlterDistributedTableMessages(Oid relationId, char *distributionColumn, bool shardCountIsNull, int shardCount, char *colocateWith, bool cascadeToColocatedIsNull, bool cascadeToColocated)
{
	/* Changing nothing is not allowed */
	if (distributionColumn == NULL && shardCountIsNull && colocateWith == NULL && (cascadeToColocatedIsNull || cascadeToColocated == false))
	{
		ereport(ERROR, (errmsg("you have to specify at least one of the distribution_column, shard_count or colocate_with parameters")));
	}

	/*Error for no operation UDF calls. First, check distribution column. */
	if (distributionColumn != NULL)
	{
		Relation relation = try_relation_open(relationId, ExclusiveLock);
		relation_close(relation, NoLock);
		Var *distributionKey = BuildDistributionKeyFromColumnName(relation, distributionColumn);

		Var *originalDistributionKey = DistPartitionKey(relationId);

		if (equal(distributionKey, originalDistributionKey))
		{
			ereport(ERROR, (errmsg("table is already distributed by %s", distributionColumn)));
		}
	}

	/* Second, check for no-op shard count UDF calls. */
	if (!shardCountIsNull)
	{
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		int originalShardCount = cacheEntry -> shardIntervalArrayLength;
		if (originalShardCount == shardCount)
		{
			ereport(ERROR, (errmsg("shard count of the table is already %d", shardCount)));
		}
	}

	/* Third, check for no-op colocate with UDF calls. */
	if (colocateWith != NULL && strcmp(colocateWith, "default") != 0 && strcmp (colocateWith, "none") != 0)
	{
		List *colocatedTableList = ColocatedTableList(relationId);
		Oid colocatedTableOid = InvalidOid;
		text *colocateWithText = cstring_to_text(colocateWith);
		Oid colocateWithTableOid = ResolveRelationId(colocateWithText, false);
		foreach_oid(colocatedTableOid, colocatedTableList)
		{
			if (colocateWithTableOid == colocatedTableOid)
			{
				ereport(ERROR, (errmsg("table is already colocated with %s", colocateWith)));
				break;
			}
		}
	}


	if (cascadeToColocated == true && distributionColumn != NULL)
	{
		ereport(ERROR, (errmsg("distribution_column changes cannot be cascaded to colocated tables")));
	}
	if (cascadeToColocated == true && shardCountIsNull && colocateWith == NULL)
	{
		ereport(ERROR, (errmsg("shard_count or colocate_with is necessary for cascading to colocated tables")));
	}
	if (cascadeToColocated == true && colocateWith != NULL && strcmp(colocateWith, "none") == 0)
	{
		ereport(ERROR, (errmsg("colocate_with := 'none' cannot be cascaded to colocated tables")));
	}
	List *colocatedTableList = ColocatedTableList(relationId);
	int colocatedTableCount = list_length(colocatedTableList) - 1;
	if (!shardCountIsNull && cascadeToColocatedIsNull && colocatedTableCount > 0)
	{
		ereport(ERROR, (errmsg("cascade_to_colocated parameter is necessary"),
						errdetail("this table is colocated with some other tables"),
						errhint("cascade_to_colocated := false will break the current colocation, "
								"cascade_to_colocated := true will change the shard count of "
								"colocated tables too.")));
	}
}
