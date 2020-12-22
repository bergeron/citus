/*-------------------------------------------------------------------------
 *
 * foreign_key_relationship.c
 *   This file contains functions for creating foreign key relationship graph
 *   between distributed tables. Created relationship graph will be hold by
 *   a static variable defined in this file until an invalidation comes in.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#if PG_VERSION_NUM >= PG_VERSION_12
#include "access/genam.h"
#endif
#include "access/htup_details.h"
#include "access/stratnum.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "access/table.h"
#endif
#include "catalog/pg_constraint.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/version_compat.h"
#include "nodes/pg_list.h"
#include "storage/lockdefs.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#if PG_VERSION_NUM >= PG_VERSION_13
#include "common/hashfn.h"
#endif
#include "utils/memutils.h"
#if PG_VERSION_NUM < PG_VERSION_12
#include "utils/rel.h"
#endif


/*
 * ForeignConstraintRelationshipGraph holds the graph data structure for foreign constraint relationship
 * between relations. We will only have single static instance of that struct and it
 * will be invalidated after change on any foreign constraint.
 */
typedef struct ForeignConstraintRelationshipGraph
{
	HTAB *nodeMap;
	bool isValid;
}ForeignConstraintRelationshipGraph;

/*
 * ForeignConstraintRelationshipNode holds the data for each node of the ForeignConstraintRelationshipGraph
 * For each node we have relation id, which is the Oid of that relation, visiting
 * information for that node in the latest DFS and the list of adjacency nodes.
 * Note that we also hold back adjacency nodes for getting referenced node over
 * that one.
 */
typedef struct ForeignConstraintRelationshipNode
{
	Oid relationId;
	bool visited;
	List *adjacencyList;
	List *backAdjacencyList;
}ForeignConstraintRelationshipNode;


/*
 * ForeignConstraintRelationshipEdge will only be used while creating the ForeignConstraintRelationshipGraph.
 * It won't show edge information on the graph, yet will be used in the pre-processing
 * phase.
 */
typedef struct ForeignConstraintRelationshipEdge
{
	Oid referencingRelationOID;
	Oid referencedRelationOID;
}ForeignConstraintRelationshipEdge;


static ForeignConstraintRelationshipGraph *fConstraintRelationshipGraph = NULL;

static List * GetRelationshipNodesForFKeyConnectedRelations(
	ForeignConstraintRelationshipNode *relationshipNode);
static List * GetAllNeighboursList(ForeignConstraintRelationshipNode *relationshipNode);
static ForeignConstraintRelationshipNode * GetRelationshipNodeForRelationId(Oid
																			relationId,
																			bool *isFound);
static void CreateForeignConstraintRelationshipGraph(void);
static List * GetNeighbourList(ForeignConstraintRelationshipNode *relationshipNode,
							   bool isReferencing);
static void SetRelationshipNodeListNotVisited(List *relationshipNodeList);
static List * GetRelationIdsFromRelationshipNodeList(List *fKeyRelationshipNodeList);
static void PopulateAdjacencyLists(void);
static int CompareForeignConstraintRelationshipEdges(const void *leftElement,
													 const void *rightElement);
static void AddForeignConstraintRelationshipEdge(Oid referencingOid, Oid referencedOid);
static ForeignConstraintRelationshipNode * CreateOrFindNode(HTAB *adjacencyLists, Oid
															relid);
static void GetConnectedListHelper(ForeignConstraintRelationshipNode *node,
								   List **adjacentNodeList, bool
								   isReferencing);
static List * GetForeignConstraintRelationshipHelper(Oid relationId, bool isReferencing);


/*
 * GetForeignKeyConnectedRelationIdList returns a list of relation id's for
 * relations that are connected to relation with relationId via a foreign
 * key graph.
 */
List *
GetForeignKeyConnectedRelationIdList(Oid relationId)
{
	/* use ShareRowExclusiveLock to prevent concurent foreign key creation */
	LOCKMODE lockMode = ShareRowExclusiveLock;
	Relation relation = try_relation_open(relationId, lockMode);
	if (!RelationIsValid(relation))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation with OID %d does not exist",
							   relationId)));
	}

	relation_close(relation, NoLock);

	bool foundInFKeyGraph = false;
	ForeignConstraintRelationshipNode *relationshipNode =
		GetRelationshipNodeForRelationId(relationId, &foundInFKeyGraph);
	if (!foundInFKeyGraph)
	{
		/*
		 * Relation could not be found in foreign key graph, then it has no
		 * foreign key relationships.
		 */
		return NIL;
	}

	List *fKeyConnectedRelationshipNodeList =
		GetRelationshipNodesForFKeyConnectedRelations(relationshipNode);
	List *fKeyConnectedRelationIdList = GetRelationIdsFromRelationshipNodeList(
		fKeyConnectedRelationshipNodeList);
	return fKeyConnectedRelationIdList;
}


/*
 * GetRelationshipNodesForFKeyConnectedRelations performs depth-first search
 * starting from input ForeignConstraintRelationshipNode and returns a list
 * of ForeignConstraintRelationshipNode objects for relations that are connected
 * to given relation node via a foreign key relationhip graph.
 */
static List *
GetRelationshipNodesForFKeyConnectedRelations(
	ForeignConstraintRelationshipNode *relationshipNode)
{
	relationshipNode->visited = true;
	List *relationshipNodeList = list_make1(relationshipNode);

	ForeignConstraintRelationshipNode *currentNode = NULL;
	foreach_ptr_append(currentNode, relationshipNodeList)
	{
		List *allNeighboursList = GetAllNeighboursList(currentNode);
		ForeignConstraintRelationshipNode *neighbourNode = NULL;
		foreach_ptr(neighbourNode, allNeighboursList)
		{
			if (neighbourNode->visited)
			{
				continue;
			}

			neighbourNode->visited = true;
			relationshipNodeList = lappend(relationshipNodeList, neighbourNode);
		}
	}

	/* reset visited flags in foreign key graph */
	SetRelationshipNodeListNotVisited(relationshipNodeList);

	return relationshipNodeList;
}


/*
 * GetAllNeighboursList returns a list of ForeignConstraintRelationshipNode
 * objects by concatenating both (referencing & referenced) adjacency lists
 * of given relationship node.
 */
static List *
GetAllNeighboursList(ForeignConstraintRelationshipNode *relationshipNode)
{
	bool isReferencing = false;
	List *referencedNeighboursList = GetNeighbourList(relationshipNode, isReferencing);

	isReferencing = true;
	List *referencingNeighboursList = GetNeighbourList(relationshipNode, isReferencing);

	/*
	 * GetNeighbourList returns list from graph as is, so first copy it as
	 * list_concat might invalidate it.
	 */
	List *allNeighboursList = list_copy(referencedNeighboursList);
	allNeighboursList = list_concat_unique_ptr(allNeighboursList,
											   referencingNeighboursList);
	return allNeighboursList;
}


/*
 * ReferencedRelationIdList is a wrapper function around GetForeignConstraintRelationshipHelper
 * to get list of relation IDs which are referenced by the given relation id.
 *
 * Note that, if relation A is referenced by relation B and relation B is referenced
 * by relation C, then the result list for relation A consists of the relation
 * IDs of relation B and relation C.
 */
List *
ReferencedRelationIdList(Oid relationId)
{
	return GetForeignConstraintRelationshipHelper(relationId, false);
}


/*
 * ReferencingRelationIdList is a wrapper function around GetForeignConstraintRelationshipHelper
 * to get list of relation IDs which are referencing to given relation id.
 *
 * Note that, if relation A is referenced by relation B and relation B is referenced
 * by relation C, then the result list for relation C consists of the relation
 * IDs of relation A and relation B.
 */
List *
ReferencingRelationIdList(Oid relationId)
{
	return GetForeignConstraintRelationshipHelper(relationId, true);
}


/*
 * GetForeignConstraintRelationshipHelper returns the list of oids referenced or
 * referencing given relation id. It is a helper function for providing results
 * to public functions ReferencedRelationIdList and ReferencingRelationIdList.
 */
static List *
GetForeignConstraintRelationshipHelper(Oid relationId, bool isReferencing)
{
	bool isFound = false;
	ForeignConstraintRelationshipNode *relationshipNode =
		GetRelationshipNodeForRelationId(relationId, &isFound);

	if (!isFound)
	{
		/*
		 * If there is no node with the given relation id, that means given table
		 * is not referencing and is not referenced by any table
		 */
		return NIL;
	}

	List *foreignNodeList = NIL;
	GetConnectedListHelper(relationshipNode, &foreignNodeList, isReferencing);

	/* reset visited flags in foreign key graph */
	SetRelationshipNodeListNotVisited(foreignNodeList);

	/* set to false separately, since we don't add itself to foreign node list */
	relationshipNode->visited = false;

	List *relationIdList = GetRelationIdsFromRelationshipNodeList(foreignNodeList);
	return relationIdList;
}


/*
 * GetRelationshipNodeForRelationId searches foreign key graph for relation
 * with relationId and returns ForeignConstraintRelationshipNode object for
 * relation if it exists in graph. Otherwise, sets isFound to false.
 *
 * Also before searching foreign key graph, this function implicitly builds
 * foreign key graph if it's invalid or not built yet.
 */
static ForeignConstraintRelationshipNode *
GetRelationshipNodeForRelationId(Oid relationId, bool *isFound)
{
	CreateForeignConstraintRelationshipGraph();

	ForeignConstraintRelationshipNode *relationshipNode =
		(ForeignConstraintRelationshipNode *) hash_search(
			fConstraintRelationshipGraph->nodeMap, &relationId,
			HASH_FIND, isFound);

	return relationshipNode;
}


/*
 * CreateForeignConstraintRelationshipGraph creates the foreign constraint relation graph using
 * foreign constraint provided by pg_constraint metadata table.
 */
static void
CreateForeignConstraintRelationshipGraph()
{
	HASHCTL info;

	/* if we have already created the graph, use it */
	if (IsForeignConstraintRelationshipGraphValid())
	{
		return;
	}

	ClearForeignConstraintRelationshipGraphContext();

	MemoryContext fConstraintRelationshipMemoryContext = AllocSetContextCreateExtended(
		CacheMemoryContext,
		"Forign Constraint Relationship Graph Context",
		ALLOCSET_DEFAULT_MINSIZE,
		ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContext oldContext = MemoryContextSwitchTo(
		fConstraintRelationshipMemoryContext);

	fConstraintRelationshipGraph = (ForeignConstraintRelationshipGraph *) palloc(
		sizeof(ForeignConstraintRelationshipGraph));
	fConstraintRelationshipGraph->isValid = false;

	/* create (oid) -> [ForeignConstraintRelationshipNode] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(ForeignConstraintRelationshipNode);
	info.hash = oid_hash;
	info.hcxt = CurrentMemoryContext;
	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	fConstraintRelationshipGraph->nodeMap = hash_create(
		"foreign key relationship map (oid)",
		32, &info, hashFlags);

	PopulateAdjacencyLists();

	fConstraintRelationshipGraph->isValid = true;
	MemoryContextSwitchTo(oldContext);
}


/*
 * IsForeignConstraintGraphValid check whether there is a valid graph.
 */
bool
IsForeignConstraintRelationshipGraphValid()
{
	if (fConstraintRelationshipGraph != NULL && fConstraintRelationshipGraph->isValid)
	{
		return true;
	}

	return false;
}


/*
 * SetForeignConstraintGraphInvalid sets the validity of the graph to false.
 */
void
SetForeignConstraintRelationshipGraphInvalid()
{
	if (fConstraintRelationshipGraph != NULL)
	{
		fConstraintRelationshipGraph->isValid = false;
	}
}


/*
 * GetConnectedListHelper is the function for getting nodes connected (or connecting) to
 * the given relation. adjacentNodeList holds the result for recursive calls and
 * by changing isReferencing caller function can select connected or connecting
 * adjacency list.
 *
 */
static void
GetConnectedListHelper(ForeignConstraintRelationshipNode *node, List **adjacentNodeList,
					   bool isReferencing)
{
	node->visited = true;

	ForeignConstraintRelationshipNode *neighbourNode = NULL;
	List *neighbourList = GetNeighbourList(node, isReferencing);
	foreach_ptr(neighbourNode, neighbourList)
	{
		if (neighbourNode->visited == false)
		{
			*adjacentNodeList = lappend(*adjacentNodeList, neighbourNode);
			GetConnectedListHelper(neighbourNode, adjacentNodeList, isReferencing);
		}
	}
}


/*
 * GetNeighbourList returns copy of relevant adjacency list of given
 * ForeignConstraintRelationshipNode object depending on the isReferencing
 * flag.
 */
static List *
GetNeighbourList(ForeignConstraintRelationshipNode *relationshipNode, bool isReferencing)
{
	if (isReferencing)
	{
		return relationshipNode->backAdjacencyList;
	}
	else
	{
		return relationshipNode->adjacencyList;
	}
}


/*
 * SetRelationshipNodeListNotVisited takes a list of ForeignConstraintRelationshipNode
 * objects and sets their visited flags to false.
 */
static void
SetRelationshipNodeListNotVisited(List *relationshipNodeList)
{
	ForeignConstraintRelationshipNode *relationshipNode = NULL;
	foreach_ptr(relationshipNode, relationshipNodeList)
	{
		relationshipNode->visited = false;
	}
}


/*
 * GetRelationIdsFromRelationshipNodeList returns list of relationId's for
 * given ForeignConstraintRelationshipNode object list.
 */
static List *
GetRelationIdsFromRelationshipNodeList(List *fKeyRelationshipNodeList)
{
	List *relationIdList = NIL;

	ForeignConstraintRelationshipNode *fKeyRelationshipNode = NULL;
	foreach_ptr(fKeyRelationshipNode, fKeyRelationshipNodeList)
	{
		Oid relationId = fKeyRelationshipNode->relationId;
		relationIdList = lappend_oid(relationIdList, relationId);
	}

	return relationIdList;
}


/*
 * PopulateAdjacencyLists gets foreign constraint relationship information from pg_constraint
 * metadata table and populates them to the foreign constraint relation graph.
 */
static void
PopulateAdjacencyLists(void)
{
	HeapTuple tuple;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Oid prevReferencingOid = InvalidOid;
	Oid prevReferencedOid = InvalidOid;
	List *frelEdgeList = NIL;

	Relation pgConstraint = table_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_contype, BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint, InvalidOid, false,
													NULL, scanKeyCount, scanKey);

	while (HeapTupleIsValid(tuple = systable_getnext(scanDescriptor)))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(tuple);

		ForeignConstraintRelationshipEdge *currentFConstraintRelationshipEdge = palloc(
			sizeof(ForeignConstraintRelationshipEdge));
		currentFConstraintRelationshipEdge->referencingRelationOID =
			constraintForm->conrelid;
		currentFConstraintRelationshipEdge->referencedRelationOID =
			constraintForm->confrelid;

		frelEdgeList = lappend(frelEdgeList, currentFConstraintRelationshipEdge);
	}

	/*
	 * Since there is no index on columns we are planning to sort tuples
	 * sorting tuples manually instead of using scan keys
	 */
	frelEdgeList = SortList(frelEdgeList, CompareForeignConstraintRelationshipEdges);

	ForeignConstraintRelationshipEdge *currentFConstraintRelationshipEdge = NULL;
	foreach_ptr(currentFConstraintRelationshipEdge, frelEdgeList)
	{
		/* we just saw this edge, no need to add it twice */
		if (currentFConstraintRelationshipEdge->referencingRelationOID ==
			prevReferencingOid &&
			currentFConstraintRelationshipEdge->referencedRelationOID ==
			prevReferencedOid)
		{
			continue;
		}

		AddForeignConstraintRelationshipEdge(
			currentFConstraintRelationshipEdge->referencingRelationOID,
			currentFConstraintRelationshipEdge->
			referencedRelationOID);

		prevReferencingOid = currentFConstraintRelationshipEdge->referencingRelationOID;
		prevReferencedOid = currentFConstraintRelationshipEdge->referencedRelationOID;
	}

	systable_endscan(scanDescriptor);
	table_close(pgConstraint, AccessShareLock);
}


/*
 * CompareForeignConstraintRelationshipEdges is a helper function to compare two
 * ForeignConstraintRelationshipEdge using referencing and referenced ids respectively.
 */
static int
CompareForeignConstraintRelationshipEdges(const void *leftElement,
										  const void *rightElement)
{
	const ForeignConstraintRelationshipEdge *leftEdge =
		*((const ForeignConstraintRelationshipEdge **) leftElement);
	const ForeignConstraintRelationshipEdge *rightEdge =
		*((const ForeignConstraintRelationshipEdge **) rightElement);

	int referencingDiff = leftEdge->referencingRelationOID -
						  rightEdge->referencingRelationOID;
	int referencedDiff = leftEdge->referencedRelationOID -
						 rightEdge->referencedRelationOID;

	if (referencingDiff != 0)
	{
		return referencingDiff;
	}

	return referencedDiff;
}


/*
 * AddForeignConstraintRelationshipEdge adds edge between the nodes having given OIDs
 * by adding referenced node to the adjacency list referencing node and adding
 * referencing node to the back adjacency list of referenced node.
 */
static void
AddForeignConstraintRelationshipEdge(Oid referencingOid, Oid referencedOid)
{
	ForeignConstraintRelationshipNode *referencingNode = CreateOrFindNode(
		fConstraintRelationshipGraph->nodeMap, referencingOid);
	ForeignConstraintRelationshipNode *referencedNode = CreateOrFindNode(
		fConstraintRelationshipGraph->nodeMap, referencedOid);

	referencingNode->adjacencyList = lappend(referencingNode->adjacencyList,
											 referencedNode);
	referencedNode->backAdjacencyList = lappend(referencedNode->backAdjacencyList,
												referencingNode);
}


/*
 * CreateOrFindNode either gets or adds new node to the foreign constraint relation graph
 */
static ForeignConstraintRelationshipNode *
CreateOrFindNode(HTAB *adjacencyLists, Oid relid)
{
	bool found = false;
	ForeignConstraintRelationshipNode *node =
		(ForeignConstraintRelationshipNode *) hash_search(adjacencyLists,
														  &relid, HASH_ENTER,
														  &found);

	if (!found)
	{
		node->adjacencyList = NIL;
		node->backAdjacencyList = NIL;
		node->visited = false;
	}

	return node;
}


/*
 * ClearForeignConstraintRelationshipGraphContext clear all the allocated memory obtained
 * for foreign constraint relationship graph. Since all the variables of relationship
 * graph was obtained within the same context, destroying hash map is enough as
 * it deletes the context.
 */
void
ClearForeignConstraintRelationshipGraphContext()
{
	if (fConstraintRelationshipGraph == NULL)
	{
		return;
	}

	hash_destroy(fConstraintRelationshipGraph->nodeMap);
	fConstraintRelationshipGraph = NULL;
}
