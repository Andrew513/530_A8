
#ifndef SFWQUERY_H
#define SFWQUERY_H

#include "ExprTree.h"
#include "MyDB_LogicalOps.h"
#include "MyDB_BPlusTreeReaderWriter.h"

// structure that stores an entire SFW query
struct SFWQuery {

private:

	// the various parts of the SQL query
	vector <ExprTreePtr> valuesToSelect;
	vector <pair <string, string>> tablesToProcess;
	vector <ExprTreePtr> allDisjunctions;
	vector <ExprTreePtr> groupingClauses;

public:
	SFWQuery () {}

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause, 
		struct CNF *cnf, struct ValueList *grouping);

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause, 
		struct CNF *cnf);

	SFWQuery (struct ValueList *selectClause, struct FromList *fromClause);
	
	// builds and optimizes a logical query plan for a SFW query, returning the resulting logical query plan
	//
	// allTables: this is the list of all of the tables currently in the system
	pair <LogicalOpPtr, double> optimizeQueryPlan (map <string, MyDB_TablePtr> &allTables);

	// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
	pair <LogicalOpPtr, double> optimizeQueryPlan (map <string, MyDB_TablePtr> &allTables,
        	MyDB_SchemaPtr totSchema, vector <ExprTreePtr> &allDisjunctions);

	// executes the given logical query plan
	void execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters, 
		map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters,
		LogicalOpPtr executeMe);

	~SFWQuery () {}

	void print ();

	#include "FriendDecls.h"
};

#endif
