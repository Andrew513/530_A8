
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"
#include "Aggregate.h"	
#include "RegularSelection.h"	

ExprTreePtr nullDude = nullptr;

// what this function does is to recurse into the ExprTreePtr extractFromMe to find all instances of aggregates.
// If it finds an aggregate, it replaces it with an auto-generated attribute name and then also generates the
// code necessary to run that aggregate.  The entries in intoMe are (generated name; agg type; code) triples.
static int counter = 0;
ExprTreePtr recurse (ExprTreePtr extractFromMe, vector <pair <string, pair <MyDB_AggType, string>>> &intoMe) {

	if (extractFromMe->isSum ()) {
		string toCompute = extractFromMe->getChild ()->toString (); 
		string attName = "sum_" + to_string (counter);
		counter++;
		intoMe.push_back (make_pair ("agg_" + attName, make_pair (SUM, toCompute)));
		return make_shared <Identifier> ("agg", attName);
	} else if (extractFromMe->isAvg ()) {	
		string toCompute = extractFromMe->getChild ()->toString (); 
		string attName = "avg_" + to_string (counter);
		counter++;
		intoMe.push_back (make_pair ("agg_" + attName, make_pair (AVG, toCompute)));
		return make_shared <Identifier> ("agg", attName);
	} else {
		if (extractFromMe->getLHS () != nullptr) {
			auto res = recurse (extractFromMe->getLHS (), intoMe);
			if (res != nullptr) 
				extractFromMe->getLHS () = res;
		}
		if (extractFromMe->getRHS () != nullptr) {
			auto res = recurse (extractFromMe->getRHS (), intoMe);
			if (res != nullptr) 
				extractFromMe->getRHS () = res;
		}
		if (extractFromMe->getChild () != nullptr) {
			auto res = recurse (extractFromMe->getChild (), intoMe);
			if (res != nullptr) 
				extractFromMe->getChild () = res;
		}
		return nullptr;
	}
}

// go through the SELECT clause of the query (all of the expressions to compute) and figure
// out the various aggregates tht we need to run
vector <pair <string, pair <MyDB_AggType, string>>> getAllAggs (vector <ExprTreePtr> &fromMe) {
	
	vector <pair <string, pair <MyDB_AggType, string>>> retVal;

	for (auto &a: fromMe) {
		auto res = recurse (a, retVal);
		if (res != nullptr)
			a = res;
	}
	return retVal;
}

void SFWQuery :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters, 
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters,
	LogicalOpPtr executeMe) {

	// execute the underlying query
	MyDB_TableReaderWriterPtr res = executeMe->execute (allTableReaderWriters, allBPlusReaderWriters);
	
	// first we see if we need to run an aggregation and if we do we run it
	vector <pair <string, pair <MyDB_AggType, string>>> aggs = getAllAggs (valuesToSelect);
	if (aggs.size () > 0 || groupingClauses.size () > 0) {

		// these are the groupings
		vector <string> groupings;

		// these are the aggregates
		vector <pair <MyDB_AggType, string>> aggsToCompute;

		// and the output schema
		MyDB_SchemaPtr mySchemaOut  = make_shared <MyDB_Schema> ();
		MyDB_RecordPtr rec = res->getEmptyRecord ();

		// add the atts for the grouping clauses
		for (auto &a: groupingClauses) {
			string code = a->toString ();
			MyDB_AttTypePtr type = rec->getType (code);	
			groupings.push_back (code);
			if (a->isId ()) {
				mySchemaOut->appendAtt (make_pair (a->getId (), type));	
			} else {
				mySchemaOut->appendAtt (make_pair ("att" + to_string (counter) + "_", type));	
				counter++;
			}
		}

		// add the atts for the aggs
		for (auto &a: aggs) {
			MyDB_AttTypePtr type = rec->getType (a.second.second);
			mySchemaOut->appendAtt (make_pair (a.first, type));	
			aggsToCompute.push_back (make_pair (a.second.first, a.second.second));
		}

		// create the output
		MyDB_TablePtr aggTable = make_shared <MyDB_Table> ("aggOut", "aggOut.bin", mySchemaOut);
               	MyDB_TableReaderWriterPtr aggTableOut = make_shared <MyDB_TableReaderWriter> (aggTable, res->getBufferMgr ());	

		// and run the aggregate
		Aggregate myAgg (res, aggTableOut, aggsToCompute, groupings, "bool[true]");
		myAgg.run ();
		res->getBufferMgr ()->killTable (res->getTable ());
		res = aggTableOut;
	}

	// now we compute the final output
	// get the output schema
	MyDB_SchemaPtr mySchemaOut  = make_shared <MyDB_Schema> ();
	MyDB_RecordPtr rec = res->getEmptyRecord ();

	vector <string> projections;
	vector <int> widths;
	cout << "|";
	for (auto &a: valuesToSelect) {
		string code = a->toString ();
		MyDB_AttTypePtr type = rec->getType (code);
		projections.push_back (code);
		string temp;
		if (a->isId ()) {
			mySchemaOut->appendAtt (make_pair (a->getId (), type));	
			temp = a->getId ();
		} else {
			temp = "att_temp" + to_string (counter);
			mySchemaOut->appendAtt (make_pair (temp, type));	
			counter++;
		}

		for (int i = 0; true; i++) {
			if (temp[i] == '_') {
				temp[i] = '.';
				break;
			}
		}
		cout << temp;
		if (temp.size () < 18) {
			widths.push_back (18);
			for (int i = temp.size (); i < 18; i++) 
				cout << " ";
		} else {
			widths.push_back (temp.size ());
		}
		cout << "|";
	}
	cout << "\n";

	// create the output
	MyDB_TablePtr outTable = make_shared <MyDB_Table> ("out", "out.bin", mySchemaOut);
        MyDB_TableReaderWriterPtr tableOut = make_shared <MyDB_TableReaderWriter> (outTable, res->getBufferMgr ());	
	RegularSelection temp (res, tableOut, "bool[true]", projections);
	temp.run ();

	// kill the input
	res->getBufferMgr ()->killTable (res->getTable ());

	// and print it
	{
		int num = 0;;
		MyDB_RecordPtr tempRec = tableOut->getEmptyRecord ();
		MyDB_RecordIteratorAltPtr myIter = tableOut->getIteratorAlt ();
		while (myIter->advance ()) {
			num++;
			myIter->getCurrent (tempRec);
			if (num <= 50) {
				tempRec->prettyPrint (widths);
			}
		}
		cout << "Found " << num << " records in all.\n";
	}

	// kill the output
	tableOut->getBufferMgr ()->killTable (tableOut->getTable ());
}

// splits the list of tables in half depending upon the bitstring which
pair <map <string, MyDB_TablePtr>, map <string, MyDB_TablePtr>> split (map <string, MyDB_TablePtr> input, unsigned which) {

	map <string, MyDB_TablePtr> first;
	map <string, MyDB_TablePtr> second;

	if (which + 1 >= (((unsigned) 1) << (unsigned) input.size ()))
		return make_pair (first, second);

	unsigned i = 0;
	for (auto &a: input) {
		unsigned mask = ((unsigned) 1) << i;
		if (which & mask) {
			first[a.first] = a.second;
		} else {
			second[a.first] = a.second;
		}
		i++;
	}
	return make_pair (first, second);
}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair <LogicalOpPtr, double> SFWQuery :: optimizeQueryPlan (map <string, MyDB_TablePtr> &allTables) {

	// set up the set of tables used
	map <string, MyDB_TablePtr> allTablesOut;
	for (auto &a : tablesToProcess) {
		allTablesOut[a.second] = allTables[a.first];
	}

	// get the output schema
	MyDB_SchemaPtr totSchema = make_shared <MyDB_Schema> ();
	for (auto &c: allTablesOut) {
		for (auto &b: c.second->getSchema ()->getAtts ()) {
			bool needIt = false;
			for (auto &a: valuesToSelect) {
				if (a->referencesAtt (c.first, b.first)) {
					needIt = true;
					break;			
				}
			}
			for (auto &a: groupingClauses) {
				if (a->referencesAtt (c.first, b.first)) {
					needIt = true;
					break;			
				}
			}
			if (needIt) {
				totSchema->getAtts ().push_back (make_pair (c.first + "_" + b.first, b.second)); 
			}
		}
	}

	return optimizeQueryPlan (allTablesOut, totSchema, allDisjunctions);
}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
static int name = 0;
pair <LogicalOpPtr, double> SFWQuery :: optimizeQueryPlan (map <string, MyDB_TablePtr> &allTables, 
	MyDB_SchemaPtr totSchema, vector <ExprTreePtr> &allDisjunctions) {

	// this is the output query plan
	LogicalOpPtr res = nullptr;

	// get the output table
	MyDB_TablePtr outTable = make_shared <MyDB_Table> ("tempTable" + std::to_string (name), 
		"tempTableLoc" + std::to_string (name), totSchema);
	name++;

	if (allTables.size () == 1) {

		// next get the input table
		auto it = allTables.begin(); 
		MyDB_TablePtr inTable = it->second->alias (it->first);

		// and get the out stats
		MyDB_Stats outStats (inTable);
		MyDB_StatsPtr finalStats = outStats.costSelection (allDisjunctions);
		
		// this is the query result!!
		res = make_shared <LogicalTableScan> (inTable, outTable, finalStats, allDisjunctions);
		return make_pair (res, finalStats->getTupleCount ());
	}

	// we have at least one join
	double best = 9e99;
	for (unsigned i = 1; i < 999999999; i++) {

		// split in half
		pair <map <string, MyDB_TablePtr>, map <string, MyDB_TablePtr>> mySplit = split (allTables, i);
		map <string, MyDB_TablePtr> &allTablesL = mySplit.first;
		map <string, MyDB_TablePtr> &allTablesR = mySplit.second;
		if (allTablesL.size () == 0 && allTablesR.size () == 0) {
			break;
		}
		if (allTablesL.size () + allTablesR.size () != allTables.size ()) {
			cout << "BAD!  Bad split.\n";
			exit (1);
		}
		
		// find the various parts of the CNF
		vector <ExprTreePtr> leftCNF; 
		vector <ExprTreePtr> rightCNF; 
		vector <ExprTreePtr> topCNF; 

		// loop through all of the disjunctions and break them apart
		for (auto &a: allDisjunctions) {
			bool inLeft = false;
			bool inRight = false;
			for (auto &b: allTablesL) {
				inLeft = a->referencesTable (b.first);
				if (inLeft)
					break;
			}
			for (auto &b: allTablesR) {
				inRight = a->referencesTable (b.first);
				if (inRight)
					break;
			}
			if (inLeft && inRight) {
				topCNF.push_back (a);
			} else if (inLeft) {
				leftCNF.push_back (a);
			} else {
				rightCNF.push_back (a);
			}
		}

		// now get the left and right schemas for the two selections
		MyDB_SchemaPtr leftSchema = make_shared <MyDB_Schema> ();
		MyDB_SchemaPtr rightSchema = make_shared <MyDB_Schema> ();
		
		// and see what we need from the left, and from the right
		for (auto &c: allTablesL) {
			for (auto &b: c.second->getSchema ()->getAtts ()) {
				bool needIt = false;
				for (auto &a: totSchema->getAtts ()) {
					if (a.first == c.first + "_" + b.first) {
						needIt = true;
					}
				}
				for (auto &a: topCNF) {
					if (a->referencesAtt (c.first, b.first)) {
						needIt = true;
					}
				}
				if (needIt) {
					leftSchema->getAtts ().push_back (make_pair (c.first + "_" + b.first, b.second));
				}
			}
		}


		// and see what we need from the left, and from the right
		for (auto &c: allTablesR) {
			for (auto &b: c.second->getSchema ()->getAtts ()) {
				bool needIt = false;
				for (auto &a: totSchema->getAtts ()) {
					if (a.first == c.first + "_" + b.first) {
						needIt = true;
					}
				}
				for (auto &a: topCNF) {
					if (a->referencesAtt (c.first, b.first)) {
						needIt = true;
					}
				}
				if (needIt) {
					rightSchema->getAtts ().push_back (make_pair (c.first + "_" + b.first, b.second));
				}
			}
		}

		
		// and it's time to build the query plan
		auto left = optimizeQueryPlan (allTablesL, leftSchema, leftCNF);
		if (left.second > 9e99) {
			cout << "BAD\n";
			for (auto &a : allTablesL) {
				cout << a.first << " ";
			}
			for (auto &a : leftCNF) {
				cout << a->toString () << " ";
			}
			cout << "\n";
			exit (1);
		}
		auto right = optimizeQueryPlan (allTablesR, rightSchema, rightCNF);
		MyDB_StatsPtr finalStats = left.first->getStats ()->costJoin (topCNF, right.first->getStats ());

		if (finalStats->getTupleCount () + left.second + right.second < best) {
			best =  left.second + right.second + finalStats->getTupleCount ();
			res = make_shared <LogicalJoin> (left.first, right.first, outTable, topCNF, finalStats);
		}
	}

	return make_pair (res, best);
}

void SFWQuery :: print () {
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess) {
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses) {
		cout << "\t" << a->toString () << "\n";
	}
}


SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf, struct ValueList *grouping) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions = cnf->disjunctions;
        groupingClauses = grouping->valuesToCompute;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
	allDisjunctions = cnf->disjunctions;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions.push_back (make_shared <BoolLiteral> (true));
}

#endif
