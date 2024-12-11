
#ifndef LOG_OP_H
#define LOG_OP_H

#include "MyDB_Stats.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "ExprTree.h"


// create a smart pointer for database tables
using namespace std;
class LogicalOp;
class LogicalTableScan;
typedef shared_ptr <LogicalOp> LogicalOpPtr;

// this is a pure virtual class that corresponds to a particular relational operation to be run
class LogicalOp {

public:

	// The MyDB_StatsPtr will point to the set of statistics that come out of executing this plan
	virtual MyDB_StatsPtr getStats () = 0;

	// execute the plan rooted at this node
	virtual MyDB_TableReaderWriterPtr execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
		map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) = 0;

	// print the logical op, including the entire tree
	void print () {
		vector <MyDB_TablePtr> outputs;
		print (0, outputs);
		cout << "\nAll tables created:\n";
		for (auto &a: outputs) {
			cout << "Table: " << a << "\n";
		}
	}

	// implemented by each subclass, to print
	virtual void print (int depth, vector <MyDB_TablePtr> &outputs) = 0;

	// return the output table info
	virtual MyDB_TablePtr getOutputTable () = 0;

	// lets us know if this is a table scan
	virtual bool isTableScan () {return false;}

	// returns a table scan or a null ptr if not possible
	virtual LogicalTableScan *toTableScan () {return nullptr;}
	
	virtual ~LogicalOp () {}
};

// a logical aggregation operation--in practice, this is going to be implemented using an Aggregate operation,
// followed by a RegularSelection operation to de-scramble the output attributes (since the Aggregate always has
// the grouping atts first, followed by the aggregates, and this might not be the order that is given in 
// exprsToCompute)... to be added in A8
class LogicalAggregate : public LogicalOp {

public: 

private:

};
	
// a logical join operation---will be implemented with a ScanJoin or a SortMergeJoin
class LogicalJoin  : public LogicalOp {

public:

	//
	// leftInputOp: this is the input operation that we are reading from on the left
	// rightInputOp: this is the input operation that we are reading from on the left
	// outputSpec: this is the table that we are going to create by running the operation
	// outputSelectionPredicate: the selection predicates to execute using the join
	// outputStats: the satistics describing the relaation created by this join
	//	
	LogicalJoin (LogicalOpPtr leftInputOp, LogicalOpPtr rightInputOp, MyDB_TablePtr outputSpec,
		vector <ExprTreePtr> &outputSelectionPredicate, MyDB_StatsPtr outputStats) : leftInputOp (leftInputOp),
		rightInputOp (rightInputOp), outputSpec (outputSpec), outputSelectionPredicate (outputSelectionPredicate),
		outputStats (outputStats) {}
			
	virtual MyDB_TableReaderWriterPtr execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
		map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) override;

	// print this tree out
	void print (int depth, vector <MyDB_TablePtr> &outputs) override {

		for (int i = 0; i < depth; i++) cout << "  ";
		cout << "****** JOIN returning " << outputStats->getTupleCount () << " tuples.\n";

		for (int i = 0; i < depth; i++) cout << "  ";
		cout << "  ** Output table: " << outputSpec->getName () << "\n";

		for (int i = 0; i < depth; i++) cout << "  ";
		cout << "  ** Predicates:\n";

		for (auto &a: outputSelectionPredicate) {
			for (int i = 0; i < depth; i++) cout << "  ";
			cout << "    " << a->toString () << "\n";
		}

		for (int i = 0; i < depth; i++) cout << "  ";
		cout << "  ** Left tree:\n";
		leftInputOp->print (depth + 1, outputs);

		for (int i = 0; i < depth; i++) cout << "  ";
		cout << "  ** Right tree:\n";
		rightInputOp->print (depth + 1, outputs);

		outputs.push_back (outputSpec);
	}

	MyDB_StatsPtr getStats () override {return outputStats;}

	MyDB_TablePtr getOutputTable () override {return outputSpec;}

private:

	LogicalOpPtr leftInputOp;
	LogicalOpPtr rightInputOp;
	MyDB_TablePtr outputSpec;
	vector <ExprTreePtr> outputSelectionPredicate;
	MyDB_StatsPtr outputStats;

};

// a logical table scan operation---will be implemented with a BPlusSelection or a RegularSelection... note that
// we assume that we only operate table scans over base tables, and not tables that are created as the result of
// running another logical operation
class LogicalTableScan : public LogicalOp {

public:

	//
	// inputSpec: this is the input table that we are operating over 
	// outputSpec: this is the table that we are going to create by running the operation
	// outputStats: the complete set of output statistics
	// selectionPred: the selection predicates to execute while we scan the input
	//
	LogicalTableScan (MyDB_TablePtr inputSpec, MyDB_TablePtr outputSpec, MyDB_StatsPtr outputStats, 
		vector <ExprTreePtr> &selectionPred) : inputSpec (inputSpec), outputSpec (outputSpec),
		outputStats (outputStats), selectionPred (selectionPred) {}

	virtual MyDB_TableReaderWriterPtr execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
		map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) override;

	// get the input table name
	string getInputTableName () {
		return inputSpec->getName ();
	}

	// get the input table
	MyDB_TablePtr getInputTable () {
		return inputSpec;
	}

	// lets us know if this is a table scan
	virtual bool isTableScan () override {
		return true;
	}

	// returns a table scan or a null ptr if not possible
	virtual LogicalTableScan *toTableScan () override {
		return this;
	}

	vector <ExprTreePtr> &getOutputSelectionPredicate () {
		return selectionPred;
	}
	
	// print this tree out
	void print (int depth, vector <MyDB_TablePtr> &outputs) override {

		for (int i = 0; i < depth; i++) cout << "  ";
		cout << "****** TABLE SCAN returning " << outputStats->getTupleCount () << " tuples.\n";

		for (int i = 0; i < depth; i++) cout << "  ";
		cout << "  ** Output table: " << outputSpec->getName () << "\n";

		for (int i = 0; i < depth; i++) cout << "  ";
		cout << "  ** Input table: " << inputSpec->getName () << "\n";

		for (int i = 0; i < depth; i++) cout << "  ";
		cout << "  ** Predicates:\n";

		for (auto &a: selectionPred) {
			for (int i = 0; i < depth; i++) cout << "  ";
			cout << "    " << a->toString () << "\n";
		}

		outputs.push_back (outputSpec);
		outputs.push_back (inputSpec);
	}
	
	MyDB_StatsPtr getStats () override {return outputStats;}

	MyDB_TablePtr getOutputTable () override {return outputSpec;}

private:

	MyDB_TablePtr inputSpec;
	MyDB_TablePtr outputSpec;
	MyDB_StatsPtr outputStats;
    vector <ExprTreePtr> selectionPred;
};

#endif
