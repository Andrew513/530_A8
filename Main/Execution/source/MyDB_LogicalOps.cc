
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "Aggregate.h"
#include "BPlusSelection.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "SortMergeJoin.h"

MyDB_TableReaderWriterPtr LogicalTableScan :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	// your code here!

	string tableName = inputSpec->getName();
	MyDB_TableReaderWriterPtr tableRW = allTableReaderWriters[tableName];
	MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(tableRW->getTable(), tableRW->getBufferMgr());

	// If a B+ Tree index exists, use it for a fast scan (optional)
	if (allBPlusReaderWriters.count(tableName) > 0) {
		MyDB_BPlusTreeReaderWriterPtr bPlusTree = allBPlusReaderWriters[tableName];
		BPlusSelection selection(bPlusTree, outputTable, nullptr, nullptr, "", {});
		selection.run();
	} else {
		// Otherwise, use a regular scan
		RegularSelection selection(tableRW, outputTable, "", {});
		selection.run();
	}
	tableRW->getBufferMgr()->killTable(outputTable->getTable());
	return outputTable;
}

MyDB_TableReaderWriterPtr LogicalJoin :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	// your code here!
	MyDB_TableReaderWriterPtr leftTableRW = leftInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
	MyDB_TableReaderWriterPtr rightTableRW = rightInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
	MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, leftTableRW->getBufferMgr());

	size_t leftTableSize = leftTableRW->getTable()->getTupleCount();
	size_t rightTableSize = rightTableRW->getTable()->getTupleCount();
	size_t bufferPages = leftTableRW->getBufferMgr()->getNumPages();

	vector<string> projections;
	for (auto &expr : outputSelectionPredicate) {
		projections.push_back(expr->toString());
	}
	auto eqalityChecks = vector<pair<string, string>>();
	
	// If the tables are too large to fit in memory, use SortMergeJoin
	if (min(leftTableSize, rightTableSize) > bufferPages / 2) {
        SortMergeJoin join(leftTableRW, rightTableRW, outputTable, "", projections, {}, "", "");
        join.run();
    } else {
        ScanJoin join(leftTableRW, rightTableRW, outputTable, "", projections, {}, "", "");
        join.run();
    }

	leftTableRW->getBufferMgr()->killTable(outputTable->getTable());
	rightTableRW->getBufferMgr()->killTable(outputTable->getTable());

	return outputTable;
}

#endif
