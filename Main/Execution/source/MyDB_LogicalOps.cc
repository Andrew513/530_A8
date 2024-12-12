
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
	if (allBPlusReaderWriters.find(inputSpec->getName()) != allBPlusReaderWriters.end()) {
		MyDB_BPlusTreeReaderWriterPtr bPlusReaderWriter = allBPlusReaderWriters[inputSpec->getName()];
		MyDB_BufferManagerPtr bufferMgr = bPlusReaderWriter->getBufferMgr();
		MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);

		// ...
	}

	MyDB_BufferManagerPtr bufferMgr = allTableReaderWriters[inputSpec->getName()]->getBufferMgr();
	MyDB_TableReaderWriterPtr inputTable = make_shared<MyDB_TableReaderWriter>(inputSpec, bufferMgr);

	vector<string> projections;
	for (auto &att: outputSpec->getSchema()->getAtts()) {
		projections.push_back("[" + att.first + "]");
	}

	MyDB_TableReaderWriterPtr outputTable = make_shared <MyDB_TableReaderWriter> (outputSpec, bufferMgr);
	string curPred = selectionPred[0]->toString();
	for (int i = 1; i < selectionPred.size(); i++) {
		curPred = "&& (" + curPred + ", " + selectionPred[i]->toString() + ")";
	}

	RegularSelection RS(inputTable, outputTable, curPred, projections);
	RS.run();

	return make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);
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
