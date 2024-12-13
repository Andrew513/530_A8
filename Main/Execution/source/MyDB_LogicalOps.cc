
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "Aggregate.h"
#include "BPlusSelection.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "SortMergeJoin.h"

MyDB_TableReaderWriterPtr LogicalTableScan::execute(
	map<string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map<string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters)
{

	// Step 1: Check if the input table exists
	if (allTableReaderWriters.find(inputSpec->getName()) == allTableReaderWriters.end())
	{
		throw runtime_error("Input table not found: " + inputSpec->getName());
	}

	MyDB_BufferManagerPtr bufferMgr;
	MyDB_TableReaderWriterPtr inputTable;
	MyDB_TableReaderWriterPtr outputTable;

	// Step 2: Prepare Projections
	vector<string> projections;
	cout << "Output Table Projections: ";
	for (auto &att : outputSpec->getSchema()->getAtts())
	{
		projections.push_back("[" + att.first + "]");
		cout << "[" + att.first + "] ";
	}
	cout << endl;

	// Step 3: Combine Selection Predicates
	string curPred;
	if (!selectionPred.empty())
	{
		curPred = selectionPred[0]->toString();
		for (size_t i = 1; i < selectionPred.size(); i++)
		{
			curPred = "&& (" + curPred + ", " + selectionPred[i]->toString() + ")";
		}
	}
	else
	{
		curPred = ""; // No filtering condition
	}
	cout << "Generated Predicate: " << curPred << endl;

	// "&& (&& (== ([l_l_shipdate], string[1994-05-12]), == ([l_l_commitdate], string[1994-05-22])), == ([l_l_receiptdate], string[1994-06-10]))"

	// Step 4: B+ Tree Optimization Check
	// if (allBPlusReaderWriters.find(inputSpec->getName()) != allBPlusReaderWriters.end()) {
	//     MyDB_BPlusTreeReaderWriterPtr bPlusReaderWriter = allBPlusReaderWriters[inputSpec->getName()];
	//     bufferMgr = bPlusReaderWriter->getBufferMgr();
	//     outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);

	//     MyDB_AttValPtr low = nullptr;
	//     MyDB_AttValPtr high = nullptr;

	//     BPlusSelection bPlusSel(bPlusReaderWriter, outputTable, low, high, curPred, projections);
	//     bPlusSel.run();

	//     cout << "B+ Tree Selection Complete." << endl;
	//     return outputTable;
	// }

	// Step 5: Fallback to Full Table Scan
	cout << "Using Regular Selection (Full Table Scan)" << endl;
	bufferMgr = allTableReaderWriters[inputSpec->getName()]->getBufferMgr();
	inputTable = make_shared<MyDB_TableReaderWriter>(inputSpec, bufferMgr);
	outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, bufferMgr);

	// Debug: Check Input Table Content
	// cout << "Input Table Content: " << endl;
	// MyDB_RecordIteratorAltPtr inputIter = inputTable->getIteratorAlt();
	// MyDB_RecordPtr tempRec = inputTable->getEmptyRecord();

	// Run RegularSelection
	RegularSelection RS(inputTable, outputTable, curPred, projections);
	RS.run();

	return outputTable;
}

MyDB_TableReaderWriterPtr LogicalJoin ::execute(map<string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
												map<string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters)
{

	string leftSelectionPredicate = "bool[true]", rightSelectionPredicate = "bool[true]";
	string leftTableName = "leftTable", rightTableName = "rightTable";
	MyDB_TableReaderWriterPtr leftTableRW, rightTableRW;
	if (leftInputOp->isTableScan()) {
		leftTableName = leftInputOp->toTableScan()->getInputTableName();
		// cout << leftTableName << " is table scan" << endl;
		if (leftInputOp->toTableScan()->getOutputSelectionPredicate().size() > 0)
		{
			leftSelectionPredicate = leftInputOp->toTableScan()->getOutputSelectionPredicate()[0]->toString();
			for (int i = 1; i < leftInputOp->toTableScan()->getOutputSelectionPredicate().size(); i++)
			{
				leftSelectionPredicate = "&& (" + leftSelectionPredicate + ", " + leftInputOp->toTableScan()->getOutputSelectionPredicate()[i]->toString() + ")";
			}
		}
		leftTableRW = make_shared<MyDB_TableReaderWriter>(leftInputOp->toTableScan()->getInputTable(), allTableReaderWriters[leftInputOp->toTableScan()->getInputTable()->getName()]->getBufferMgr());
	}
	else {
		// cout << "leftInputOp is not table scan" << endl;
		leftTableRW = leftInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
	}

	if (rightInputOp->isTableScan()) {
		rightTableName = rightInputOp->toTableScan()->getInputTableName();
		// cout << rightTableName << " is table scan" << endl;
		if (rightInputOp->toTableScan()->getOutputSelectionPredicate().size() > 0)
		{
			rightSelectionPredicate = rightInputOp->toTableScan()->getOutputSelectionPredicate()[0]->toString();
			for (int i = 1; i < rightInputOp->toTableScan()->getOutputSelectionPredicate().size(); i++)
			{
				rightSelectionPredicate = "&& (" + rightSelectionPredicate + ", " + rightInputOp->toTableScan()->getOutputSelectionPredicate()[i]->toString() + ")";
			}
		}
		rightTableRW = make_shared<MyDB_TableReaderWriter>(rightInputOp->toTableScan()->getInputTable(), allTableReaderWriters[rightInputOp->toTableScan()->getInputTable()->getName()]->getBufferMgr());
	}
	else {
		// cout << "rightInputOp is not table scan" << endl;
		rightTableRW = rightInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
	}
	// cout << "Join " << leftTableName << " " << rightTableName << endl;
	MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, leftTableRW->getBufferMgr());

	size_t leftTableSize = leftTableRW->getTable()->lastPage() + 1;
	size_t rightTableSize = rightTableRW->getTable()->lastPage() + 1;
	size_t bufferPages = leftTableRW->getBufferMgr()->getNumPages();

	string finalSelectionPredicate = "bool[true]";
	if (outputSelectionPredicate.size() > 0) {
		finalSelectionPredicate = outputSelectionPredicate[0]->toString();
		for (int i = 1; i < outputSelectionPredicate.size(); i++)
		{
			finalSelectionPredicate = "&& (" + finalSelectionPredicate + ", " + outputSelectionPredicate[i]->toString() + ")";
		}
	}

	vector<string> projections;
	for (auto &att : outputSpec->getSchema()->getAtts()) {
		projections.push_back("[" + att.first + "]");
	}

	auto equalityChecks = vector<pair<string, string>>();
	for (auto &pred : outputSelectionPredicate) {
		if (pred->isEq()) {
			if (leftTableRW->getTable()->getSchema()->getAttByName(pred->getLHS()->getId()).second != nullptr)
			{
				equalityChecks.push_back(make_pair("[" + pred->getLHS()->getId() + "]", "[" + pred->getRHS()->getId() + "]"));
			}
			else
			{
				equalityChecks.push_back(make_pair("[" + pred->getRHS()->getId() + "]", "[" + pred->getLHS()->getId() + "]"));
			}
		}
	}
	cout << "Equality Checks: ";
	for (auto &eq : equalityChecks)
	{
		cout << "eq: first: " << eq.first << " eq.second: " << eq.second <<endl;
	}

	cout<<"min pages: "<<min(leftTableSize, rightTableSize)<<endl;
	// If the tables are too large to fit in memory, use SortMergeJoin
	if (min(leftTableSize, rightTableSize) > bufferPages / 2) {
		// cout << "Using SortMergeJoin: " << leftTableName << " " << rightTableName << endl;
		SortMergeJoin join(leftTableRW, rightTableRW, outputTable, finalSelectionPredicate, projections, equalityChecks.empty() ? make_pair("", "") : equalityChecks[0], leftSelectionPredicate, rightSelectionPredicate);
		join.run();
		// cout << "Done SortMergeJoin: " << leftTableName << " " << rightTableName << endl;
	}
	else {
		// cout << "Using ScanJoin: " << leftTableName << " " << rightTableName << endl;
		ScanJoin join(leftTableRW, rightTableRW, outputTable, finalSelectionPredicate, projections, equalityChecks, leftSelectionPredicate, rightSelectionPredicate);
		join.run();
		// cout << "Done ScanJoin: " << leftTableName << " " << rightTableName << endl;
	}

	// leftTableRW->getBufferMgr()->killTable(leftTableRW->getTable());
	// rightTableRW->getBufferMgr()->killTable(rightTableRW->getTable());

	return outputTable;
}

#endif
