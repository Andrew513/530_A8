
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
    map<string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

    // Step 1: Check if the input table exists
    if (allTableReaderWriters.find(inputSpec->getName()) == allTableReaderWriters.end()) {
        throw runtime_error("Input table not found: " + inputSpec->getName());
    }

    MyDB_BufferManagerPtr bufferMgr;
    MyDB_TableReaderWriterPtr inputTable;
    MyDB_TableReaderWriterPtr outputTable;

    // Step 2: Prepare Projections
    vector<string> projections;
    cout << "Output Table Projections: ";
    for (auto &att : outputSpec->getSchema()->getAtts()) {
        projections.push_back("[" + att.first + "]");
        cout << "[" + att.first + "] ";
    }
    cout << endl;

    // Step 3: Combine Selection Predicates
    string curPred;
    if (!selectionPred.empty()) {
        curPred = selectionPred[0]->toString();
        for (size_t i = 1; i < selectionPred.size(); i++) {
            curPred = "&& (" + curPred + ", " + selectionPred[i]->toString() + ")";
        }
    } else {
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
	
	vector<pair<string, string>> equalityChecks;
    for (auto &expr : outputSelectionPredicate) {
        string exprStr = expr->toString();
        size_t pos = exprStr.find("==");
        if (pos != string::npos) {
            string left = exprStr.substr(0, pos - 1);     // Left attribute
            string right = exprStr.substr(pos + 3);       // Right attribute
            equalityChecks.push_back(make_pair(left, right));
        }
    }

	string leftSelectionPredicate, rightSelectionPredicate;
	for (auto &expr : outputSelectionPredicate) {
        string exprStr = expr->toString();
        if (exprStr.find("==") == string::npos) {  // Exclude equality checks
            if (exprStr.find("left") != string::npos) {
                leftSelectionPredicate = exprStr;
            } else if (exprStr.find("right") != string::npos) {
                rightSelectionPredicate = exprStr;
            }
        }
    }

	string finalSelectionPredicate = "&& (&& (< ([l_l_commitdate], [l_l_receiptdate]), < ([l_l_shipdate], [l_l_commitdate])))";
	// for (auto &expr : outputSelectionPredicate) {
	// 	string exprStr = expr->toString();
	// 	if (exprStr.find("==") == string::npos && exprStr.find("left.") != string::npos && exprStr.find("right.") != string::npos) {
	// 		// Condition involves attributes from both tables → Final Selection Predicate
	// 		finalSelectionPredicate = exprStr;
	// 		break; // Assuming one final selection predicate for simplicity
	// 	}
	// }
	cout << "Final Selection Predicate: " << finalSelectionPredicate << endl;
	
	// If the tables are too large to fit in memory, use SortMergeJoin
	if (min(leftTableSize, rightTableSize) > bufferPages / 2) {
        SortMergeJoin join(leftTableRW, rightTableRW, outputTable, finalSelectionPredicate, projections, equalityChecks[0], leftSelectionPredicate, rightSelectionPredicate);
        join.run();
    } else {
        ScanJoin join(leftTableRW, rightTableRW, outputTable, finalSelectionPredicate, projections, equalityChecks, leftSelectionPredicate, rightSelectionPredicate);
        join.run();
    }

	leftTableRW->getBufferMgr()->killTable(outputTable->getTable());
	rightTableRW->getBufferMgr()->killTable(outputTable->getTable());

	return outputTable;
}

#endif
