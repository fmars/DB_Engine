#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

typedef struct {
	int tableId;
	string columnName;
	AttrType columnType;
	unsigned int columnLength;
	int num;
} ColumnsTableEntry;

typedef struct {
	int tableId;
	string tableName;
	string fileName;
	vector<ColumnsTableEntry> columns;
} TablesTableEntry;

typedef struct {
	map<string, string> attrMap;
} IndexesTableEntry;

// Basics of the catalog tables
#define TABLES_TABLE_NAME ("Tables")
#define COLUMNS_TABLE_NAME ("Columns")
#define TABLES_FILE_NAME TABLES_TABLE_NAME
#define COLUMNS_FILE_NAME COLUMNS_TABLE_NAME
#define TABLES_TABLE_ID (0)
#define COLUMNS_TABLE_ID (1)

#define TABLE_ID_ATTR_NAME ("table-id")
#define TABLE_NAME_ATTR_NAME ("table-name")
#define FILE_NAME_ATTR_NAME ("file-name")
#define COLUMN_NAME_ATTR_NAME ("column-name")
#define COLUMN_TYPE_ATTR_NAME ("column-type")
#define COLUMN_LENGTH_ATTR_NAME ("column-length")
#define COLUMN_NUM_ATTR_NAME ("num")

#define TABLE_NAME_LIMIT (PAGE_SIZE / 16)
#define FILE_NAME_LIMIT (PAGE_SIZE / 16)
#define COLUMN_NAME_LIMIT (PAGE_SIZE / 16)

// Basics of the Indexes table
#define INDEXES_TABLE_NAME ("Indexes")
#define INDEX_ATTR_ATTR_NAME ("index-attr")
#define INDEX_FILE_ATTR_NAME ("index-file")

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

//class RM_ScanIterator {
class RM_ScanIterator: public RBFM_ScanIterator {
public:
	RM_ScanIterator();
	~RM_ScanIterator();

	// "data" follows the same format as RelationManager::insertTuple()
	RC getNextTuple(RID &rid, void *data);
	RC close();

	RC setupIterator(const string& fileName,
			const vector<Attribute>& recordDescriptor,
			const string& conditionAttribute, const CompOp& compOp,
			const void* value, const vector<string>& attributeNames);
};

class RM_IndexScanIterator: public IX_ScanIterator {
public:
	RM_IndexScanIterator();  	// Constructor
	~RM_IndexScanIterator(); 	// Destructor

	// "key" follows the same format as in IndexManager::insertEntry()
	RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
	RC close();             			// Terminate index scan

	RC setupIterator(const string& fileName, const Attribute& attribute,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive);
};

// Relation Manager
class RelationManager {
public:
	static RelationManager* instance();

	RC createTable(const string &tableName, const vector<Attribute> &attrs);

	RC deleteTable(const string &tableName);

	RC getAttributes(const string &tableName, vector<Attribute> &attrs);

	RC insertTuple(const string &tableName, const void *data, RID &rid);

	RC deleteTuples(const string &tableName);

	RC deleteTuple(const string &tableName, const RID &rid);

	// Assume the rid does not change after update
	RC updateTuple(const string &tableName, const void *data, const RID &rid);

	RC readTuple(const string &tableName, const RID &rid, void *data);

	RC readAttribute(const string &tableName, const RID &rid,
			const string &attributeName, void *data);

	RC reorganizePage(const string &tableName, const unsigned pageNumber);

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(const string &tableName, const string &conditionAttribute,
			const CompOp compOp,         // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RM_ScanIterator &rm_ScanIterator);

	RC createIndex(const string &tableName, const string &attributeName);

	RC destroyIndex(const string &tableName, const string &attributeName);

	// indexScan returns an iterator to allow the caller to go through qualified entries in index
	RC indexScan(const string &tableName, const string &attributeName,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator);

// Extra credit
public:
	RC dropAttribute(const string &tableName, const string &attributeName);

	RC addAttribute(const string &tableName, const Attribute &attr);

	RC reorganizeTable(const string &tableName);

protected:
	RelationManager();
	~RelationManager();

private:
	static RelationManager *_rm;

private:
	const static int TYPE_INT_MINIMUM;
	const static int TYPE_INT_MAXIMUM;
	const static float TYPE_REAL_NINF;
	const static float TYPE_REAL_PINF;
	const static char TYPE_VAR_CHAR_N[4];
	const static char TYPE_VAR_CHAR_P[5];

	/**********************auxiliary method*********************/
private:
	map<string, TablesTableEntry> tablesTable;

	RC getFileName(const string &tableName, string &fileName);
	RC getTableId(const string &tableName, int &tableID);
	RC openFile(const string &tableName, FileHandle &fileHandle);
	RC closeFile(FileHandle& fileHandle);

	void writeDataFormat(const vector<Attribute>& recordDescriptor,
			const void *dataIn, void * const dataOut);

	// Indexes table kept in memory at runtime
	map<string, IndexesTableEntry> indexesTable;

	RC updateIndexesTuple(const string& tableName, const RID& rid,
			const bool& opFlag);
	RC updateIndexesInsertTuple(const string& tableName, const RID& rid);
	RC updateIndexesDeleteTuple(const string& tableName, const RID& rid);
};

#endif
