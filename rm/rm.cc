#include <cstring>
#include <cstdio>
#include <climits>
#include <limits>

#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance() {
	if (!_rm)
		_rm = new RelationManager();

	return _rm;
}

static void buildTablesRec(char* data, const int tableID,
		const string tableName, const string fileName);
static void buildColumnsRec(char* data, const int tableID,
		const Attribute column, const int columnNum);

RelationManager::RelationManager() {
	FileHandle handleTables;
	FileHandle handleColumns;
	RecordBasedFileManager* pRBFManager = RecordBasedFileManager::instance();
	unsigned char flagNotExist = 0;
	Attribute attrTableID = { TABLE_ID_ATTR_NAME, TypeInt, sizeof(int) };
	Attribute attrTableName = { TABLE_NAME_ATTR_NAME, TypeVarChar,
	TABLE_NAME_LIMIT };
	Attribute attrFileName =
			{ FILE_NAME_ATTR_NAME, TypeVarChar, FILE_NAME_LIMIT };
	Attribute attrColumnName = { COLUMN_NAME_ATTR_NAME, TypeVarChar,
	COLUMN_NAME_LIMIT };
	Attribute attrColumnType = { COLUMN_TYPE_ATTR_NAME, TypeInt, sizeof(int) };
	Attribute attrColumnLength =
			{ COLUMN_LENGTH_ATTR_NAME, TypeInt, sizeof(int) };
	Attribute attrColumnNum = { COLUMN_NUM_ATTR_NAME, TypeInt, sizeof(int) };
	vector<Attribute> recDescTables;
	vector<Attribute> recDescColumns;

	recDescTables.push_back(attrTableID);
	recDescTables.push_back(attrTableName);
	recDescTables.push_back(attrFileName);
	recDescColumns.push_back(attrTableID);
	recDescColumns.push_back(attrColumnName);
	recDescColumns.push_back(attrColumnType);
	recDescColumns.push_back(attrColumnLength);
	recDescColumns.push_back(attrColumnNum);

	if (pRBFManager->openFile(TABLES_FILE_NAME, handleTables) < 0)
		flagNotExist |= 1;
	if (pRBFManager->openFile(COLUMNS_FILE_NAME, handleColumns) < 0)
		flagNotExist |= 2;
	if ((flagNotExist & 1) > 0 || (flagNotExist & 2) > 0) {
		if ((flagNotExist & 1) == 0) {
			pRBFManager->closeFile(handleTables);
			pRBFManager->destroyFile(TABLES_FILE_NAME);
		} // If Tables exists but Columns does NOT
		if ((flagNotExist & 2) == 0) {
			pRBFManager->closeFile(handleColumns);
			pRBFManager->destroyFile(COLUMNS_FILE_NAME);
		} // If Columns exists but Tables does NOT

		char recData[PAGE_SIZE];
		RID ridSink;

		// To create the catalog tables
		// To create Tables
		pRBFManager->createFile(TABLES_FILE_NAME);
		pRBFManager->openFile(TABLES_FILE_NAME, handleTables);
		handleTables.setSystemFlag(true);
		buildTablesRec(recData,
		TABLES_TABLE_ID, string(TABLES_TABLE_NAME), string(TABLES_FILE_NAME));
		pRBFManager->insertRecord(handleTables, recDescTables, recData,
				ridSink);
		buildTablesRec(recData,
		COLUMNS_TABLE_ID, string(COLUMNS_TABLE_NAME),
				string(COLUMNS_FILE_NAME));
		pRBFManager->insertRecord(handleTables, recDescTables, recData,
				ridSink);

		// To create Columns
		pRBFManager->createFile(COLUMNS_FILE_NAME);
		pRBFManager->openFile(COLUMNS_FILE_NAME, handleColumns);
		handleColumns.setSystemFlag(true);
		buildColumnsRec(recData, TABLES_TABLE_ID, attrTableID, 0);
		pRBFManager->insertRecord(handleColumns, recDescColumns, recData,
				ridSink);
		buildColumnsRec(recData, TABLES_TABLE_ID, attrTableName, 1);
		pRBFManager->insertRecord(handleColumns, recDescColumns, recData,
				ridSink);
		buildColumnsRec(recData, TABLES_TABLE_ID, attrFileName, 2);
		pRBFManager->insertRecord(handleColumns, recDescColumns, recData,
				ridSink);
		buildColumnsRec(recData, COLUMNS_TABLE_ID, attrTableID, 0);
		pRBFManager->insertRecord(handleColumns, recDescColumns, recData,
				ridSink);
		buildColumnsRec(recData, COLUMNS_TABLE_ID, attrColumnName, 1);
		pRBFManager->insertRecord(handleColumns, recDescColumns, recData,
				ridSink);
		buildColumnsRec(recData, COLUMNS_TABLE_ID, attrColumnType, 2);
		pRBFManager->insertRecord(handleColumns, recDescColumns, recData,
				ridSink);
		buildColumnsRec(recData, COLUMNS_TABLE_ID, attrColumnLength, 3);
		pRBFManager->insertRecord(handleColumns, recDescColumns, recData,
				ridSink);
		buildColumnsRec(recData, COLUMNS_TABLE_ID, attrColumnNum, 4);
		pRBFManager->insertRecord(handleColumns, recDescColumns, recData,
				ridSink);
	}

	RBFM_ScanIterator it;
	RID rid;
	char recDataSink[PAGE_SIZE];
	char attrData[PAGE_SIZE];

	// Read records from Tables
	vector<string> attrStrTables;
	map<int, string> tablesTableIndex;

	attrStrTables.push_back(attrTableID.name);
	attrStrTables.push_back(attrTableName.name);
	attrStrTables.push_back(attrFileName.name);
	pRBFManager->scan(handleTables, recDescTables, "", NO_OP, NULL,
			attrStrTables, it);
	while (it.getNextRecord(rid, recDataSink) != RBFM_EOF) {
		TablesTableEntry entry;
		pRBFManager->readAttribute(handleTables, recDescTables, rid,
				attrTableID.name, attrData);
		entry.tableId = *(int*) attrData;
		pRBFManager->readAttribute(handleTables, recDescTables, rid,
				attrTableName.name, attrData);
		attrData[*(int*) attrData + sizeof(int)] = '\0';
		entry.tableName = string(attrData + sizeof(int));
		pRBFManager->readAttribute(handleTables, recDescTables, rid,
				attrFileName.name, attrData);
		attrData[*(int*) attrData + sizeof(int)] = '\0';
		entry.fileName = string(attrData + sizeof(int));
		tablesTable[entry.tableName] = entry;
		tablesTableIndex[entry.tableId] = entry.tableName;
	}
	it.close();

	// Read records from Columns
	vector<string> attrStrColumns;

	attrStrColumns.push_back(attrTableID.name);
	attrStrColumns.push_back(attrColumnName.name);
	attrStrColumns.push_back(attrColumnType.name);
	attrStrColumns.push_back(attrColumnLength.name);
	pRBFManager->scan(handleColumns, recDescColumns, "", NO_OP, NULL,
			attrStrColumns, it);
	while (it.getNextRecord(rid, recDataSink) != RBFM_EOF) {
		ColumnsTableEntry entry;

		pRBFManager->readAttribute(handleColumns, recDescColumns, rid,
				attrTableID.name, attrData);
		entry.tableId = *(int*) attrData;
		pRBFManager->readAttribute(handleColumns, recDescColumns, rid,
				attrColumnName.name, attrData);
		attrData[*(int*) attrData + sizeof(int)] = '\0';
		entry.columnName = string(attrData + sizeof(int));
		pRBFManager->readAttribute(handleColumns, recDescColumns, rid,
				attrColumnType.name, attrData);
		entry.columnType = *(AttrType*) attrData;
		pRBFManager->readAttribute(handleColumns, recDescColumns, rid,
				attrColumnLength.name, attrData);
		entry.columnLength = *(int*) attrData;
		pRBFManager->readAttribute(handleColumns, recDescColumns, rid,
				attrColumnNum.name, attrData);
		entry.num = *(int*) attrData;
		tablesTable[tablesTableIndex[entry.tableId]].columns.push_back(entry);
	}
	it.close();

	pRBFManager->closeFile(handleTables);
	pRBFManager->closeFile(handleColumns);

	// Attempt to open Indexes and read
	FileHandle handleIndexes;
	vector<Attribute> recDescIndexes;
	Attribute attrIndexAttr = { INDEX_ATTR_ATTR_NAME, TypeVarChar,
	COLUMN_NAME_LIMIT };
	Attribute attrIndexFile = { INDEX_FILE_ATTR_NAME, TypeVarChar,
	FILE_NAME_LIMIT };

	recDescIndexes.push_back(attrTableID);
	recDescIndexes.push_back(attrIndexAttr);
	recDescIndexes.push_back(attrIndexFile);

	if (this->openFile(string(INDEXES_TABLE_NAME), handleIndexes) < 0) {
		// Failed to open Indexes, now attempt to create
		this->createTable(string(INDEXES_TABLE_NAME), recDescIndexes);
		this->openFile(string(INDEXES_TABLE_NAME), handleIndexes);
	}
	this->closeFile(handleIndexes);

	RM_ScanIterator rmIt;
	vector<string> attrStrIndexes;

	attrStrIndexes.push_back(attrTableID.name);
	attrStrIndexes.push_back(attrIndexAttr.name);
	attrStrIndexes.push_back(attrIndexFile.name);

	// Read records from Indexes
	this->scan(string(INDEXES_TABLE_NAME), "", NO_OP, NULL, attrStrIndexes,
			rmIt);
	while (rmIt.getNextTuple(rid, recDataSink) != RM_EOF) {
		int indexedTableID;
		string indexAttr;
		string indexFile;

		// Read a tuple from Indexes
		this->readAttribute(INDEXES_TABLE_NAME, rid, attrTableID.name,
				attrData);
		indexedTableID = *(int*) attrData;
		this->readAttribute(INDEXES_TABLE_NAME, rid, attrIndexAttr.name,
				attrData);
		attrData[*(int*) attrData + sizeof(int)] = '\0';
		indexAttr = string(attrData + sizeof(int));
		this->readAttribute(INDEXES_TABLE_NAME, rid, attrIndexFile.name,
				attrData);
		attrData[*(int*) attrData + sizeof(int)] = '\0';
		indexFile = string(attrData + sizeof(int));

		string tableName = tablesTableIndex[indexedTableID];

		if (indexesTable.count(tableName) < 1) { // If this table has NO record yet
			IndexesTableEntry entry;

			indexesTable[tableName] = entry;
		} // Insert an entry for this table
		  // Add the indexed attribute to the corresponding table's record
		indexesTable[tableName].attrMap[indexAttr] = indexFile;
	}
	rmIt.close();
}

static void buildTablesRec(char* data, const int tableID,
		const string tableName, const string fileName) {
	*(int*) data = tableID;
	data += sizeof(int);
	*(int*) data = tableName.length();
	data += sizeof(int);
	strcpy(data, tableName.c_str());
	data += tableName.length();
	*(int*) data = fileName.length();
	data += sizeof(int);
	strcpy(data, fileName.c_str());
}

static void buildColumnsRec(char* data, const int tableID,
		const Attribute column, const int columnNum) {
	*(int*) data = tableID;
	data += sizeof(int);
	*(int*) data = column.name.length();
	data += sizeof(int);
	strcpy(data, column.name.c_str());
	data += column.name.length();
	*(int*) data = column.type;
	data += sizeof(int);
	*(int*) data = column.length;
	data += sizeof(int);
	*(int*) data = columnNum;
}

RelationManager::~RelationManager() {
}

RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> &attrs) {
	if (tablesTable.count(tableName) > 0)
		return -1;
	if (RecordBasedFileManager::instance()->createFile(tableName) < 0)
		return -1;

	map<string, TablesTableEntry>::iterator it;
	vector<int> existingTableID;

	for (it = tablesTable.begin(); it != tablesTable.end(); ++it)
		existingTableID.push_back(it->second.tableId);
	for (unsigned int j = existingTableID.size() - 1; j > 0; --j) {
		bool flagExch = false;
		for (unsigned int k = 0; k < j; ++k) {
			if (existingTableID.at(k) > existingTableID.at(k + 1)) {
				int t = existingTableID.at(k);

				existingTableID.at(k) = existingTableID.at(k + 1);
				existingTableID.at(k + 1) = t;
				flagExch = true;
			}
		}
		if (!flagExch)
			break;
	}

	unsigned int k;
	TablesTableEntry tablesTableEntry;

	for (k = 0; k < existingTableID.size() - 1; ++k)
		if (existingTableID.at(k) + 1 < existingTableID.at(k + 1))
			break;
	tablesTableEntry.tableId = existingTableID.at(k) + 1;
	tablesTableEntry.tableName = tableName;
	tablesTableEntry.fileName = tableName;
	tablesTable[tableName] = tablesTableEntry;

	char recData[PAGE_SIZE];
	RID ridSink;

	buildTablesRec(recData, tablesTableEntry.tableId,
			tablesTableEntry.tableName, tablesTableEntry.fileName);
	insertTuple(string(TABLES_TABLE_NAME), recData, ridSink);

	for (unsigned int k = 0; k < attrs.size(); ++k)
		addAttribute(tableName, attrs.at(k));
	return 0;
}

RC RelationManager::deleteTable(const string &tableName) {
//change tables table on disk
	RM_ScanIterator rm_ScanIterator;
	string table = TABLES_TABLE_NAME;
	string conditionAttribute = TABLE_NAME_ATTR_NAME;
	CompOp comOp = EQ_OP;
	vector<string> attributeNames;
	if (scan(table, conditionAttribute, comOp, (void*) tableName.c_str(),
			attributeNames, rm_ScanIterator) != 0)
		return -1;
	RID rid;
	char data[PAGE_SIZE];
	if (rm_ScanIterator.getNextTuple(rid, data) != 0)
		return -1;
	if (deleteTuple(table, rid) != 0)
		return -1;
	rm_ScanIterator.close();

//change columns table in disk
	int tableId;
	if (getTableId(tableName, tableId) != 0)
		return -1;
	table = COLUMNS_TABLE_NAME;
	conditionAttribute = TABLE_ID_ATTR_NAME;
	comOp = EQ_OP;
	if (scan(table, conditionAttribute, comOp, (void*) (&tableId),
			attributeNames, rm_ScanIterator) != 0)
		return -1;
	while (rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
		if (deleteTuple(table, rid) != 0)
			return -1;
	rm_ScanIterator.close();

//destroy corresponding file in disk
	string fileName;
	if (getFileName(tableName, fileName) != 0)
		return -1;
	RecordBasedFileManager* pRBFManager = RecordBasedFileManager::instance();
	if (pRBFManager->destroyFile(fileName) != 0)
		return -1;

//change tables table in memory
	tablesTable.erase(tableName);
	return 0;
}

RC RelationManager::getAttributes(const string &tableName,
		vector<Attribute> &attrs) {
	if (tablesTable.count(tableName) == 0)
		return -1;
	vector<ColumnsTableEntry> columns = tablesTable[tableName].columns;

	for (unsigned int i = 0; i < columns.size(); i++) {
		for (unsigned int j = 0; j < columns.size(); j++)
			if (columns[j].num == (int) i) {
				struct Attribute attr;
				attr.name = columns[j].columnName;
				attr.type = columns[j].columnType;
				attr.length = columns[j].columnLength;
				attrs.push_back(attr);
				continue;
			}
	}
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid) {
	char dataFormat[PAGE_SIZE];
	vector<Attribute> recordDescriptor;
	FileHandle fileHandle;

	if (getAttributes(tableName, recordDescriptor) != 0)
		return -1;
	writeDataFormat(recordDescriptor, data, dataFormat);
	if (openFile(tableName, fileHandle) != 0)
		return -1;
	if (RecordBasedFileManager::instance()->insertRecord(fileHandle,
			recordDescriptor, dataFormat, rid) < 0)
		return -1;
	if (closeFile(fileHandle) < 0)
		return -1;
	if (updateIndexesInsertTuple(tableName, rid) < 0)
		return -1;
	return 0;
}

RC RelationManager::deleteTuples(const string &tableName) {
	FileHandle fileHandle;

	if (openFile(tableName, fileHandle) != 0)
		return -1;
	if (RecordBasedFileManager::instance()->deleteRecords(fileHandle) < 0) {
		if (closeFile(fileHandle) < 0)
			return -1;
		return -1;
	}
	if (closeFile(fileHandle) < 0)
		return -1;

	// Delete all entries from all index files of the table
	if (indexesTable.count(tableName) > 0) {
		IndexManager* ixManager = IndexManager::instance();
		map<string, string>& indexes = indexesTable[tableName].attrMap;
		map<string, string>::iterator itIndexes = indexes.begin();
		vector<Attribute> tableAttr;
		map<string, Attribute> mapAttr;
//		char keyData[PAGE_SIZE];

		// Get all attributes of the given table and keep in a map
		if (getAttributes(tableName, tableAttr) < 0)
			return -1;
		for (unsigned int j = 0; j < tableAttr.size(); ++j)
			mapAttr[tableAttr.at(j).name] = tableAttr.at(j);

		// Iterator through all indexed attributes of the table
		for (; itIndexes != indexes.end(); ++itIndexes) {
			const string& indexAttr = itIndexes->first;
			const string& indexFile = itIndexes->second;
			FileHandle handleIndexFile;

			// Open the index file and delete all entries
			if (ixManager->openFile(indexFile, handleIndexFile) < 0)
				return -1;

			IX_ScanIterator ixIt;
			RID tRID;
			char tKey[PAGE_SIZE];

			if (ixManager->scan(handleIndexFile, mapAttr[indexAttr], NULL, NULL,
					true, true, ixIt) < 0) {
				ixManager->closeFile(handleIndexFile);
				return -1;
			}
			while (ixIt.getNextEntry(tRID, tKey) != IX_EOF) {
				if (ixManager->deleteEntry(handleIndexFile, mapAttr[indexAttr],
						tKey, tRID) < 0) {
					ixManager->closeFile(handleIndexFile);
					ixIt.close();
					return -1;
				}
			}
			ixIt.close();
			if (ixManager->closeFile(handleIndexFile) < 0)
				return -1;
		}
	}

	return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	vector<Attribute> recordDescriptor;
	FileHandle fileHandle;

	if (updateIndexesDeleteTuple(tableName, rid) < 0)
		return -1;
	if (getAttributes(tableName, recordDescriptor) != 0)
		return -1;
	if (openFile(tableName, fileHandle) != 0)
		return -1;
	if (RecordBasedFileManager::instance()->deleteRecord(fileHandle,
			recordDescriptor, rid) < 0) {
		if (closeFile(fileHandle) < 0)
			return -1;
		return -1;
	}
	if (closeFile(fileHandle) < 0)
		return -1;
	return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid) {
	char dataFormat[PAGE_SIZE];
	vector<Attribute> recordDescriptor;
	FileHandle fileHandle;

	if (updateIndexesDeleteTuple(tableName, rid) < 0)
		return -1;
	if (getAttributes(tableName, recordDescriptor) != 0)
		return -1;
	writeDataFormat(recordDescriptor, data, dataFormat);
	if (openFile(tableName, fileHandle) != 0)
		return -1;
	if (RecordBasedFileManager::instance()->updateRecord(fileHandle,
			recordDescriptor, dataFormat, rid) != 0) {
		if (closeFile(fileHandle) < 0)
			return -1;
		return -1;
	}
	if (closeFile(fileHandle) < 0)
		return -1;
	if (updateIndexesInsertTuple(tableName, rid) < 0)
		return -1;
	return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
		void *data) {
	vector<Attribute> recordDescriptor;
	if (getAttributes(tableName, recordDescriptor) != 0)
		return -1;

	FileHandle fileHandle;
	if (openFile(tableName, fileHandle) != 0)
		return -1;
	if (RecordBasedFileManager::instance()->readRecord(fileHandle,
			recordDescriptor, rid, data) != 0) {
		if (closeFile(fileHandle) < 0)
			return -1;
		return -1;
	}
	if (closeFile(fileHandle) < 0)
		return -1;
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data) {
	vector<Attribute> recordDescriptor;
	if (getAttributes(tableName, recordDescriptor) != 0)
		return -1;

	FileHandle fileHandle;
	if (openFile(tableName, fileHandle) != 0)
		return -1;
	if (RecordBasedFileManager::instance()->readAttribute(fileHandle,
			recordDescriptor, rid, attributeName, data) != 0) {
		if (closeFile(fileHandle) < 0)
			return -1;
		return -1;
	}
	if (closeFile(fileHandle) < 0)
		return -1;
	return 0;
}

RC RelationManager::reorganizePage(const string &tableName,
		const unsigned pageNumber) {
	vector<Attribute> recordDescriptor;
	if (getAttributes(tableName, recordDescriptor) != 0)
		return -1;

	FileHandle fileHandle;
	if (openFile(tableName, fileHandle) != 0)
		return -1;
	if (RecordBasedFileManager::instance()->reorganizePage(fileHandle,
			recordDescriptor, pageNumber) != 0) {
		if (closeFile(fileHandle) < 0)
			return -1;
		return -1;
	}
	if (closeFile(fileHandle) < 0)
		return -1;
	return 0;

}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RM_ScanIterator &rm_ScanIterator) {
	if (tablesTable.count(tableName) == 0)
		return -1;

	string fileName = tablesTable[tableName].fileName;
	vector<Attribute> recordDescriptor;

	getAttributes(tableName, recordDescriptor);
	return rm_ScanIterator.setupIterator(fileName, recordDescriptor,
			conditionAttribute, compOp, value, attributeNames);
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName,
		const string &attributeName) {
//get the iterator
	RM_ScanIterator rm_ScanIterator;
	string table = COLUMNS_TABLE_NAME;
	string conditionAttribute = COLUMN_NAME_ATTR_NAME;
	CompOp comOp = EQ_OP;
	vector<string> attributeNames;
	attributeNames.push_back(TABLE_ID_ATTR_NAME);
	attributeNames.push_back(COLUMN_NAME_ATTR_NAME);
	attributeNames.push_back(COLUMN_TYPE_ATTR_NAME);
	attributeNames.push_back(COLUMN_LENGTH_ATTR_NAME);
	attributeNames.push_back(COLUMN_NUM_ATTR_NAME);

	if (scan(table, conditionAttribute, comOp, (void*) attributeName.c_str(),
			attributeNames, rm_ScanIterator) != 0)
		return -1;

//use iterator to find the rid
	int tableId;
	if (getTableId(tableName, tableId) != 0)
		return -1;
	RID rid;
	char data[PAGE_SIZE];
	if (rm_ScanIterator.getNextTuple(rid, data) != 0)
		return -1;
	while (*(int*) data != tableId)
		if (rm_ScanIterator.getNextTuple(rid, data) != 0)
			return -1;
	rm_ScanIterator.close();

//update the corresponding tuple in column table on disk
	void* p = data;
	p = (int*) p + 1;
	int l = *(int*) p;
	*(int*) p = 0;
	p = (int*) p + 1;
	memcpy(p, (char*) p + l, 3 * sizeof(int));

	if (updateTuple(tableName, data, rid) != 0)
		return -1;

//update the corresponding entry in tablesTable in memory
	vector<ColumnsTableEntry> *pcolumns = &tablesTable[tableName].columns;
	for (unsigned int i = 0; i < pcolumns->size(); i++)
		if ((*pcolumns)[i].columnName.compare(attributeName) == 0)
			(*pcolumns)[i].columnName = "";

	return 0;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName,
		const Attribute &attr) {
//update the corresponding entry in tablesTable in memory
	vector<ColumnsTableEntry> *pcolumns = &tablesTable[tableName].columns;
	int tableId;
	if (getTableId(tableName, tableId) != 0)
		return -1;
	int num = pcolumns->size();

	ColumnsTableEntry columnsTableEntry;
	columnsTableEntry.tableId = tableId;
	columnsTableEntry.columnName = attr.name;
	columnsTableEntry.columnType = attr.type;
	columnsTableEntry.columnLength = attr.length;
	columnsTableEntry.num = num;

	pcolumns->push_back(columnsTableEntry);

//insert the record into columns talbe on the disk
	char data[PAGE_SIZE] = { 0 };
	buildColumnsRec(data, tableId, attr, num);
	RID rid;
	if (insertTuple(COLUMNS_TABLE_NAME, data, rid) != 0)
		return -1;
	return 0;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName) {
	vector<Attribute> attrs;
	FileHandle fileHandle;

	if (getAttributes(tableName, attrs) < 0)
		return -1;
	if (openFile(tableName, fileHandle) < 0)
		return -1;
	RecordBasedFileManager::instance()->reorganizeFile(fileHandle, attrs);
	if (closeFile(fileHandle) < 0)
		return -1;
	return 0;
}

RC RelationManager::getFileName(const string &tableName, string &fileName) {
	if (tablesTable.count(tableName) == 0)
		return -1;
	fileName = tablesTable[tableName].tableName;
	return 0;
}

RC RelationManager::getTableId(const string &tableName, int &tableId) {
	if (tablesTable.count(tableName) == 0)
		return -1;
	tableId = tablesTable[tableName].tableId;
	return 0;
}

RC RelationManager::openFile(const string &tableName, FileHandle &fileHandle) {
	string fileName;
	if (getFileName(tableName, fileName) != 0)
		return -1;
	if (RecordBasedFileManager::instance()->openFile(fileName, fileHandle) < 0)
		return -1;
	return 0;
}

RC RelationManager::closeFile(FileHandle &fileHandle) {
	if (RecordBasedFileManager::instance()->closeFile(fileHandle) < 0)
		return -1;
	return 0;
}

void RelationManager::writeDataFormat(const vector<Attribute>& recordDescriptor,
		const void *dataIn, void * const dataOut) {
	void *p = dataOut;
	const void *q = dataIn;
	for (unsigned int i = 0; i < recordDescriptor.size(); i++) {
		if (recordDescriptor[i].name.length() == 0) {
			if (recordDescriptor[i].type == TypeInt)
				p = (int*) p + 1;
			else if (recordDescriptor[i].type == TypeReal)
				p = (int*) p + 1;
			else if (recordDescriptor[i].type == TypeVarChar) {
				*(int*) p = 0;
				p = (int*) p + 1;
			}
		} else {
			if (recordDescriptor[i].type == TypeInt) {
				*(int*) p = *(int*) q;
				p = (int*) p + 1;
				q = (int*) q + 1;
			} else if (recordDescriptor[i].type == TypeReal) {
				*(int*) p = *(int*) q;
				p = (int*) p + 1;
				q = (int*) q + 1;
			} else if (recordDescriptor[i].type == TypeVarChar) {
				int l = *(int*) q;
				*(int*) p = l;
				p = (int*) p + 1;
				q = (int*) q + 1;
				memcpy(p, q, l);
				p = (char*) p + l;
				q = (char*) q + l;
			}
		}
	}
}

RM_ScanIterator::RM_ScanIterator() :
		RBFM_ScanIterator() {
	;
}

RM_ScanIterator::~RM_ScanIterator() {
	;
}

RC RM_ScanIterator::getNextTuple(RID& rid, void* data) {
	return RBFM_ScanIterator::getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
	return RBFM_ScanIterator::close();
}

RC RM_ScanIterator::setupIterator(const string& fileName,
		const vector<Attribute>& recordDescriptor,
		const string& conditionAttribute, const CompOp& compOp,
		const void* value, const vector<string>& attributeNames) {
	return RBFM_ScanIterator::setupIterator(fileName, recordDescriptor,
			conditionAttribute, compOp, value, attributeNames);
}

RC RelationManager::createIndex(const string &tableName,
		const string &attributeName) {
	//create an entry in indexesTable
	if (indexesTable.count(tableName) == 0) {
		IndexesTableEntry tte;
		indexesTable[tableName] = tte;
	}
	//create an entry in IndexesTableEntry 
	if (indexesTable[tableName].attrMap.count(attributeName) != 0)
		return -1;
	string indexFileName = tableName + "_" + attributeName;
	indexesTable[tableName].attrMap[attributeName] = indexFileName;
	if (IndexManager::instance()->createFile(indexFileName.c_str()) < 0)
		return -1;

	//insert the new attribute to index file
	int tableID;
	if (getTableId(tableName, tableID) != 0)
		return -1;
	char data[PAGE_SIZE] = { 0 };
	char*p = data;
	*((int*) p) = tableID;
	p += sizeof(int);
	*((int*) p) = attributeName.size();
	p += sizeof(int);
	for (unsigned int i = 0; i < attributeName.size(); i++)
		*(p++) = attributeName[i];
	*((int*) p) = indexFileName.size();
	p += sizeof(int);
	for (unsigned int i = 0; i < indexFileName.size(); i++)
		*(p++) = indexFileName[i];
	RID rid;
	if (insertTuple(string(INDEXES_TABLE_NAME), (void*) data, rid) != 0)
		return -1;

	//build indexes for existing data in the table
	IndexManager* indexManager = IndexManager::instance();
	FileHandle fileHandle;
	if (indexManager->openFile(indexFileName, fileHandle) != 0)
		return -1;

	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0)
		return -1;
	unsigned int attrI;
	for (attrI = 0; attrI < attrs.size(); attrI++)
		if (attrs[attrI].name.compare(attributeName) == 0)
			break;
	if (attrI == attrs.size())
		return -1;

	RM_ScanIterator itr;
	string conditionAttribute;
	CompOp compOp = NO_OP;
//	void *value;
	vector<string> attributeNames;
	attributeNames.push_back(attributeName);
	if (scan(tableName, conditionAttribute, compOp, NULL, attributeNames, itr)
			!= 0)
		return -1;

	char scandata[PAGE_SIZE];
	while (itr.getNextTuple(rid, &scandata) == 0) {
		if (indexManager->insertEntry(fileHandle, attrs[attrI], scandata, rid)
				!= 0) {
			itr.close();
			return -1;
		}
	}
	itr.close();
	return 0;
}

RC RelationManager::destroyIndex(const string &tableName,
		const string &attributeName) {
	if (indexesTable.count(tableName) == 0)
		return -1;

	if (indexesTable[tableName].attrMap.count(attributeName) == 0)
		return -1;
	string indexFileName = indexesTable[tableName].attrMap[attributeName];
	if (IndexManager::instance()->destroyFile(indexFileName.c_str()) < 0)
		return -1;
	indexesTable[tableName].attrMap.erase(attributeName);

	//delete the attribute in index table file
	RM_ScanIterator itr;
	int tableID;
	if (getTableId(tableName, tableID) != 0)
		return -1;
	string conditionAttribute = TABLE_ID_ATTR_NAME;
	CompOp compOp = EQ_OP;
	void *value = &tableID;
	vector<string> attributeNames;
	attributeNames.push_back(string(INDEX_ATTR_ATTR_NAME));
	if (scan(INDEXES_TABLE_NAME, conditionAttribute, compOp, value,
			attributeNames, itr) != 0)
		return -1;

	RID rid;
	char data[PAGE_SIZE];
	while (itr.getNextTuple(rid, &data) == 0) {
		if ((int) attributeName.size() == *(int*) data) {
			int f = 1;
			for (unsigned int i = 0; i < attributeName.size(); i++)
				if (attributeName[i] != data[sizeof(int) + i]) {
					f = 0;
					break;
				}
			if (f) {
				deleteTuple(INDEXES_TABLE_NAME, rid);
				itr.close();
				return 0;
			}
		}
	}
	itr.close();
	return -1;
}

const int RelationManager::TYPE_INT_MINIMUM = INT_MIN;
const int RelationManager::TYPE_INT_MAXIMUM = INT_MAX;
const float RelationManager::TYPE_REAL_NINF =
		-numeric_limits<float>::infinity();
const float RelationManager::TYPE_REAL_PINF =
		+numeric_limits<float>::infinity();
const char RelationManager::TYPE_VAR_CHAR_N[4] = { 0 };
const char RelationManager::TYPE_VAR_CHAR_P[5] = { 1, 0, 0, 0, CHAR_MAX };

RC RelationManager::indexScan(const string &tableName,
		const string &attributeName, const void *lowKey, const void *highKey,
		bool lowKeyInclusive, bool highKeyInclusive,
		RM_IndexScanIterator &rm_IndexScanIterator) {
	if (indexesTable.count(tableName) > 0) {
//		IndexManager* ixManager = IndexManager::instance();
		map<string, string>& indexes = indexesTable[tableName].attrMap;
		map<string, string>::iterator itIndexes = indexes.begin();
		vector<Attribute> tableAttr;
		map<string, Attribute> mapAttr;
//		char keyData[PAGE_SIZE];

		// Get all attributes of the given table and keep in a map
		if (getAttributes(tableName, tableAttr) < 0)
			return -1;
		for (unsigned int j = 0; j < tableAttr.size(); ++j)
			mapAttr[tableAttr.at(j).name] = tableAttr.at(j);

		// Get name of the index file and the corresponding Attribute object
		if (indexes.count(attributeName) < 1) // This attribute is not indexed
			return -1;

		const string& indexFile = indexes[attributeName];
		Attribute& attribute = mapAttr[attributeName];
		const void* lowKeyMod;
		const void* highKeyMod;

		if (lowKey)
			lowKeyMod = lowKey;
		else
			switch (attribute.type) {
			case TypeInt:
				lowKeyMod = &TYPE_INT_MINIMUM;
				break;
			case TypeReal:
				lowKeyMod = &TYPE_REAL_NINF;
				break;
			case TypeVarChar:
				lowKeyMod = &TYPE_VAR_CHAR_N;
				break;
			}
		if (highKey)
			highKeyMod = highKey;
		else
			switch (attribute.type) {
			case TypeInt:
				highKeyMod = &TYPE_INT_MAXIMUM;
				break;
			case TypeReal:
				highKeyMod = &TYPE_REAL_PINF;
				break;
			case TypeVarChar:
				highKeyMod = &TYPE_VAR_CHAR_P;
				break;
			}

		rm_IndexScanIterator.setupIterator(indexFile, attribute, lowKeyMod,
				highKeyMod, lowKeyInclusive, highKeyInclusive);
	} else
		return -1;
	return 0;
}

RM_IndexScanIterator::RM_IndexScanIterator() :
		IX_ScanIterator() {
	;
}

RM_IndexScanIterator::~RM_IndexScanIterator() {
	;
}

RC RM_IndexScanIterator::getNextEntry(RID& rid, void* key) {
	return IX_ScanIterator::getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close() {
	return IX_ScanIterator::close();
}
RC RelationManager::updateIndexesTuple(const string& tableName, const RID& rid,
		const bool& opFlag) {
	if (indexesTable.count(tableName) > 0) {
		IndexManager* ixManager = IndexManager::instance();
		map<string, string>& indexes = indexesTable[tableName].attrMap;
		map<string, string>::iterator itIndexes = indexes.begin();
		vector<Attribute> tableAttr;
		map<string, Attribute> mapAttr;
		char keyData[PAGE_SIZE];

		// Get all attributes of the given table and keep in a map
		if (getAttributes(tableName, tableAttr) < 0)
			return -1;
		for (unsigned int j = 0; j < tableAttr.size(); ++j)
			mapAttr[tableAttr.at(j).name] = tableAttr.at(j);

		// Iterator through all indexed attributes of the table
		for (; itIndexes != indexes.end(); ++itIndexes) {
			const string& indexAttr = itIndexes->first;
			const string& indexFile = itIndexes->second;
			FileHandle handleIndexFile;
			RC opRes = 0;

			// Read data of the indexed attribute using the rid, as the key
			if (readAttribute(tableName, rid, indexAttr, keyData) < 0)
				return -1;

			// Open the index file and insert/delete an entry
			if (ixManager->openFile(indexFile, handleIndexFile) < 0)
				return -1;
			if (opFlag)
				opRes = ixManager->insertEntry(handleIndexFile,
						mapAttr[indexAttr], keyData, rid);
			else
				opRes = ixManager->deleteEntry(handleIndexFile,
						mapAttr[indexAttr], keyData, rid);
			if (ixManager->closeFile(handleIndexFile) < 0)
				return -1;
			if (opRes < 0)
				return -1;
		}
	}
	return 0;
}

RC RelationManager::updateIndexesInsertTuple(const string& tableName,
		const RID& rid) {
	return updateIndexesTuple(tableName, rid, true);
}

RC RelationManager::updateIndexesDeleteTuple(const string& tableName,
		const RID& rid) {
	return updateIndexesTuple(tableName, rid, false);
}

RC RM_IndexScanIterator::setupIterator(const string& fileName,
		const Attribute& attribute, const void *lowKey, const void *highKey,
		bool lowKeyInclusive, bool highKeyInclusive) {
	return IX_ScanIterator::setupIterator(fileName, attribute, lowKey, highKey,
			lowKeyInclusive, highKeyInclusive);
}
