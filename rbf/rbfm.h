#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>

#include "../rbf/pfm.h"

using namespace std;

// Record ID
typedef struct {
	PageNum pageNum;
	unsigned slotNum;
} RID;

// Attribute
typedef enum {
	TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
	string name;     // attribute name
	AttrType type;     // attribute type
	AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
	EQ_OP = 0,  // =
	LT_OP,         // <
	GT_OP,         // >
	LE_OP,         // <=
	GE_OP,         // >=
	NE_OP,         // !=
	NO_OP          // no condition
} CompOp;

// Header Page Entry
typedef struct {
	PageNum pageNum;
	unsigned freeSpace;
} HeaderPageEntry;

#define ENTRY_IN_HEADER_PAGE ((PAGE_SIZE-sizeof(unsigned)*2)/sizeof(HeaderPageEntry))

// Header Page
typedef struct {
	unsigned localPageCount;
	PageNum nextHeaderPage;
	HeaderPageEntry entries[ENTRY_IN_HEADER_PAGE];
} HeaderPage;

// Data Page
typedef struct {
	unsigned int* freeSpaceOffset;
	unsigned int* recordCount;
	void* records;
	int* slotDirectory;

	unsigned int* headerEntryPage;
	unsigned int* headerEntryID;
} DataPage;

#define TOMBSTONE (-1)
//used as the file name to reorganizeFile
#define FILE_NAME_TEMP_REORG ("TEMPFILE")
/****************************************************************************
 The scan iterator is NOT required to be implemented for part 1 of the project
 *****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
	RBFM_ScanIterator();
	~RBFM_ScanIterator();

	// "data" follows the same format as RecordBasedFileManager::insertRecord()
	RC getNextRecord(RID &rid, void *data);
	RC close();

	RC setupIterator(const FileHandle& fileHandle,
			const vector<Attribute>& recordDescriptor,
			const string& conditionAttribute, const CompOp& compOp,
			const void* value, const vector<string>& attributeNames);
protected:
	RC setupIterator(const string& fileName,
			const vector<Attribute>& recordDescriptor,
			const string& conditionAttribute, const CompOp& compOp,
			const void* value, const vector<string>& attributeNames);

private:
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	string conditionAttribute;
	CompOp compOp;
	const void* value;
	vector<string> attributeNames;

	AttrType conditionType;
	RID nextRID;
};

class RecordBasedFileManager {
public:
	static RecordBasedFileManager* instance();

	RC createFile(const string &fileName);

	RC destroyFile(const string &fileName);

	RC openFile(const string &fileName, FileHandle &fileHandle);

	RC closeFile(FileHandle &fileHandle);

	//  Format of the data passed into the function is the following:
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
	RC insertRecord(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor, const void *data,
			RID &rid);

	RC readRecord(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor, const RID &rid,
			void *data);

	// This method will be mainly used for debugging/testing
	RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

	/********************* auxiliary method *************************/
	// get the actual number of space needed to store the record
	unsigned getRecordLength(const vector<Attribute> &recordDescriptor,
			const void *data);

	// find the page which has enough free space to store the record
	// if find the page with enough space, return the page num
	// else return 0 denotes that need to append a new page to the file
	PageNum findRecordPage(FileHandle &fileHandle, const unsigned length);

	// add a new data page to the file because all of the existed pages dont't satisfy the request
	// return the new created page number
	// create a new header page if needed
	PageNum newPage(FileHandle &fileHandle);

	// increase the total page count recorded at the first header page
//	RC increasePageCount(FileHandle &fileHandle, const unsigned n);

	// Check if the given RID is valid
	const RC checkRID(FileHandle& fileHandle, const RID& rid) const;

	/**************************************************************************************************************************************************************
	 ***************************************************************************************************************************************************************
	 IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
	 ***************************************************************************************************************************************************************
	 ***************************************************************************************************************************************************************/
	RC deleteRecords(FileHandle &fileHandle);

	RC deleteRecord(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor, const RID &rid);

	// Assume the rid does not change after update
	RC updateRecord(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor, const void *data,
			const RID &rid);

	RC readAttribute(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor, const RID &rid,
			const string attributeName, void *data);

	RC reorganizePage(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor,
			const unsigned pageNumber);

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
			const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RBFM_ScanIterator &rbfm_ScanIterator);

// Extra credit for part 2 of the project, please ignore for part 1 of the project
public:
	RC reorganizeFile(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor);

protected:
	RecordBasedFileManager();
	~RecordBasedFileManager();

private:
	static RecordBasedFileManager *_rbf_manager;
};

#endif
