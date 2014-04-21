#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cfloat>

#include "../rbf/rbfm.h"

#define IX_EOF (-1)  // end of the index scan

#define IX_ENTRY_NOT_FOUND (16)
#define IX_ENTRY_DUPLICATE (17)

typedef struct {
	unsigned int* parentPage;
	unsigned int* lSibPage;
	unsigned int* rSibPage;
	unsigned int* level;
	unsigned int* count;
	unsigned int* freeSpace;
} PageHeaderSection;

#define HEADER_SEC_SIZE (sizeof(unsigned int) * 6)

class IX_ScanIterator;

class IndexManager {
public:
	static IndexManager* instance();

	RC createFile(const string &fileName);

	RC destroyFile(const string &fileName);

	RC openFile(const string &fileName, FileHandle &fileHandle);

	RC closeFile(FileHandle &fileHandle);

	// The following two functions are using the following format for the passed key value.
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	RC insertEntry(FileHandle &fileHandle, const Attribute &attribute,
			const void *key, const RID &rid);  // Insert new index entry
	RC deleteEntry(FileHandle &fileHandle, const Attribute &attribute,
			const void *key, const RID &rid);  // Delete index entry

	// scan() returns an iterator to allow the caller to go through the results
	// one by one in the range(lowKey, highKey).
	// For the format of "lowKey" and "highKey", please see insertEntry()
	// If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
	// should be included in the scan
	// If lowKey is null, then the range is -infinity to highKey
	// If highKey is null, then the range is lowKey to +infinity
	RC scan(FileHandle &fileHandle, const Attribute &attribute,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator);

	//some auxiliary functions
	int compare(const Attribute &attribute, const void* a, const void* b);
	void buildHeaderSection(PageHeaderSection& headerSection,
			const void* pageData);

	PageNum findKeyPage(FileHandle &fileHandle, const Attribute &attribute,
			const void *key);
	unsigned int findKeyOffset(const void *data, const Attribute &attribute,
			const void *key);

//insert
	RC simpleInsertEntry(void *pageData, const Attribute &attribute,
			const void *insertedEntry, const PageNum &extraPageNum);

	RC splitPage(FileHandle &fileHandle, const Attribute &attribute,
			const PageNum& pageNum, PageNum &pageNumSplit,
			const void *insertedEntry, void *keyUp);

	RC insertIndexEntry(FileHandle &fileHandle, const Attribute &attribute,
			const PageNum &indexPageNum, const PageNum &splitPageNum,
			const void* key, const PageNum &extraPageNum);

//delete
	RC simpleDeleteEntry(void *pageData, const Attribute &attribute,
			const void *deletedEntry);

	RC redistributePage(FileHandle &fileHandle, const Attribute &Attribute,
			const PageNum &pageNum);

	RC redistributePage(FileHandle &fileHandle, const Attribute &attribute,
			const PageNum &pageNumA, const PageNum &pageNumB);

	void findEntryDown(FileHandle &fileHandle, const Attribute &attribute,
			const PageNum &pageNum, void *entryDown);

	RC mergePage(FileHandle &fileHandle, const Attribute &attribute,
			const PageNum& pageNumA, const PageNum &pageNumB,
			const void* entryDown);

	RC deleteIndexEntry(FileHandle &fileHandle, const Attribute &attribute,
			const PageNum &pageNum, const void* deletedEntry);

	void printTree(FileHandle fileHandle);

protected:
	IndexManager();                            // Constructor
	~IndexManager();                            // Destructor

private:
	static IndexManager *_index_manager;

	const static int TYPE_INT_MINIMUM;
	const static int TYPE_INT_MAXIMUM;
	const static float TYPE_REAL_NINF;
	const static float TYPE_REAL_PINF;
	const static char TYPE_VAR_CHAR_N[4];
	const static char TYPE_VAR_CHAR_P[5];
};

class IX_ScanIterator {
public:
	IX_ScanIterator();  							// Constructor
	~IX_ScanIterator(); 							// Destructor

	RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
	RC close();             						// Terminate index scan

	RC setupIterator(FileHandle &fileHandle, const Attribute &attribute,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive);

protected:
	RC setupIterator(const string& fileName, const Attribute &attribute,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive);

private:
	FileHandle fileHandle;
	Attribute attribute;
	const void* lowKey;
	const void* highKey;
	bool lowKeyInclusive;
	bool highKeyInclusive;

//	PageNum nextEntryPageNum;
//	unsigned int nextEntryOffset;
	bool nextEntryAvailable;
	char nextEntry[PAGE_SIZE];
};

// print out the error message for a given return code
void IX_PrintError(RC rc);

#endif
