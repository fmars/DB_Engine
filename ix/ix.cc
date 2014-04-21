#include <iostream>
#include <cstring>
#include <cstdio>
#include <climits>
#include <limits>
#include <cassert>

#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance() {
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager() {
}

IndexManager::~IndexManager() {
}

RC IndexManager::createFile(const string &fileName) {
	if (PagedFileManager::instance()->createFile(fileName.c_str()) < 0)
		return -1;

	FileHandle handle;
	char babyIndexPage[PAGE_SIZE] = { 0 };
	char babyLeafPage[PAGE_SIZE] = { 0 };
	PageHeaderSection hdrIndex;
	PageHeaderSection hdrLeaf;
	char* babyIndexPageEnt = babyIndexPage + HEADER_SEC_SIZE;
//	char* babyLeafPageEnt = babyLeafPage + HEADER_SEC_SIZE;

	buildHeaderSection(hdrIndex, babyIndexPage);
	buildHeaderSection(hdrLeaf, babyLeafPage);
	*(hdrIndex.parentPage) = 0;
	*(hdrIndex.level) = 1;
	*(hdrIndex.count) = 0;
	*(hdrIndex.freeSpace) = PAGE_SIZE - HEADER_SEC_SIZE - sizeof(PageNum);
	*(hdrIndex.lSibPage) = 0;
	*(hdrIndex.rSibPage) = 0;
	*(PageNum*) (babyIndexPageEnt) = 1;
	*(hdrLeaf.parentPage) = 0;
	*(hdrLeaf.level) = 0;
	*(hdrLeaf.count) = 0;
	*(hdrLeaf.freeSpace) = PAGE_SIZE - HEADER_SEC_SIZE;
	*(hdrLeaf.lSibPage) = 0;
	*(hdrLeaf.rSibPage) = 0;

	PagedFileManager::instance()->openFile(fileName.c_str(), handle);
	handle.appendPage(babyIndexPage);
	handle.appendPage(babyLeafPage);
	PagedFileManager::instance()->closeFile(handle);
	return 0;
}

RC IndexManager::destroyFile(const string &fileName) {
	return PagedFileManager::instance()->destroyFile(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle) {
	return PagedFileManager::instance()->openFile(fileName.c_str(), fileHandle);
}

RC IndexManager::closeFile(FileHandle &fileHandle) {
	return PagedFileManager::instance()->closeFile(fileHandle);
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute,
		const void *key, const RID &rid) {
	PageNum pageNum = findKeyPage(fileHandle, attribute, key);
	char pageData[PAGE_SIZE] = { 0 };
	fileHandle.readPage(pageNum, pageData);
	PageHeaderSection header;
	buildHeaderSection(header, pageData);

	unsigned int length = 0;
	if (attribute.type == TypeInt || attribute.type == TypeReal)
		length = sizeof(int) + sizeof(RID);
	else if (attribute.type == TypeVarChar)
		length = sizeof(int) + *(int*) key + sizeof(RID);

	char insertedEntry[PAGE_SIZE] = { 0 };
	memcpy(insertedEntry, key, length - sizeof(RID));
	memcpy(insertedEntry + length - sizeof(RID), &rid, sizeof(RID));

	if (*(header.freeSpace) >= length) {
		RC rc;
		rc = simpleInsertEntry(pageData, attribute, insertedEntry, 0);
		if (rc != 0)
			return rc;
		if (fileHandle.writePage(pageNum, pageData) != 0)
			return -1;
	} else {
		PageNum pageNumSplit;
		char keyUp[PAGE_SIZE];
		RC rc;
		rc = splitPage(fileHandle, attribute, pageNum, pageNumSplit,
				insertedEntry, keyUp);
		if (rc != 0)
			return rc;
		if (insertIndexEntry(fileHandle, attribute, *(header.parentPage),
				pageNumSplit, keyUp, pageNum) != 0) {
			assert((int )pageNumSplit != -1);
			return -1;
		}
	}
	return 0;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute,
		const void *key, const RID &rid) {
	char deletedEntry[PAGE_SIZE] = { 0 };
	if (attribute.type == TypeInt || attribute.type == TypeReal) {
		memcpy(deletedEntry, key, sizeof(int));
		memcpy(deletedEntry + sizeof(int), &rid, sizeof(RID));
	} else if (attribute.type == TypeVarChar) {
		memcpy(deletedEntry, key, sizeof(int) + *(int*) key);
		memcpy(deletedEntry + sizeof(int) + *(int*) key, &rid, sizeof(RID));
	}

	PageNum pageNum = findKeyPage(fileHandle, attribute, key);
	char pageData[PAGE_SIZE] = { 0 };
	fileHandle.readPage(pageNum, pageData);
	int ret = simpleDeleteEntry(pageData, attribute, deletedEntry);

	if (ret != 0)
		return ret;
	fileHandle.writePage(pageNum, pageData);

	PageHeaderSection header;
	buildHeaderSection(header, pageData);
	unsigned int usedSpace = PAGE_SIZE - *(header.freeSpace) - HEADER_SEC_SIZE;
	//just delete this entry is ok
	if (usedSpace * 2 > PAGE_SIZE - HEADER_SEC_SIZE)
		return 0;
	//need merge and deleteIndexEntry
	else if (usedSpace * 2 < PAGE_SIZE - HEADER_SEC_SIZE) {
		char pageLeftData[PAGE_SIZE] = { 0 };
		PageNum pageLeftNum = *(header.lSibPage);
		if (fileHandle.readPage(pageLeftNum, pageLeftData) != 0)
			return -1;
		PageHeaderSection leftHeader;
		buildHeaderSection(leftHeader, pageLeftData);
		/*
		 if(pageLeftNum!=0 && *(header.parentPage)==*(leftHeader.parentPage) && usedSpace+(PAGE_SIZE-*(leftHeader.freeSpace)-HEADER_SEC_SIZE)>PAGE_SIZE-HEADER_SEC_SIZE){
		 if(redistributePage(fileHandle,attribute,pageNum,pageLeftNum)!=0)
		 return -1;
		 return 0;
		 }
		 */

		char pageRightData[PAGE_SIZE] = { 0 };
		PageNum pageRightNum = *(header.rSibPage);
		if (fileHandle.readPage(pageRightNum, pageRightData) != 0)
			return -1;
		PageHeaderSection rightHeader;
		buildHeaderSection(rightHeader, pageRightData);
		/*
		 if(pageRightNum!=0 && *(header.parentPage)==*(rightHeader.parentPage) && usedSpace+(PAGE_SIZE-*(rightHeader.freeSpace)-HEADER_SEC_SIZE)>PAGE_SIZE-HEADER_SEC_SIZE){
		 if(redistributePage(fileHandle,attribute,pageNum,pageRightNum)!=0)
		 return -1;
		 return 0;
		 }
		 */

		//need to do the merge operation
		char entryDown[PAGE_SIZE] = { 0 };
		if (pageLeftNum
				!= 0&& *(header.parentPage)==*(leftHeader.parentPage) && usedSpace+(PAGE_SIZE-*(leftHeader.freeSpace)-HEADER_SEC_SIZE)<PAGE_SIZE-HEADER_SEC_SIZE) {
			//if need to decrease height of the tree
			// NO! You should NOT decrease height if page 0 is the only index page!
			/*	if(*(header.parentPage)==0){
			 char page0Data[PAGE_SIZE]={0};
			 if(fileHandle.readPage(0,page0Data)!=0)
			 return -1;
			 PageHeaderSection page0Header;
			 buildHeaderSection(page0Header,page0Data);
			 if(*(page0Header.count)==1){
			 findEntryDown(fileHandle,attribute,pageNum,entryDown);
			 assert(*(leftHeader.level) > 0);
			 fileHandle.writePage(0,pageLeftData);
			 *(header.lSibPage)=0;
			 fileHandle.writePage(pageNum,pageData);
			 mergePage(fileHandle,attribute,0,pageNum,entryDown);
			 return 0;
			 }
			 }*/
			//normal merge
			findEntryDown(fileHandle, attribute, pageNum, entryDown);
			mergePage(fileHandle, attribute, pageLeftNum, pageNum, entryDown);
			deleteIndexEntry(fileHandle, attribute, *(header.parentPage),
					entryDown);
			return 0;
		}
		if (pageRightNum
				!= 0&& *(header.parentPage)==*(rightHeader.parentPage) && usedSpace+(PAGE_SIZE-*(rightHeader.freeSpace)-HEADER_SEC_SIZE)<PAGE_SIZE-HEADER_SEC_SIZE) {
			//if need to decrease height of the tree
			// NO! You should NOT decrease height if page 0 is the only index page!
			/*	if(*(header.parentPage)==0){
			 char page0Data[PAGE_SIZE]={0};
			 if(fileHandle.readPage(0,page0Data)!=0)
			 return -1;
			 PageHeaderSection page0Header;
			 buildHeaderSection(page0Header,page0Data);
			 if(*(page0Header.count)==1){
			 findEntryDown(fileHandle,attribute,pageRightNum,entryDown);
			 assert(*(header.level) > 0);
			 fileHandle.writePage(0,pageData);
			 *(rightHeader.lSibPage)=0;
			 fileHandle.writePage(pageRightNum,pageRightData);
			 mergePage(fileHandle,attribute,0,pageRightNum,entryDown);
			 return 0;
			 }
			 }*/
			//normal merge
			findEntryDown(fileHandle, attribute, pageRightNum, entryDown);
			mergePage(fileHandle, attribute, pageNum, pageRightNum, entryDown);
			deleteIndexEntry(fileHandle, attribute, *(header.parentPage),
					entryDown);
			return 0;
		}
	}
	return 0;

}

const int IndexManager::TYPE_INT_MINIMUM = INT_MIN;
const int IndexManager::TYPE_INT_MAXIMUM = INT_MAX;
const float IndexManager::TYPE_REAL_NINF = -numeric_limits<float>::infinity();
const float IndexManager::TYPE_REAL_PINF = +numeric_limits<float>::infinity();
const char IndexManager::TYPE_VAR_CHAR_N[4] = { 0 };
const char IndexManager::TYPE_VAR_CHAR_P[5] = { 1, 0, 0, 0, CHAR_MAX };

RC IndexManager::scan(FileHandle &fileHandle, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {
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
	return ix_ScanIterator.setupIterator(fileHandle, attribute, lowKeyMod,
			highKeyMod, lowKeyInclusive, highKeyInclusive);
}

static unsigned int getKeySize(const Attribute& attribute, const void* key);

RC IndexManager::splitPage(FileHandle &fileHandle, const Attribute &attribute,
		const PageNum& pageNum, PageNum &pageNumSplit,
		const void *insertedEntry, void *keyUp) {
	char oldPage[PAGE_SIZE];
	char newPage[PAGE_SIZE] = { 0 };
	PageHeaderSection hdrOld;
	PageHeaderSection hdrNew;
	char* oldEnt = oldPage + HEADER_SEC_SIZE;
	char* newEnt = newPage + HEADER_SEC_SIZE;

	if (fileHandle.readPage(pageNum, oldPage) < 0)
		return -1;
	buildHeaderSection(hdrOld, oldPage);
	buildHeaderSection(hdrNew, newPage);
	*(hdrNew.parentPage) = *(hdrOld.parentPage);
	*(hdrNew.level) = *(hdrOld.level);
	*(hdrNew.count) = 0;
	/*	if(*(hdrNew.level) == 0) // This is a leaf page
	 *(hdrNew.freeSpace) = PAGE_SIZE - HEADER_SEC_SIZE;
	 else // This is an index page
	 *(hdrNew.freeSpace) = PAGE_SIZE - HEADER_SEC_SIZE - sizeof(PageNum);*/
	*(hdrNew.freeSpace) = PAGE_SIZE - HEADER_SEC_SIZE;
	*(hdrNew.lSibPage) = pageNum;
	*(hdrNew.rSibPage) = *(hdrOld.rSibPage);
//	*(hdrOld.rSibPage); //= the pageNum of the new page, don't know

	// Copy the entries and cut the old page
	if (*(hdrNew.level) == 0) { // If this is a leaf page
		unsigned int entID = 0;
		char* ptr = oldEnt;

		for (unsigned int keySize; entID < *(hdrOld.count) / 2; ++entID) {
			keySize = getKeySize(attribute, ptr);
			ptr += keySize + sizeof(RID);
		} // Skip half of the entries
		*(hdrOld.freeSpace) = PAGE_SIZE - (ptr - oldPage);

		char* ptrNew = newEnt;

		for (unsigned int keySize; entID < *(hdrOld.count); ++entID) {
			keySize = getKeySize(attribute, ptr);
			if (*hdrNew.count == 0) // Copy up
				memcpy(keyUp, ptr, keySize /*+ sizeof(RID)*/);
			memcpy(ptrNew, ptr, keySize + sizeof(RID));
			ptrNew += keySize + sizeof(RID);
			ptr += keySize + sizeof(RID);
			*(hdrNew.count) += 1;
			*(hdrNew.freeSpace) -= keySize + sizeof(RID);
		} // Copy the other half of the entries
		*(hdrOld.count) = *(hdrOld.count) / 2;
	} else { //If this is an index page
		unsigned int entID = 0;
		char* ptr = oldEnt + sizeof(PageNum);

		for (unsigned int keySize; entID < *(hdrOld.count) / 2; ++entID) {
			keySize = getKeySize(attribute, ptr);
			ptr += keySize + sizeof(PageNum);
		} // Skip half of the entries
		*(hdrOld.freeSpace) = PAGE_SIZE - (ptr - oldPage);

		char* ptrNew = newEnt;

		for (unsigned int keySize; entID < *(hdrOld.count); ++entID) {
			keySize = getKeySize(attribute, ptr);
			if (*(hdrNew.freeSpace) == PAGE_SIZE - HEADER_SEC_SIZE) { // Push up
				memcpy(keyUp, ptr, keySize);
				ptr += keySize;
				memcpy(ptrNew, ptr, sizeof(PageNum));
				ptr += sizeof(PageNum);
				ptrNew += sizeof(PageNum);
				*(hdrNew.freeSpace) -= sizeof(PageNum);
			} else { // Copy
				memcpy(ptrNew, ptr, keySize + sizeof(PageNum));
				ptrNew += keySize + sizeof(PageNum);
				ptr += keySize + sizeof(PageNum);
				*(hdrNew.count) += 1;
				*(hdrNew.freeSpace) -= keySize + sizeof(PageNum);
			}
		} // Copy the other half of the entries
		*(hdrOld.count) = *(hdrOld.count) / 2;
	}
	if (compare(attribute, insertedEntry, keyUp) < 0) { // Simply insert left
		RC rc;
		rc = simpleInsertEntry(oldPage, attribute, insertedEntry, 0);
		if (rc != 0)
			return rc;
	} else { // Simply insert right
		RC rc;
		rc = simpleInsertEntry(newPage, attribute, insertedEntry, 0);
		if (rc != 0)
			return rc;
	}

	// Write the two pages to the file
	if (fileHandle.appendPage(newPage) < 0)
		return -1;
	pageNumSplit = *(hdrOld.rSibPage) = fileHandle.getNumberOfPages() - 1;
	if (fileHandle.writePage(pageNum, oldPage) < 0)
		return -1;

	// Change lSibPage of the previous right sibling of the page to split
	if (*(hdrNew.rSibPage) > 0) {
		char updateSib[PAGE_SIZE] = { 0 };
		PageHeaderSection hdrSib;

		if (fileHandle.readPage(*(hdrNew.rSibPage), updateSib) < 0)
			return -1;
		buildHeaderSection(hdrSib, updateSib);
		*(hdrSib.lSibPage) = pageNumSplit;
		if (fileHandle.writePage(*(hdrNew.rSibPage), updateSib) < 0)
			return -1;
	}

	// Update all children of the split-out index page
	if (*(hdrNew.level) > 0) { // If this is an index page
		unsigned int entID = 0;
		char* ptrNew = newEnt /*+ sizeof(PageNum)*/;

		for (unsigned int keySize; true; ++entID) {
			// Update the corresponding child
			char updateChildPage[PAGE_SIZE];
			PageHeaderSection hdrChild;

			if (fileHandle.readPage(*(PageNum*) ptrNew, updateChildPage) < 0)
				return -1;
			buildHeaderSection(hdrChild, updateChildPage);
			*(hdrChild.parentPage) = pageNumSplit;
			if (fileHandle.writePage(*(PageNum*) ptrNew, updateChildPage) < 0)
				return -1;
			ptrNew += sizeof(PageNum);
			if (entID >= *(hdrNew.count))
				break;
			keySize = getKeySize(attribute, ptrNew);
			ptrNew += keySize;
		}
	}
	return 0;
}

static unsigned int getKeySize(const Attribute& attribute, const void* key) {
	switch (attribute.type) {
	case TypeInt:
		return sizeof(int);
	case TypeReal:
		return sizeof(float);
	case TypeVarChar:
		return sizeof(int) + *(int*) key;
	}
	return 0;
}

IX_ScanIterator::IX_ScanIterator() :
		lowKey(NULL), highKey(NULL), lowKeyInclusive(false), highKeyInclusive(
				false),
// nextEntryPageNum(0), nextEntryOffset(0),
		nextEntryAvailable(false) {
//	memset(nextEntry, 0, PAGE_SIZE);
	;
}

IX_ScanIterator::~IX_ScanIterator() {
	IndexManager::instance()->closeFile(fileHandle);
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) { //
	IndexManager* ixm = IndexManager::instance();

//	cout << "getNextEntry: ";
	if (!nextEntryAvailable) {
		rid.pageNum = 0;
		rid.slotNum = 0;
		*(int*) key = 0;
		return IX_EOF;
	}

	char pageData[PAGE_SIZE];
	PageHeaderSection header;
	unsigned int nextEntryPageNum; //= ixm->findKeyPage(fileHandle, attribute, nextEntry);
	unsigned int nextEntryOffset;

	ixm->buildHeaderSection(header, pageData);

	// Offset might be affected by deletion, need recovery
	nextEntryPageNum = ixm->findKeyPage(fileHandle, attribute, nextEntry);
	if (fileHandle.readPage(nextEntryPageNum, pageData) < 0)
		return -41; // Consistency fault
	nextEntryOffset = ixm->findKeyOffset(pageData, attribute, nextEntry);
	for (; true;) {
		if (fileHandle.readPage(nextEntryPageNum, pageData) < 0) {
			IndexManager::instance()->closeFile(fileHandle);
			return -1;
		} // Load the leaf page

		if (nextEntryOffset >= PAGE_SIZE - *(header.freeSpace)) { // Offset out of range
			if (*(header.rSibPage) > 0) { // If there is a right sibling
				nextEntryPageNum = *(header.rSibPage);
				nextEntryOffset = HEADER_SEC_SIZE;
				continue;
			} // Else the iteration has reached EOF
			nextEntryPageNum = 0;
			nextEntryOffset = 0;
			rid.pageNum = 0;
			rid.slotNum = 0;
			*(int*) key = 0;
			nextEntryAvailable = false;
//			cout << "-> EOF" << endl;
			return IX_EOF;
		}

		char* ptr = pageData + nextEntryOffset;
		unsigned int keySize = getKeySize(attribute, ptr);
		unsigned int entSize = keySize + sizeof(RID);
		RID* nextEntryRID = (RID*) (nextEntry + keySize);

		if (ixm->compare(attribute, ptr, nextEntry) == 0) {
			ptr += keySize;
			if (((RID*) ptr)->pageNum == nextEntryRID->pageNum
					&& ((RID*) ptr)->slotNum == nextEntryRID->slotNum)
				break;
		}
		nextEntryOffset += entSize;
	}

	char* ptr = pageData + nextEntryOffset;
	unsigned int keySize = getKeySize(attribute, ptr);
	unsigned int entSize = keySize + sizeof(RID);
	RID* nextEntryRID = (RID*) (nextEntry + keySize);

	memcpy(key, ptr, keySize);
	ptr += keySize;
	memcpy(&rid, nextEntryRID, sizeof(RID));
//	cout << rid.pageNum << " " << rid.slotNum << " ";
//	printf("Iterating: pageNum = %d, offset = %d\n", nextEntryPageNum, nextEntryOffset);

	// Maintain nextEntry
	nextEntryOffset += entSize;
	for (; true;) {
		if (fileHandle.readPage(nextEntryPageNum, pageData) < 0) {
			IndexManager::instance()->closeFile(fileHandle);
			return -1;
		} // Load the leaf page

		if (nextEntryOffset >= PAGE_SIZE - *(header.freeSpace)) { // Offset out of range
			if (*(header.rSibPage) > 0) { // If there is a right sibling
				nextEntryPageNum = *(header.rSibPage);
				nextEntryOffset = HEADER_SEC_SIZE;
				continue;
			} // Else the iteration has reached EOF
			nextEntryPageNum = 0;
			nextEntryOffset = 0;
			nextEntryAvailable = false;
//			cout << "-> EOF" << endl;
			return 0;
		}

		char* ptr = pageData + nextEntryOffset;
		unsigned int keySize = getKeySize(attribute, ptr);
		unsigned int entSize = keySize + sizeof(RID);

		if (!((ixm->compare(attribute, ptr, lowKey) > 0
				|| (lowKeyInclusive && ixm->compare(attribute, ptr, lowKey) == 0))
				&& (ixm->compare(attribute, ptr, highKey) < 0
						|| (highKeyInclusive
								&& ixm->compare(attribute, ptr, highKey) == 0)))) { // If not in range: go to the next entry
			nextEntryOffset += entSize;
		} else
			break;
	}

	ptr = pageData + nextEntryOffset;
	keySize = getKeySize(attribute, ptr);
	entSize = keySize + sizeof(RID);

	memcpy(nextEntry, ptr, entSize);
	nextEntryAvailable = true;
//	cout << "-> " << ((RID*)(nextEntry + keySize))->pageNum << " "
//			<< ((RID*)(nextEntry + keySize))->slotNum << endl;
	return 0;
}

RC IX_ScanIterator::close() {
//	nextEntryPageNum = 0;
//	nextEntryOffset = 0;
	nextEntryAvailable = false;
	IndexManager::instance()->closeFile(fileHandle);
	return 0;
}

void IX_PrintError(RC rc) {
	if (rc == IX_ENTRY_DUPLICATE)
		cerr << "IX ERROR: Attempt to insert a duplicate entry" << endl;
	else if (rc == IX_ENTRY_NOT_FOUND)
		cerr << "IX ERROR: Attempt to delete an entry that does not exist"
				<< endl;
	else
		cerr << "IX ERROR: Error #" << rc << endl;
}

void IndexManager::buildHeaderSection(PageHeaderSection& headerSection,
		const void* pageData) {
	headerSection.parentPage = (unsigned int*) pageData;
	headerSection.lSibPage = (unsigned int*) pageData + 1;
	headerSection.rSibPage = (unsigned int*) pageData + 2;
	headerSection.level = (unsigned int*) pageData + 3;
	headerSection.count = (unsigned int*) pageData + 4;
	headerSection.freeSpace = (unsigned int*) pageData + 5;
}

int IndexManager::compare(const Attribute &attribute, const void *a,
		const void*b) {
	if (attribute.type == TypeInt) {
		if (*(int*) a > *(int*) b)
			return 1;
		else if (*(int*) a == *(int*) b)
			return 0;
		else
			return -1;
	} else if (attribute.type == TypeReal) {
		if (*(float*) a > *(float*) b)
			return 1;
		else if (*(float*) a == *(float*) b)
			return 0;
		else
			return -1;
	} else if (attribute.type == TypeVarChar) {
		char sa[PAGE_SIZE] = { 0 };
		char sb[PAGE_SIZE] = { 0 };
		memcpy(sa, (char*)a+sizeof(int), *(int*) a);
		memcpy(sb, (char*)b+sizeof(int), *(int*) b);
		string a(sa);
		string b(sb);
		return a.compare(b);
	}
	return 0;
}

PageNum IndexManager::findKeyPage(FileHandle &fileHandle,
		const Attribute &attribute, const void *key) {
	char pageData[PAGE_SIZE] = { 0 };
	if (fileHandle.readPage(0, pageData) != 0)
		return -1;
	PageNum pageNum = 0;
	PageHeaderSection header;
	buildHeaderSection(header, pageData);
	while (*(header.level) != 0) {
		unsigned int offset = findKeyOffset(pageData, attribute, key);
		if (offset >= PAGE_SIZE - *(header.freeSpace))
			pageNum = *((int*) ((char*) pageData + offset) - 1);
		else if (compare(attribute, key, pageData + offset) < 0)
			pageNum = *((int*) ((char*) pageData + offset) - 1);
		else if (compare(attribute, key, pageData + offset) == 0) {
			//consider the situation that duplicate keys exist
			if (*(header.level) > 1) {
				PageNum tempPageNum = *((int*) ((char*) pageData + offset) - 1);
				char tempPageData[PAGE_SIZE] = { 0 };
				fileHandle.readPage(tempPageNum, tempPageData);
				PageHeaderSection tempHeader;
				buildHeaderSection(tempHeader, tempPageData);
				//just consider the type==TypeInt to reduce the work load
				int find = 0;
				char *ptr = tempPageData + HEADER_SEC_SIZE + sizeof(int);
				for (unsigned int i = 0; i < *(header.count); i++) {
					if (compare(attribute, ptr, key) == 0) {
						find = 1;
						break;
					} else {
						if (attribute.type == TypeInt
								|| attribute.type == TypeReal)
							ptr = ptr + sizeof(int) * 2;
						else if (attribute.type == TypeVarChar)
							ptr = ptr + sizeof(int) * 2 + *(int*) (ptr);
					}
				}
				if (find)
					pageNum = tempPageNum;
				else {
					if (attribute.type == TypeInt || attribute.type == TypeReal)
						offset += sizeof(int);
					else if (attribute.type == TypeVarChar) {
						int l = *((int*) ((char*) pageData + offset));
						offset += sizeof(int) + l;
					}
					pageNum = *((int*) ((char*) pageData + offset));

					offset += sizeof(int);
					pageNum = *((int*) ((char*) pageData + offset));
				}
			} else if (*(header.level) == 1) {
				PageNum tempPageNum = *((int*) ((char*) pageData + offset) - 1);
				char tempPageData[PAGE_SIZE] = { 0 };
				fileHandle.readPage(tempPageNum, tempPageData);
				PageHeaderSection tempHeader;
				buildHeaderSection(tempHeader, tempPageData);
				//just consider the type==TypeInt to reduce the work load
				int find = 0;
				char *ptr = tempPageData + HEADER_SEC_SIZE;
				for (unsigned int i = 0; i < *(header.count); i++) {
					if (compare(attribute, ptr, key) == 0) {
						find = 1;
						break;
					} else {
						if (attribute.type == TypeInt
								|| attribute.type == TypeReal)
							ptr = ptr + sizeof(int) * 3;
						else if (attribute.type == TypeVarChar)
							ptr = ptr + sizeof(int) * 3 + *(int*) (ptr);
					}
				}
				if (find)
					pageNum = tempPageNum;
				else {
					if (attribute.type == TypeInt || attribute.type == TypeReal)
						offset += sizeof(int);
					else if (attribute.type == TypeVarChar) {
						int l = *((int*) ((char*) pageData + offset));
						offset += sizeof(int) + l;
					}
					pageNum = *((int*) ((char*) pageData + offset));
				}

			}
		} else {
			if (attribute.type == TypeInt || attribute.type == TypeReal)
				offset += sizeof(int);
			else if (attribute.type == TypeVarChar) {
				int l = *((int*) ((char*) pageData + offset));
				offset += sizeof(int) + l;
			}
			pageNum = *((int*) ((char*) pageData + offset));
		}
		if (fileHandle.readPage(pageNum, pageData) != 0)
			return -1;
		buildHeaderSection(header, pageData);
	}
	return pageNum;
}

unsigned int IndexManager::findKeyOffset(const void *data,
		const Attribute &attribute, const void *key) {
	unsigned int offset;
	PageHeaderSection header;
	buildHeaderSection(header, data);
	if (*(header.level) == 0) {
		offset = HEADER_SEC_SIZE;
		unsigned int i = 0;
		while (i < *(header.count)
				&& compare(attribute, key, (char*) data + offset) > 0) {
			i++;
			if (attribute.type == TypeInt || attribute.type == TypeReal)
				offset += sizeof(int) + sizeof(int) * 2;
			else if (attribute.type == TypeVarChar) {
				int l = *(int*) ((char*) data + offset);
				offset += l + sizeof(int) + sizeof(int) * 2;
			}
		}
		return offset;
	} else /*if(*(header.level)!=0)*/{
		offset = HEADER_SEC_SIZE + sizeof(int);
		unsigned int i = 0;
		while (i < *(header.count)
				&& compare(attribute, key, (char*) data + offset) > 0) {
			i++;
			if (attribute.type == TypeInt || attribute.type == TypeReal)
				offset += sizeof(int) + sizeof(int);
			else if (attribute.type == TypeVarChar) {
				int l = *(int*) ((char*) data + offset);
				offset += l + sizeof(int) + sizeof(int);
			}
		}
		assert(offset <= 4096);
		return offset;
	}
	return 0;
}

RC IndexManager::insertIndexEntry(FileHandle &fileHandle,
		const Attribute &attribute, const PageNum &indexPageNum,
		const PageNum &splitPageNum, const void* key,
		const PageNum &extraPageNum) {
	char pageData[PAGE_SIZE] = { 0 };
	if (fileHandle.readPage(indexPageNum, pageData) != 0)
		return -1;
	PageHeaderSection header;
	buildHeaderSection(header, pageData);
	unsigned int length = 0;
	if (attribute.type == TypeInt || attribute.type == TypeReal)
		length = sizeof(int) * 2;
	else if (attribute.type == TypeVarChar)
		length = sizeof(int) * 2 + *(int*) key;

	char foo[PAGE_SIZE] = { 0 };
	memcpy(foo, key, length - sizeof(int));
	*(int*) ((char*) foo + length - sizeof(int)) = splitPageNum;
	//have enough space to insert the entry
//	if(*(header.freeSpace)>=length){
	if (*(header.freeSpace) > (PAGE_SIZE - HEADER_SEC_SIZE) / 4
			&& *(header.freeSpace) >= length) {
		RC rc;
		if ((rc = simpleInsertEntry(pageData, attribute, foo, extraPageNum))
				!= 0)
			return rc;

		if (fileHandle.writePage(indexPageNum, pageData) != 0)
			return -1;
	} else {
		//doesn't have enough space
		//if it is #0 page
		PageNum newIndexPageNum = indexPageNum;
		PageHeaderSection newHeader = header;

		if (indexPageNum == 0) {
			newIndexPageNum = fileHandle.getNumberOfPages();
			char newPageData[PAGE_SIZE] = { 0 };
			memcpy(newPageData, pageData, PAGE_SIZE);
			buildHeaderSection(newHeader, newPageData);
			*(newHeader.parentPage) = 0;
			fileHandle.appendPage(newPageData);

			unsigned int entID = 0;
			char* ptrNew = newPageData + HEADER_SEC_SIZE;

			for (unsigned int keySize; true; ++entID) {
				// Update the corresponding child
				char updateChildPage[PAGE_SIZE];
				PageHeaderSection hdrChild;

				if (fileHandle.readPage(*(PageNum*) ptrNew, updateChildPage)
						< 0)
					return -1;
				buildHeaderSection(hdrChild, updateChildPage);
				*(hdrChild.parentPage) = newIndexPageNum;
				if (fileHandle.writePage(*(PageNum*) ptrNew, updateChildPage)
						< 0)
					return -1;
				ptrNew += sizeof(PageNum);
				if (entID >= *(newHeader.count))
					break;
				keySize = getKeySize(attribute, ptrNew);
				ptrNew += keySize;
			}
			{
				char updateChildPage[PAGE_SIZE];
				PageHeaderSection hdrChild;

				if (fileHandle.readPage(splitPageNum, updateChildPage) < 0)
					return -1;
				buildHeaderSection(hdrChild, updateChildPage);
				*(hdrChild.parentPage) = newIndexPageNum;
				if (fileHandle.writePage(splitPageNum, updateChildPage) < 0)
					return -1;
			}

			*(header.level) = *(header.level) + 1;
			*(header.count) = 0;
			*(PageNum*) (pageData + HEADER_SEC_SIZE) = newIndexPageNum;
			*(header.freeSpace) = PAGE_SIZE - HEADER_SEC_SIZE - sizeof(PageNum);
			fileHandle.writePage(0, pageData);
		}

		PageNum pageNumSplit;
		char keyUp[PAGE_SIZE] = { 0 };
		RC rc;
		rc = splitPage(fileHandle, attribute, newIndexPageNum, pageNumSplit,
				foo, keyUp);
		if (rc != 0)
			return rc;
		insertIndexEntry(fileHandle, attribute, *(newHeader.parentPage),
				pageNumSplit, keyUp, newIndexPageNum);
	}
	return 0;
}

RC IndexManager::simpleInsertEntry(void *pageData, const Attribute &attribute,
		const void *insertedEntry, const PageNum& extraPageNum) {
	PageHeaderSection header;

	buildHeaderSection(header, pageData);

	unsigned int insertPos = findKeyOffset(pageData, attribute, insertedEntry);
	//check if the inserted key has already existed
	if (*(header.level) == 0) {
		int l = 0;
		if (attribute.type == TypeInt || attribute.type == TypeReal)
			l = sizeof(int) * 3;
		else if (attribute.type == TypeVarChar)
			l = sizeof(int) * 3 + *(int*) ((char*) pageData + insertPos);
		if (memcmp(insertedEntry, (char*) pageData + insertPos, l) == 0)
			return IX_ENTRY_DUPLICATE;
	}

	// Possibility of duplicated keys in an index page
	if (*(header.level) > 0) { // If this is an index page
		char* ptr = (char*) pageData + insertPos;

		for (; insertPos < PAGE_SIZE - *(header.freeSpace);) {
			unsigned int ptrKeySize = getKeySize(attribute, ptr);
			int resComp = compare(attribute, insertedEntry, ptr);

			assert(resComp <= 0);
			if (resComp < 0)
				break; // Else resComp == 0
			insertPos += ptrKeySize + sizeof(PageNum);
			if (*(PageNum*) (ptr + ptrKeySize) == extraPageNum)
				break;
		}
	}

	char cpBuffer[PAGE_SIZE] = { 0 };
	char* ptr = (char*) pageData + insertPos;
	unsigned int entSize = getKeySize(attribute, insertedEntry);

	memcpy(cpBuffer, ptr, PAGE_SIZE - (ptr - (char*) pageData));
	if (*(header.level) == 0) // If this is a leaf page
		entSize += sizeof(RID);
	else
		// If this is an index page
		entSize += sizeof(PageNum);
	memcpy(ptr, insertedEntry, entSize);
	ptr += entSize;
	*(header.freeSpace) -= entSize;
	*(header.count) += 1;
	memcpy(ptr, cpBuffer, PAGE_SIZE - (ptr - (char*) pageData));
	return 0;
}

RC IndexManager::deleteIndexEntry(FileHandle &fileHandle,
		const Attribute &attribute, const PageNum &pageNum,
		const void* deletedEntry) {
	char pageData[PAGE_SIZE] = { 0 };
	if (fileHandle.readPage(pageNum, pageData) != 0)
		return -1;
	PageHeaderSection header;
	buildHeaderSection(header, pageData);
	int ret = simpleDeleteEntry(pageData, attribute, deletedEntry);

	if (ret != 0)
		return ret;
	if (fileHandle.writePage(pageNum, pageData) != 0)
		return -1;

	buildHeaderSection(header, pageData);
	unsigned int usedSpace = PAGE_SIZE - *(header.freeSpace) - HEADER_SEC_SIZE;
	//just delete this entry is ok
	if (usedSpace * 2 > PAGE_SIZE - HEADER_SEC_SIZE)
		return 0;
	//need to do redistribute or merge
	else if (usedSpace * 2 < PAGE_SIZE - HEADER_SEC_SIZE) {
		char pageLeftData[PAGE_SIZE] = { 0 };
		PageNum pageLeftNum = *(header.lSibPage);
		if (fileHandle.readPage(pageLeftNum, pageLeftData) != 0)
			return -1;
		PageHeaderSection leftHeader;
		buildHeaderSection(leftHeader, pageLeftData);
		/*
		 if(pageLeftNum!=0 && *(header.parentPage)==*(leftHeader.parentPage) && usedSpace+(PAGE_SIZE-*(leftHeader.freeSpace)-HEADER_SEC_SIZE)>PAGE_SIZE-HEADER_SEC_SIZE){
		 if(redistributePage(fileHandle,attribute,pageNum,pageLeftNum)!=0)
		 return -1;
		 return 0;
		 }
		 */

		char pageRightData[PAGE_SIZE] = { 0 };
		PageNum pageRightNum = *(header.rSibPage);
		if (fileHandle.readPage(pageRightNum, pageRightData) != 0)
			return -1;
		PageHeaderSection rightHeader;
		buildHeaderSection(rightHeader, pageRightData);
		/*
		 if(pageRightNum!=0 && *(header.parentPage)==*(rightHeader.parentPage) && usedSpace+(PAGE_SIZE-*(rightHeader.freeSpace)-HEADER_SEC_SIZE)>PAGE_SIZE-HEADER_SEC_SIZE){
		 if(redistributePage(fileHandle,attribute,pageNum,pageRightNum)!=0)
		 return -1;
		 return 0;
		 }
		 */

		//need to do the merge operation
		char entryDown[PAGE_SIZE] = { 0 };
		if (pageLeftNum
				!= 0&& *(header.parentPage)==*(leftHeader.parentPage) && usedSpace+(PAGE_SIZE-*(leftHeader.freeSpace)-HEADER_SEC_SIZE)<PAGE_SIZE-HEADER_SEC_SIZE) {
			//if need to decrease height of the tree
			if (*(header.parentPage) == 0) {
				char page0Data[PAGE_SIZE] = { 0 };
				if (fileHandle.readPage(0, page0Data) != 0)
					return -1;
				PageHeaderSection page0Header;
				buildHeaderSection(page0Header, page0Data);
				if (*(page0Header.count) == 1) {
					findEntryDown(fileHandle, attribute, pageNum, entryDown);
					fileHandle.writePage(0, pageLeftData);

					// Update children of the new page 0
					unsigned int entID = 0;
					char* ptrNew = pageLeftData + HEADER_SEC_SIZE;

					for (unsigned int keySize; true; ++entID) {
						// Update the corresponding child
						char updateChildPage[PAGE_SIZE];
						PageHeaderSection hdrChild;

						if (fileHandle.readPage(*(PageNum*) ptrNew,
								updateChildPage) < 0)
							return -1;
						buildHeaderSection(hdrChild, updateChildPage);
						*(hdrChild.parentPage) = 0;
						if (fileHandle.writePage(*(PageNum*) ptrNew,
								updateChildPage) < 0)
							return -1;
						ptrNew += sizeof(PageNum);
						if (entID >= *(leftHeader.count))
							break;
						keySize = getKeySize(attribute, ptrNew);
						ptrNew += keySize;
					} // End of update

					*(header.lSibPage) = 0;
					fileHandle.writePage(pageNum, pageData);
					mergePage(fileHandle, attribute, 0, pageNum, entryDown);
					return 0;
				}
			}
			//normal merge
			findEntryDown(fileHandle, attribute, pageNum, entryDown);
			if (pageLeftNum == pageNum)
				assert(pageLeftNum != pageNum);
			mergePage(fileHandle, attribute, pageLeftNum, pageNum, entryDown);
			deleteIndexEntry(fileHandle, attribute, *(header.parentPage),
					entryDown);
			return 0;
		}
		if (pageRightNum
				!= 0&& *(header.parentPage)==*(rightHeader.parentPage) && usedSpace+(PAGE_SIZE-*(rightHeader.freeSpace)-HEADER_SEC_SIZE)<PAGE_SIZE-HEADER_SEC_SIZE) {
			//if need to decrease height of the tree
			if (*(header.parentPage) == 0) {
				char page0Data[PAGE_SIZE] = { 0 };
				if (fileHandle.readPage(0, page0Data) != 0)
					return -1;
				PageHeaderSection page0Header;
				buildHeaderSection(page0Header, page0Data);
				if (*(page0Header.count) == 1) {
					findEntryDown(fileHandle, attribute, pageRightNum,
							entryDown);
					fileHandle.writePage(0, pageData);

					// Update children of the new page 0
					unsigned int entID = 0;
					char* ptrNew = pageData + HEADER_SEC_SIZE;

					for (unsigned int keySize; true; ++entID) {
						// Update the corresponding child
						char updateChildPage[PAGE_SIZE];
						PageHeaderSection hdrChild;

						if (fileHandle.readPage(*(PageNum*) ptrNew,
								updateChildPage) < 0)
							return -1;
						buildHeaderSection(hdrChild, updateChildPage);
						*(hdrChild.parentPage) = 0;
						if (fileHandle.writePage(*(PageNum*) ptrNew,
								updateChildPage) < 0)
							return -1;
						ptrNew += sizeof(PageNum);
						if (entID >= *(header.count))
							break;
						keySize = getKeySize(attribute, ptrNew);
						ptrNew += keySize;
					} // End of update

					*(rightHeader.lSibPage) = 0;
					fileHandle.writePage(pageRightNum, pageRightData);
					mergePage(fileHandle, attribute, 0, pageRightNum,
							entryDown);
					return 0;
				}
			}
			//normal merge
			findEntryDown(fileHandle, attribute, pageRightNum, entryDown);
			if (pageRightNum == pageNum)
				assert(pageRightNum != pageNum);
			mergePage(fileHandle, attribute, pageNum, pageRightNum, entryDown);
			deleteIndexEntry(fileHandle, attribute, *(header.parentPage),
					entryDown);
			return 0;
		}
	}
	return 0;
}

void IndexManager::findEntryDown(FileHandle &fileHandle,
		const Attribute &attribute, const PageNum &pageNum, void *entryDown) {
	PageNum pagePNum;
	char pageData[PAGE_SIZE] = { 0 };
	char pagePData[PAGE_SIZE] = { 0 };
	PageHeaderSection header;
	PageHeaderSection pHeader;
	fileHandle.readPage(pageNum, pageData);
	buildHeaderSection(header, pageData);
	pagePNum = *(header.parentPage);
	fileHandle.readPage(pagePNum, pagePData);
	buildHeaderSection(pHeader, pagePData);
	char *p = pagePData + HEADER_SEC_SIZE + sizeof(int);
	char *q = p;
	if (attribute.type == TypeInt || attribute.type == TypeReal)
		q = q + sizeof(int);
	else if (attribute.type == TypeVarChar)
		q = q + sizeof(int) + *(int*) p;
	for (unsigned int i = 0; i < *(pHeader.count); i++) {
		if (*(unsigned int*) q == pageNum) {
			memcpy(entryDown, p, q - p + sizeof(int));
			return;
		} else {
			p = q + sizeof(int);
			q = p;
			if (attribute.type == TypeInt || attribute.type == TypeReal)
				q = q + sizeof(int);
			else if (attribute.type == TypeVarChar)
				q = q + sizeof(int) + *(int*) p;
		}
	}
}

RC IndexManager::simpleDeleteEntry(void *pageData, const Attribute &attribute,
		const void *deletedEntry) {
	PageHeaderSection header;
	unsigned int keyPos = findKeyOffset(pageData, attribute, deletedEntry);
	char cpBuffer[PAGE_SIZE] = { 0 };
	char* ptr = (char*) pageData + keyPos;
	unsigned int keySize = getKeySize(attribute, deletedEntry);
	unsigned int entSize;

	buildHeaderSection(header, pageData);
	if (ptr - (char*) pageData >= (PAGE_SIZE - *(header.freeSpace))
			|| compare(attribute, deletedEntry, ptr) != 0)
		return IX_ENTRY_NOT_FOUND; //-1; // Entry key not found
	if (*(header.level) == 0) { // If this is a leaf page
		entSize = keySize + sizeof(RID);
		while (ptr - (char*) pageData < (PAGE_SIZE - *(header.freeSpace))
				&& compare(attribute, deletedEntry, ptr) == 0
				&& (((RID*) ((char*) deletedEntry + keySize))->pageNum
						!= ((RID*) (ptr + keySize))->pageNum
						|| ((RID*) ((char*) deletedEntry + keySize))->slotNum
								!= ((RID*) (ptr + keySize))->slotNum)) {
//			ptr += entSize = keySize + sizeof(RID);
			ptr += entSize;
		}
	} else { // If this is an index page
		entSize = keySize + sizeof(PageNum);
		while (ptr - (char*) pageData < (PAGE_SIZE - *(header.freeSpace))
				&& compare(attribute, deletedEntry, ptr) == 0
				&& *(PageNum*) ((char*) deletedEntry + keySize)
						!= *(PageNum*) (ptr + keySize)) {
//			ptr += entSize = keySize + sizeof(PageNum);
			ptr += entSize;
		}
	}
	if (compare(attribute, deletedEntry, ptr) != 0)
		return IX_ENTRY_NOT_FOUND; //-1; // Entry RID not found
	memcpy(cpBuffer, ptr + entSize,
	PAGE_SIZE - (ptr + entSize - (char*) pageData));
	memcpy(ptr, cpBuffer, PAGE_SIZE - (ptr + entSize - (char*) pageData));
	*(header.freeSpace) += entSize;
	*(header.count) -= 1;
	return 0;
}

//static void printHeaderSection(const PageHeaderSection& header, const string& label);

RC IndexManager::mergePage(FileHandle &fileHandle, const Attribute &attribute,
		const PageNum& pageNumA, const PageNum &pageNumB,
		const void* entryDown) {
	char pageDataA[PAGE_SIZE];
	char pageDataB[PAGE_SIZE];
	PageHeaderSection hdrA;
	PageHeaderSection hdrB;

//	printf("mergePage: pageNumA = %d, pageNumB = %d\n", pageNumA, pageNumB);
	assert(pageNumA != pageNumB);

	if (fileHandle.readPage(pageNumA, pageDataA) < 0)
		return -1;
	if (fileHandle.readPage(pageNumB, pageDataB) < 0)
		return -1;
	buildHeaderSection(hdrA, pageDataA);
	buildHeaderSection(hdrB, pageDataB);

	unsigned int insertPos = findKeyOffset(pageDataA, attribute, entryDown);
	char* ptr = pageDataA + insertPos;
	char* entB = pageDataB + HEADER_SEC_SIZE;
	unsigned int keySize = getKeySize(attribute, entryDown);
	char pageEmpty[PAGE_SIZE] = { 0 };

//	printHeaderSection(hdrA, "A");
//	printHeaderSection(hdrB, "B");
	*(hdrA.rSibPage) = *(hdrB.rSibPage);

	if (*(hdrA.level) > 0) { // If this is an index page
		memcpy(ptr, entryDown, keySize); // Pull down
		ptr += keySize;
		*(hdrA.freeSpace) -= keySize;
		*(hdrA.count) += 1;
	}

	unsigned int entID = *(hdrA.count);

	// Copy entries from B to A
//	printf("Copy: A[%d] <- B[%d] %d\n",
//			ptr - pageDataA, entB - pageDataB,
//			PAGE_SIZE - HEADER_SEC_SIZE - *(hdrB.freeSpace));
	memcpy(ptr, entB, PAGE_SIZE - HEADER_SEC_SIZE - *(hdrB.freeSpace));
	*(hdrA.freeSpace) -= PAGE_SIZE - HEADER_SEC_SIZE - *(hdrB.freeSpace);
	*(hdrA.count) += *(hdrB.count);

	// Write the two pages to the file
	if (fileHandle.writePage(pageNumA, pageDataA) < 0)
		return -1;
	if (fileHandle.writePage(pageNumB, pageEmpty) < 0)
		return -1;

	// Change lSibPage of the previous right sibling of the page to wipe
	if (*(hdrA.rSibPage) > 0) {
		char updateSib[PAGE_SIZE] = { 0 };
		PageHeaderSection hdrSib;

		if (fileHandle.readPage(*(hdrA.rSibPage), updateSib) < 0)
			return -1;
		buildHeaderSection(hdrSib, updateSib);
		*(hdrSib.lSibPage) = pageNumA;
		if (fileHandle.writePage(*(hdrA.rSibPage), updateSib) < 0)
			return -1;
	}

	// Update all children of the page to wipe
	if (*(hdrA.level) > 0) { // If this is an index page
		for (unsigned int keySize; true; ++entID) {
			// Update the corresponding child
			char updateChildPage[PAGE_SIZE];
			PageHeaderSection hdrChild;

			if (fileHandle.readPage(*(PageNum*) ptr, updateChildPage) < 0)
				return -1;
			buildHeaderSection(hdrChild, updateChildPage);
			*(hdrChild.parentPage) = pageNumA;
			if (fileHandle.writePage(*(PageNum*) ptr, updateChildPage) < 0)
				return -1;
			ptr += sizeof(PageNum);
			if (entID >= *(hdrA.count))
				break;
			keySize = getKeySize(attribute, ptr);
			ptr += keySize;
		}
	}
//	printHeaderSection(hdrA, "A");
	return 0;
}

/*
 static void printHeaderSection(const PageHeaderSection& header, const string& label){
 cout << label << ": "
 << "parent = " << *(header.parentPage) << ", "
 << "lSib = " << *(header.lSibPage) << ", "
 << "rSib = " << *(header.rSibPage) << ", "
 << "level = " << *(header.level) << ", "
 << "count = " << *(header.count) << ", "
 << "freeSp = " << *(header.freeSpace) << endl;
 }*/

RC IX_ScanIterator::setupIterator(FileHandle &fileHandle,
		const Attribute &attribute, const void *lowKey, const void *highKey,
		bool lowKeyInclusive, bool highKeyInclusive) {
	return setupIterator(fileHandle.getNameStr(), attribute, lowKey, highKey,
			lowKeyInclusive, highKeyInclusive);
}

RC IX_ScanIterator::setupIterator(const string& fileName,
		const Attribute &attribute, const void *lowKey, const void *highKey,
		bool lowKeyInclusive, bool highKeyInclusive) {
	IndexManager* ixm = IndexManager::instance();

	if (ixm->openFile(fileName, this->fileHandle) < 0)
		return -1;
	this->attribute = attribute;
	this->lowKey = lowKey;
	this->highKey = highKey;
	this->lowKeyInclusive = lowKeyInclusive;
	this->highKeyInclusive = highKeyInclusive;

	char pageData[PAGE_SIZE] = { 0 };
	PageHeaderSection header;
	unsigned int nextEntryPageNum = ixm->findKeyPage(fileHandle, attribute,
			lowKey);
	unsigned int nextEntryOffset;

	ixm->buildHeaderSection(header, pageData);
	nextEntryOffset = HEADER_SEC_SIZE; //ixm->findKeyOffset(pageData, attribute, lowKey);
	for (; true;) {
		if (fileHandle.readPage(nextEntryPageNum, pageData) < 0) {
			IndexManager::instance()->closeFile(fileHandle);
			return -1;
		} // Load the leaf page

		if (nextEntryOffset >= PAGE_SIZE - *(header.freeSpace)) { // Offset out of range
			if (*(header.rSibPage) > 0) { // If there is a right sibling
				nextEntryPageNum = *(header.rSibPage);
				nextEntryOffset = HEADER_SEC_SIZE;
				continue;
			} // Else the iteration has reached EOF
			nextEntryPageNum = 0;
			nextEntryOffset = 0;
			nextEntryAvailable = false;
			return 0;
		}

		char* ptr = pageData + nextEntryOffset;
		unsigned int keySize = getKeySize(attribute, ptr);
		unsigned int entSize = keySize + sizeof(RID);

		if (!((ixm->compare(attribute, ptr, lowKey) > 0
				|| (lowKeyInclusive && ixm->compare(attribute, ptr, lowKey) == 0))
				&& (ixm->compare(attribute, ptr, highKey) < 0
						|| (highKeyInclusive
								&& ixm->compare(attribute, ptr, highKey) == 0)))) { // If not in range: go to the next entry
			nextEntryOffset += entSize;
		} else
			break;
	}

	char* ptr = pageData + nextEntryOffset;
	unsigned int keySize = getKeySize(attribute, ptr);
	unsigned int entSize = keySize + sizeof(RID);

	memcpy(nextEntry, ptr, entSize);
	nextEntryAvailable = true;
	return 0;
}

void IndexManager::printTree(FileHandle fileHandle) {
	char data[PAGE_SIZE] = { 0 };
	PageHeaderSection header;
	buildHeaderSection(header, data);
	PageNum pageNum = 0;
	PageNum next;
	do {
		next = pageNum;
		pageNum = *(int*) (data + HEADER_SEC_SIZE);
		fileHandle.readPage(next, data);
		printf("PageNum=%d ", next);
		while (*(header.rSibPage) != 0) {
			next = *(header.rSibPage);
			fileHandle.readPage(next, data);
			printf("PageNum=%d ", next);
		}
		printf("\n\n\n");
	} while (*(header.level) != 0);
}
