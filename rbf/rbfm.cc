#include <cstdio>
#include <cstring>
#include <vector>
#include <cassert>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	if (PagedFileManager::instance()->createFile(fileName.c_str()) < 0)
		return -1;

	FileHandle handle;
	HeaderPage header = { 0 };

	PagedFileManager::instance()->openFile(fileName.c_str(), handle);
	handle.appendPage(&header);
	PagedFileManager::instance()->closeFile(handle);
	return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return PagedFileManager::instance()->destroyFile(fileName.c_str());
}

RC RecordBasedFileManager::openFile(const string &fileName,
		FileHandle &fileHandle) {
	return PagedFileManager::instance()->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	unsigned int length = getRecordLength(recordDescriptor, data) + sizeof(int);
	PageNum pageNum = findRecordPage(fileHandle, length);

	if (pageNum == 0) {
		if (length > PAGE_SIZE - 4 * sizeof(int))
			return -1;
		pageNum = newPage(fileHandle);
	}

	unsigned char pageData[PAGE_SIZE];
	DataPage pageDesc;

	if (fileHandle.readPage(pageNum, pageData))
		return -1;
	pageDesc.records = pageData;
	pageDesc.slotDirectory = (int*) (pageData + PAGE_SIZE) - 5;
	pageDesc.freeSpaceOffset = (unsigned int*) pageDesc.slotDirectory + 2;
	pageDesc.recordCount = (unsigned int*) pageDesc.slotDirectory + 1;
	pageDesc.headerEntryPage = (unsigned int*) pageDesc.slotDirectory + 4;
	pageDesc.headerEntryID = (unsigned int*) pageDesc.slotDirectory + 3;

	unsigned int freeSpaceContiguous = PAGE_SIZE
			- sizeof(int) * (4 + *(pageDesc.recordCount))
			- *(pageDesc.freeSpaceOffset);

	// If contiguous free space is NOT enough to save the record
	if (freeSpaceContiguous < length) {
		reorganizePage(fileHandle, recordDescriptor, pageNum);
		return insertRecord(fileHandle, recordDescriptor, data, rid);
	}

	unsigned int* fieldCount = (unsigned int*) (pageData
			+ *(pageDesc.freeSpaceOffset));
	unsigned int* fieldOffset = fieldCount + 1;
	const char* pData = (const char*) data;

	*fieldCount = recordDescriptor.size();

	for (unsigned int k = 0; k < *fieldCount; ++k) {
		unsigned int varL;

		switch (recordDescriptor.at(k).type) {
		case TypeInt:
			varL = sizeof(int);
			break;
		case TypeReal:
			varL = sizeof(float);
			break;
		case TypeVarChar:
			varL = *(const unsigned int*) pData + sizeof(unsigned int);
			break;
		}
		if (k < 1) {
			memcpy((unsigned int*) fieldCount + 1 + *fieldCount, pData, varL);
			fieldOffset[k] = sizeof(unsigned int) * (1 + *fieldCount) + varL;
		} else {
			memcpy((char*) fieldCount + fieldOffset[k - 1], pData, varL);
			fieldOffset[k] = fieldOffset[k - 1] + varL;
		}
		pData += varL;
	}

	unsigned int r;

	for (r = 0; r < *(pageDesc.recordCount); ++r)
		if (*(pageDesc.slotDirectory - r) < 0)
			break;
	if (r >= *(pageDesc.recordCount)) {
		*(pageDesc.recordCount) += 1;
	}
	*(pageDesc.slotDirectory - r) = *(pageDesc.freeSpaceOffset);
	*(pageDesc.freeSpaceOffset) += fieldOffset[*fieldCount - 1];
	rid.pageNum = pageNum;
	rid.slotNum = r;
	if (fileHandle.writePage(pageNum, pageData) < 0)
		return -1;

	unsigned int headerData[PAGE_SIZE / sizeof(int)] = { 0 };
	HeaderPage* headerPage = (HeaderPage*) headerData;

	/*	PageNum headerPageNum=0;

	 while(fileHandle.readPage(headerPageNum, headerData) == 0){
	 HeaderPage* headerPage = (HeaderPage*)headerData;

	 for(unsigned i = 0; i < headerPage->localPageCount; i++)
	 if(headerPage->entries[i].pageNum == pageNum){
	 headerPage->entries[i].freeSpace -= length;
	 if(fileHandle.writePage(headerPageNum, headerData) < 0)
	 return -1;
	 return 0;
	 }
	 headerPageNum = headerPage->nextHeaderPage;
	 if(headerPageNum == 0)
	 return -1;
	 }*/

	if (fileHandle.readPage(*(pageDesc.headerEntryPage), headerData) < 0)
		return -1;
	headerPage->entries[*(pageDesc.headerEntryID)].freeSpace -= length;
	if (fileHandle.writePage(*(pageDesc.headerEntryPage), headerData) < 0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::printRecord(
		const vector<Attribute> &recordDescriptor, const void *data) {
	const char* pData = (const char*) data;

	for (unsigned int k = 0; k < recordDescriptor.size(); ++k) {
		switch (recordDescriptor.at(k).type) {
		case TypeInt:
			printf("%d", *(int*) pData);
			pData += sizeof(int);
			break;
		case TypeReal:
			printf("%f", *(float*) pData);
			pData += sizeof(float);
			break;
		case TypeVarChar:
			for (int t = 0; t < *(int*) pData; ++t) {
				printf("%c", *((char*) pData + t + sizeof(int)));
			}
			pData += sizeof(int) + *(int*) pData;
			break;
		}
		if (k < recordDescriptor.size() - 1)
			printf(", ");
	}
	printf("\n");
	return 0;
}

unsigned RecordBasedFileManager::getRecordLength(
		const vector<Attribute> &recordDescriptor, const void *data) {
	int length = (1 + recordDescriptor.size()) * sizeof(unsigned);
	const char* p = (const char*) data;

	for (unsigned int i = 0; i < recordDescriptor.size(); i++) {
		if (recordDescriptor[i].type == TypeInt) {
			length += sizeof(int);
			p += sizeof(int);
		} else if (recordDescriptor[i].type == TypeReal) {
			length += sizeof(float);
			p += sizeof(float);
		} else if (recordDescriptor[i].type == TypeVarChar) {
			int l = *(int*) p;
			length += sizeof(int) + l;
			p += sizeof(int) + l;
		}
	}
	return length;
}

PageNum RecordBasedFileManager::findRecordPage(FileHandle &fileHandle,
		const unsigned length) {
	unsigned int data[PAGE_SIZE / sizeof(int)] = { 0 };
	PageNum headerPageNum = 0;

	while (fileHandle.readPage(headerPageNum, data) == 0) {
		HeaderPage* headerPage = (HeaderPage*) data;

		for (unsigned i = 0; i < headerPage->localPageCount; i++)
			if (headerPage->entries[i].freeSpace >= length + sizeof(int))
				return headerPage->entries[i].pageNum;
		headerPageNum = headerPage->nextHeaderPage;
		if (headerPageNum == 0)
			return 0;
	}
	return 0;
}

PageNum RecordBasedFileManager::newPage(FileHandle &fileHandle) {
	char pageData[PAGE_SIZE] = { 0 };

	if (fileHandle.appendPage(pageData) != 0)
		return 0;

	PageNum dataPageNum = fileHandle.getNumberOfPages() - 1;
	PageNum headerPageNum = 0;
	PageNum lastHeaderPageNum = 0;
	int headerData[PAGE_SIZE / sizeof(int)] = { 0 };
	bool f = true;
	HeaderPage* headerPage;
	DataPage pageDesc;

	pageDesc.records = pageData;
	pageDesc.slotDirectory = (int*) (pageData + PAGE_SIZE) - 5;
	pageDesc.freeSpaceOffset = (unsigned int*) pageDesc.slotDirectory + 2;
	pageDesc.recordCount = (unsigned int*) pageDesc.slotDirectory + 1;
	pageDesc.headerEntryPage = (unsigned int*) pageDesc.slotDirectory + 4;
	pageDesc.headerEntryID = (unsigned int*) pageDesc.slotDirectory + 3;

	while (f && fileHandle.readPage(headerPageNum, headerData) == 0) {
		headerPage = (HeaderPage*) headerData;
		if (headerPage->localPageCount < (PAGE_SIZE / sizeof(int) - 2) / 2) {
			headerPage->entries[headerPage->localPageCount].pageNum =
					dataPageNum;
			headerPage->entries[headerPage->localPageCount].freeSpace =
			PAGE_SIZE - 4 * sizeof(int);
			*(pageDesc.headerEntryPage) = headerPageNum;
			*(pageDesc.headerEntryID) = headerPage->localPageCount++;
			if (fileHandle.writePage(headerPageNum, headerPage) < 0)
				return -1;
			if (fileHandle.writePage(dataPageNum, pageData) < 0)
				return -1;
			return dataPageNum;
		} else {
			lastHeaderPageNum = headerPageNum;
			headerPageNum = headerPage->nextHeaderPage;
			if (headerPageNum == 0)
				f = false;
		}
	}
	if (!f) {
		HeaderPage newHeaderPage;

		newHeaderPage.localPageCount = 1;
		newHeaderPage.nextHeaderPage = 0;
		newHeaderPage.entries[0].pageNum = dataPageNum;
		newHeaderPage.entries[0].freeSpace = PAGE_SIZE - 4 * sizeof(int);
		if (fileHandle.appendPage(&newHeaderPage) != 0)
			return 0;
		headerPageNum = fileHandle.getNumberOfPages() - 1;
		*(pageDesc.headerEntryPage) = headerPageNum;
		*(pageDesc.headerEntryID) = 0;
		headerPage->nextHeaderPage = headerPageNum;
		if (fileHandle.writePage(lastHeaderPageNum, headerPage) < 0)
			return -1;
		if (fileHandle.writePage(dataPageNum, pageData) < 0)
			return -1;
		return dataPageNum;
	}
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	char pageData[PAGE_SIZE];
	DataPage pageDesc;

	if (fileHandle.readPage(rid.pageNum, pageData) != 0)
		return -1;
	pageDesc.records = pageData;
	pageDesc.slotDirectory = (int*) (pageData + PAGE_SIZE) - 5;
	pageDesc.freeSpaceOffset = (unsigned int*) pageDesc.slotDirectory + 2;
	pageDesc.recordCount = (unsigned int*) pageDesc.slotDirectory + 1;
	pageDesc.headerEntryPage = (unsigned int*) pageDesc.slotDirectory + 4;
	pageDesc.headerEntryID = (unsigned int*) pageDesc.slotDirectory + 3;

	if (checkRID(fileHandle, rid) < 0)
		return -1;

	int offset = *(pageDesc.slotDirectory - rid.slotNum);
	char* p = pageData + offset;

	if (*(int*) p == TOMBSTONE) {
		return readRecord(fileHandle, recordDescriptor,
				*(RID*) (p + sizeof(int)), data);
	}

	int dif = recordDescriptor.size() - *(int*) p;
	int length = *((int*) p + *(int*) p) - (1 + *(int*) p) * sizeof(int);

	p += *(int*) p * sizeof(int) + sizeof(int);
	memcpy(data, p, length);
	for (int i = 0; i < dif; i++)
		*(int*) (((char*) data + length) + i) = 0;

	return 0;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const unsigned pageNumber) {
	char pageData[PAGE_SIZE] = { 0 };
	DataPage pageDesc;

	if (fileHandle.readPage(pageNumber, pageData))
		return -1;
	pageDesc.records = pageData;
	pageDesc.slotDirectory = (int*) (pageData + PAGE_SIZE) - 5;
	pageDesc.freeSpaceOffset = (unsigned int*) pageDesc.slotDirectory + 2;
	pageDesc.recordCount = (unsigned int*) pageDesc.slotDirectory + 1;
	pageDesc.headerEntryPage = (unsigned int*) pageDesc.slotDirectory + 4;
	pageDesc.headerEntryID = (unsigned int*) pageDesc.slotDirectory + 3;

	char cpyBuffer[PAGE_SIZE];
	char* pNew = pageData;
	vector<bool> reorganized(*(pageDesc.recordCount), false);

	for (unsigned int k = 0; k < *(pageDesc.recordCount); ++k) {
		unsigned int cp_min;

		for (cp_min = 0;
				cp_min < *(pageDesc.recordCount) && reorganized.at(cp_min);
				++cp_min)
			;
		for (unsigned int j = 0; j < *(pageDesc.recordCount); ++j)
			if (!reorganized.at(j)
					&& *(pageDesc.slotDirectory - j)
							< *(pageDesc.slotDirectory - cp_min))
				cp_min = j;
		reorganized.at(cp_min) = true;
		if (*(pageDesc.slotDirectory - cp_min) < 0)
			continue;

		int* fieldCount = (int*) (pageData + *(pageDesc.slotDirectory - cp_min));
		int recordLen;

		if (*fieldCount == TOMBSTONE)
			recordLen = sizeof(int) * 3;
		else
			recordLen = fieldCount[*fieldCount];
		memcpy(cpyBuffer, fieldCount, recordLen);
		memcpy(pNew, cpyBuffer, recordLen);
		*(pageDesc.slotDirectory - cp_min) = pNew - pageData;
		pNew += recordLen;
	}
	*(pageDesc.freeSpaceOffset) = pNew - pageData;
	if (fileHandle.writePage(pageNumber, pageData) < 0)
		return -1;
	for (unsigned int k = 0; k < *(pageDesc.recordCount); ++k)
		assert(reorganized.at(k));
	return 0;
}

RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle) {
	char headerData[PAGE_SIZE] = { 0 };

	if (fileHandle.readPage(0, headerData) != 0)
		return -1;

	HeaderPage* headerPage;

	headerPage = (HeaderPage*) headerData;
	headerPage->localPageCount = 0;
	headerPage->nextHeaderPage = 0;
	if (fileHandle.writePage(0, headerData) != 0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {
	char pageData[PAGE_SIZE] = { 0 };
	DataPage pageDesc;

	if (fileHandle.readPage(rid.pageNum, pageData) != 0)
		return -1;
	pageDesc.records = pageData;
	pageDesc.slotDirectory = (int*) (pageData + PAGE_SIZE) - 5;
	pageDesc.freeSpaceOffset = (unsigned int*) pageDesc.slotDirectory + 2;
	pageDesc.recordCount = (unsigned int*) pageDesc.slotDirectory + 1;
	pageDesc.headerEntryPage = (unsigned int*) pageDesc.slotDirectory + 4;
	pageDesc.headerEntryID = (unsigned int*) pageDesc.slotDirectory + 3;

	*(pageDesc.slotDirectory - rid.slotNum) = -1;
	if (fileHandle.writePage(rid.pageNum, pageData) != 0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data,
		const RID &rid) {
	//insert the new data 
	RID rid2;
	insertRecord(fileHandle, recordDescriptor, data, rid2);

	//change the free space
	char dataOld[PAGE_SIZE] = { 0 };
	readRecord(fileHandle, recordDescriptor, rid, dataOld);

	int lengthOld = getRecordLength(recordDescriptor, dataOld);
	int lengthNew = sizeof(int) + sizeof(RID);

	//find the rid in header page
	/*	bool flag = false;
	 unsigned int headerData[PAGE_SIZE / sizeof(int)] = {0};

	 PageNum headerPageNum = 0;
	 while(!flag && fileHandle.readPage(headerPageNum, headerData) == 0){
	 HeaderPage* headerPage = (HeaderPage*)headerData;
	 for(unsigned i = 0; i < headerPage->localPageCount; i++)
	 if(headerPage->entries[i].pageNum == rid.pageNum){
	 headerPage->entries[i].freeSpace += lengthOld - lengthNew;
	 if(fileHandle.writePage(headerPageNum, headerPage) < 0)
	 return -1;
	 flag = true;
	 break;
	 }
	 headerPageNum = headerPage->nextHeaderPage;
	 }*/

	//change the old record to TOMBSTONE
	char pageData[PAGE_SIZE] = { 0 };
	DataPage pageDesc;

	if (fileHandle.readPage(rid.pageNum, pageData) != 0)
		return -1;
	pageDesc.records = pageData;
	pageDesc.slotDirectory = (int*) (pageData + PAGE_SIZE) - 5;
	pageDesc.freeSpaceOffset = (unsigned int*) pageDesc.slotDirectory + 2;
	pageDesc.recordCount = (unsigned int*) pageDesc.slotDirectory + 1;
	pageDesc.headerEntryPage = (unsigned int*) pageDesc.slotDirectory + 4;
	pageDesc.headerEntryID = (unsigned int*) pageDesc.slotDirectory + 3;

//	int offset = PAGE_SIZE - 1 - (2 + rid.slotNum) * sizeof(int);
	int offset = *(pageDesc.slotDirectory - rid.slotNum);

	*(int*) (pageData + offset) = TOMBSTONE;
	memcpy(pageData + offset + sizeof(int), &rid2, sizeof(RID));
	if (fileHandle.writePage(rid.pageNum, pageData) < 0)
		return -1;

	unsigned int headerData[PAGE_SIZE / sizeof(int)] = { 0 };
	HeaderPage* headerPage = (HeaderPage*) headerData;

	if (fileHandle.readPage(*(pageDesc.headerEntryPage), headerData) < 0)
		return -1;
	headerPage->entries[*(pageDesc.headerEntryID)].freeSpace += lengthOld
			- lengthNew;
	if (fileHandle.writePage(*(pageDesc.headerEntryPage), headerData) < 0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid,
		const string attributeName, void *data) {
	char dataAux[PAGE_SIZE] = { 0 };
	readRecord(fileHandle, recordDescriptor, rid, dataAux);
	char* p = dataAux;
	unsigned int i = 0;

	while (i < recordDescriptor.size()
			&& recordDescriptor[i].name.compare(attributeName) != 0) {
		if (recordDescriptor[i].type == TypeInt)
			p += sizeof(int);
		else if (recordDescriptor[i].type == TypeReal)
			p += sizeof(float);
		else if (recordDescriptor[i].type == TypeVarChar)
			p += sizeof(int) + *(int*) p;
		i++;
	}
	if (i == recordDescriptor.size())
		return -1;
	if (recordDescriptor[i].type == TypeInt)
		memcpy(data, p, sizeof(int));
	else if (recordDescriptor[i].type == TypeReal)
		memcpy(data, p, sizeof(float));
	else if (recordDescriptor[i].type == TypeVarChar)
		memcpy(data, p, sizeof(int) + *(int*) p);
	return 0;
}

RBFM_ScanIterator::RBFM_ScanIterator() :
		conditionAttribute(""), compOp(NO_OP), value(NULL), conditionType(
				TypeInt) {
	nextRID.pageNum = 0;
	nextRID.slotNum = 0;
}

RBFM_ScanIterator::~RBFM_ScanIterator() {
	RecordBasedFileManager::instance()->closeFile(fileHandle);
}

RC RBFM_ScanIterator::setupIterator(const FileHandle& fileHandle,
		const vector<Attribute>& recordDescriptor,
		const string& conditionAttribute, const CompOp& compOp,
		const void* value, const vector<string>& attributeNames) {
	return setupIterator(fileHandle.getNameStr(), recordDescriptor,
			conditionAttribute, compOp, value, attributeNames);
}
RC RBFM_ScanIterator::setupIterator(const string& fileName,
		const vector<Attribute>& recordDescriptor,
		const string& conditionAttribute, const CompOp& compOp,
		const void* value, const vector<string>& attributeNames) {
	RecordBasedFileManager::instance()->openFile(fileName, this->fileHandle);
	this->recordDescriptor = recordDescriptor;
	this->conditionAttribute = conditionAttribute;
	this->compOp = compOp;
	this->value = value;
	this->attributeNames = attributeNames;
	for (unsigned int k = 0; k < recordDescriptor.size(); ++k)
		if (recordDescriptor.at(k).name == conditionAttribute)
			conditionType = recordDescriptor.at(k).type;

	char headerData[PAGE_SIZE];
	HeaderPage* headerPage = (HeaderPage*) headerData;

	if (this->fileHandle.readPage(0, headerData) < 0) {
		RecordBasedFileManager::instance()->closeFile(this->fileHandle);
		return -1;
	}
	if (headerPage->localPageCount == 0)
		nextRID.pageNum = 0;
	else
		nextRID.pageNum = headerPage->entries[0].pageNum;
	nextRID.slotNum = 0;
	return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator) {

	return rbfm_ScanIterator.setupIterator(fileHandle, recordDescriptor,
			conditionAttribute, compOp, value, attributeNames);
}

static bool checkValue(const AttrType& type, const CompOp& compOp,
		const void* data, const void* value);

RC RBFM_ScanIterator::getNextRecord(RID& rid, void* data) {
	if (nextRID.pageNum == 0) {
		nextRID.slotNum = 0;
		rid.pageNum = 0;
		rid.slotNum = 0;
		return RBFM_EOF;
	}

	char pageData[PAGE_SIZE];
	char headerData[PAGE_SIZE];
	char attrData[PAGE_SIZE];
	DataPage pageDesc;
	HeaderPage* headerPage = (HeaderPage*) headerData;

	if (fileHandle.readPage(nextRID.pageNum, pageData) < 0) {
		nextRID.pageNum = 0;
		nextRID.slotNum = 0;
		rid.pageNum = 0;
		rid.slotNum = 0;
		return RBFM_EOF;
	}
	pageDesc.records = pageData;
	pageDesc.slotDirectory = (int*) (pageData + PAGE_SIZE) - 5;
	pageDesc.freeSpaceOffset = (unsigned int*) pageDesc.slotDirectory + 2;
	pageDesc.recordCount = (unsigned int*) pageDesc.slotDirectory + 1;
	pageDesc.headerEntryPage = (unsigned int*) pageDesc.slotDirectory + 4;
	pageDesc.headerEntryID = (unsigned int*) pageDesc.slotDirectory + 3;

	unsigned int headerEntryPage = *(pageDesc.headerEntryPage);
	unsigned int headerEntryID = *(pageDesc.headerEntryID);

	if (fileHandle.readPage(headerEntryPage, headerData) < 0)
		return -41; // Consistency fault
	while (true) {
		if (headerEntryID + 1 > headerPage->localPageCount) {
			if (headerPage->nextHeaderPage == 0) {
				nextRID.pageNum = 0;
				break;
			}
			headerEntryPage = headerPage->nextHeaderPage;
			if (fileHandle.readPage(headerEntryPage, headerData) < 0)
				return -41; // Consistency fault
			headerEntryID = 0;
			continue;
		}
		nextRID.pageNum = headerPage->entries[headerEntryID].pageNum;
		if (fileHandle.readPage(nextRID.pageNum, pageData) < 0)
			return -41; // Consistency fault
		if (nextRID.slotNum > *(pageDesc.recordCount) - 1) {
			headerEntryID += 1;
			nextRID.slotNum = 0;
			continue;
		}
		if (RecordBasedFileManager::instance()->checkRID(fileHandle, nextRID)
				< 0) {
			nextRID.slotNum += 1;
			continue;
		}
		if (*(int*) (pageData + *(pageDesc.slotDirectory - nextRID.slotNum))
				== TOMBSTONE) {
			nextRID.slotNum += 1;
			continue;
		} // Ignore tombstone in case of duplication
		RecordBasedFileManager::instance()->readAttribute(fileHandle,
				recordDescriptor, nextRID, conditionAttribute, attrData);
		if (!checkValue(conditionType, compOp, attrData, value)) {
			nextRID.slotNum += 1;
			continue;
		}
		break;
	}
	if (nextRID.pageNum == 0) {
		nextRID.slotNum = 0;
		rid.pageNum = 0;
		rid.slotNum = 0;
		return RBFM_EOF;
	}

//	char retrData[PAGE_SIZE];
	char* p = (char*) data;
	map<string, AttrType> recordTypes;

//	RecordBasedFileManager::instance()->readRecord(fileHandle,
//			recordDescriptor, nextRID, retrData);
	for (unsigned int k = 0; k < recordDescriptor.size(); ++k)
		recordTypes[recordDescriptor.at(k).name] = recordDescriptor.at(k).type;
	for (unsigned int k = 0; k < attributeNames.size(); ++k) {
		RecordBasedFileManager::instance()->readAttribute(fileHandle,
				recordDescriptor, nextRID, attributeNames.at(k), p);
		switch (recordTypes[attributeNames.at(k)]) {
		case TypeInt:
			p += sizeof(int);
			break;
		case TypeReal:
			p += sizeof(float);
			break;
		case TypeVarChar:
			p += sizeof(int) + *(int*) p;
			break;
		}
	}
	rid = nextRID;
	nextRID.slotNum += 1;
	return 0;
}

static bool checkValue(const AttrType& type, const CompOp& compOp,
		const void* data, const void* value) {
	switch (type) {
	case TypeInt:
		switch (compOp) {
		case EQ_OP:
			if (*(int*) data == *(int*) value)
				return true;
			break;
		case LT_OP:
			if (*(int*) data < *(int*) value)
				return true;
			break;
		case GT_OP:
			if (*(int*) data > *(int*) value)
				return true;
			break;
		case LE_OP:
			if (*(int*) data <= *(int*) value)
				return true;
			break;
		case GE_OP:
			if (*(int*) data >= *(int*) value)
				return true;
			break;
		case NE_OP:
			if (*(int*) data != *(int*) value)
				return true;
			break;
		case NO_OP:
			return true;
		}
		break;
	case TypeReal:
		switch (compOp) {
		case EQ_OP:
			if (*(float*) data == *(float*) value)
				return true;
			break;
		case LT_OP:
			if (*(float*) data < *(float*) value)
				return true;
			break;
		case GT_OP:
			if (*(float*) data > *(float*) value)
				return true;
			break;
		case LE_OP:
			if (*(float*) data <= *(float*) value)
				return true;
			break;
		case GE_OP:
			if (*(float*) data >= *(float*) value)
				return true;
			break;
		case NE_OP:
			if (*(float*) data != *(float*) value)
				return true;
			break;
		case NO_OP:
			return true;
		}
		break;
	case TypeVarChar:
		*((char*) data + *(int*) data + sizeof(int)) = '\0';

		string strData((char*) data + sizeof(int));
		string strValue((char*) value);

		switch (compOp) {
		case EQ_OP:
			if (strData == strValue)
				return true;
			break;
		case LT_OP:
			if (strData < strValue)
				return true;
			break;
		case GT_OP:
			if (strData > strValue)
				return true;
			break;
		case LE_OP:
			if (strData <= strValue)
				return true;
			break;
		case GE_OP:
			if (strData >= strValue)
				return true;
			break;
		case NE_OP:
			if (strData != strValue)
				return true;
			break;
		case NO_OP:
			return true;
		}
		break;
	}
	return false;
}

RC RBFM_ScanIterator::close() {
	nextRID.pageNum = 0;
	nextRID.slotNum = 0;
	RecordBasedFileManager::instance()->closeFile(fileHandle);
	return 0;
}

const RC RecordBasedFileManager::checkRID(FileHandle& fileHandle,
		const RID& rid) const {
	char pageData[PAGE_SIZE];
	char headerData[PAGE_SIZE];
	DataPage pageDesc;
	HeaderPage* headerPage = (HeaderPage*) headerData;

	pageDesc.records = pageData;
	pageDesc.slotDirectory = (int*) (pageData + PAGE_SIZE) - 5;
	pageDesc.freeSpaceOffset = (unsigned int*) pageDesc.slotDirectory + 2;
	pageDesc.recordCount = (unsigned int*) pageDesc.slotDirectory + 1;
	pageDesc.headerEntryPage = (unsigned int*) pageDesc.slotDirectory + 4;
	pageDesc.headerEntryID = (unsigned int*) pageDesc.slotDirectory + 3;
	if (fileHandle.readPage(rid.pageNum, pageData) < 0)
		return -101; // Cannot find data page
	if (fileHandle.readPage(*(pageDesc.headerEntryPage), headerData) < 0)
		return -102; // Cannot find header page
	if (*(pageDesc.headerEntryID) >= headerPage->localPageCount)
		return -103; // The data page cannot find its entry in the header page
	if (headerPage->entries[*(pageDesc.headerEntryID)].pageNum != rid.pageNum)
		return -104; // The data page cannot find its entry in the header page
	if (rid.slotNum >= *(pageDesc.recordCount))
		return -105; // The record slot was dropped
	if (*(pageDesc.slotDirectory - rid.slotNum) < 0)
		return -106; // The record was dropped
	return 0;
}

RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor) {
	string fileNameTemp = FILE_NAME_TEMP_REORG;
	if (createFile(fileNameTemp) != 0)
		return -1;
	FileHandle fileHandleTemp;
	if (openFile(fileNameTemp, fileHandleTemp) != 0)
		return -1;

	string conditionAttribute;
	CompOp compOp = NO_OP;
	void *value = NULL;
	vector<string> attributeNames;
	RBFM_ScanIterator it;
	for (unsigned int i = 0; i < recordDescriptor.size(); i++)
		attributeNames.push_back(recordDescriptor[i].name);
	if (scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value,
			attributeNames, it) != 0)
		return -1;

	RID rid;
	vector<RID> ridTemp;
	char data[PAGE_SIZE];
	while (it.getNextRecord(rid, data) != RBFM_EOF) {
		insertRecord(fileHandleTemp, recordDescriptor, data, rid);
		ridTemp.push_back(rid);
	}

	if (deleteRecords(fileHandle) != 0)
		return -1;

	for (unsigned int i = 0; i < ridTemp.size(); i++) {
		if (readRecord(fileHandleTemp, recordDescriptor, ridTemp[i], data) != 0)
			return -1;
		if (insertRecord(fileHandle, recordDescriptor, data, rid) != 0)
			return -1;
	}
	if (closeFile(fileHandleTemp) != 0)
		return -1;
	if (destroyFile(fileNameTemp) != 0)
		return -1;
	return 0;
}

