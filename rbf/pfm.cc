#include <cstdio>
#include <string>

#include "pfm.h"

using namespace std;

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance() {
	if (!_pf_manager)
		_pf_manager = new PagedFileManager();

	return _pf_manager;
}

PagedFileManager::PagedFileManager() {
}

PagedFileManager::~PagedFileManager() {
	map<string, pair<FILE*, unsigned int> >::iterator it;

	for (it = fileHandleCounter.begin(); it != fileHandleCounter.end(); ++it)
		fclose(it->second.first);
}

RC PagedFileManager::createFile(const char *fileName) {
	FILE* pFile;

	pFile = fopen(fileName, "rb");
	if (pFile != NULL) {
		fclose(pFile);
		return -13; // File exists
	}

	pFile = fopen(fileName, "wb");
	if (pFile == NULL)
		return -14; // Cannot open file for writing
	fclose(pFile);
	return 0;
}

RC PagedFileManager::destroyFile(const char *fileName) {
	string fileNameStr(fileName);

	if (fileHandleCounter.count(fileName) > 0)
		return -11; // File is open
	if (remove(fileName) < 0)
		return -15; // Cannot remove file
	return 0;
}

RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle) {
	string fileNameStr(fileName);

	if (fileHandle.isOpen())
		return -21; // Handle is open

	if (fileHandleCounter.count(fileNameStr) == 0) {
		FILE* pFile;

		pFile = fopen(fileName, "rb+");
		if (pFile == NULL)
			return -12; // File does not exist

		fileHandle.setupHandle(fileNameStr, pFile);
		fileHandleCounter[fileNameStr] = pair<FILE*, unsigned int>(pFile, 1);
	} else {
		fileHandle.setupHandle(fileNameStr,
				fileHandleCounter[fileNameStr].first);
		fileHandleCounter[fileNameStr].second += 1;
	}
	return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
	if (!fileHandle.isOpen())
		return -20; // Handle is not open
	if (fileHandleCounter.count(fileHandle.getNameStr()) == 0)
		return -30; // Counter status invalid

	fileHandleCounter[fileHandle.getNameStr()].second -= 1;
	if (fileHandleCounter[fileHandle.getNameStr()].second == 0) {
		fclose(fileHandleCounter[fileHandle.getNameStr()].first);
		fileHandleCounter.erase(fileHandle.getNameStr());
	}
	fileHandle.closeHandle();
	return 0;
}

const int PagedFileManager::getHandleCount(const string& fileNameStr) {
	if (fileHandleCounter.count(fileNameStr) == 0)
		return -1; // Name not found in counter
	return fileHandleCounter[fileNameStr].second;
}

FileHandle::FileHandle() :
		fileNameStr(""), pFile(NULL), totalPageCount(0), flagSystem(255) {
}

FileHandle::~FileHandle() {
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
	fseek(pFile, (long int) PAGE_SIZE * (pageNum + 1), SEEK_SET);

	if (fread(data, PAGE_SIZE, 1, pFile) < 1)
		return -1;
	return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
	if (pageNum >= getNumberOfPages())
		return -1;
	fseek(pFile, (long int) PAGE_SIZE * (pageNum + 1), SEEK_SET);
	if (fwrite(data, PAGE_SIZE, 1, pFile) < 1)
		return -1;
	return 0;
}

RC FileHandle::appendPage(const void *data) {
	if (getNumberOfPages() + 1 == 0)
		return -1;
	fseek(pFile, (long int) PAGE_SIZE * (totalPageCount + 1), SEEK_SET);
	if (fwrite(data, PAGE_SIZE, 1, pFile) < 1)
		return -1;
	totalPageCount += 1;
	fseek(pFile, 0, SEEK_SET);
	if (fwrite(&totalPageCount, sizeof(unsigned int), 1, pFile) < 1)
		return -1;
	return 0;
}

unsigned FileHandle::getNumberOfPages() {
	if (totalPageCount == 0) {
		fseek(pFile, 0, SEEK_SET);
		if (feof(pFile))
			return 0;
		fread(&totalPageCount, sizeof(unsigned int), 1, pFile);
	}
	return totalPageCount;
}

bool FileHandle::isOpen() {
	if (pFile == NULL) {
		fileNameStr = "";
		return false;
	}
	return true;
}

void FileHandle::setupHandle(const std::string& argNameStr, FILE* argFile) {
	fileNameStr = argNameStr;
	pFile = argFile;
	totalPageCount = 0;
	flagSystem = 255;
}

void FileHandle::closeHandle() {
	setupHandle("", NULL);
}

const string& FileHandle::getNameStr() const {
	return fileNameStr;
}

const bool FileHandle::getSystemFlag() {
	if (flagSystem != 0 && flagSystem != -1) {
		fseek(pFile, sizeof(unsigned int), SEEK_SET);
		if (feof(pFile))
			return false;
		fread(&flagSystem, sizeof(int), 1, pFile);
	}
	if (flagSystem == -1)
		return true;
	return false;
}

RC FileHandle::setSystemFlag(const bool& flag) {
	if (getSystemFlag() == flag)
		return 0;
	if (flag)
		flagSystem = -1;
	else
		flagSystem = 0;
	fseek(pFile, sizeof(unsigned int), SEEK_SET);
	if (feof(pFile))
		return -1;
	if (fwrite(&flagSystem, sizeof(int), 1, pFile) < 1)
		return -1;
	return 0;
}
