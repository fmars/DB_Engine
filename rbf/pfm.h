#ifndef _pfm_h_
#define _pfm_h_

#include <string>
#include <map>

typedef int RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096

class FileHandle;

class PagedFileManager {
public:
	static PagedFileManager* instance();   // Access to the _pf_manager instance

	RC createFile(const char *fileName);                    // Create a new file
	RC destroyFile(const char *fileName);                      // Destroy a file
	RC openFile(const char *fileName, FileHandle &fileHandle); // Open a file
	RC closeFile(FileHandle &fileHandle);                       // Close a file

	// Used in debug, return the count of open handles of an open file
	const int getHandleCount(const std::string& fileNameStr);

protected:
	PagedFileManager();                                   // Constructor
	~PagedFileManager();                                  // Destructor

private:
	static PagedFileManager *_pf_manager;

	std::map<std::string, std::pair<FILE*, unsigned int> > fileHandleCounter;
};

class FileHandle {
public:
	FileHandle();                                         // Default constructor
	~FileHandle();                                        // Destructor

	RC readPage(PageNum pageNum, void *data);             // Get a specific page
	RC writePage(PageNum pageNum, const void *data);    // Write a specific page
	RC appendPage(const void *data);                   // Append a specific page
	unsigned getNumberOfPages();          // Get the number of pages in the file

	bool isOpen();
	const std::string& getNameStr() const;
	void setupHandle(const std::string& argNameStr, FILE* argFile);
	void closeHandle();

	// User should NOT call member functions that operate on System flag
	// These functions are designed for the RM layer
	const bool getSystemFlag();
	RC setSystemFlag(const bool& flag);

private:
	std::string fileNameStr;
	FILE* pFile;
	unsigned int totalPageCount;
	int flagSystem;
};

#endif
