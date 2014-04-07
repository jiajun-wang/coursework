#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../pf/pf.h"
#include "../rm/rm.h"

#define IX_EOF (-1)  // end of the index scan

using namespace std;

struct NextChildPointerOffset
{
	unsigned nextChildPointer;
	bool isNewFirstChild;
	int newMiddleKeyOffset;
};

const int NOT_FOUND = -2;

class IX_IndexHandle;

class IX_Manager
{
public:
  const static int LEAF_FREE_SPACE_INDEX_OFFSET = PF_PAGE_SIZE - 3 * sizeof(int);
  const static int NEXT_NODE_POINTER_OFFSET = PF_PAGE_SIZE - 2 * sizeof(int);
  const static int PREV_NODE_POINTER_OFFSET = PF_PAGE_SIZE - sizeof(int);
  const static int NON_LEAF_FREE_SPACE_INDEX_OFFSET = PF_PAGE_SIZE - sizeof(int);

  static IX_Manager * Instance();

  RC CreateIndex(const string tableName, const string attributeName);  // create new index

  RC DestroyIndex(const string tableName, const string attributeName);  // destroy an index

  RC OpenIndex(const string tableName, const string attributeName, IX_IndexHandle &indexHandle);  // open an index

  RC CloseIndex(IX_IndexHandle &indexHandle);  // close index

  RC createLeafNode(PF_FileHandle &pf_FileHandle, const unsigned nextNodePointer, const unsigned prevNodePointer,
			void *nodeBuffer, unsigned &nodePointer);

  RC createNonLeafNode(PF_FileHandle &pf_FileHandle, void *nodeBuffer, unsigned &nodePointer);

protected:
  IX_Manager();                                // Constructor

  ~IX_Manager();                               // Destructor

private:
  static IX_Manager *_ix_manager;

  string generateIndexName(const string tableName, const string attributeName);

  RC initializeIndex(const string indexName, const AttrType attrType);
};

class IX_IndexHandle
{
public:
  IX_IndexHandle();                           // Constructor

  ~IX_IndexHandle();                          // Destructor

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry

  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry

  PF_FileHandle & getFileHandle();

  string produceKeyString(const int attributeType, const void *key);

  int findNextChildPointerOffset(const void *nodeBuffer, const int freeSpaceIndex, const int attributeType,
			const void *key, string keyString, bool isEqualAllowed);

  vector<NextChildPointerOffset> findNextChildPointerOffset(const void *nodeBuffer, const int freeSpaceIndex,
			const int attributeType, const void *key, string keyString);

  int findEntryPositionToInsert(const void *nodeBuffer, const int freeSpaceIndex, const int attributeType,
			const void *key, string keyString);

  RC findEntryPositionToDelete(const void *nodeBuffer, const int freeSpaceIndex, const int attributeType,
			const void *key, string keyString, const RID &rid, int &position);

  bool isDeletedAfterScan();

  void setDeletedAfterScan(bool deletedAfterScan);

  unsigned getDeletedEntryNodePointer();

  int getDeletedEntryPosition();

  unsigned getNextEntryNodePointer();

  int getNextEntryPosition();

private:
  PF_FileHandle pf_FileHandle;
  bool deletedAfterScan;
  unsigned deletedEntryNodePointer;
  int deletedEntryPosition;
  unsigned nextEntryNodePointer;
  int nextEntryPosition;

  RC insertRecursively(const unsigned nodePointer, const void *key, const RID &rid, void *&newEntryKey,
			unsigned &newEntryPointer, const int indexTreeHeight, const int currentHeight, const int attributeType);

  RC deleteRecursively(const unsigned parentPointer, const unsigned nodePointer, const void *key, const RID &rid,
			bool &isDeleting, const bool isFirstChild, const int middleKeyOffset, unsigned &mergedNodePointer,
			const int indexTreeHeight, const int currentHeight, const int attributeType);

  int calculateKeyLength(const void *entryKey, const int attributeType);
};

class IX_IndexScan
{
public:
  IX_IndexScan();  								// Constructor

  ~IX_IndexScan(); 								// Destructor

  // for the format of "value", please see IX_IndexHandle::InsertEntry()
  RC OpenScan(IX_IndexHandle &indexHandle, CompOp compOp, void *value);  // Initialize index scan

  RC GetNextEntry(RID &rid);  // Get next matching entry

  RC CloseScan();             // Terminate index scan

private:
  IX_IndexHandle *ix_IndexHandle;
  CompOp compOp;
  void *value;
  string valueString;
  int attributeType;
  unsigned currentLeafNodePointer;
  int currentEntryPosition;
  bool isClosed;
  int lastEntryLength;
};

// print out the error message for a given return code
void IX_PrintError(RC rc);

#endif
