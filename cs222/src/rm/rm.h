#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <ostream>

#include "../pf/pf.h"

using namespace std;

// Record ID
typedef struct {
  unsigned pageNum;
  unsigned slotNum;  // position starts from 1
} RID;

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;
typedef unsigned AttrLength;

struct Attribute
{
    string name;       // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

struct AttributeWithPosition
{
	Attribute attribute;
	int position;
};

inline bool operator<(const AttributeWithPosition &a, const AttributeWithPosition &b) {
	return a.position < b.position;
}

struct Schema
{
	int version;
	string tableName;
	vector<Attribute> attributeVector;
};

// Comparison Operator
typedef enum {
	EQ_OP = 0,  // =
    LT_OP,      // <
    GT_OP,      // >
    LE_OP,      // <=
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;

#define RM_EOF (-1)  // end of a scan operator
const string CATALOG_TABLE_NAME = "catalog";
const string TABLE_NAME = "TableName";
const string SCHEMA_VERSION = "SchemaVersion";  // schema version starts from 1
const string COLUMN_NAME = "ColumnName";
const string POSITION = "Position";  // position starts from 0
const string TYPE = "Type";
const string LENGTH = "Length";
const int MAX_TABLE_NAME_LENGTH = 50;
const int MAX_COLUMN_NAME_LENGTH = 50;
const int MAX_COLUMN_TUPLE_LENGTH = MAX_TABLE_NAME_LENGTH + MAX_COLUMN_NAME_LENGTH + 6 * sizeof(int);

// RM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator
{
public:
  RM_ScanIterator();

  ~RM_ScanIterator();

  void setFileHandle(PF_FileHandle &pf_FileHandle);

  void setSchemaVector(const vector<Schema> &schemaVector);

  void setConditionAttribute(const string conditionAttribute);

  void setCompOp(const CompOp compOp);

  void setValue(const void *value);

  void setAttributeNames(const vector<string> &attributeNames);

  void initialize();

  bool compareAttributeValue(const int attributeValue);

  bool compareAttributeValue(const float attributeValue);

  bool compareAttributeValue(const string attributeValue);

  // "data" follows the same format as RM::insertTuple()
  RC getNextTuple(RID &rid, void *data);

  RC close();

private:
  PF_FileHandle pf_FileHandle;
  vector<Schema> schemaVector;
  string conditionAttribute;
  CompOp compOp;
  const void *value;
  vector<string> attributeNames;
  RID cursor;

  RC getNextRecursively(unsigned pageNumber, unsigned slotNumber, RID &rid, void *data);

  RC getStoredTuple(void *pageBuffer, int recordPointer, int recordLength, RID &rid, void *data);
};

// Record Manager
class RM
{
public:
  const static int FREE_SPACE_INDEX_OFFSET = PF_PAGE_SIZE - 4 * sizeof(int);
  const static int FREE_SPACE_CAPACITY_OFFSET = PF_PAGE_SIZE - 3 * sizeof(int);
  const static int NEXT_PAGE_NUMBER_OFFSET = PF_PAGE_SIZE - 2 * sizeof(int);
  const static int PREV_PAGE_NUMBER_OFFSET = PF_PAGE_SIZE - sizeof(int);
  const static int SLOT_SIZE = sizeof(char) + 3 * sizeof(int);
  const static char RECORD_STORED_STATUS = (char) 0;
  const static char RECORD_DELETED_STATUS = (char) 1;
  const static char RECORD_MOVED_STATUS = (char) 2;
  const static char RECORD_NESTED_STATUS = (char) 3;
  const static int MINIMUM_FREE_SPACE = 12;

  static RM * Instance();

  static RC locateSlot(const void *pageBuffer, const unsigned targetSlotNumber, int &slotOffset);

  RC createTable(const string tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string tableName);

  RC getAttributes(const string tableName, vector<Attribute> &attrs);

  //  Format of the data passed into the function is the following:
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
  RC insertTuple(const string tableName, const void *data, RID &rid);

  RC deleteTuples(const string tableName);

  RC deleteTuple(const string tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string tableName, const void *data, const RID &rid);

  RC readTuple(const string tableName, const RID &rid, void *data);

  RC readAttribute(const string tableName, const RID &rid, const string attributeName, void *data);

  RC reorganizePage(const string tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string tableName,
      const string conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC findLatestSchema(const string tableName, Schema &schema);

// Extra credit
public:
  RC dropAttribute(const string tableName, const string attributeName);

  RC addAttribute(const string tableName, const Attribute attr);

  RC reorganizeTable(const string tableName);

protected:
  RM();

  ~RM();

private:
  static RM *_rm;

  RC createCatalogFile();

  RC createDataPage(PF_FileHandle &pf_FileHandle, const unsigned prevPageNumber);

  RC findSchemaVector(const string tableName, vector<Schema> &schemaVector);

  int prepareColumnTuple(const string tableName, const int schemaVersion, const Attribute &attribute,
			const int position, void *tuple);

  RC insertSchema(const string tableName, const vector<Attribute> &attributeVector);

  RC insertSchema(const string tableName, int version, const vector<Attribute> &attributeVector);

  RC deleteAllSchemas(const string tableName);

  int calculateTupleLength(const void *data, const vector<Attribute> &attributeVector);

  RC initializeTable(const string tableName);

  RC insertTuple(const string tableName, const void *data, RID &rid, const bool isMoved);

  RC deleteRecursively(PF_FileHandle &pf_FileHandle, const string tableName, const RID &rid);

  RC updateRecursively(PF_FileHandle &pf_FileHandle, const string tableName, const void *data, const RID &rid);

  RC readRecursively(PF_FileHandle &pf_FileHandle, const string tableName, const RID &rid,
			const string attributeName, void *data, const bool isReadingAttribute);

  RC turnFreePageToFull(PF_FileHandle &pf_FileHandle, void *pageBuffer, unsigned pageNumber);

  RC turnFullPageToFree(PF_FileHandle &pf_FileHandle, void *pageBuffer, unsigned pageNumber);

  RC alterNextPrevPointer(PF_FileHandle &pf_FileHandle, const unsigned sourcePageNumber, const bool isNext,
			const unsigned destinationPageNumber);

  vector<string> prepareCatalogAttributeVector();

  void * initializeNewPageBuffer(int &newFreeSpaceIndex, int &newFreeSpaceCapacity, int &newSlotOffset);

  RC appendFreeSpaceFullPage(PF_FileHandle &pf_FileHandle, void *pageBuffer, unsigned pageNumber,
			bool isFreeSpacePage, int freeSpaceIndex, int freeSpaceCapacity, unsigned lastPageNumber);
};

#endif
