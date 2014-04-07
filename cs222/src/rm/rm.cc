#include <cstdlib>
#include <cstring>
#include <map>
#include <algorithm>

#include "rm.h"

RM *RM::_rm = 0;

RM * RM::Instance() {
	if (!_rm) {
		_rm = new RM();
	}

	return _rm;
}

RM::RM() {
}

RM::~RM() {
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs) {
	if (createCatalogFile() == FAILURE || PF_Manager::Instance()->CreateFile(tableName.c_str()) == FAILURE) {
		return FAILURE;
	}

	if (insertSchema(tableName, attrs) == SUCCESS) {
		return initializeTable(tableName);
	}

	return FAILURE;
}

RC RM::initializeTable(const string tableName) {
	PF_Manager *pf_Manager = PF_Manager::Instance();
	PF_FileHandle pf_FileHandle;

	if (pf_Manager->OpenFile(tableName.c_str(), pf_FileHandle) == SUCCESS) {
		void *headPageData = malloc(PF_PAGE_SIZE);
		int firstFreeSpacePageNumber = 1;
		int firstFullPageNumber = END_PAGE_NUMBER;

		memset((char *) headPageData, 0, PF_PAGE_SIZE);
		memcpy((char *) headPageData, &firstFreeSpacePageNumber, sizeof(int));
		memcpy((char *) headPageData + sizeof(int), &firstFullPageNumber, sizeof(int));

		RC isheadPageCreated = pf_FileHandle.AppendPage(headPageData);

		free(headPageData);

		if (isheadPageCreated == SUCCESS) {
			if (createDataPage(pf_FileHandle, 0) == SUCCESS) {
				pf_Manager->CloseFile(pf_FileHandle);

				return SUCCESS;
			}
		}
	}

	pf_Manager->CloseFile(pf_FileHandle);

	return FAILURE;
}

RC RM::deleteTable(const string tableName) {
	return PF_Manager::Instance()->DestroyFile(tableName.c_str()) && deleteAllSchemas(tableName);
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs) {
	Schema schema;

	if (findLatestSchema(tableName, schema) == SUCCESS) {
		for (unsigned i = 0; i < schema.attributeVector.size(); i++) {
			attrs.push_back(schema.attributeVector.at(i));
		}

		return SUCCESS;
	}

	return FAILURE;
}

RC RM::insertTuple(const string tableName, const void *data, RID &rid) {
	return insertTuple(tableName, data, rid, false);
}

RC RM::insertTuple(const string tableName, const void *data, RID &rid, const bool isMoved) {
	Schema schema;

	if (findLatestSchema(tableName, schema) == SUCCESS) {
		int actualTupleLength = calculateTupleLength(data, schema.attributeVector);
		PF_Manager *pf_Manager = PF_Manager::Instance();
		PF_FileHandle pf_FileHandle;

		if (pf_Manager->OpenFile(tableName.c_str(), pf_FileHandle) == SUCCESS) {
			void *headPageBuffer = malloc(PF_PAGE_SIZE);

			if (pf_FileHandle.ReadPage(0, headPageBuffer) == SUCCESS) {
				int freeSpacePageNumber = 0;
				memcpy(&freeSpacePageNumber, (char *) headPageBuffer, sizeof(int));

				free(headPageBuffer);
				bool pageFound = false;

				while (!pageFound) {
					void *pageBuffer = malloc(PF_PAGE_SIZE);

					if (pf_FileHandle.ReadPage(freeSpacePageNumber, pageBuffer) == SUCCESS) {
						int freeSpaceCapacity = 0;
						memcpy(&freeSpaceCapacity, (char *) pageBuffer + FREE_SPACE_CAPACITY_OFFSET, sizeof(int));

						if (actualTupleLength + SLOT_SIZE <= freeSpaceCapacity) {
							pageFound = true;
							int freeSpaceIndex = 0;
							memcpy(&freeSpaceIndex, (char *) pageBuffer + FREE_SPACE_INDEX_OFFSET, sizeof(int));

							int newSlotNumber = 1;
							int oldSlotOffset = freeSpaceIndex + freeSpaceCapacity;

							if (oldSlotOffset != FREE_SPACE_INDEX_OFFSET) {
								memcpy(&newSlotNumber, (char *) pageBuffer + oldSlotOffset, sizeof(int));
								newSlotNumber++;
							}

							rid.pageNum = freeSpacePageNumber;
							rid.slotNum = newSlotNumber;

							int schemaVersion = schema.version;
							memcpy((char *) pageBuffer + freeSpaceIndex, &schemaVersion, sizeof(int));
							memcpy((char *) pageBuffer + freeSpaceIndex + sizeof(int), (char *) data, actualTupleLength
									- sizeof(int));

							int newSlotOffset = oldSlotOffset - SLOT_SIZE;
							memcpy((char *) pageBuffer + newSlotOffset, &newSlotNumber, sizeof(int));
							newSlotOffset += sizeof(int);

							char recordStatus = RECORD_STORED_STATUS;
							if (isMoved) {
								recordStatus = RECORD_NESTED_STATUS;
							}
							memcpy((char *) pageBuffer + newSlotOffset, &recordStatus, sizeof(char));
							newSlotOffset += sizeof(char);

							memcpy((char *) pageBuffer + newSlotOffset, &freeSpaceIndex, sizeof(int));
							newSlotOffset += sizeof(int);

							memcpy((char *) pageBuffer + newSlotOffset, &actualTupleLength, sizeof(int));

							freeSpaceIndex += actualTupleLength;
							memcpy((char *) pageBuffer + FREE_SPACE_INDEX_OFFSET, &freeSpaceIndex, sizeof(int));

							freeSpaceCapacity -= actualTupleLength + SLOT_SIZE;
							memcpy((char *) pageBuffer + FREE_SPACE_CAPACITY_OFFSET, &freeSpaceCapacity, sizeof(int));

							if (freeSpaceCapacity < MINIMUM_FREE_SPACE) {
								if (turnFreePageToFull(pf_FileHandle, pageBuffer, freeSpacePageNumber) == FAILURE) {
									free(pageBuffer);
									pf_Manager->CloseFile(pf_FileHandle);

									return FAILURE;
								}
							}

							if (pf_FileHandle.WritePage(freeSpacePageNumber, pageBuffer) == FAILURE) {
								free(pageBuffer);
								pf_Manager->CloseFile(pf_FileHandle);

								return FAILURE;
							}

						} else {
							int nextFreeSpacePageNumber = 0;
							memcpy(&nextFreeSpacePageNumber, (char *) pageBuffer + NEXT_PAGE_NUMBER_OFFSET, sizeof(int));

							if (nextFreeSpacePageNumber == END_PAGE_NUMBER) {
								if (createDataPage(pf_FileHandle, freeSpacePageNumber) == SUCCESS) {
									nextFreeSpacePageNumber = pf_FileHandle.GetNumberOfPages() - 1;
									memcpy((char *) pageBuffer + NEXT_PAGE_NUMBER_OFFSET, &nextFreeSpacePageNumber,
											sizeof(int));

									if (pf_FileHandle.WritePage(freeSpacePageNumber, pageBuffer) == FAILURE) {
										free(pageBuffer);
										pf_Manager->CloseFile(pf_FileHandle);

										return FAILURE;
									}

								} else {
									free(pageBuffer);
									pf_Manager->CloseFile(pf_FileHandle);

									return FAILURE;
								}
							}

							freeSpacePageNumber = nextFreeSpacePageNumber;
						}

					} else {
						free(pageBuffer);
						pf_Manager->CloseFile(pf_FileHandle);

						return FAILURE;
					}

					free(pageBuffer);
				}

				pf_Manager->CloseFile(pf_FileHandle);

				return SUCCESS;

			} else {
				free(headPageBuffer);
			}
		}

		pf_Manager->CloseFile(pf_FileHandle);
	}

	return FAILURE;
}

RC RM::turnFreePageToFull(PF_FileHandle &pf_FileHandle, void *pageBuffer, unsigned pageNumber) {
	int nextPageNumber = 0;
	int prevPageNumber = 0;
	memcpy(&nextPageNumber, (char *) pageBuffer + NEXT_PAGE_NUMBER_OFFSET, sizeof(int));
	memcpy(&prevPageNumber, (char *) pageBuffer + PREV_PAGE_NUMBER_OFFSET, sizeof(int));

	void *headPageBuffer = malloc(PF_PAGE_SIZE);

	if (pf_FileHandle.ReadPage(0, headPageBuffer) == SUCCESS) {
		if (prevPageNumber == 0) {
			int nextPageNumberAfterHead = nextPageNumber;

			if (nextPageNumber == END_PAGE_NUMBER) {
				if (createDataPage(pf_FileHandle, 0) == SUCCESS) {
					nextPageNumberAfterHead = pf_FileHandle.GetNumberOfPages() - 1;

				} else {
					return FAILURE;
				}
			}

			memcpy((char *) headPageBuffer, &nextPageNumberAfterHead, sizeof(int));

		} else {
			if (alterNextPrevPointer(pf_FileHandle, prevPageNumber, true, nextPageNumber) == FAILURE) {
				free(headPageBuffer);

				return FAILURE;
			}
		}

		if (nextPageNumber != END_PAGE_NUMBER) {
			if (alterNextPrevPointer(pf_FileHandle, nextPageNumber, false, prevPageNumber) == FAILURE) {
				free(headPageBuffer);

				return FAILURE;
			}
		}

		int firstFullPageNumber = 0;
		memcpy(&firstFullPageNumber, (char *) headPageBuffer + sizeof(int), sizeof(int));
		memcpy((char *) pageBuffer + NEXT_PAGE_NUMBER_OFFSET, &firstFullPageNumber, sizeof(int));
		memcpy((char *) headPageBuffer + sizeof(int), &pageNumber, sizeof(int));

		int headPageNumber = 0;
		memcpy((char *) pageBuffer + PREV_PAGE_NUMBER_OFFSET, &headPageNumber, sizeof(int));

		if (firstFullPageNumber != END_PAGE_NUMBER) {
			if (alterNextPrevPointer(pf_FileHandle, firstFullPageNumber, false, pageNumber) == FAILURE) {
				free(headPageBuffer);

				return FAILURE;
			}
		}

		if (pf_FileHandle.WritePage(0, headPageBuffer) == SUCCESS) {
			free(headPageBuffer);

			return SUCCESS;
		}
	}

	free(headPageBuffer);

	return FAILURE;
}

RC RM::turnFullPageToFree(PF_FileHandle &pf_FileHandle, void *pageBuffer, unsigned pageNumber) {
	int nextPageNumber = 0;
	int prevPageNumber = 0;
	memcpy(&nextPageNumber, (char *) pageBuffer + NEXT_PAGE_NUMBER_OFFSET, sizeof(int));
	memcpy(&prevPageNumber, (char *) pageBuffer + PREV_PAGE_NUMBER_OFFSET, sizeof(int));

	void *headPageBuffer = malloc(PF_PAGE_SIZE);

	if (pf_FileHandle.ReadPage(0, headPageBuffer) == SUCCESS) {
		if (prevPageNumber == 0) {
			memcpy((char *) headPageBuffer + sizeof(int), &nextPageNumber, sizeof(int));

		} else {
			if (alterNextPrevPointer(pf_FileHandle, prevPageNumber, true, nextPageNumber) == FAILURE) {
				free(headPageBuffer);

				return FAILURE;
			}
		}

		if (nextPageNumber != END_PAGE_NUMBER) {
			if (alterNextPrevPointer(pf_FileHandle, nextPageNumber, false, prevPageNumber) == FAILURE) {
				free(headPageBuffer);

				return FAILURE;
			}
		}

		int firstFreePageNumber = 0;
		memcpy(&firstFreePageNumber, (char *) headPageBuffer, sizeof(int));
		memcpy((char *) pageBuffer + NEXT_PAGE_NUMBER_OFFSET, &firstFreePageNumber, sizeof(int));
		memcpy((char *) headPageBuffer, &pageNumber, sizeof(int));

		int headPageNumber = 0;
		memcpy((char *) pageBuffer + PREV_PAGE_NUMBER_OFFSET, &headPageNumber, sizeof(int));

		if (firstFreePageNumber != END_PAGE_NUMBER) {
			if (alterNextPrevPointer(pf_FileHandle, firstFreePageNumber, false, pageNumber) == FAILURE) {
				free(headPageBuffer);

				return FAILURE;
			}
		}

		if (pf_FileHandle.WritePage(0, headPageBuffer) == SUCCESS) {
			free(headPageBuffer);

			return SUCCESS;
		}
	}

	free(headPageBuffer);

	return FAILURE;
}

RC RM::alterNextPrevPointer(PF_FileHandle &pf_FileHandle, const unsigned sourcePageNumber, const bool isNext,
		const unsigned destinationPageNumber) {

	void *sourcePageBuffer = malloc(PF_PAGE_SIZE);

	if (pf_FileHandle.ReadPage(sourcePageNumber, sourcePageBuffer) == SUCCESS) {
		if (sourcePageNumber == 0) {
			// for head page, next page means first free space page, prev page means first full page
			int freeSpaceFullOffset = 0;
			if (!isNext) {
				freeSpaceFullOffset = sizeof(int);
			}

			memcpy((char *) sourcePageBuffer + freeSpaceFullOffset, &destinationPageNumber, sizeof(int));

		} else {
			int nextPrevOffset = 0;
			if (isNext) {
				nextPrevOffset = NEXT_PAGE_NUMBER_OFFSET;
			} else {
				nextPrevOffset = PREV_PAGE_NUMBER_OFFSET;
			}

			memcpy((char *) sourcePageBuffer + nextPrevOffset, &destinationPageNumber, sizeof(int));
		}

		if (pf_FileHandle.WritePage(sourcePageNumber, sourcePageBuffer) == SUCCESS) {
			free(sourcePageBuffer);

			return SUCCESS;
		}
	}

	free(sourcePageBuffer);

	return FAILURE;
}

RC RM::deleteTuples(const string tableName) {
	PF_Manager *pf_Manager = PF_Manager::Instance();

	if (pf_Manager->DestroyFile(tableName.c_str()) == SUCCESS) {
		if (pf_Manager->CreateFile(tableName.c_str()) == SUCCESS) {
			return initializeTable(tableName);
		}
	}

	return FAILURE;
}

RC RM::deleteTuple(const string tableName, const RID &rid) {
	PF_Manager *pf_Manager = PF_Manager::Instance();
	PF_FileHandle pf_FileHandle;

	if (pf_Manager->OpenFile(tableName.c_str(), pf_FileHandle) == SUCCESS) {
		RC isTupleDeleted = deleteRecursively(pf_FileHandle, tableName, rid);

		pf_Manager->CloseFile(pf_FileHandle);

		return isTupleDeleted;
	}

	pf_Manager->CloseFile(pf_FileHandle);

	return FAILURE;
}

RC RM::deleteRecursively(PF_FileHandle &pf_FileHandle, const string tableName, const RID &rid) {
	void *pageBuffer = malloc(PF_PAGE_SIZE);

	if (pf_FileHandle.ReadPage(rid.pageNum, pageBuffer) == SUCCESS) {
		int slotOffset = FREE_SPACE_INDEX_OFFSET;

		if (locateSlot(pageBuffer, rid.slotNum, slotOffset) == FAILURE) {
			free(pageBuffer);

			return FAILURE;
		}

		char recordStatus = (char) 0;
		memcpy(&recordStatus, (char *) pageBuffer + slotOffset + sizeof(int), sizeof(char));

		if (recordStatus == RECORD_STORED_STATUS || recordStatus == RECORD_NESTED_STATUS) {
			char recordDeletedStatus = RECORD_DELETED_STATUS;
			int recordPointer = 0;
			int recordLength = 0;
			memcpy((char *) pageBuffer + slotOffset + sizeof(int), &recordDeletedStatus, sizeof(char));
			memcpy((char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), &recordPointer, sizeof(int));
			memcpy((char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), &recordLength, sizeof(int));

			if (pf_FileHandle.WritePage(rid.pageNum, pageBuffer) == SUCCESS) {
				free(pageBuffer);

				return SUCCESS;
			}

			free(pageBuffer);

			return FAILURE;

		} else if (recordStatus == RECORD_MOVED_STATUS) {
			unsigned nextPageNumber = 0;
			unsigned nextSlotNumber = 0;
			memcpy(&nextPageNumber, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), sizeof(int));
			memcpy(&nextSlotNumber, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), sizeof(int));

			RID nextRid;
			nextRid.pageNum = nextPageNumber;
			nextRid.slotNum = nextSlotNumber;

			char recordDeletedStatus = RECORD_DELETED_STATUS;
			int recordPointer = 0;
			int recordLength = 0;
			memcpy((char *) pageBuffer + slotOffset + sizeof(int), &recordDeletedStatus, sizeof(char));
			memcpy((char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), &recordPointer, sizeof(int));
			memcpy((char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), &recordLength, sizeof(int));

			if (pf_FileHandle.WritePage(rid.pageNum, pageBuffer) == FAILURE) {
				free(pageBuffer);

				return FAILURE;
			}

			free(pageBuffer);

			return deleteRecursively(pf_FileHandle, tableName, nextRid);

		} else {
			free(pageBuffer);

			return FAILURE;
		}

	} else {
		free(pageBuffer);

		return FAILURE;
	}
}

RC RM::updateTuple(const string tableName, const void *data, const RID &rid) {
	PF_Manager *pf_Manager = PF_Manager::Instance();
	PF_FileHandle pf_FileHandle;

	if (pf_Manager->OpenFile(tableName.c_str(), pf_FileHandle) == SUCCESS) {
		RC isTupleUpdated = updateRecursively(pf_FileHandle, tableName, data, rid);

		pf_Manager->CloseFile(pf_FileHandle);

		return isTupleUpdated;
	}

	pf_Manager->CloseFile(pf_FileHandle);

	return FAILURE;
}

RC RM::updateRecursively(PF_FileHandle &pf_FileHandle, const string tableName, const void *data, const RID &rid) {
	void *pageBuffer = malloc(PF_PAGE_SIZE);

	if (pf_FileHandle.ReadPage(rid.pageNum, pageBuffer) == SUCCESS) {
		int slotOffset = FREE_SPACE_INDEX_OFFSET;

		if (locateSlot(pageBuffer, rid.slotNum, slotOffset) == FAILURE) {
			free(pageBuffer);

			return FAILURE;
		}

		char recordStatus = (char) 0;
		memcpy(&recordStatus, (char *) pageBuffer + slotOffset + sizeof(int), sizeof(char));

		if (recordStatus == RECORD_STORED_STATUS || recordStatus == RECORD_NESTED_STATUS) {
			int recordPointer = 0;
			int recordLength = 0;
			memcpy(&recordPointer, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), sizeof(int));
			memcpy(&recordLength, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), sizeof(int));

			Schema schema;

			if (findLatestSchema(tableName, schema) == SUCCESS) {
				int schemaVersion = schema.version;
				int dataLength = calculateTupleLength(data, schema.attributeVector);

				if (dataLength <= recordLength) {
					memcpy((char *) pageBuffer + recordPointer, &schemaVersion, sizeof(int));
					memcpy((char *) pageBuffer + recordPointer + sizeof(int), (char *) data, dataLength - sizeof(int));
					memcpy((char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), &dataLength, sizeof(int));

					if (pf_FileHandle.WritePage(rid.pageNum, pageBuffer) == SUCCESS) {
						free(pageBuffer);

						return SUCCESS;
					}

				} else {
					int freeSpaceCapacity = 0;
					memcpy(&freeSpaceCapacity, (char *) pageBuffer + FREE_SPACE_CAPACITY_OFFSET, sizeof(int));

					if (dataLength + SLOT_SIZE <= freeSpaceCapacity) {
						int freeSpaceIndex = 0;
						memcpy(&freeSpaceIndex, (char *) pageBuffer + FREE_SPACE_INDEX_OFFSET, sizeof(int));

						memcpy((char *) pageBuffer + freeSpaceIndex, &schemaVersion, sizeof(int));
						memcpy((char *) pageBuffer + freeSpaceIndex + sizeof(int), (char *) data, dataLength
								- sizeof(int));
						memcpy((char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), &freeSpaceIndex,
								sizeof(int));
						memcpy((char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), &dataLength,
								sizeof(int));

						freeSpaceIndex += dataLength;
						freeSpaceCapacity -= dataLength;
						memcpy((char *) pageBuffer + FREE_SPACE_INDEX_OFFSET, &freeSpaceIndex, sizeof(int));
						memcpy((char *) pageBuffer + FREE_SPACE_CAPACITY_OFFSET, &freeSpaceCapacity, sizeof(int));

						if (freeSpaceCapacity < MINIMUM_FREE_SPACE) {
							if (turnFreePageToFull(pf_FileHandle, pageBuffer, rid.pageNum) == FAILURE) {
								free(pageBuffer);

								return FAILURE;
							}
						}

						if (pf_FileHandle.WritePage(rid.pageNum, pageBuffer) == SUCCESS) {
							free(pageBuffer);

							return SUCCESS;
						}

					} else {
						RID newRid;

						if (insertTuple(tableName, data, newRid, true) == SUCCESS) {
							char recordMovedStatus = RECORD_MOVED_STATUS;
							unsigned newPageNumber = newRid.pageNum;
							unsigned newSlotNumber = newRid.slotNum;
							memcpy((char *) pageBuffer + slotOffset + sizeof(int), &recordMovedStatus, sizeof(char));
							memcpy((char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), &newPageNumber,
									sizeof(int));
							memcpy((char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), &newSlotNumber,
									sizeof(int));

							if (pf_FileHandle.WritePage(rid.pageNum, pageBuffer) == SUCCESS) {
								free(pageBuffer);

								return SUCCESS;
							}
						}
					}
				}
			}

			free(pageBuffer);

			return FAILURE;

		} else if (recordStatus == RECORD_MOVED_STATUS) {
			unsigned nextPageNumber = 0;
			unsigned nextSlotNumber = 0;
			memcpy(&nextPageNumber, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), sizeof(int));
			memcpy(&nextSlotNumber, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), sizeof(int));

			RID nextRid;
			nextRid.pageNum = nextPageNumber;
			nextRid.slotNum = nextSlotNumber;

			free(pageBuffer);

			return updateRecursively(pf_FileHandle, tableName, data, nextRid);

		} else {
			free(pageBuffer);

			return FAILURE;
		}

	} else {
		free(pageBuffer);

		return FAILURE;
	}
}

RC RM::readTuple(const string tableName, const RID &rid, void *data) {
	PF_Manager *pf_Manager = PF_Manager::Instance();
	PF_FileHandle pf_FileHandle;

	if (pf_Manager->OpenFile(tableName.c_str(), pf_FileHandle) == SUCCESS) {
		RC isTupleRead = readRecursively(pf_FileHandle, tableName, rid, "", data, false);

		pf_Manager->CloseFile(pf_FileHandle);

		return isTupleRead;
	}

	pf_Manager->CloseFile(pf_FileHandle);

	return FAILURE;
}

RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data) {
	PF_Manager *pf_Manager = PF_Manager::Instance();
	PF_FileHandle pf_FileHandle;

	if (pf_Manager->OpenFile(tableName.c_str(), pf_FileHandle) == SUCCESS) {
		RC isAttributeRead = readRecursively(pf_FileHandle, tableName, rid, attributeName, data, true);

		pf_Manager->CloseFile(pf_FileHandle);

		return isAttributeRead;
	}

	pf_Manager->CloseFile(pf_FileHandle);

	return FAILURE;
}

RC RM::readRecursively(PF_FileHandle &pf_FileHandle, const string tableName, const RID &rid,
		const string attributeName, void *data, const bool isReadingAttribute) {

	void *pageBuffer = malloc(PF_PAGE_SIZE);

	if (pf_FileHandle.ReadPage(rid.pageNum, pageBuffer) == SUCCESS) {
		int slotOffset = FREE_SPACE_INDEX_OFFSET;

		if (locateSlot(pageBuffer, rid.slotNum, slotOffset) == FAILURE) {
			free(pageBuffer);

			return FAILURE;
		}

		char recordStatus = (char) 0;
		memcpy(&recordStatus, (char *) pageBuffer + slotOffset + sizeof(int), sizeof(char));

		if (recordStatus == RECORD_STORED_STATUS || recordStatus == RECORD_NESTED_STATUS) {
			int recordPointer = 0;
			int recordLength = 0;
			memcpy(&recordPointer, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), sizeof(int));
			memcpy(&recordLength, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), sizeof(int));

			if (isReadingAttribute) {
				int schemaVersion = 0;
				memcpy(&schemaVersion, (char *) pageBuffer + recordPointer, sizeof(int));

				vector<Schema> schemaVector;
				int schemaIndex = 0;

				if (findSchemaVector(tableName, schemaVector) == FAILURE) {
					free(pageBuffer);

					return FAILURE;
				}

				for (unsigned i = 0; i < schemaVector.size(); i++) {
					if (schemaVector.at(i).version == schemaVersion) {
						schemaIndex = i;
						break;
					}
				}

				Schema schema = schemaVector.at(schemaIndex);
				int attributeValueOffset = 0;
				int attributeValueLength = 0;

				for (unsigned i = 0; i < schema.attributeVector.size(); i++) {
					if (schema.attributeVector.at(i).name == attributeName) {
						switch (schema.attributeVector.at(i).type) {
							case TypeInt: {
								attributeValueLength = sizeof(int);
								break;
							}

							case TypeReal: {
								attributeValueLength = sizeof(float);
								break;
							}

							case TypeVarChar: {
								memcpy(&attributeValueLength, (char *) pageBuffer + recordPointer + sizeof(int)
										+ attributeValueOffset, sizeof(int));
								attributeValueOffset += sizeof(int);
								break;
							}
						}

						break;

					} else {
						switch (schema.attributeVector.at(i).type) {
							case TypeInt: {
								attributeValueOffset += sizeof(int);
								break;
							}

							case TypeReal: {
								attributeValueOffset += sizeof(float);
								break;
							}

							case TypeVarChar: {
								memcpy(&attributeValueLength, (char *) pageBuffer + recordPointer + sizeof(int)
										+ attributeValueOffset, sizeof(int));
								attributeValueOffset += sizeof(int) + attributeValueLength;
								break;
							}
						}
					}
				}

				memcpy((char *) data, (char *) pageBuffer + recordPointer + sizeof(int) + attributeValueOffset,
						attributeValueLength);

			} else {
				memcpy((char *) data, (char *) pageBuffer + recordPointer + sizeof(int), recordLength - sizeof(int));
			}

			free(pageBuffer);

			return SUCCESS;

		} else if (recordStatus == RECORD_MOVED_STATUS) {
			unsigned nextPageNumber = 0;
			unsigned nextSlotNumber = 0;
			memcpy(&nextPageNumber, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), sizeof(int));
			memcpy(&nextSlotNumber, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), sizeof(int));

			RID nextRid;
			nextRid.pageNum = nextPageNumber;
			nextRid.slotNum = nextSlotNumber;

			free(pageBuffer);

			return readRecursively(pf_FileHandle, tableName, nextRid, attributeName, data, isReadingAttribute);

		} else {
			free(pageBuffer);

			return FAILURE;
		}

	} else {
		free(pageBuffer);

		return FAILURE;
	}
}

RC RM::locateSlot(const void *pageBuffer, const unsigned targetSlotNumber, int &slotOffset) {
	unsigned slotNumber = 0;
	slotOffset = FREE_SPACE_INDEX_OFFSET;
	int freeSpaceIndex = 0;
	int freeSpaceCapacity = 0;

	memcpy(&freeSpaceIndex, (char *) pageBuffer + FREE_SPACE_INDEX_OFFSET, sizeof(int));
	memcpy(&freeSpaceCapacity, (char *) pageBuffer + FREE_SPACE_CAPACITY_OFFSET, sizeof(int));

	do {
		slotOffset -= SLOT_SIZE;
		memcpy(&slotNumber, (char *) pageBuffer + slotOffset, sizeof(int));
	} while (slotNumber != targetSlotNumber && slotOffset >= freeSpaceIndex + freeSpaceCapacity + SLOT_SIZE);

	if (slotNumber != targetSlotNumber) {
		return FAILURE;
	}

	return SUCCESS;
}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber) {
	if (pageNumber == 0) {
		return SUCCESS;
	}

	PF_Manager *pf_Manager = PF_Manager::Instance();
	PF_FileHandle pf_FileHandle;

	if (pf_Manager->OpenFile(tableName.c_str(), pf_FileHandle) == SUCCESS) {
		void *pageBuffer = malloc(PF_PAGE_SIZE);
		void *newPageBuffer = malloc(PF_PAGE_SIZE);
		int newFreeSpaceIndex = 0;
		int newFreeSpaceCapacity = PF_PAGE_SIZE - 4 * sizeof(int);
		int newSlotOffset = FREE_SPACE_INDEX_OFFSET - SLOT_SIZE;

		memset(newPageBuffer, 0, PF_PAGE_SIZE);

		if (pf_FileHandle.ReadPage(pageNumber, pageBuffer) == SUCCESS) {
			int freeSpaceIndex = 0;
			int freeSpaceCapacity = 0;

			memcpy(&freeSpaceIndex, (char *) pageBuffer + FREE_SPACE_INDEX_OFFSET, sizeof(int));
			memcpy(&freeSpaceCapacity, (char *) pageBuffer + FREE_SPACE_CAPACITY_OFFSET, sizeof(int));

			for (int slotOffset = FREE_SPACE_INDEX_OFFSET - SLOT_SIZE; slotOffset >= freeSpaceIndex + freeSpaceCapacity; slotOffset
					-= SLOT_SIZE) {

				char recordStatus = (char) 0;
				memcpy(&recordStatus, (char *) pageBuffer + slotOffset + sizeof(int), sizeof(char));

				if (recordStatus == RECORD_STORED_STATUS || recordStatus == RECORD_NESTED_STATUS) {
					int recordPointer = 0;
					int recordLength = 0;
					memcpy(&recordPointer, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), sizeof(int));
					memcpy(&recordLength, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char),
							sizeof(int));

					memcpy((char *) newPageBuffer + newFreeSpaceIndex, (char *) pageBuffer + recordPointer,
							recordLength);
					memcpy((char *) newPageBuffer + newSlotOffset, (char *) pageBuffer + slotOffset, SLOT_SIZE);
					memcpy((char *) newPageBuffer + newSlotOffset + sizeof(int) + sizeof(char), &newFreeSpaceIndex,
							sizeof(int));

					newFreeSpaceIndex += recordLength;
					newFreeSpaceCapacity -= recordLength + SLOT_SIZE;
					newSlotOffset -= SLOT_SIZE;

				} else if (recordStatus == RECORD_DELETED_STATUS) {
					continue;

				} else if (recordStatus == RECORD_MOVED_STATUS) {
					memcpy((char *) newPageBuffer + newSlotOffset, (char *) pageBuffer + slotOffset, SLOT_SIZE);

					newFreeSpaceCapacity -= SLOT_SIZE;
					newSlotOffset -= SLOT_SIZE;
				}
			}

			memcpy((char *) newPageBuffer + FREE_SPACE_INDEX_OFFSET, &newFreeSpaceIndex, sizeof(int));
			memcpy((char *) newPageBuffer + FREE_SPACE_CAPACITY_OFFSET, &newFreeSpaceCapacity, sizeof(int));
			memcpy((char *) newPageBuffer + NEXT_PAGE_NUMBER_OFFSET, (char *) pageBuffer + NEXT_PAGE_NUMBER_OFFSET,
					sizeof(int));
			memcpy((char *) newPageBuffer + PREV_PAGE_NUMBER_OFFSET, (char *) pageBuffer + PREV_PAGE_NUMBER_OFFSET,
					sizeof(int));

			if (freeSpaceCapacity < MINIMUM_FREE_SPACE && newFreeSpaceCapacity >= MINIMUM_FREE_SPACE) {
				if (turnFullPageToFree(pf_FileHandle, newPageBuffer, pageNumber) == FAILURE) {
					free(pageBuffer);
					free(newPageBuffer);
					pf_Manager->CloseFile(pf_FileHandle);

					return FAILURE;
				}
			}

			if (pf_FileHandle.WritePage(pageNumber, newPageBuffer) == SUCCESS) {
				free(pageBuffer);
				free(newPageBuffer);
				pf_Manager->CloseFile(pf_FileHandle);

				return SUCCESS;
			}
		}

		free(pageBuffer);
		free(newPageBuffer);
		pf_Manager->CloseFile(pf_FileHandle);

		return FAILURE;
	}

	pf_Manager->CloseFile(pf_FileHandle);

	return FAILURE;
}

RC RM::scan(const string tableName, const string conditionAttribute, const CompOp compOp, const void *value,
		const vector<string> &attributeNames, RM_ScanIterator &rm_ScanIterator) {

	vector<Schema> schemaVector;
	vector<Schema> qualifiedSchemaVector;

	if (!attributeNames.empty() && findSchemaVector(tableName, schemaVector) == SUCCESS) {
		for (unsigned i = 0; i < schemaVector.size(); i++) {
			vector<Attribute> attributeVector = schemaVector.at(i).attributeVector;
			bool hasCondition = conditionAttribute.empty();

			if (!hasCondition) {
				for (unsigned j = 0; j < attributeVector.size(); j++) {
					string attributeName = attributeVector.at(j).name;

					if (conditionAttribute == attributeName) {
						hasCondition = true;
						break;
					}
				}
			}

			if (hasCondition) {
				bool qualified = true;

				for (unsigned j = 0; j < attributeNames.size(); j++) {
					bool hasAttribute = false;

					for (unsigned k = 0; k < attributeVector.size(); k++) {
						string attributeName = attributeVector.at(k).name;

						if (attributeNames.at(j) == attributeName) {
							hasAttribute = true;
							break;
						}
					}

					if (!hasAttribute) {
						qualified = false;
						break;
					}
				}

				if (qualified) {
					qualifiedSchemaVector.push_back(schemaVector.at(i));
				}
			}
		}

		if (!qualifiedSchemaVector.empty()) {
			PF_FileHandle pf_FileHandle;

			if (PF_Manager::Instance()->OpenFile(tableName.c_str(), pf_FileHandle) == SUCCESS) {
				rm_ScanIterator.setFileHandle(pf_FileHandle);
				rm_ScanIterator.setSchemaVector(qualifiedSchemaVector);
				rm_ScanIterator.setConditionAttribute(conditionAttribute);
				rm_ScanIterator.setCompOp(compOp);
				rm_ScanIterator.setValue(value);
				rm_ScanIterator.setAttributeNames(attributeNames);
				rm_ScanIterator.initialize();

				return SUCCESS;
			}
		}
	}

	return FAILURE;
}

RC RM::dropAttribute(const string tableName, const string attributeName) {
	Schema schema;

	if (findLatestSchema(tableName, schema) == SUCCESS) {
		vector<Attribute> attributeVector;
		bool isAttributeDropped;

		for (unsigned i = 0; i < schema.attributeVector.size(); i++) {
			if (schema.attributeVector.at(i).name != attributeName) {
				attributeVector.push_back(schema.attributeVector.at(i));
			} else {
				isAttributeDropped = true;
			}
		}

		return isAttributeDropped ? insertSchema(tableName, schema.version + 1, attributeVector) : SUCCESS;
	}

	return FAILURE;
}

RC RM::addAttribute(const string tableName, const Attribute attr) {
	Schema schema;

	if (findLatestSchema(tableName, schema) == SUCCESS) {
		vector<Attribute> attributeVector;

		for (unsigned i = 0; i < schema.attributeVector.size(); i++) {
			attributeVector.push_back(schema.attributeVector.at(i));
		}

		attributeVector.push_back(attr);

		return insertSchema(tableName, schema.version + 1, attributeVector);
	}

	return FAILURE;
}

RC RM::reorganizeTable(const string tableName) {
	PF_Manager *pf_Manager = PF_Manager::Instance();
	PF_FileHandle pf_FileHandle;

	if (pf_Manager->OpenFile(tableName.c_str(), pf_FileHandle) == SUCCESS) {
		void *headPageBuffer = malloc(PF_PAGE_SIZE);

		if (pf_FileHandle.ReadPage(0, headPageBuffer) == SUCCESS) {
			int endPageNumber = END_PAGE_NUMBER;
			memcpy((char *) headPageBuffer, &endPageNumber, sizeof(int));
			memcpy((char *) headPageBuffer + sizeof(int), &endPageNumber, sizeof(int));

			if (pf_FileHandle.WritePage(0, headPageBuffer) == FAILURE) {
				free(headPageBuffer);
				pf_Manager->CloseFile(pf_FileHandle);

				return FAILURE;
			}

		} else {
			free(headPageBuffer);
			pf_Manager->CloseFile(pf_FileHandle);

			return FAILURE;
		}

		free(headPageBuffer);

		unsigned inputPageCount = pf_FileHandle.GetNumberOfPages();
		unsigned outputPageNumber = 1;
		unsigned lastFreeSpacePageNumber = 0;
		unsigned lastFullPageNumber = 0;

		int newFreeSpaceIndex = 0;
		int newFreeSpaceCapacity = PF_PAGE_SIZE - 4 * sizeof(int);
		int newSlotOffset = FREE_SPACE_INDEX_OFFSET - SLOT_SIZE;
		void *newPageBuffer = initializeNewPageBuffer(newFreeSpaceIndex, newFreeSpaceCapacity, newSlotOffset);

		for (unsigned inputPageNumber = 1; inputPageNumber < inputPageCount; inputPageNumber++) {
			void *pageBuffer = malloc(PF_PAGE_SIZE);

			if (pf_FileHandle.ReadPage(inputPageNumber, pageBuffer) == SUCCESS) {
				int freeSpaceIndex = 0;
				int freeSpaceCapacity = 0;

				memcpy(&freeSpaceIndex, (char *) pageBuffer + FREE_SPACE_INDEX_OFFSET, sizeof(int));
				memcpy(&freeSpaceCapacity, (char *) pageBuffer + FREE_SPACE_CAPACITY_OFFSET, sizeof(int));

				for (int slotOffset = FREE_SPACE_INDEX_OFFSET - SLOT_SIZE; slotOffset >= freeSpaceIndex
						+ freeSpaceCapacity; slotOffset -= SLOT_SIZE) {

					char recordStatus = (char) 0;
					memcpy(&recordStatus, (char *) pageBuffer + slotOffset + sizeof(int), sizeof(char));

					if (recordStatus == RECORD_STORED_STATUS || recordStatus == RECORD_NESTED_STATUS) {
						int recordPointer = 0;
						int recordLength = 0;
						memcpy(&recordPointer, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char),
								sizeof(int));
						memcpy(&recordLength, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char),
								sizeof(int));

						if (recordLength + SLOT_SIZE > newFreeSpaceCapacity) {
							if (appendFreeSpaceFullPage(pf_FileHandle, newPageBuffer, outputPageNumber, true,
									newFreeSpaceIndex, newFreeSpaceCapacity, lastFreeSpacePageNumber) == SUCCESS) {

								free(newPageBuffer);

								newPageBuffer = initializeNewPageBuffer(newFreeSpaceIndex, newFreeSpaceCapacity,
										newSlotOffset);
								lastFreeSpacePageNumber = outputPageNumber++;

							} else {
								free(pageBuffer);
								free(newPageBuffer);
								pf_Manager->CloseFile(pf_FileHandle);

								return FAILURE;
							}
						}

						memcpy((char *) newPageBuffer + newFreeSpaceIndex, (char *) pageBuffer + recordPointer,
								recordLength);
						memcpy((char *) newPageBuffer + newSlotOffset, (char *) pageBuffer + slotOffset, SLOT_SIZE);
						memcpy((char *) newPageBuffer + newSlotOffset + sizeof(int) + sizeof(char), &newFreeSpaceIndex,
								sizeof(int));

						if (recordStatus == RECORD_NESTED_STATUS) {
							char newRecordStatus = RECORD_STORED_STATUS;
							memcpy((char *) newPageBuffer + newSlotOffset + sizeof(int), &newRecordStatus, sizeof(char));
						}

						newFreeSpaceIndex += recordLength;
						newFreeSpaceCapacity -= recordLength + SLOT_SIZE;
						newSlotOffset -= SLOT_SIZE;

						if (newFreeSpaceCapacity < MINIMUM_FREE_SPACE) {
							if (appendFreeSpaceFullPage(pf_FileHandle, newPageBuffer, outputPageNumber, false,
									newFreeSpaceIndex, newFreeSpaceCapacity, lastFullPageNumber) == SUCCESS) {

								free(newPageBuffer);

								newPageBuffer = initializeNewPageBuffer(newFreeSpaceIndex, newFreeSpaceCapacity,
										newSlotOffset);
								lastFullPageNumber = outputPageNumber++;

							} else {
								free(pageBuffer);
								free(newPageBuffer);
								pf_Manager->CloseFile(pf_FileHandle);

								return FAILURE;
							}
						}

					} else if (recordStatus == RECORD_DELETED_STATUS || recordStatus == RECORD_MOVED_STATUS) {
						continue;
					}
				}

			} else {
				free(pageBuffer);
				free(newPageBuffer);
				pf_Manager->CloseFile(pf_FileHandle);

				return FAILURE;
			}

			free(pageBuffer);
		}

		while (outputPageNumber < inputPageCount) {
			if (appendFreeSpaceFullPage(pf_FileHandle, newPageBuffer, outputPageNumber, true, newFreeSpaceIndex,
					newFreeSpaceCapacity, lastFreeSpacePageNumber) == SUCCESS) {

				free(newPageBuffer);

				newPageBuffer = initializeNewPageBuffer(newFreeSpaceIndex, newFreeSpaceCapacity, newSlotOffset);
				lastFreeSpacePageNumber = outputPageNumber++;

			} else {
				free(newPageBuffer);
				pf_Manager->CloseFile(pf_FileHandle);

				return FAILURE;
			}
		}

		free(newPageBuffer);
		pf_Manager->CloseFile(pf_FileHandle);

		return SUCCESS;
	}

	pf_Manager->CloseFile(pf_FileHandle);

	return FAILURE;
}

RC RM::createCatalogFile() {
	PF_Manager *pf_Manager = PF_Manager::Instance();

	if (!pf_Manager->IsFileExisting(CATALOG_TABLE_NAME.c_str())) {
		if (pf_Manager->CreateFile(CATALOG_TABLE_NAME.c_str()) == SUCCESS) {
			return initializeTable(CATALOG_TABLE_NAME);

		} else {
			return FAILURE;
		}
	}

	return SUCCESS;
}

RC RM::createDataPage(PF_FileHandle &pf_FileHandle, const unsigned prevPageNumber) {
	void *pageData = malloc(PF_PAGE_SIZE);
	int freeSpaceIndex = 0;
	int freeSpaceCapacity = PF_PAGE_SIZE - 4 * sizeof(int);
	int nextPageNumber = END_PAGE_NUMBER;

	memset((char *) pageData, 0, PF_PAGE_SIZE);
	memcpy((char *) pageData + FREE_SPACE_INDEX_OFFSET, &freeSpaceIndex, sizeof(int));
	memcpy((char *) pageData + FREE_SPACE_CAPACITY_OFFSET, &freeSpaceCapacity, sizeof(int));
	memcpy((char *) pageData + NEXT_PAGE_NUMBER_OFFSET, &nextPageNumber, sizeof(int));
	memcpy((char *) pageData + PREV_PAGE_NUMBER_OFFSET, &prevPageNumber, sizeof(int));

	RC isPageCreated = pf_FileHandle.AppendPage(pageData);

	free(pageData);

	return isPageCreated;
}

RC RM::findSchemaVector(const string tableName, vector<Schema> &schemaVector) {
	if (CATALOG_TABLE_NAME == tableName) {
		Schema schema;
		schema.tableName = CATALOG_TABLE_NAME;
		schema.version = 1;

		vector<Attribute> attributeVector;

		Attribute tableNameAttribute;
		tableNameAttribute.name = TABLE_NAME;
		tableNameAttribute.type = TypeVarChar;
		tableNameAttribute.length = MAX_TABLE_NAME_LENGTH;

		attributeVector.push_back(tableNameAttribute);

		Attribute schemaVersionAttribute;
		schemaVersionAttribute.name = SCHEMA_VERSION;
		schemaVersionAttribute.type = TypeInt;
		schemaVersionAttribute.length = sizeof(int);

		attributeVector.push_back(schemaVersionAttribute);

		Attribute columnNameAttribute;
		columnNameAttribute.name = COLUMN_NAME;
		columnNameAttribute.type = TypeVarChar;
		columnNameAttribute.length = MAX_COLUMN_NAME_LENGTH;

		attributeVector.push_back(columnNameAttribute);

		Attribute positionAttribute;
		positionAttribute.name = POSITION;
		positionAttribute.type = TypeInt;
		positionAttribute.length = sizeof(int);

		attributeVector.push_back(positionAttribute);

		Attribute typeAttribute;
		typeAttribute.name = TYPE;
		typeAttribute.type = TypeInt;
		typeAttribute.length = sizeof(int);

		attributeVector.push_back(typeAttribute);

		Attribute lengthAttribute;
		lengthAttribute.name = LENGTH;
		lengthAttribute.type = TypeInt;
		lengthAttribute.length = sizeof(int);

		attributeVector.push_back(lengthAttribute);

		schema.attributeVector = attributeVector;

		schemaVector.push_back(schema);

		return SUCCESS;
	}

	vector<string> attributeVector = prepareCatalogAttributeVector();
	RM_ScanIterator rm_ScanIterator;

	if (scan(CATALOG_TABLE_NAME, TABLE_NAME, EQ_OP, tableName.c_str(), attributeVector, rm_ScanIterator) == SUCCESS) {
		RID rid;
		void *columnTuple = malloc(MAX_COLUMN_TUPLE_LENGTH);
		map<int, vector<AttributeWithPosition> > versionMap;
		map<int, vector<AttributeWithPosition> >::iterator versionMapIterator;

		while (rm_ScanIterator.getNextTuple(rid, columnTuple) != RM_EOF) {
			int offset = 0;

			int tableNameLength;
			memcpy(&tableNameLength, (char *) columnTuple, sizeof(int));
			offset += sizeof(int);

			char *tableNameInTuple = new char[tableNameLength + 1];
			memcpy(tableNameInTuple, (char *) columnTuple + offset, tableNameLength);
			tableNameInTuple[tableNameLength] = '\0';
			offset += tableNameLength;

			int version;
			memcpy(&version, (char *) columnTuple + offset, sizeof(int));
			offset += sizeof(int);

			int columnNameLength;
			memcpy(&columnNameLength, (char *) columnTuple + offset, sizeof(int));
			offset += sizeof(int);

			char *columnName = new char[columnNameLength + 1];
			memcpy(columnName, (char *) columnTuple + offset, columnNameLength);
			columnName[columnNameLength] = '\0';
			offset += columnNameLength;

			int position;
			memcpy(&position, (char *) columnTuple + offset, sizeof(int));
			offset += sizeof(int);

			int type;
			memcpy(&type, (char *) columnTuple + offset, sizeof(int));
			offset += sizeof(int);

			int length;
			memcpy(&length, (char *) columnTuple + offset, sizeof(int));
			offset += sizeof(int);

			Attribute attribute;
			attribute.name = string(columnName);
			attribute.type = (AttrType) type;
			attribute.length = (AttrLength) length;
			AttributeWithPosition attributeWithPosition;
			attributeWithPosition.attribute = attribute;
			attributeWithPosition.position = position;

			versionMapIterator = versionMap.find(version);

			if (versionMapIterator == versionMap.end()) {
				Schema schema;
				schema.tableName = tableName;
				schema.version = version;
				schemaVector.push_back(schema);

				vector<AttributeWithPosition> attributeWithPositionVector;
				attributeWithPositionVector.push_back(attributeWithPosition);
				versionMap[version] = attributeWithPositionVector;

			} else {
				versionMapIterator->second.push_back(attributeWithPosition);
			}

			delete[] tableNameInTuple;
			delete[] columnName;
		}

		for (unsigned i = 0; i < schemaVector.size(); i++) {
			versionMapIterator = versionMap.find(schemaVector.at(i).version);
			vector<AttributeWithPosition> attributeWithPositionVector = versionMapIterator->second;

			sort(attributeWithPositionVector.begin(), attributeWithPositionVector.end());

			vector<Attribute> attributeVectorInSchema;
			vector<AttributeWithPosition>::iterator attributeWPIterator = attributeWithPositionVector.begin();
			for (; attributeWPIterator != attributeWithPositionVector.end(); ++attributeWPIterator) {
				attributeVectorInSchema.push_back(attributeWPIterator->attribute);
			}
			schemaVector.at(i).attributeVector = attributeVectorInSchema;
		}

		free(columnTuple);
		rm_ScanIterator.close();

		return SUCCESS;

	} else {
		rm_ScanIterator.close();

		return FAILURE;
	}
}

RC RM::findLatestSchema(const string tableName, Schema &schema) {
	vector<Schema> schemaVector;

	if (findSchemaVector(tableName, schemaVector) == SUCCESS) {
		if (schemaVector.empty()) {
			schema.tableName = tableName;
			schema.version = 0;

		} else {
			int maxVersion = 0;
			int latestSchemaIndex = 0;

			for (unsigned i = 0; i < schemaVector.size(); i++) {
				if (schemaVector.at(i).version > maxVersion) {
					latestSchemaIndex = i;
					maxVersion = schemaVector.at(i).version;
				}
			}

			schema.tableName = tableName;
			schema.version = maxVersion;

			for (unsigned i = 0; i < schemaVector.at(latestSchemaIndex).attributeVector.size(); i++) {
				schema.attributeVector.push_back(schemaVector.at(latestSchemaIndex).attributeVector.at(i));
			}
		}

		return SUCCESS;
	}

	return FAILURE;
}

int RM::prepareColumnTuple(const string tableName, const int schemaVersion, const Attribute &attribute,
		const int position, void *tuple) {

	int offset = 0;
	int tableNameLength = tableName.length();
	int columnNameLength = attribute.name.length();

	memcpy((char *) tuple + offset, &tableNameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) tuple + offset, tableName.c_str(), tableNameLength);
	offset += tableNameLength;

	memcpy((char *) tuple + offset, &schemaVersion, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) tuple + offset, &columnNameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) tuple + offset, attribute.name.c_str(), columnNameLength);
	offset += columnNameLength;

	memcpy((char *) tuple + offset, &position, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) tuple + offset, &attribute.type, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) tuple + offset, &attribute.length, sizeof(int));
	offset += sizeof(int);

	return offset;
}

RC RM::insertSchema(const string tableName, const vector<Attribute> &attributeVector) {
	Schema schema;

	if (findLatestSchema(tableName, schema) == SUCCESS) {
		if (schema.version != 0 && schema.attributeVector.size() == attributeVector.size()) {
			bool sameSchema = true;

			for (unsigned i = 0; i < schema.attributeVector.size(); i++) {
				if (schema.attributeVector.at(i).name != attributeVector.at(i).name
						|| schema.attributeVector.at(i).type != attributeVector.at(i).type
						|| schema.attributeVector.at(i).length != attributeVector.at(i).length) {

					sameSchema = false;
					break;
				}
			}

			if (sameSchema) {
				return SUCCESS;
			}
		}

		return insertSchema(tableName, schema.version + 1, attributeVector);
	}

	return FAILURE;
}

RC RM::insertSchema(const string tableName, int version, const vector<Attribute> &attributeVector) {
	for (unsigned i = 0; i < attributeVector.size(); i++) {
		RID rid;
		void *tuple = malloc(MAX_COLUMN_TUPLE_LENGTH);

		prepareColumnTuple(tableName, version, attributeVector.at(i), i, tuple);
		RC isTupleInserted = insertTuple(CATALOG_TABLE_NAME, tuple, rid);

		free(tuple);

		if (isTupleInserted == FAILURE) {
			return FAILURE;
		}
	}

	return SUCCESS;
}

RC RM::deleteAllSchemas(const string tableName) {
	vector<string> attributeVector = prepareCatalogAttributeVector();
	RM_ScanIterator rm_ScanIterator;

	if (scan(CATALOG_TABLE_NAME, TABLE_NAME, EQ_OP, tableName.c_str(), attributeVector, rm_ScanIterator) == SUCCESS) {
		RID rid;
		void *columnTuple = malloc(MAX_COLUMN_TUPLE_LENGTH);

		while (rm_ScanIterator.getNextTuple(rid, columnTuple) != RM_EOF) {
			deleteTuple(tableName, rid);
		}

		free(columnTuple);
		rm_ScanIterator.close();

		return SUCCESS;

	} else {
		rm_ScanIterator.close();

		return FAILURE;
	}
}

vector<string> RM::prepareCatalogAttributeVector() {
	vector<string> attributeVector;
	attributeVector.push_back(TABLE_NAME);
	attributeVector.push_back(SCHEMA_VERSION);
	attributeVector.push_back(COLUMN_NAME);
	attributeVector.push_back(POSITION);
	attributeVector.push_back(TYPE);
	attributeVector.push_back(LENGTH);

	return attributeVector;
}

int RM::calculateTupleLength(const void *data, const vector<Attribute> &attributeVector) {
	int tupleLength = 0;

	for (unsigned i = 0; i < attributeVector.size(); i++) {
		switch (attributeVector.at(i).type) {
			case TypeInt: {
				tupleLength += sizeof(int);
				break;
			}

			case TypeReal: {
				tupleLength += sizeof(float);
				break;
			}

			case TypeVarChar: {
				int stringLength = 0;
				memcpy(&stringLength, (char *) data + tupleLength, sizeof(int));
				tupleLength += sizeof(int) + stringLength;
				break;
			}
		}
	}

	return tupleLength + sizeof(int);
}

void * RM::initializeNewPageBuffer(int &newFreeSpaceIndex, int &newFreeSpaceCapacity, int &newSlotOffset) {
	void *newPageBuffer = malloc(PF_PAGE_SIZE);
	newFreeSpaceIndex = 0;
	newFreeSpaceCapacity = PF_PAGE_SIZE - 4 * sizeof(int);
	newSlotOffset = FREE_SPACE_INDEX_OFFSET - SLOT_SIZE;

	memset(newPageBuffer, 0, PF_PAGE_SIZE);

	return newPageBuffer;
}

RC RM::appendFreeSpaceFullPage(PF_FileHandle &pf_FileHandle, void *pageBuffer, unsigned pageNumber,
		bool isFreeSpacePage, int freeSpaceIndex, int freeSpaceCapacity, unsigned lastPageNumber) {

	memcpy((char *) pageBuffer + FREE_SPACE_INDEX_OFFSET, &freeSpaceIndex, sizeof(int));
	memcpy((char *) pageBuffer + FREE_SPACE_CAPACITY_OFFSET, &freeSpaceCapacity, sizeof(int));

	int nextPageNumber = END_PAGE_NUMBER;
	memcpy((char *) pageBuffer + NEXT_PAGE_NUMBER_OFFSET, &nextPageNumber, sizeof(int));
	memcpy((char *) pageBuffer + PREV_PAGE_NUMBER_OFFSET, &lastPageNumber, sizeof(int));

	if (alterNextPrevPointer(pf_FileHandle, lastPageNumber, isFreeSpacePage ? true : lastPageNumber != 0, pageNumber)
			== SUCCESS) {

		if (pf_FileHandle.WritePage(pageNumber, pageBuffer) == SUCCESS) {
			return SUCCESS;
		}
	}

	return FAILURE;
}

RM_ScanIterator::RM_ScanIterator() {
}

RM_ScanIterator::~RM_ScanIterator() {
}

void RM_ScanIterator::setFileHandle(PF_FileHandle &pf_FileHandle) {
	this->pf_FileHandle = pf_FileHandle;
}

void RM_ScanIterator::setSchemaVector(const vector<Schema> &schemaVector) {
	this->schemaVector = schemaVector;
}

void RM_ScanIterator::setConditionAttribute(const string conditionAttribute) {
	this->conditionAttribute = conditionAttribute;
}

void RM_ScanIterator::setCompOp(const CompOp compOp) {
	this->compOp = compOp;
}

void RM_ScanIterator::setValue(const void *value) {
	this->value = value;
}

void RM_ScanIterator::setAttributeNames(const vector<string> &attributeNames) {
	this->attributeNames = attributeNames;
}

void RM_ScanIterator::initialize() {
	cursor.pageNum = 1;
	cursor.slotNum = 0;
}

bool RM_ScanIterator::compareAttributeValue(const int attributeValue) {
	int valueToCompare = *(int *) value;

	switch (compOp) {
		case EQ_OP: {
			return attributeValue == valueToCompare;
		}

		case LT_OP: {
			return attributeValue < valueToCompare;
		}

		case GT_OP: {
			return attributeValue > valueToCompare;
		}

		case LE_OP: {
			return attributeValue <= valueToCompare;
		}

		case GE_OP: {
			return attributeValue >= valueToCompare;
		}

		case NE_OP: {
			return attributeValue != valueToCompare;;
		}

		case NO_OP: {
			return true;
		}

		default: {
			return true;
		}
	}
}

bool RM_ScanIterator::compareAttributeValue(const float attributeValue) {
	float valueToCompare = *(float *) value;

	switch (compOp) {
		case EQ_OP: {
			return attributeValue == valueToCompare;
		}

		case LT_OP: {
			return attributeValue < valueToCompare;
		}

		case GT_OP: {
			return attributeValue > valueToCompare;
		}

		case LE_OP: {
			return attributeValue <= valueToCompare;
		}

		case GE_OP: {
			return attributeValue >= valueToCompare;
		}

		case NE_OP: {
			return attributeValue != valueToCompare;;
		}

		case NO_OP: {
			return true;
		}

		default: {
			return true;
		}
	}
}

bool RM_ScanIterator::compareAttributeValue(const string attributeValue) {
	string valueToCompare = string((char *) value);

	switch (compOp) {
		case EQ_OP: {
			return attributeValue == valueToCompare;
		}

		case LT_OP: {
			return attributeValue < valueToCompare;
		}

		case GT_OP: {
			return attributeValue > valueToCompare;
		}

		case LE_OP: {
			return attributeValue <= valueToCompare;
		}

		case GE_OP: {
			return attributeValue >= valueToCompare;
		}

		case NE_OP: {
			return attributeValue != valueToCompare;;
		}

		case NO_OP: {
			return true;
		}

		default: {
			return true;
		}
	}
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	for (; cursor.pageNum < pf_FileHandle.GetNumberOfPages(); cursor.pageNum++, cursor.slotNum = 0) {
		void *pageBuffer = malloc(PF_PAGE_SIZE);

		if (pf_FileHandle.ReadPage(cursor.pageNum, pageBuffer) == SUCCESS) {
			int freeSpaceIndex = 0;
			int freeSpaceCapacity = 0;
			memcpy(&freeSpaceIndex, (char *) pageBuffer + RM::FREE_SPACE_INDEX_OFFSET, sizeof(int));
			memcpy(&freeSpaceCapacity, (char *) pageBuffer + RM::FREE_SPACE_CAPACITY_OFFSET, sizeof(int));

			if (RM::FREE_SPACE_INDEX_OFFSET - RM::SLOT_SIZE < freeSpaceIndex + freeSpaceCapacity) {
				free(pageBuffer);
				continue;
			}

			for (int slotOffset = RM::FREE_SPACE_INDEX_OFFSET - RM::SLOT_SIZE; slotOffset >= freeSpaceIndex
					+ freeSpaceCapacity; slotOffset -= RM::SLOT_SIZE) {

				unsigned slotNumber = 0;
				memcpy(&slotNumber, (char *) pageBuffer + slotOffset, sizeof(int));

				if (slotNumber <= cursor.slotNum) {
					continue;

				} else {
					cursor.slotNum = slotNumber;
				}

				char recordStatus = (char) 0;
				memcpy(&recordStatus, (char *) pageBuffer + slotOffset + sizeof(int), sizeof(char));

				if (recordStatus == RM::RECORD_DELETED_STATUS || recordStatus == RM::RECORD_NESTED_STATUS) {
					continue;

				} else if (recordStatus == RM::RECORD_MOVED_STATUS) {
					unsigned nextPageNumber = 0;
					unsigned nextSlotNumber = 0;
					memcpy(&nextPageNumber, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), sizeof(int));
					memcpy(&nextSlotNumber, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char),
							sizeof(int));

					if (getNextRecursively(nextPageNumber, nextSlotNumber, rid, data) == SUCCESS) {
						free(pageBuffer);

						return SUCCESS;

					} else {
						continue;
					}

				} else if (recordStatus == RM::RECORD_STORED_STATUS) {
					int recordPointer = 0;
					int recordLength = 0;
					memcpy(&recordPointer, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), sizeof(int));
					memcpy(&recordLength, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char),
							sizeof(int));

					if (getStoredTuple(pageBuffer, recordPointer, recordLength, rid, data) == SUCCESS) {
						free(pageBuffer);

						return SUCCESS;

					} else {
						continue;
					}
				}
			}
		}

		free(pageBuffer);
	}

	return RM_EOF;
}

RC RM_ScanIterator::getNextRecursively(unsigned pageNumber, unsigned slotNumber, RID &rid, void *data) {
	void *pageBuffer = malloc(PF_PAGE_SIZE);

	if (pf_FileHandle.ReadPage(pageNumber, pageBuffer) == SUCCESS) {
		int slotOffset = RM::FREE_SPACE_INDEX_OFFSET;

		if (RM::locateSlot(pageBuffer, slotNumber, slotOffset) == FAILURE) {
			free(pageBuffer);

			return FAILURE;
		}

		char recordStatus = (char) 0;
		memcpy(&recordStatus, (char *) pageBuffer + slotOffset + sizeof(int), sizeof(char));

		if (recordStatus == RM::RECORD_STORED_STATUS || recordStatus == RM::RECORD_NESTED_STATUS) {
			int recordPointer = 0;
			int recordLength = 0;
			memcpy(&recordPointer, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), sizeof(int));
			memcpy(&recordLength, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), sizeof(int));

			if (getStoredTuple(pageBuffer, recordPointer, recordLength, rid, data) == SUCCESS) {
				free(pageBuffer);

				return SUCCESS;

			} else {
				free(pageBuffer);

				return FAILURE;
			}

		} else if (recordStatus == RM::RECORD_MOVED_STATUS) {
			unsigned nextPageNumber = 0;
			unsigned nextSlotNumber = 0;
			memcpy(&nextPageNumber, (char *) pageBuffer + slotOffset + sizeof(int) + sizeof(char), sizeof(int));
			memcpy(&nextSlotNumber, (char *) pageBuffer + slotOffset + 2 * sizeof(int) + sizeof(char), sizeof(int));

			free(pageBuffer);

			return getNextRecursively(nextPageNumber, nextSlotNumber, rid, data);

		} else {
			free(pageBuffer);

			return FAILURE;
		}

	} else {
		free(pageBuffer);

		return FAILURE;
	}
}

RC RM_ScanIterator::getStoredTuple(void *pageBuffer, int recordPointer, int recordLength, RID &rid, void *data) {
	int schemaVersion = 0;
	memcpy(&schemaVersion, (char *) pageBuffer + recordPointer, sizeof(int));

	bool schemaQualified = false;
	unsigned schemaIndex = 0;
	for (unsigned i = 0; i < schemaVector.size(); i++) {
		if (schemaVector.at(i).version == schemaVersion) {
			schemaQualified = true;
			schemaIndex = i;
			break;
		}
	}

	if (!schemaQualified) {
		return FAILURE;
	}

	void *record = malloc(recordLength - sizeof(int));
	memcpy((char *) record, (char *) pageBuffer + recordPointer + sizeof(int), recordLength - sizeof(int));

	void *temporaryData = malloc(recordLength - sizeof(int));
	bool conditionFailed = false;
	int attributeOffset = 0;
	int returnDataLength = 0;

	for (unsigned i = 0; i < schemaVector.at(schemaIndex).attributeVector.size(); i++) {
		Attribute attribute = schemaVector.at(schemaIndex).attributeVector.at(i);

		switch (attribute.type) {
			case TypeInt: {
				int attributeValue = 0;
				memcpy(&attributeValue, (char *) record + attributeOffset, sizeof(int));

				if (attribute.name == conditionAttribute) {
					conditionFailed = !compareAttributeValue(attributeValue);
				}

				if (!conditionFailed) {
					bool needed = false;
					for (unsigned j = 0; j < attributeNames.size(); j++) {
						if (attribute.name == attributeNames.at(j)) {
							needed = true;
							break;
						}
					}

					if (needed) {
						memcpy((char *) temporaryData + returnDataLength, &attributeValue, sizeof(int));
						returnDataLength += sizeof(int);
					}
				}

				attributeOffset += sizeof(int);
				break;
			}

			case TypeReal: {
				float attributeValue = 0.0;
				memcpy(&attributeValue, (char *) record + attributeOffset, sizeof(float));

				if (attribute.name == conditionAttribute) {
					conditionFailed = !compareAttributeValue(attributeValue);
				}

				if (!conditionFailed) {
					bool needed = false;
					for (unsigned j = 0; j < attributeNames.size(); j++) {
						if (attribute.name == attributeNames.at(j)) {
							needed = true;
							break;
						}
					}

					if (needed) {
						memcpy((char *) temporaryData + returnDataLength, &attributeValue, sizeof(float));
						returnDataLength += sizeof(float);
					}
				}

				attributeOffset += sizeof(float);
				break;
			}

			case TypeVarChar: {
				int stringLength = 0;
				memcpy(&stringLength, (char *) record + attributeOffset, sizeof(int));

				char *attributeValue = new char[stringLength + 1];
				memcpy((char *) attributeValue, (char *) record + attributeOffset + sizeof(int), stringLength);
				attributeValue[stringLength] = '\0';

				if (attribute.name == conditionAttribute) {
					conditionFailed = !compareAttributeValue(string(attributeValue));
				}

				if (!conditionFailed) {
					bool needed = false;
					for (unsigned j = 0; j < attributeNames.size(); j++) {
						if (attribute.name == attributeNames.at(j)) {
							needed = true;
							break;
						}
					}

					if (needed) {
						memcpy((char *) temporaryData + returnDataLength, (char *) record + attributeOffset,
								sizeof(int) + stringLength);
						returnDataLength += sizeof(int) + stringLength;
					}
				}

				delete[] attributeValue;
				attributeOffset += sizeof(int) + stringLength;
				break;
			}
		}

		if (conditionFailed) {
			break;
		}
	}

	if (!conditionFailed) {
		rid.pageNum = cursor.pageNum;
		rid.slotNum = cursor.slotNum;
		memcpy((char *) data, (char *) temporaryData, returnDataLength);

		free(record);
		free(temporaryData);

		return SUCCESS;

	} else {
		free(record);
		free(temporaryData);

		return FAILURE;
	}
}

RC RM_ScanIterator::close() {
	return PF_Manager::Instance()->CloseFile(pf_FileHandle);
}
