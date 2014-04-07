#include <cstdlib>
#include <cstring>
#include <iostream>

#include "ix.h"

IX_Manager *IX_Manager::_ix_manager = 0;

IX_Manager * IX_Manager::Instance() {
	if (!_ix_manager) {
		_ix_manager = new IX_Manager();
	}

	return _ix_manager;
}

IX_Manager::IX_Manager() {
}

IX_Manager::~IX_Manager() {
}

RC IX_Manager::CreateIndex(const string tableName, const string attributeName) {
	string indexName = generateIndexName(tableName, attributeName);

	if (PF_Manager::Instance()->CreateFile(indexName.c_str()) == SUCCESS) {
		Schema schema;

		if (RM::Instance()->findLatestSchema(tableName, schema) == SUCCESS) {
			AttrType attrType = TypeInt;
			bool isAttributeFound = false;

			for (unsigned i = 0; i < schema.attributeVector.size(); i++) {
				if (schema.attributeVector.at(i).name == attributeName) {
					attrType = schema.attributeVector.at(i).type;
					isAttributeFound = true;
					break;
				}
			}

			if (isAttributeFound) {
				return initializeIndex(indexName, attrType);
			}
		}
	}

	return FAILURE;
}

string IX_Manager::generateIndexName(const string tableName, const string attributeName) {
	return tableName + "_" + attributeName + "_index";
}

RC IX_Manager::initializeIndex(const string indexName, const AttrType attrType) {
	PF_Manager *pf_Manager = PF_Manager::Instance();
	PF_FileHandle pf_FileHandle;

	if (pf_Manager->OpenFile(indexName.c_str(), pf_FileHandle) == SUCCESS) {
		void *headPageData = malloc(PF_PAGE_SIZE);
		unsigned rootNodePointer = 1;
		int indexTreeHeight = 0;
		int attributeType = attrType;

		memset((char *) headPageData, 0, PF_PAGE_SIZE);
		memcpy((char *) headPageData, &rootNodePointer, sizeof(int));
		memcpy((char *) headPageData + sizeof(int), &indexTreeHeight, sizeof(int));
		memcpy((char *) headPageData + sizeof(int) * 2, &attributeType, sizeof(int));

		RC isheadPageCreated = pf_FileHandle.AppendPage(headPageData);

		free(headPageData);

		if (isheadPageCreated == SUCCESS) {
			void *nodeBuffer = malloc(PF_PAGE_SIZE);
			unsigned nodePointer = 0;

			bool isLeafNodeCreated = createLeafNode(pf_FileHandle, 0, 0, nodeBuffer, nodePointer);

			free(nodeBuffer);
			pf_Manager->CloseFile(pf_FileHandle);

			return isLeafNodeCreated;
		}
	}

	pf_Manager->CloseFile(pf_FileHandle);

	return FAILURE;
}

RC IX_Manager::createLeafNode(PF_FileHandle &pf_FileHandle, const unsigned nextNodePointer,
		const unsigned prevNodePointer, void *nodeBuffer, unsigned &nodePointer) {

	int freeSpaceIndex = 0;
	memset((char *) nodeBuffer, 0, PF_PAGE_SIZE);
	memcpy((char *) nodeBuffer + LEAF_FREE_SPACE_INDEX_OFFSET, &freeSpaceIndex, sizeof(int));
	memcpy((char *) nodeBuffer + NEXT_NODE_POINTER_OFFSET, &nextNodePointer, sizeof(int));
	memcpy((char *) nodeBuffer + PREV_NODE_POINTER_OFFSET, &prevNodePointer, sizeof(int));

	if (pf_FileHandle.AppendPage(nodeBuffer) == SUCCESS) {
		nodePointer = pf_FileHandle.GetNumberOfPages() - 1;

		return SUCCESS;
	}

	return FAILURE;
}

RC IX_Manager::createNonLeafNode(PF_FileHandle &pf_FileHandle, void *nodeBuffer, unsigned &nodePointer) {
	int freeSpaceIndex = 0;
	memset((char *) nodeBuffer, 0, PF_PAGE_SIZE);
	memcpy((char *) nodeBuffer + NON_LEAF_FREE_SPACE_INDEX_OFFSET, &freeSpaceIndex, sizeof(int));

	if (pf_FileHandle.AppendPage(nodeBuffer) == SUCCESS) {
		nodePointer = pf_FileHandle.GetNumberOfPages() - 1;

		return SUCCESS;
	}

	return FAILURE;
}

RC IX_Manager::DestroyIndex(const string tableName, const string attributeName) {
	return PF_Manager::Instance()->DestroyFile(generateIndexName(tableName, attributeName).c_str());
}

RC IX_Manager::OpenIndex(const string tableName, const string attributeName, IX_IndexHandle &indexHandle) {
	return PF_Manager::Instance()->OpenFile(generateIndexName(tableName, attributeName).c_str(),
			indexHandle.getFileHandle());
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle) {
	return PF_Manager::Instance()->CloseFile(indexHandle.getFileHandle());;
}

IX_IndexHandle::IX_IndexHandle() {
	deletedAfterScan = false;
	deletedEntryNodePointer = 0;
	deletedEntryPosition = 0;
	nextEntryNodePointer = 0;
	nextEntryPosition = 0;
}

IX_IndexHandle::~IX_IndexHandle() {
}

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid) {
	void *headPageBuffer = malloc(PF_PAGE_SIZE);

	if (pf_FileHandle.ReadPage(0, headPageBuffer) == SUCCESS) {
		unsigned rootNodePointer = 0;
		int indexTreeHeight = 0;
		int attributeType = 0;

		memcpy(&rootNodePointer, (char *) headPageBuffer, sizeof(int));
		memcpy(&indexTreeHeight, (char *) headPageBuffer + sizeof(int), sizeof(int));
		memcpy(&attributeType, (char *) headPageBuffer + sizeof(int) * 2, sizeof(int));

		void *newEntryKey = NULL;
		unsigned newEntryPointer = 0;

		if (insertRecursively(rootNodePointer, key, rid, newEntryKey, newEntryPointer, indexTreeHeight, 0,
				attributeType) == SUCCESS) {

			if (newEntryKey != NULL) {
				void *newNodeBuffer = malloc(PF_PAGE_SIZE);
				unsigned newNodePointer = 0;

				if (IX_Manager::Instance()->createNonLeafNode(pf_FileHandle, newNodeBuffer, newNodePointer) == SUCCESS) {
					int newEntryKeyLength = calculateKeyLength(newEntryKey, attributeType);
					int newContentLength = newEntryKeyLength + sizeof(int) * 2;

					memcpy((char *) newNodeBuffer, &rootNodePointer, sizeof(int));
					memcpy((char *) newNodeBuffer + sizeof(int), (char *) newEntryKey, newEntryKeyLength);
					memcpy((char *) newNodeBuffer + sizeof(int) + newEntryKeyLength, &newEntryPointer, sizeof(int));
					memcpy((char *) newNodeBuffer + IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, &newContentLength,
							sizeof(int));

					if (pf_FileHandle.WritePage(newNodePointer, newNodeBuffer) == SUCCESS) {
						indexTreeHeight++;
						memcpy((char *) headPageBuffer, &newNodePointer, sizeof(int));
						memcpy((char *) headPageBuffer + sizeof(int), &indexTreeHeight, sizeof(int));

						if (pf_FileHandle.WritePage(0, headPageBuffer) == SUCCESS) {
							free(newNodeBuffer);
							free(headPageBuffer);
							free(newEntryKey);

							return SUCCESS;
						}
					}
				}

				free(newNodeBuffer);
				free(headPageBuffer);
				free(newEntryKey);

				return FAILURE;
			}

			free(headPageBuffer);

			return SUCCESS;
		}
	}

	free(headPageBuffer);

	return FAILURE;
}

RC IX_IndexHandle::insertRecursively(const unsigned nodePointer, const void *key, const RID &rid, void *&newEntryKey,
		unsigned &newEntryPointer, const int indexTreeHeight, const int currentHeight, const int attributeType) {

	void *nodeBuffer = malloc(PF_PAGE_SIZE);
	string keyString = produceKeyString(attributeType, key);

	if (pf_FileHandle.ReadPage(nodePointer, nodeBuffer) == SUCCESS) {

		if (currentHeight != indexTreeHeight) {
			int freeSpaceIndex = 0;
			memcpy(&freeSpaceIndex, (char *) nodeBuffer + IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, sizeof(int));

			int nextChildPointerOffset = findNextChildPointerOffset(nodeBuffer, freeSpaceIndex, attributeType, key,
					keyString, false);

			unsigned nextChildPointer = 0;
			memcpy(&nextChildPointer, (char *) nodeBuffer + nextChildPointerOffset, sizeof(int));

			if (insertRecursively(nextChildPointer, key, rid, newEntryKey, newEntryPointer, indexTreeHeight,
					currentHeight + 1, attributeType) == SUCCESS) {

				if (newEntryKey == NULL) {
					free(nodeBuffer);

					return SUCCESS;

				} else {
					void *helpBuffer = malloc(PF_PAGE_SIZE * 2);

					int insertOffset = nextChildPointerOffset + sizeof(int);
					int newEntryKeyLength = calculateKeyLength(newEntryKey, attributeType);
					int remainingLength = freeSpaceIndex - insertOffset;
					int freeSpaceCapacity = IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET - freeSpaceIndex;

					memset((char *) helpBuffer, 0, PF_PAGE_SIZE * 2);
					memcpy((char *) helpBuffer, (char *) nodeBuffer, insertOffset);
					memcpy((char *) helpBuffer + insertOffset, (char *) newEntryKey, newEntryKeyLength);
					memcpy((char *) helpBuffer + insertOffset + newEntryKeyLength, &newEntryPointer, sizeof(int));

					if (remainingLength > 0) {
						memcpy((char *) helpBuffer + insertOffset + newEntryKeyLength + sizeof(int),
								(char *) nodeBuffer + insertOffset, remainingLength);
					}

					if (newEntryKeyLength + (int) sizeof(int) <= freeSpaceCapacity) {
						int nodeContentLength = freeSpaceIndex + newEntryKeyLength + sizeof(int);

						memcpy((char *) nodeBuffer, (char *) helpBuffer, nodeContentLength);
						memcpy((char *) nodeBuffer + IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, &nodeContentLength,
								sizeof(int));

						free(helpBuffer);

						if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {
							free(newEntryKey);
							free(nodeBuffer);
							newEntryKey = NULL;

							return SUCCESS;

						} else {
							free(newEntryKey);
							free(nodeBuffer);
							newEntryKey = NULL;

							return FAILURE;
						}

					} else {
						int splitThreshold = (IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET) / 2;

						for (int i = sizeof(int); i < freeSpaceIndex + newEntryKeyLength + (int) sizeof(int); i
								+= sizeof(int)) {

							int keyLength = calculateKeyLength((char *) helpBuffer + i, attributeType);

							if (i + keyLength > splitThreshold || i + keyLength + (int) sizeof(int) > splitThreshold) {
								free(newEntryKey);
								newEntryKey = malloc(keyLength);
								memcpy((char *) newEntryKey, (char *) helpBuffer + i, keyLength);

								memset((char *) nodeBuffer + i, 0, IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET - i);
								memcpy((char *) nodeBuffer + IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, &i,
										sizeof(int));

								if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {
									void *newNodeBuffer = malloc(PF_PAGE_SIZE);
									unsigned newNodePointer = 0;

									if (IX_Manager::Instance()->createNonLeafNode(pf_FileHandle, newNodeBuffer,
											newNodePointer) == SUCCESS) {

										int newNodeContentLength = freeSpaceIndex + newEntryKeyLength + sizeof(int) - i
												- keyLength;

										memcpy((char *) newNodeBuffer, (char *) helpBuffer + i + keyLength,
												newNodeContentLength);
										memcpy((char *) newNodeBuffer + IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET,
												&newNodeContentLength, sizeof(int));

										if (pf_FileHandle.WritePage(newNodePointer, newNodeBuffer) == SUCCESS) {
											newEntryPointer = newNodePointer;

											free(helpBuffer);
											free(nodeBuffer);
											free(newNodeBuffer);

											return SUCCESS;
										}
									}

									free(newNodeBuffer);
								}

								free(helpBuffer);
								free(nodeBuffer);
								free(newEntryKey);

								return FAILURE;

							} else {
								i += keyLength;
							}
						}
					}
				}

			} else {
				free(nodeBuffer);

				return FAILURE;
			}

		} else {
			int freeSpaceIndex = 0;
			memcpy(&freeSpaceIndex, (char *) nodeBuffer + IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET, sizeof(int));

			int entryPosition = findEntryPositionToInsert(nodeBuffer, freeSpaceIndex, attributeType, key, keyString);

			void *helpBuffer = malloc(PF_PAGE_SIZE * 2);

			int keyLength = calculateKeyLength(key, attributeType);
			int remainingLength = freeSpaceIndex - entryPosition;
			int freeSpaceCapacity = IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET - freeSpaceIndex;

			memset((char *) helpBuffer, 0, PF_PAGE_SIZE * 2);
			memcpy((char *) helpBuffer, (char *) nodeBuffer, entryPosition);
			memcpy((char *) helpBuffer + entryPosition, (char *) key, keyLength);
			memcpy((char *) helpBuffer + entryPosition + keyLength, &rid.pageNum, sizeof(int));
			memcpy((char *) helpBuffer + entryPosition + keyLength + sizeof(int), &rid.slotNum, sizeof(int));

			if (remainingLength > 0) {
				memcpy((char *) helpBuffer + entryPosition + keyLength + sizeof(int) * 2, (char *) nodeBuffer
						+ entryPosition, remainingLength);
			}

			if (keyLength + (int) sizeof(int) * 2 <= freeSpaceCapacity) {
				int nodeContentLength = freeSpaceIndex + keyLength + sizeof(int) * 2;

				memcpy((char *) nodeBuffer, (char *) helpBuffer, nodeContentLength);
				memcpy((char *) nodeBuffer + IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET, &nodeContentLength, sizeof(int));

				free(helpBuffer);

				if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {
					free(nodeBuffer);
					newEntryKey = NULL;

					return SUCCESS;

				} else {
					free(nodeBuffer);
					newEntryKey = NULL;

					return FAILURE;
				}

			} else {
				int splitThreshold = IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET / 2;

				for (int i = 0; i < freeSpaceIndex + keyLength + (int) sizeof(int) * 2; i += sizeof(int) * 2) {
					int leafKeyLength = calculateKeyLength((char *) helpBuffer + i, attributeType);

					if (i + leafKeyLength > splitThreshold || i + leafKeyLength + (int) sizeof(int) * 2
							> splitThreshold) {

						newEntryKey = malloc(leafKeyLength);
						memcpy((char *) newEntryKey, (char *) helpBuffer + i, leafKeyLength);

						unsigned oldNextNodePointer = 0;
						memcpy(&oldNextNodePointer, (char *) nodeBuffer + IX_Manager::NEXT_NODE_POINTER_OFFSET,
								sizeof(int));

						void *newNodeBuffer = malloc(PF_PAGE_SIZE);
						unsigned newNodePointer = 0;

						if (IX_Manager::Instance()->createLeafNode(pf_FileHandle, oldNextNodePointer, nodePointer,
								newNodeBuffer, newNodePointer) == SUCCESS) {

							int newNodeContentLength = freeSpaceIndex + keyLength + sizeof(int) * 2 - i;

							memcpy((char *) newNodeBuffer, (char *) helpBuffer + i, newNodeContentLength);
							memcpy((char *) newNodeBuffer + IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET,
									&newNodeContentLength, sizeof(int));

							if (pf_FileHandle.WritePage(newNodePointer, newNodeBuffer) == SUCCESS) {
								newEntryPointer = newNodePointer;

								memset((char *) nodeBuffer + i, 0, IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET - i);
								memcpy((char *) nodeBuffer + IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET, &i, sizeof(int));
								memcpy((char *) nodeBuffer + IX_Manager::NEXT_NODE_POINTER_OFFSET, &newNodePointer,
										sizeof(int));

								if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {

									if (oldNextNodePointer != 0) {
										void *oldNextNodeBuffer = malloc(PF_PAGE_SIZE);

										if (pf_FileHandle.ReadPage(oldNextNodePointer, oldNextNodeBuffer) == SUCCESS) {
											memcpy((char *) oldNextNodeBuffer + IX_Manager::PREV_NODE_POINTER_OFFSET,
													&newNodePointer, sizeof(int));

											if (pf_FileHandle.WritePage(oldNextNodePointer, oldNextNodeBuffer)
													== SUCCESS) {
												free(helpBuffer);
												free(nodeBuffer);
												free(newNodeBuffer);
												free(oldNextNodeBuffer);

												return SUCCESS;
											}
										}

										free(oldNextNodeBuffer);

									} else {
										free(helpBuffer);
										free(nodeBuffer);
										free(newNodeBuffer);

										return SUCCESS;
									}
								}
							}
						}

						free(newNodeBuffer);
						free(helpBuffer);
						free(nodeBuffer);
						free(newEntryKey);

						return FAILURE;

					} else {
						i += leafKeyLength;
					}
				}
			}
		}
	}

	free(nodeBuffer);

	return FAILURE;
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid) {
	void *headPageBuffer = malloc(PF_PAGE_SIZE);

	if (pf_FileHandle.ReadPage(0, headPageBuffer) == SUCCESS) {
		unsigned rootNodePointer = 0;
		int indexTreeHeight = 0;
		int attributeType = 0;

		memcpy(&rootNodePointer, (char *) headPageBuffer, sizeof(int));
		memcpy(&indexTreeHeight, (char *) headPageBuffer + sizeof(int), sizeof(int));
		memcpy(&attributeType, (char *) headPageBuffer + sizeof(int) * 2, sizeof(int));

		unsigned rootParentPointer = 0;
		bool isDeleting = false;
		unsigned mergedNodePointer = 0;

		if (deleteRecursively(rootParentPointer, rootNodePointer, key, rid, isDeleting, false, 0, mergedNodePointer,
				indexTreeHeight, 0, attributeType) == SUCCESS) {

			if (isDeleting) {
				indexTreeHeight--;
				memcpy((char *) headPageBuffer, &mergedNodePointer, sizeof(int));
				memcpy((char *) headPageBuffer + sizeof(int), &indexTreeHeight, sizeof(int));

				if (pf_FileHandle.WritePage(0, headPageBuffer) == FAILURE) {
					free(headPageBuffer);

					return FAILURE;
				}
			}

			deletedAfterScan = true;

			free(headPageBuffer);

			return SUCCESS;
		}
	}

	free(headPageBuffer);

	return FAILURE;
}

RC IX_IndexHandle::deleteRecursively(const unsigned parentPointer, const unsigned nodePointer, const void *key,
		const RID &rid, bool &isDeleting, const bool isFirstChild, const int middleKeyOffset,
		unsigned &mergedNodePointer, const int indexTreeHeight, const int currentHeight, const int attributeType) {

	void *nodeBuffer = malloc(PF_PAGE_SIZE);
	string keyString = produceKeyString(attributeType, key);

	if (pf_FileHandle.ReadPage(nodePointer, nodeBuffer) == SUCCESS) {

		if (currentHeight != indexTreeHeight) {
			int freeSpaceIndex = 0;
			memcpy(&freeSpaceIndex, (char *) nodeBuffer + IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, sizeof(int));

			vector<NextChildPointerOffset> nextChildPointerOffsetVector = findNextChildPointerOffset(nodeBuffer,
					freeSpaceIndex, attributeType, key, keyString);

			for (unsigned vectorIndex = 0; vectorIndex < nextChildPointerOffsetVector.size(); vectorIndex++) {
				unsigned nextChildPointer = nextChildPointerOffsetVector.at(vectorIndex).nextChildPointer;
				bool isNewFirstChild = nextChildPointerOffsetVector.at(vectorIndex).isNewFirstChild;
				int newMiddleKeyOffset = nextChildPointerOffsetVector.at(vectorIndex).newMiddleKeyOffset;

				RC deletionReturn = deleteRecursively(nodePointer, nextChildPointer, key, rid, isDeleting,
						isNewFirstChild, newMiddleKeyOffset, mergedNodePointer, indexTreeHeight, currentHeight + 1,
						attributeType);

				if (deletionReturn == FAILURE) {
					isDeleting = false;
					mergedNodePointer = 0;

					free(nodeBuffer);

					return FAILURE;

				} else if (deletionReturn == SUCCESS) {
					if (!isDeleting) {
						free(nodeBuffer);

						return SUCCESS;

					} else {
						void *helpBuffer = malloc(PF_PAGE_SIZE);

						int middleEntryLength = calculateKeyLength((char *) nodeBuffer + newMiddleKeyOffset,
								attributeType) + sizeof(int);
						int newContentLength = freeSpaceIndex - middleEntryLength;
						int remainingLength = newContentLength - newMiddleKeyOffset;

						memset((char *) helpBuffer, 0, PF_PAGE_SIZE);
						memcpy((char *) helpBuffer, (char *) nodeBuffer, newMiddleKeyOffset);

						if (remainingLength > 0) {
							memcpy((char *) helpBuffer + newMiddleKeyOffset, (char *) nodeBuffer + newMiddleKeyOffset
									+ middleEntryLength, remainingLength);
						}

						if (currentHeight == 0 || newContentLength >= IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET / 2) {
							if (currentHeight == 0 && newContentLength == sizeof(int)) {
								isDeleting = true;

								return SUCCESS;
							}

							memset((char *) nodeBuffer, 0, PF_PAGE_SIZE);
							memcpy((char *) nodeBuffer, (char *) helpBuffer, newContentLength);
							memcpy((char *) nodeBuffer + IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET,
									&newContentLength, sizeof(int));

							free(helpBuffer);

							if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {
								isDeleting = false;
								mergedNodePointer = 0;

								free(nodeBuffer);

								return SUCCESS;

							} else {
								isDeleting = false;
								mergedNodePointer = 0;

								free(nodeBuffer);

								return FAILURE;
							}

						} else {
							void *mergedBuffer = malloc(PF_PAGE_SIZE * 3);
							void *parentBuffer = malloc(PF_PAGE_SIZE);
							void *siblingBuffer = malloc(PF_PAGE_SIZE);

							memset((char *) mergedBuffer, 0, PF_PAGE_SIZE * 3);

							if (pf_FileHandle.ReadPage(parentPointer, parentBuffer) == SUCCESS) {
								int parentMiddleKeyLength = calculateKeyLength((char *) parentBuffer + middleKeyOffset,
										attributeType);
								int siblingPointer = 0;

								if (isFirstChild) {
									memcpy(&siblingPointer, (char *) parentBuffer + middleKeyOffset
											+ parentMiddleKeyLength, sizeof(int));
								} else {
									memcpy(&siblingPointer, (char *) parentBuffer + middleKeyOffset - sizeof(int),
											sizeof(int));
								}

								if (pf_FileHandle.ReadPage(siblingPointer, siblingBuffer) == SUCCESS) {
									int siblingContentLength = 0;

									memcpy(&siblingContentLength, (char *) siblingBuffer
											+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, sizeof(int));

									if (isFirstChild) {
										memcpy((char *) mergedBuffer, (char *) helpBuffer, newContentLength);
										memcpy((char *) mergedBuffer + newContentLength, (char *) parentBuffer
												+ middleKeyOffset, parentMiddleKeyLength);
										memcpy((char *) mergedBuffer + newContentLength + parentMiddleKeyLength,
												(char *) siblingBuffer, siblingContentLength);

									} else {
										memcpy((char *) mergedBuffer, (char *) siblingBuffer, siblingContentLength);
										memcpy((char *) mergedBuffer + siblingContentLength, (char *) parentBuffer
												+ middleKeyOffset, parentMiddleKeyLength);
										memcpy((char *) mergedBuffer + siblingContentLength + parentMiddleKeyLength,
												(char *) helpBuffer, newContentLength);
									}

									int totalMergedLength = newContentLength + parentMiddleKeyLength
											+ siblingContentLength;

									if (totalMergedLength <= IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET) {
										if (isFirstChild) {
											memset((char *) nodeBuffer, 0, PF_PAGE_SIZE);
											memcpy((char *) nodeBuffer, (char *) mergedBuffer, totalMergedLength);
											memcpy((char *) nodeBuffer + IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET,
													&totalMergedLength, sizeof(int));

											if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {
												isDeleting = true;
												mergedNodePointer = nodePointer;

												free(nodeBuffer);
												free(mergedBuffer);
												free(parentBuffer);
												free(helpBuffer);
												free(siblingBuffer);

												return SUCCESS;

											} else {
												isDeleting = false;
												mergedNodePointer = 0;

												free(nodeBuffer);
												free(mergedBuffer);
												free(parentBuffer);
												free(helpBuffer);
												free(siblingBuffer);

												return FAILURE;
											}

										} else {
											memset((char *) siblingBuffer, 0, PF_PAGE_SIZE);
											memcpy((char *) siblingBuffer, (char *) mergedBuffer, totalMergedLength);
											memcpy((char *) siblingBuffer
													+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, &totalMergedLength,
													sizeof(int));

											if (pf_FileHandle.WritePage(siblingPointer, siblingBuffer) == SUCCESS) {
												isDeleting = true;
												mergedNodePointer = siblingPointer;

												free(nodeBuffer);
												free(mergedBuffer);
												free(parentBuffer);
												free(helpBuffer);
												free(siblingBuffer);

												return SUCCESS;

											} else {
												isDeleting = false;
												mergedNodePointer = 0;

												free(nodeBuffer);
												free(mergedBuffer);
												free(parentBuffer);
												free(helpBuffer);
												free(siblingBuffer);

												return FAILURE;
											}
										}

									} else {
										for (int i = sizeof(int); i < totalMergedLength; i += sizeof(int)) {
											int keyLength =
													calculateKeyLength((char *) mergedBuffer + i, attributeType);

											if (i + keyLength > totalMergedLength / 2 || i + keyLength
													+ (int) sizeof(int) > totalMergedLength / 2) {

												int parentContentLength = 0;
												memcpy(&parentContentLength, (char *) parentBuffer
														+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, sizeof(int));

												if (parentContentLength - parentMiddleKeyLength + keyLength
														<= IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET) {

													int remainingParentLength = parentContentLength - middleKeyOffset
															- parentMiddleKeyLength;
													int newParentLength = middleKeyOffset + keyLength
															+ remainingParentLength;

													void *middleBuffer = malloc(PF_PAGE_SIZE);

													if (remainingParentLength > 0) {
														memset((char *) middleBuffer, 0, PF_PAGE_SIZE);
														memcpy((char *) middleBuffer, (char *) parentBuffer
																+ middleKeyOffset + parentMiddleKeyLength,
																remainingParentLength);
													}

													memset((char *) parentBuffer + middleKeyOffset, 0,
															parentContentLength - middleKeyOffset);
													memcpy((char *) parentBuffer + middleKeyOffset,
															(char *) mergedBuffer + i, keyLength);
													memcpy((char *) parentBuffer
															+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET,
															&newParentLength, sizeof(int));

													if (remainingParentLength > 0) {
														memcpy((char *) parentBuffer + middleKeyOffset + keyLength,
																(char *) middleBuffer, remainingParentLength);
													}

													free(middleBuffer);

													if (pf_FileHandle.WritePage(parentPointer, parentBuffer) == SUCCESS) {

														if (isFirstChild) {
															memset((char *) nodeBuffer, 0, PF_PAGE_SIZE);
															memcpy((char *) nodeBuffer, (char *) mergedBuffer, i);
															memcpy((char *) nodeBuffer
																	+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, &i,
																	sizeof(int));

															if (pf_FileHandle.WritePage(nodePointer, nodeBuffer)
																	== SUCCESS) {
																int remainingLength = totalMergedLength - i - keyLength;

																memset((char *) siblingBuffer, 0, PF_PAGE_SIZE);
																memcpy((char *) siblingBuffer, (char *) mergedBuffer
																		+ i + keyLength, remainingLength);
																memcpy((char *) siblingBuffer
																		+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET,
																		&remainingLength, sizeof(int));

																if (pf_FileHandle.WritePage(siblingPointer,
																		siblingBuffer) == SUCCESS) {

																	isDeleting = false;
																	mergedNodePointer = 0;

																	free(nodeBuffer);
																	free(mergedBuffer);
																	free(parentBuffer);
																	free(helpBuffer);
																	free(siblingBuffer);

																	return SUCCESS;
																}
															}

															isDeleting = false;
															mergedNodePointer = 0;

															free(nodeBuffer);
															free(mergedBuffer);
															free(parentBuffer);
															free(helpBuffer);
															free(siblingBuffer);

															return FAILURE;

														} else {
															memset((char *) siblingBuffer, 0, PF_PAGE_SIZE);
															memcpy((char *) siblingBuffer, (char *) mergedBuffer, i);
															memcpy((char *) siblingBuffer
																	+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, &i,
																	sizeof(int));

															if (pf_FileHandle.WritePage(siblingPointer, siblingBuffer)
																	== SUCCESS) {
																int remainingLength = totalMergedLength - i - keyLength;

																memset((char *) nodeBuffer, 0, PF_PAGE_SIZE);
																memcpy((char *) nodeBuffer, (char *) mergedBuffer + i
																		+ keyLength, remainingLength);
																memcpy((char *) nodeBuffer
																		+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET,
																		&remainingLength, sizeof(int));

																if (pf_FileHandle.WritePage(nodePointer, nodeBuffer)
																		== SUCCESS) {

																	isDeleting = false;
																	mergedNodePointer = 0;

																	free(nodeBuffer);
																	free(mergedBuffer);
																	free(parentBuffer);
																	free(helpBuffer);
																	free(siblingBuffer);

																	return SUCCESS;
																}
															}

															isDeleting = false;
															mergedNodePointer = 0;

															free(nodeBuffer);
															free(mergedBuffer);
															free(parentBuffer);
															free(helpBuffer);
															free(siblingBuffer);

															return FAILURE;
														}

													} else {
														isDeleting = false;
														mergedNodePointer = 0;

														free(nodeBuffer);
														free(mergedBuffer);
														free(parentBuffer);
														free(helpBuffer);
														free(siblingBuffer);

														return FAILURE;
													}

												} else {
													memset((char *) nodeBuffer, 0, PF_PAGE_SIZE);
													memcpy((char *) nodeBuffer, (char *) helpBuffer, newContentLength);
													memcpy((char *) nodeBuffer
															+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET,
															&newContentLength, sizeof(int));

													free(mergedBuffer);
													free(parentBuffer);
													free(helpBuffer);
													free(siblingBuffer);

													if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {
														isDeleting = false;
														mergedNodePointer = 0;

														free(nodeBuffer);

														return SUCCESS;

													} else {
														isDeleting = false;
														mergedNodePointer = 0;

														free(nodeBuffer);

														return FAILURE;
													}
												}

												break;
											}

											i += keyLength;
										}
									}
								}
							}

							free(mergedBuffer);
							free(parentBuffer);
							free(siblingBuffer);
						}

						free(helpBuffer);
					}
				}
			}

			isDeleting = false;
			mergedNodePointer = 0;

			free(nodeBuffer);

			return NOT_FOUND;

		} else {
			int freeSpaceIndex = 0;
			memcpy(&freeSpaceIndex, (char *) nodeBuffer + IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET, sizeof(int));

			RC findingReturn = findEntryPositionToDelete(nodeBuffer, freeSpaceIndex, attributeType, key, keyString,
					rid, deletedEntryPosition);

			if (findingReturn == SUCCESS) {
				void *helpBuffer = malloc(PF_PAGE_SIZE);

				deletedEntryNodePointer = nodePointer;
				int entryLength = calculateKeyLength(key, attributeType) + sizeof(int) * 2;
				int remainingLength = freeSpaceIndex - deletedEntryPosition - entryLength;
				int newContentLength = freeSpaceIndex - entryLength;

				memset((char *) helpBuffer, 0, PF_PAGE_SIZE);
				memcpy((char *) helpBuffer, (char *) nodeBuffer, deletedEntryPosition);

				if (remainingLength > 0) {
					memcpy((char *) helpBuffer + deletedEntryPosition, (char *) nodeBuffer + deletedEntryPosition
							+ entryLength, remainingLength);
				}

				if (currentHeight == 0 || newContentLength >= IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET / 2) {
					memset((char *) nodeBuffer, 0, IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET);
					memcpy((char *) nodeBuffer, (char *) helpBuffer, newContentLength);
					memcpy((char *) nodeBuffer + IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET, &newContentLength,
							sizeof(int));

					free(helpBuffer);

					if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {
						isDeleting = false;
						mergedNodePointer = 0;

						nextEntryNodePointer = deletedEntryNodePointer;
						nextEntryPosition = deletedEntryPosition;

						free(nodeBuffer);

						return SUCCESS;

					} else {
						isDeleting = false;
						mergedNodePointer = 0;

						free(nodeBuffer);

						return FAILURE;
					}

				} else {
					void *mergedBuffer = malloc(PF_PAGE_SIZE * 2);
					void *parentBuffer = malloc(PF_PAGE_SIZE);
					void *siblingBuffer = malloc(PF_PAGE_SIZE);

					memset((char *) mergedBuffer, 0, PF_PAGE_SIZE * 2);

					if (pf_FileHandle.ReadPage(parentPointer, parentBuffer) == SUCCESS) {
						int parentMiddleKeyLength = calculateKeyLength((char *) parentBuffer + middleKeyOffset,
								attributeType);
						int siblingPointer = 0;

						if (isFirstChild) {
							memcpy(&siblingPointer, (char *) parentBuffer + middleKeyOffset + parentMiddleKeyLength,
									sizeof(int));
						} else {
							memcpy(&siblingPointer, (char *) parentBuffer + middleKeyOffset - sizeof(int), sizeof(int));
						}

						if (pf_FileHandle.ReadPage(siblingPointer, siblingBuffer) == SUCCESS) {
							int siblingContentLength = 0;

							memcpy(&siblingContentLength, (char *) siblingBuffer
									+ IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET, sizeof(int));

							if (isFirstChild) {
								memcpy((char *) mergedBuffer, (char *) helpBuffer, newContentLength);
								memcpy((char *) mergedBuffer + newContentLength, (char *) siblingBuffer,
										siblingContentLength);

							} else {
								memcpy((char *) mergedBuffer, (char *) siblingBuffer, siblingContentLength);
								memcpy((char *) mergedBuffer + siblingContentLength, (char *) helpBuffer,
										newContentLength);
							}

							int totalMergedLength = newContentLength + siblingContentLength;

							if (totalMergedLength <= IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET) {
								unsigned oldNextNodePointer = 0;

								if (isFirstChild) {
									memcpy(&oldNextNodePointer, (char *) siblingBuffer
											+ IX_Manager::NEXT_NODE_POINTER_OFFSET, sizeof(int));

									memset((char *) nodeBuffer, 0, IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET);
									memcpy((char *) nodeBuffer, (char *) mergedBuffer, totalMergedLength);
									memcpy((char *) nodeBuffer + IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET,
											&totalMergedLength, sizeof(int));
									memcpy((char *) nodeBuffer + IX_Manager::NEXT_NODE_POINTER_OFFSET,
											&oldNextNodePointer, sizeof(int));

									if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {
										if (oldNextNodePointer != 0) {
											void *oldNextNodeBuffer = malloc(PF_PAGE_SIZE);

											if (pf_FileHandle.ReadPage(oldNextNodePointer, oldNextNodeBuffer)
													== SUCCESS) {

												memcpy((char *) oldNextNodeBuffer
														+ IX_Manager::PREV_NODE_POINTER_OFFSET, &nodePointer,
														sizeof(int));

												if (pf_FileHandle.WritePage(oldNextNodePointer, oldNextNodeBuffer)
														== SUCCESS) {

													isDeleting = true;
													mergedNodePointer = nodePointer;

													nextEntryNodePointer = nodePointer;
													nextEntryPosition = deletedEntryPosition;

													free(nodeBuffer);
													free(helpBuffer);
													free(mergedBuffer);
													free(parentBuffer);
													free(siblingBuffer);
													free(oldNextNodeBuffer);

													return SUCCESS;
												}
											}

											isDeleting = false;
											mergedNodePointer = 0;

											free(nodeBuffer);
											free(helpBuffer);
											free(mergedBuffer);
											free(parentBuffer);
											free(siblingBuffer);
											free(oldNextNodeBuffer);

											return FAILURE;

										} else {
											isDeleting = true;
											mergedNodePointer = nodePointer;

											nextEntryNodePointer = nodePointer;
											nextEntryPosition = deletedEntryPosition;

											free(nodeBuffer);
											free(helpBuffer);
											free(mergedBuffer);
											free(parentBuffer);
											free(siblingBuffer);

											return SUCCESS;
										}

									} else {
										isDeleting = false;
										mergedNodePointer = 0;

										free(nodeBuffer);
										free(helpBuffer);
										free(mergedBuffer);
										free(parentBuffer);
										free(siblingBuffer);

										return FAILURE;
									}

								} else {
									memcpy(&oldNextNodePointer, (char *) nodeBuffer
											+ IX_Manager::NEXT_NODE_POINTER_OFFSET, sizeof(int));

									memset((char *) siblingBuffer, 0, IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET);
									memcpy((char *) siblingBuffer, (char *) mergedBuffer, totalMergedLength);
									memcpy((char *) siblingBuffer + IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET,
											&totalMergedLength, sizeof(int));
									memcpy((char *) siblingBuffer + IX_Manager::NEXT_NODE_POINTER_OFFSET,
											&oldNextNodePointer, sizeof(int));

									if (pf_FileHandle.WritePage(siblingPointer, siblingBuffer) == SUCCESS) {
										if (oldNextNodePointer != 0) {
											void *oldNextNodeBuffer = malloc(PF_PAGE_SIZE);

											if (pf_FileHandle.ReadPage(oldNextNodePointer, oldNextNodeBuffer)
													== SUCCESS) {

												memcpy((char *) oldNextNodeBuffer
														+ IX_Manager::PREV_NODE_POINTER_OFFSET, &siblingPointer,
														sizeof(int));

												if (pf_FileHandle.WritePage(oldNextNodePointer, oldNextNodeBuffer)
														== SUCCESS) {

													isDeleting = true;
													mergedNodePointer = siblingPointer;

													nextEntryNodePointer = siblingPointer;
													nextEntryPosition = siblingContentLength + deletedEntryPosition;

													free(nodeBuffer);
													free(helpBuffer);
													free(mergedBuffer);
													free(parentBuffer);
													free(siblingBuffer);
													free(oldNextNodeBuffer);

													return SUCCESS;
												}
											}

											isDeleting = false;
											mergedNodePointer = 0;

											free(nodeBuffer);
											free(helpBuffer);
											free(mergedBuffer);
											free(parentBuffer);
											free(siblingBuffer);
											free(oldNextNodeBuffer);

											return FAILURE;

										} else {
											isDeleting = true;
											mergedNodePointer = siblingPointer;

											nextEntryNodePointer = siblingPointer;
											nextEntryPosition = siblingContentLength + deletedEntryPosition;

											free(nodeBuffer);
											free(helpBuffer);
											free(mergedBuffer);
											free(parentBuffer);
											free(siblingBuffer);

											return SUCCESS;
										}

									} else {
										isDeleting = false;
										mergedNodePointer = 0;

										free(nodeBuffer);
										free(helpBuffer);
										free(mergedBuffer);
										free(parentBuffer);
										free(siblingBuffer);

										return FAILURE;
									}
								}

							} else {
								for (int i = 0; i < totalMergedLength; i += sizeof(int) * 2) {
									int keyLength = calculateKeyLength((char *) mergedBuffer + i, attributeType);

									if (i + keyLength > totalMergedLength / 2 || i + keyLength + (int) sizeof(int) * 2
											> totalMergedLength / 2) {

										int parentContentLength = 0;
										memcpy(&parentContentLength, (char *) parentBuffer
												+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET, sizeof(int));

										if (parentContentLength - parentMiddleKeyLength + keyLength
												<= IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET) {

											int remainingParentLength = parentContentLength - middleKeyOffset
													- parentMiddleKeyLength;
											int newParentLength = middleKeyOffset + keyLength + remainingParentLength;

											void *middleBuffer = malloc(PF_PAGE_SIZE);

											if (remainingParentLength > 0) {
												memset((char *) middleBuffer, 0, PF_PAGE_SIZE);
												memcpy((char *) middleBuffer, (char *) parentBuffer + middleKeyOffset
														+ parentMiddleKeyLength, remainingParentLength);
											}

											memset((char *) parentBuffer + middleKeyOffset, 0, parentContentLength
													- middleKeyOffset);
											memcpy((char *) parentBuffer + middleKeyOffset, (char *) mergedBuffer + i,
													keyLength);
											memcpy(
													(char *) parentBuffer
															+ IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET,
													&newParentLength, sizeof(int));

											if (remainingParentLength > 0) {
												memcpy((char *) parentBuffer + middleKeyOffset + keyLength,
														(char *) middleBuffer, remainingParentLength);
											}

											free(middleBuffer);

											if (pf_FileHandle.WritePage(parentPointer, parentBuffer) == SUCCESS) {

												if (isFirstChild) {
													memset((char *) nodeBuffer, 0,
															IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET);
													memcpy((char *) nodeBuffer, (char *) mergedBuffer, i);
													memcpy((char *) nodeBuffer
															+ IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET, &i, sizeof(int));

													if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {
														int remainingLength = totalMergedLength - i;

														memset((char *) siblingBuffer, 0,
																IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET);
														memcpy((char *) siblingBuffer, (char *) mergedBuffer + i,
																remainingLength);
														memcpy((char *) siblingBuffer
																+ IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET,
																&remainingLength, sizeof(int));

														if (pf_FileHandle.WritePage(siblingPointer, siblingBuffer)
																== SUCCESS) {

															isDeleting = false;
															mergedNodePointer = 0;

															nextEntryNodePointer
																	= i <= deletedEntryPosition ? siblingPointer
																			: nodePointer;
															nextEntryPosition
																	= i <= deletedEntryPosition ? deletedEntryPosition
																			- i : deletedEntryPosition;

															free(nodeBuffer);
															free(mergedBuffer);
															free(parentBuffer);
															free(helpBuffer);
															free(siblingBuffer);

															return SUCCESS;
														}
													}

													isDeleting = false;
													mergedNodePointer = 0;

													free(nodeBuffer);
													free(mergedBuffer);
													free(parentBuffer);
													free(helpBuffer);
													free(siblingBuffer);

													return FAILURE;

												} else {
													memset((char *) siblingBuffer, 0,
															IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET);
													memcpy((char *) siblingBuffer, (char *) mergedBuffer, i);
													memcpy((char *) siblingBuffer
															+ IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET, &i, sizeof(int));

													if (pf_FileHandle.WritePage(siblingPointer, siblingBuffer)
															== SUCCESS) {
														int remainingLength = totalMergedLength - i;

														memset((char *) nodeBuffer, 0,
																IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET);
														memcpy((char *) nodeBuffer, (char *) mergedBuffer + i,
																remainingLength);
														memcpy((char *) nodeBuffer
																+ IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET,
																&remainingLength, sizeof(int));

														if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {

															isDeleting = false;
															mergedNodePointer = 0;

															int newPosition = siblingContentLength
																	+ deletedEntryPosition;
															nextEntryNodePointer = i <= newPosition ? nodePointer
																	: siblingPointer;
															nextEntryPosition = i <= newPosition ? newPosition - i
																	: newPosition;

															free(nodeBuffer);
															free(mergedBuffer);
															free(parentBuffer);
															free(helpBuffer);
															free(siblingBuffer);

															return SUCCESS;
														}
													}

													isDeleting = false;
													mergedNodePointer = 0;

													free(nodeBuffer);
													free(mergedBuffer);
													free(parentBuffer);
													free(helpBuffer);
													free(siblingBuffer);

													return FAILURE;
												}

											} else {
												isDeleting = false;
												mergedNodePointer = 0;

												free(nodeBuffer);
												free(mergedBuffer);
												free(parentBuffer);
												free(helpBuffer);
												free(siblingBuffer);

												return FAILURE;
											}

										} else {
											memset((char *) nodeBuffer, 0, IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET);
											memcpy((char *) nodeBuffer, (char *) helpBuffer, newContentLength);
											memcpy((char *) nodeBuffer + IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET,
													&newContentLength, sizeof(int));

											free(mergedBuffer);
											free(parentBuffer);
											free(helpBuffer);
											free(siblingBuffer);

											if (pf_FileHandle.WritePage(nodePointer, nodeBuffer) == SUCCESS) {
												isDeleting = false;
												mergedNodePointer = 0;

												nextEntryNodePointer = deletedEntryNodePointer;
												nextEntryPosition = deletedEntryPosition;

												free(nodeBuffer);

												return SUCCESS;

											} else {
												isDeleting = false;
												mergedNodePointer = 0;

												free(nodeBuffer);

												return FAILURE;
											}
										}

										break;
									}

									i += keyLength;
								}
							}
						}
					}

					free(mergedBuffer);
					free(parentBuffer);
					free(siblingBuffer);
				}

				free(helpBuffer);

			} else {
				isDeleting = false;
				mergedNodePointer = 0;

				free(nodeBuffer);

				return findingReturn;
			}
		}
	}

	isDeleting = false;
	mergedNodePointer = 0;

	free(nodeBuffer);

	return FAILURE;
}

int IX_IndexHandle::calculateKeyLength(const void *entryKey, const int attributeType) {
	int keyLength = sizeof(int);
	if ((AttrType) attributeType == TypeVarChar) {
		memcpy(&keyLength, (char *) entryKey, sizeof(int));
		keyLength += sizeof(int);
	}

	return keyLength;
}

string IX_IndexHandle::produceKeyString(const int attributeType, const void *key) {
	string keyString = "";
	if ((AttrType) attributeType == TypeVarChar && key != NULL) {
		int keyLength = 0;
		memcpy(&keyLength, (char *) key, sizeof(int));

		char *varCharKey = new char[keyLength + 1];
		memcpy((char *) varCharKey, (char *) key + sizeof(int), keyLength);
		varCharKey[keyLength] = '\0';

		keyString = string(varCharKey);
		delete[] varCharKey;
	}

	return keyString;
}

int IX_IndexHandle::findNextChildPointerOffset(const void *nodeBuffer, const int freeSpaceIndex,
		const int attributeType, const void *key, string keyString, bool isEqualAllowed) {

	int nextChildPointerOffset = 0;
	bool isNextChildPointerFound = false;

	for (int i = sizeof(int); i < freeSpaceIndex; i += sizeof(int)) {
		switch ((AttrType) attributeType) {
			case TypeVarChar: {
				int varCharLength = 0;
				memcpy(&varCharLength, (char *) nodeBuffer + i, sizeof(int));

				char *varCharContent = new char[varCharLength + 1];
				memcpy(varCharContent, (char *) nodeBuffer + i + sizeof(int), varCharLength);
				varCharContent[varCharLength] = '\0';

				if (keyString < string(varCharContent)) {
					isNextChildPointerFound = true;

				} else if (isEqualAllowed && keyString == string(varCharContent)) {
					isNextChildPointerFound = true;

				} else {
					i += sizeof(int) + varCharLength;
				}

				delete[] varCharContent;
				break;
			}

			case TypeInt: {
				int keyInt = 0;
				memcpy(&keyInt, (char *) key, sizeof(int));

				int intContent = 0;
				memcpy(&intContent, (char *) nodeBuffer + i, sizeof(int));

				if (keyInt < intContent) {
					isNextChildPointerFound = true;

				} else if (isEqualAllowed && keyInt == intContent) {
					isNextChildPointerFound = true;

				} else {
					i += sizeof(int);
				}

				break;
			}

			case TypeReal: {
				float keyReal = 0.0;
				memcpy(&keyReal, (char *) key, sizeof(float));

				float realContent = 0.0;
				memcpy(&realContent, (char *) nodeBuffer + i, sizeof(float));

				if (keyReal < realContent) {
					isNextChildPointerFound = true;

				} else if (isEqualAllowed && keyReal == realContent) {
					isNextChildPointerFound = true;

				} else {
					i += sizeof(float);
				}

				break;
			}
		}

		if (isNextChildPointerFound) {
			nextChildPointerOffset = i - sizeof(int);
			break;
		}
	}

	if (!isNextChildPointerFound) {
		nextChildPointerOffset = freeSpaceIndex - sizeof(int);
	}

	return nextChildPointerOffset;
}

vector<NextChildPointerOffset> IX_IndexHandle::findNextChildPointerOffset(const void *nodeBuffer,
		const int freeSpaceIndex, const int attributeType, const void *key, string keyString) {

	vector<NextChildPointerOffset> nextChildPointerOffsetVector;
	int prevKeyOffset = 0;
	bool isNextChildPointerFound = false;
	bool isLastPointerFound = false;

	for (int i = sizeof(int); i < freeSpaceIndex; i += sizeof(int)) {
		switch ((AttrType) attributeType) {
			case TypeVarChar: {
				int varCharLength = 0;
				memcpy(&varCharLength, (char *) nodeBuffer + i, sizeof(int));

				char *varCharContent = new char[varCharLength + 1];
				memcpy(varCharContent, (char *) nodeBuffer + i + sizeof(int), varCharLength);
				varCharContent[varCharLength] = '\0';

				if (keyString < string(varCharContent)) {
					isNextChildPointerFound = true;
					isLastPointerFound = true;

				} else if (keyString == string(varCharContent)) {
					isNextChildPointerFound = true;

				} else {
					prevKeyOffset = i;
					i += sizeof(int) + varCharLength;
				}

				delete[] varCharContent;
				break;
			}

			case TypeInt: {
				int keyInt = 0;
				memcpy(&keyInt, (char *) key, sizeof(int));

				int intContent = 0;
				memcpy(&intContent, (char *) nodeBuffer + i, sizeof(int));

				if (keyInt < intContent) {
					isNextChildPointerFound = true;
					isLastPointerFound = true;

				} else if (keyInt == intContent) {
					isNextChildPointerFound = true;

				} else {
					prevKeyOffset = i;
					i += sizeof(int);
				}

				break;
			}

			case TypeReal: {
				float keyReal = 0.0;
				memcpy(&keyReal, (char *) key, sizeof(float));

				float realContent = 0.0;
				memcpy(&realContent, (char *) nodeBuffer + i, sizeof(float));

				if (keyReal < realContent) {
					isNextChildPointerFound = true;
					isLastPointerFound = true;

				} else if (keyReal == realContent) {
					isNextChildPointerFound = true;

				} else {
					prevKeyOffset = i;
					i += sizeof(float);
				}

				break;
			}
		}

		if (isNextChildPointerFound) {
			unsigned nextChildPointer = 0;
			memcpy(&nextChildPointer, (char *) nodeBuffer + i - sizeof(int), sizeof(int));

			bool isNewFirstChild = i - sizeof(int) == 0;

			NextChildPointerOffset nextChildPointerOffset;
			nextChildPointerOffset.nextChildPointer = nextChildPointer;
			nextChildPointerOffset.isNewFirstChild = isNewFirstChild;
			nextChildPointerOffset.newMiddleKeyOffset = isNewFirstChild ? sizeof(int) : prevKeyOffset;

			nextChildPointerOffsetVector.push_back(nextChildPointerOffset);
		}

		if (isLastPointerFound) {
			break;
		}
	}

	if (!isNextChildPointerFound) {
		unsigned nextChildPointer = 0;
		memcpy(&nextChildPointer, (char *) nodeBuffer + freeSpaceIndex - sizeof(int), sizeof(int));

		bool isNewFirstChild = freeSpaceIndex - sizeof(int) == 0;

		NextChildPointerOffset nextChildPointerOffset;
		nextChildPointerOffset.nextChildPointer = nextChildPointer;
		nextChildPointerOffset.isNewFirstChild = isNewFirstChild;
		nextChildPointerOffset.newMiddleKeyOffset = isNewFirstChild ? sizeof(int) : prevKeyOffset;

		nextChildPointerOffsetVector.push_back(nextChildPointerOffset);
	}

	return nextChildPointerOffsetVector;
}

int IX_IndexHandle::findEntryPositionToInsert(const void *nodeBuffer, const int freeSpaceIndex,
		const int attributeType, const void *key, string keyString) {

	int entryPosition = 0;
	bool isEntryPositionFound = false;

	for (int i = 0; i < freeSpaceIndex; i += sizeof(int) * 2) {
		switch ((AttrType) attributeType) {
			case TypeVarChar: {
				int varCharLength = 0;
				memcpy(&varCharLength, (char *) nodeBuffer + i, sizeof(int));

				char *varCharContent = new char[varCharLength + 1];
				memcpy(varCharContent, (char *) nodeBuffer + i + sizeof(int), varCharLength);
				varCharContent[varCharLength] = '\0';

				if (keyString < string(varCharContent)) {
					isEntryPositionFound = true;
				} else {
					i += sizeof(int) + varCharLength;
				}

				delete[] varCharContent;
				break;
			}

			case TypeInt: {
				int keyInt = 0;
				memcpy(&keyInt, (char *) key, sizeof(int));

				int intContent = 0;
				memcpy(&intContent, (char *) nodeBuffer + i, sizeof(int));

				if (keyInt < intContent) {
					isEntryPositionFound = true;
				} else {
					i += sizeof(int);
				}

				break;
			}

			case TypeReal: {
				float keyReal = 0.0;
				memcpy(&keyReal, (char *) key, sizeof(float));

				float realContent = 0.0;
				memcpy(&realContent, (char *) nodeBuffer + i, sizeof(float));

				if (keyReal < realContent) {
					isEntryPositionFound = true;
				} else {
					i += sizeof(float);
				}

				break;
			}
		}

		if (isEntryPositionFound) {
			entryPosition = i;
			break;
		}
	}

	if (!isEntryPositionFound) {
		entryPosition = freeSpaceIndex;
	}

	return entryPosition;
}

RC IX_IndexHandle::findEntryPositionToDelete(const void *nodeBuffer, const int freeSpaceIndex, const int attributeType,
		const void *key, string keyString, const RID &rid, int &position) {

	bool isSameKey = false;
	bool isEntryPositionFound = false;
	int keyLength = 0;

	for (int i = 0; i < freeSpaceIndex; i += keyLength + sizeof(int) * 2) {
		isSameKey = false;

		switch ((AttrType) attributeType) {
			case TypeVarChar: {
				int varCharLength = 0;
				memcpy(&varCharLength, (char *) nodeBuffer + i, sizeof(int));

				char *varCharContent = new char[varCharLength + 1];
				memcpy(varCharContent, (char *) nodeBuffer + i + sizeof(int), varCharLength);
				varCharContent[varCharLength] = '\0';

				keyLength = sizeof(int) + varCharLength;

				if (keyString == string(varCharContent)) {
					isSameKey = true;
				}

				delete[] varCharContent;
				break;
			}

			case TypeInt: {
				int keyInt = 0;
				memcpy(&keyInt, (char *) key, sizeof(int));

				int intContent = 0;
				memcpy(&intContent, (char *) nodeBuffer + i, sizeof(int));

				keyLength = sizeof(int);

				if (keyInt == intContent) {
					isSameKey = true;
				}

				break;
			}

			case TypeReal: {
				float keyReal = 0.0;
				memcpy(&keyReal, (char *) key, sizeof(float));

				float realContent = 0.0;
				memcpy(&realContent, (char *) nodeBuffer + i, sizeof(float));

				keyLength = sizeof(int);

				if (keyReal == realContent) {
					isSameKey = true;
				}

				break;
			}
		}

		if (isSameKey) {
			unsigned pageNumber = 0;
			unsigned slotNumber = 0;
			memcpy(&pageNumber, (char *) nodeBuffer + i + keyLength, sizeof(int));
			memcpy(&slotNumber, (char *) nodeBuffer + i + keyLength + sizeof(int), sizeof(int));

			if (pageNumber == rid.pageNum && slotNumber == rid.slotNum) {
				isEntryPositionFound = true;
				position = i;

				break;
			}
		}
	}

	return isEntryPositionFound ? SUCCESS : NOT_FOUND;
}

PF_FileHandle & IX_IndexHandle::getFileHandle() {
	return pf_FileHandle;
}

bool IX_IndexHandle::isDeletedAfterScan() {
	return deletedAfterScan;
}

void IX_IndexHandle::setDeletedAfterScan(bool deletedAfterScan) {
	this->deletedAfterScan = deletedAfterScan;
}

unsigned IX_IndexHandle::getDeletedEntryNodePointer() {
	return deletedEntryNodePointer;
}

int IX_IndexHandle::getDeletedEntryPosition() {
	return deletedEntryPosition;
}

unsigned IX_IndexHandle::getNextEntryNodePointer() {
	return nextEntryNodePointer;
}

int IX_IndexHandle::getNextEntryPosition() {
	return nextEntryPosition;
}

IX_IndexScan::IX_IndexScan() {
	value = NULL;
	attributeType = 0;
	currentLeafNodePointer = 0;
	currentEntryPosition = 0;
	isClosed = true;
	lastEntryLength = 0;
}

IX_IndexScan::~IX_IndexScan() {
}

RC IX_IndexScan::OpenScan(IX_IndexHandle &indexHandle, CompOp compOp, void *value) {
	if (compOp == NE_OP || ((compOp != NO_OP) && value == NULL)) {
		return FAILURE;
	}

	this->ix_IndexHandle = &indexHandle;
	this->compOp = compOp;
	this->value = value;

	void *headPageBuffer = malloc(PF_PAGE_SIZE);

	if (ix_IndexHandle->getFileHandle().ReadPage(0, headPageBuffer) == SUCCESS) {
		unsigned nodePointer = 0;
		int indexTreeHeight = 0;
		int attrType = 0;

		memcpy(&nodePointer, (char *) headPageBuffer, sizeof(int));
		memcpy(&indexTreeHeight, (char *) headPageBuffer + sizeof(int), sizeof(int));
		memcpy(&attrType, (char *) headPageBuffer + sizeof(int) * 2, sizeof(int));

		free(headPageBuffer);

		valueString = ix_IndexHandle->produceKeyString(attrType, value);
		attributeType = attrType;

		void *nodeBuffer = malloc(PF_PAGE_SIZE);

		for (int i = 0; i <= indexTreeHeight; i++) {
			if (ix_IndexHandle->getFileHandle().ReadPage(nodePointer, nodeBuffer) == SUCCESS) {

				if (i != indexTreeHeight) {
					int nextChildPointerOffset = 0;

					if (compOp == EQ_OP || compOp == GE_OP || compOp == GT_OP) {
						int freeSpaceIndex = 0;
						memcpy(&freeSpaceIndex, (char *) nodeBuffer + IX_Manager::NON_LEAF_FREE_SPACE_INDEX_OFFSET,
								sizeof(int));

						nextChildPointerOffset = ix_IndexHandle->findNextChildPointerOffset(nodeBuffer, freeSpaceIndex,
								attributeType, value, valueString, true);
					}

					memcpy(&nodePointer, (char *) nodeBuffer + nextChildPointerOffset, sizeof(int));

				} else {
					currentLeafNodePointer = nodePointer;
					currentEntryPosition = 0;
				}

			} else {
				free(nodeBuffer);

				return FAILURE;
			}
		}

		free(nodeBuffer);

		isClosed = false;

		return SUCCESS;
	}

	free(headPageBuffer);

	return FAILURE;
}

RC IX_IndexScan::GetNextEntry(RID &rid) {
	if (isClosed) {
		return FAILURE;
	}

	if (ix_IndexHandle->isDeletedAfterScan() && ix_IndexHandle->getDeletedEntryNodePointer() == currentLeafNodePointer
			&& ix_IndexHandle->getDeletedEntryPosition() == currentEntryPosition - lastEntryLength) {

		currentLeafNodePointer = ix_IndexHandle->getNextEntryNodePointer();
		currentEntryPosition = ix_IndexHandle->getNextEntryPosition();

		ix_IndexHandle->setDeletedAfterScan(false);
	}

	void *nodeBuffer = malloc(PF_PAGE_SIZE);

	while (currentLeafNodePointer != 0) {
		if (ix_IndexHandle->getFileHandle().ReadPage(currentLeafNodePointer, nodeBuffer) == SUCCESS) {

			int freeSpaceIndex = 0;
			memcpy(&freeSpaceIndex, (char *) nodeBuffer + IX_Manager::LEAF_FREE_SPACE_INDEX_OFFSET, sizeof(int));

			if (currentEntryPosition >= freeSpaceIndex) {
				memcpy(&currentLeafNodePointer, (char *) nodeBuffer + IX_Manager::NEXT_NODE_POINTER_OFFSET, sizeof(int));
				currentEntryPosition = 0;
				continue;
			}

			while (currentEntryPosition < freeSpaceIndex) {
				bool isEntryFound = compOp == NO_OP;
				bool isViolationFound = compOp == NE_OP;
				int keyLength = 0;

				switch ((AttrType) attributeType) {
					case TypeVarChar: {
						int varCharLength = 0;
						memcpy(&varCharLength, (char *) nodeBuffer + currentEntryPosition, sizeof(int));
						keyLength = varCharLength + sizeof(int);

						if (compOp != NO_OP && compOp != NE_OP) {
							char *varCharContent = new char[varCharLength + 1];
							memcpy(varCharContent, (char *) nodeBuffer + currentEntryPosition + sizeof(int),
									varCharLength);
							varCharContent[varCharLength] = '\0';
							string varCharString = string(varCharContent);

							switch (compOp) {
								case EQ_OP: {
									if (varCharString == valueString) {
										isEntryFound = true;

									} else if (varCharString > valueString) {
										isViolationFound = true;
									}

									break;
								}

								case LT_OP: {
									if (varCharString < valueString) {
										isEntryFound = true;

									} else if (varCharString >= valueString) {
										isViolationFound = true;
									}

									break;
								}

								case GT_OP: {
									if (varCharString > valueString) {
										isEntryFound = true;
									}

									break;
								}

								case LE_OP: {
									if (varCharString <= valueString) {
										isEntryFound = true;

									} else if (varCharString > valueString) {
										isViolationFound = true;
									}

									break;
								}

								case GE_OP: {
									if (varCharString >= valueString) {
										isEntryFound = true;
									}

									break;
								}

								default: {
									isViolationFound = true;
									break;
								}
							}

							delete[] varCharContent;
						}

						break;
					}

					case TypeInt: {
						keyLength = sizeof(int);

						if (compOp != NO_OP && compOp != NE_OP) {
							int valueInt = 0;
							memcpy(&valueInt, (char *) value, sizeof(int));

							int intContent = 0;
							memcpy(&intContent, (char *) nodeBuffer + currentEntryPosition, sizeof(int));

							switch (compOp) {
								case EQ_OP: {
									if (intContent == valueInt) {
										isEntryFound = true;

									} else if (intContent > valueInt) {
										isViolationFound = true;
									}

									break;
								}

								case LT_OP: {
									if (intContent < valueInt) {
										isEntryFound = true;

									} else if (intContent >= valueInt) {
										isViolationFound = true;
									}

									break;
								}

								case GT_OP: {
									if (intContent > valueInt) {
										isEntryFound = true;
									}

									break;
								}

								case LE_OP: {
									if (intContent <= valueInt) {
										isEntryFound = true;

									} else if (intContent > valueInt) {
										isViolationFound = true;
									}

									break;
								}

								case GE_OP: {
									if (intContent >= valueInt) {
										isEntryFound = true;
									}

									break;
								}

								default: {
									isViolationFound = true;
									break;
								}
							}
						}

						break;
					}

					case TypeReal: {
						keyLength = sizeof(float);

						if (compOp != NO_OP && compOp != NE_OP) {
							float valueReal = 0.0;
							memcpy(&valueReal, (char *) value, sizeof(float));

							float realContent = 0.0;
							memcpy(&realContent, (char *) nodeBuffer + currentEntryPosition, sizeof(float));

							switch (compOp) {
								case EQ_OP: {
									if (realContent == valueReal) {
										isEntryFound = true;

									} else if (realContent > valueReal) {
										isViolationFound = true;
									}

									break;
								}

								case LT_OP: {
									if (realContent < valueReal) {
										isEntryFound = true;

									} else if (realContent >= valueReal) {
										isViolationFound = true;
									}

									break;
								}

								case GT_OP: {
									if (realContent > valueReal) {
										isEntryFound = true;
									}

									break;
								}

								case LE_OP: {
									if (realContent <= valueReal) {
										isEntryFound = true;

									} else if (realContent > valueReal) {
										isViolationFound = true;
									}

									break;
								}

								case GE_OP: {
									if (realContent >= valueReal) {
										isEntryFound = true;
									}

									break;
								}

								default: {
									isViolationFound = true;
									break;
								}
							}
						}

						break;
					}
				}

				if (isViolationFound) {
					free(nodeBuffer);

					return IX_EOF;
				}

				lastEntryLength = keyLength + sizeof(int) * 2;
				currentEntryPosition += lastEntryLength;

				if (isEntryFound) {
					memcpy(&rid.pageNum, (char *) nodeBuffer + currentEntryPosition - sizeof(int) * 2, sizeof(int));
					memcpy(&rid.slotNum, (char *) nodeBuffer + currentEntryPosition - sizeof(int), sizeof(int));

					free(nodeBuffer);

					return SUCCESS;
				}
			}

			if (currentEntryPosition == freeSpaceIndex) {
				memcpy(&currentLeafNodePointer, (char *) nodeBuffer + IX_Manager::NEXT_NODE_POINTER_OFFSET, sizeof(int));
				currentEntryPosition = 0;
			}

		} else {
			free(nodeBuffer);

			return IX_EOF;
		}
	}

	free(nodeBuffer);

	return IX_EOF;
}

RC IX_IndexScan::CloseScan() {
	isClosed = true;

	return SUCCESS;
}

void IX_PrintError(RC rc) {
	if (rc == SUCCESS) {
		cerr << "SUCCESS" << endl;

	} else if (rc == FAILURE) {
		cerr << "FAILURE" << endl;
	}
}
