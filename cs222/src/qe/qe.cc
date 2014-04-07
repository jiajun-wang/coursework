#include <cstdlib>
#include <cstring>
#include <sstream>

#include "qe.h"

Filter::Filter(Iterator *input, const Condition &condition) {
	inputIterator = input;
	filterCondition = condition;

	inputIterator->getAttributes(attributeVector);
}

Filter::~Filter() {
}

RC Filter::getNextTuple(void *data) {
	if (filterCondition.bRhsIsAttr) {
		return FAILURE;
	}

	void *tupleBuffer = malloc(TUPLE_BUFFER_SIZE);
	memset((char *) tupleBuffer, 0, TUPLE_BUFFER_SIZE);

	int bufferOffset = 0;
	bool tupleFound = false;

	while (inputIterator->getNextTuple(tupleBuffer) != QE_EOF) {
		if (filterCondition.lhsAttr == "") {
			tupleFound = true;
		}

		for (unsigned i = 0; i < attributeVector.size(); i++) {
			if (!tupleFound && attributeVector.at(i).name == filterCondition.lhsAttr && attributeVector.at(i).type
					== filterCondition.rhsValue.type) {

				switch (attributeVector.at(i).type) {
					case TypeInt: {
						int leftInt = 0;
						memcpy(&leftInt, (char *) tupleBuffer + bufferOffset, sizeof(int));
						int rightInt = *(int *) filterCondition.rhsValue.data;

						switch (filterCondition.op) {
							case EQ_OP: {
								tupleFound = leftInt == rightInt;
								break;
							}

							case LT_OP: {
								tupleFound = leftInt < rightInt;
								break;
							}

							case GT_OP: {
								tupleFound = leftInt > rightInt;
								break;
							}

							case LE_OP: {
								tupleFound = leftInt <= rightInt;
								break;
							}

							case GE_OP: {
								tupleFound = leftInt >= rightInt;
								break;
							}

							case NE_OP: {
								tupleFound = leftInt != rightInt;
								break;
							}

							case NO_OP: {
								tupleFound = true;
								break;
							}

							default: {
								tupleFound = false;
								break;
							}
						}

						break;
					}

					case TypeReal: {
						float leftFloat = 0.0;
						memcpy(&leftFloat, (char *) tupleBuffer + bufferOffset, sizeof(float));
						float rightFloat = *(float *) filterCondition.rhsValue.data;

						switch (filterCondition.op) {
							case EQ_OP: {
								tupleFound = leftFloat == rightFloat;
								break;
							}

							case LT_OP: {
								tupleFound = leftFloat < rightFloat;
								break;
							}

							case GT_OP: {
								tupleFound = leftFloat > rightFloat;
								break;
							}

							case LE_OP: {
								tupleFound = leftFloat <= rightFloat;
								break;
							}

							case GE_OP: {
								tupleFound = leftFloat >= rightFloat;
								break;
							}

							case NE_OP: {
								tupleFound = leftFloat != rightFloat;
								break;
							}

							case NO_OP: {
								tupleFound = true;
								break;
							}

							default: {
								tupleFound = false;
								break;
							}
						}

						break;
					}

					case TypeVarChar: {
						int varCharLength = 0;
						memcpy(&varCharLength, (char *) tupleBuffer + bufferOffset, sizeof(int));

						char *leftVarChar = new char[varCharLength + 1];
						memcpy(leftVarChar, (char *) tupleBuffer + bufferOffset + sizeof(int), varCharLength);
						leftVarChar[varCharLength] = '\0';
						string leftString = string(leftVarChar);

						memcpy(&varCharLength, (char *) filterCondition.rhsValue.data, sizeof(int));

						char *rightVarChar = new char[varCharLength + 1];
						memcpy(rightVarChar, (char *) filterCondition.rhsValue.data + sizeof(int), varCharLength);
						rightVarChar[varCharLength] = '\0';
						string rightString = string(rightVarChar);

						switch (filterCondition.op) {
							case EQ_OP: {
								tupleFound = leftString == rightString;
								break;
							}

							case LT_OP: {
								tupleFound = leftString < rightString;
								break;
							}

							case GT_OP: {
								tupleFound = leftString > rightString;
								break;
							}

							case LE_OP: {
								tupleFound = leftString <= rightString;
								break;
							}

							case GE_OP: {
								tupleFound = leftString >= rightString;
								break;
							}

							case NE_OP: {
								tupleFound = leftString != rightString;
								break;
							}

							case NO_OP: {
								tupleFound = true;
								break;
							}

							default: {
								tupleFound = false;
								break;
							}
						}

						delete[] leftVarChar;
						delete[] rightVarChar;

						break;
					}
				}
			}

			int attributeLength = sizeof(int);

			if (attributeVector.at(i).type == TypeReal) {
				attributeLength = sizeof(float);

			} else if (attributeVector.at(i).type == TypeVarChar) {
				memcpy(&attributeLength, (char *) tupleBuffer + bufferOffset, sizeof(int));
				attributeLength += sizeof(int);
			}

			bufferOffset += attributeLength;
		}

		if (tupleFound) {
			memcpy((char *) data, (char *) tupleBuffer, bufferOffset);

			free(tupleBuffer);

			return SUCCESS;
		}

		memset((char *) tupleBuffer, 0, TUPLE_BUFFER_SIZE);
		bufferOffset = 0;
	}

	free(tupleBuffer);

	return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();

	for (unsigned i = 0; i < attributeVector.size(); i++) {
		attrs.push_back(attributeVector.at(i));
	}
}

Project::Project(Iterator *input, const vector<string> &attrNames) {
	inputIterator = input;
	attributeNameVector = attrNames;

	inputIterator->getAttributes(fullAttributeVector);

	for (unsigned i = 0; i < attributeNameVector.size(); i++) {
		for (unsigned j = 0; j < fullAttributeVector.size(); j++) {
			if (fullAttributeVector.at(j).name == attributeNameVector.at(i)) {
				attributeVector.push_back(fullAttributeVector.at(j));
				break;
			}
		}
	}
}

Project::~Project() {
}

RC Project::getNextTuple(void *data) {
	void *tupleBuffer = malloc(TUPLE_BUFFER_SIZE);
	memset((char *) tupleBuffer, 0, TUPLE_BUFFER_SIZE);

	int bufferOffset = 0;
	int returnBufferOffset = 0;

	if (inputIterator->getNextTuple(tupleBuffer) != QE_EOF) {
		void *returnBuffer = malloc(TUPLE_BUFFER_SIZE);
		memset((char *) returnBuffer, 0, TUPLE_BUFFER_SIZE);

		for (unsigned i = 0; i < attributeNameVector.size(); i++) {
			bufferOffset = 0;

			for (unsigned j = 0; j < fullAttributeVector.size(); j++) {
				int attributeLength = sizeof(int);

				if (fullAttributeVector.at(j).type == TypeReal) {
					attributeLength = sizeof(float);

				} else if (fullAttributeVector.at(j).type == TypeVarChar) {
					memcpy(&attributeLength, (char *) tupleBuffer + bufferOffset, sizeof(int));
					attributeLength += sizeof(int);
				}

				if (fullAttributeVector.at(j).name == attributeNameVector.at(i)) {
					memcpy((char *) returnBuffer + returnBufferOffset, (char *) tupleBuffer + bufferOffset,
							attributeLength);
					returnBufferOffset += attributeLength;

					break;
				}

				bufferOffset += attributeLength;
			}
		}

		memcpy((char *) data, (char *) returnBuffer, returnBufferOffset);

		free(tupleBuffer);
		free(returnBuffer);

		return SUCCESS;
	}

	free(tupleBuffer);

	return QE_EOF;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();

	for (unsigned i = 0; i < attributeVector.size(); i++) {
		attrs.push_back(attributeVector.at(i));
	}
}

NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
	leftIterator = leftIn;
	rightIterator = rightIn;
	joinCondition = condition;

	leftIterator->getAttributes(leftAttributeVector);
	rightIterator->getAttributes(rightAttributeVector);

	leftBuffer = NULL;
	leftValue.type = TypeInt;
	leftValue.data = NULL;
	leftBufferLength = 0;
}

NLJoin::~NLJoin() {
	/*if (leftBuffer != NULL) {
	 free(leftBuffer);
	 }

	 if (leftValue.data != NULL) {
	 free(leftValue.data);
	 }*/
}

RC NLJoin::getNextTuple(void *data) {
	if (!joinCondition.bRhsIsAttr) {
		return FAILURE;
	}

	while (true) {
		if (leftBuffer == NULL) {
			leftBuffer = malloc(TUPLE_BUFFER_SIZE);
			memset((char *) leftBuffer, 0, TUPLE_BUFFER_SIZE);

			leftValue.data = malloc(TUPLE_BUFFER_SIZE);
			memset((char *) leftValue.data, 0, TUPLE_BUFFER_SIZE);

			if (leftIterator->getNextTuple(leftBuffer) != QE_EOF) {
				leftBufferLength = 0;

				for (unsigned i = 0; i < leftAttributeVector.size(); i++) {
					int attributeLength = sizeof(int);

					if (leftAttributeVector.at(i).type == TypeReal) {
						attributeLength = sizeof(float);

					} else if (leftAttributeVector.at(i).type == TypeVarChar) {
						memcpy(&attributeLength, (char *) leftBuffer + leftBufferLength, sizeof(int));
						attributeLength += sizeof(int);
					}

					if (leftAttributeVector.at(i).name == joinCondition.lhsAttr) {
						leftValue.type = leftAttributeVector.at(i).type;
						memcpy((char *) leftValue.data, (char *) leftBuffer + leftBufferLength, attributeLength);
					}

					leftBufferLength += attributeLength;
				}

			} else {
				free(leftBuffer);
				free(leftValue.data);

				return QE_EOF;
			}
		}

		void *rightBuffer = malloc(TUPLE_BUFFER_SIZE);
		memset((char *) rightBuffer, 0, TUPLE_BUFFER_SIZE);

		while (rightIterator->getNextTuple(rightBuffer) != QE_EOF) {
			int bufferOffset = 0;
			bool tupleFound = false;

			for (unsigned i = 0; i < rightAttributeVector.size(); i++) {
				int attributeLength = sizeof(int);

				if (rightAttributeVector.at(i).type == TypeReal) {
					attributeLength = sizeof(float);

				} else if (rightAttributeVector.at(i).type == TypeVarChar) {
					memcpy(&attributeLength, (char *) rightBuffer + bufferOffset, sizeof(int));
					attributeLength += sizeof(int);
				}

				if (!tupleFound && rightAttributeVector.at(i).name == joinCondition.rhsAttr && rightAttributeVector.at(
						i).type == leftValue.type) {

					switch (leftValue.type) {
						case TypeInt: {
							int leftInt = *(int *) leftValue.data;
							int rightInt = 0;
							memcpy(&rightInt, (char *) rightBuffer + bufferOffset, sizeof(int));

							switch (joinCondition.op) {
								case EQ_OP: {
									tupleFound = leftInt == rightInt;
									break;
								}

								case LT_OP: {
									tupleFound = leftInt < rightInt;
									break;
								}

								case GT_OP: {
									tupleFound = leftInt > rightInt;
									break;
								}

								case LE_OP: {
									tupleFound = leftInt <= rightInt;
									break;
								}

								case GE_OP: {
									tupleFound = leftInt >= rightInt;
									break;
								}

								case NE_OP: {
									tupleFound = leftInt != rightInt;
									break;
								}

								default: {
									tupleFound = false;
									break;
								}
							}

							break;
						}

						case TypeReal: {
							float leftFloat = *(float *) leftValue.data;
							float rightFloat = 0.0;
							memcpy(&rightFloat, (char *) rightBuffer + bufferOffset, sizeof(float));

							switch (joinCondition.op) {
								case EQ_OP: {
									tupleFound = leftFloat == rightFloat;
									break;
								}

								case LT_OP: {
									tupleFound = leftFloat < rightFloat;
									break;
								}

								case GT_OP: {
									tupleFound = leftFloat > rightFloat;
									break;
								}

								case LE_OP: {
									tupleFound = leftFloat <= rightFloat;
									break;
								}

								case GE_OP: {
									tupleFound = leftFloat >= rightFloat;
									break;
								}

								case NE_OP: {
									tupleFound = leftFloat != rightFloat;
									break;
								}

								default: {
									tupleFound = false;
									break;
								}
							}

							break;
						}

						case TypeVarChar: {
							int varCharLength = 0;
							memcpy(&varCharLength, (char *) leftValue.data, sizeof(int));

							char *leftVarChar = new char[varCharLength + 1];
							memcpy(leftVarChar, (char *) leftValue.data + sizeof(int), varCharLength);
							leftVarChar[varCharLength] = '\0';
							string leftString = string(leftVarChar);

							memcpy(&varCharLength, (char *) rightBuffer + bufferOffset, sizeof(int));

							char *rightVarChar = new char[varCharLength + 1];
							memcpy(rightVarChar, (char *) rightBuffer + bufferOffset + sizeof(int), varCharLength);
							rightVarChar[varCharLength] = '\0';
							string rightString = string(rightVarChar);

							switch (joinCondition.op) {
								case EQ_OP: {
									tupleFound = leftString == rightString;
									break;
								}

								case LT_OP: {
									tupleFound = leftString < rightString;
									break;
								}

								case GT_OP: {
									tupleFound = leftString > rightString;
									break;
								}

								case LE_OP: {
									tupleFound = leftString <= rightString;
									break;
								}

								case GE_OP: {
									tupleFound = leftString >= rightString;
									break;
								}

								case NE_OP: {
									tupleFound = leftString != rightString;
									break;
								}

								default: {
									tupleFound = false;
									break;
								}
							}

							delete[] leftVarChar;
							delete[] rightVarChar;

							break;
						}
					}
				}

				bufferOffset += attributeLength;
			}

			if (tupleFound) {
				memcpy((char *) data, (char *) leftBuffer, leftBufferLength);
				memcpy((char *) data + leftBufferLength, (char *) rightBuffer, bufferOffset);

				free(rightBuffer);

				return SUCCESS;
			}
		}

		free(leftBuffer);
		free(leftValue.data);
		free(rightBuffer);

		leftBuffer = NULL;
		leftValue.type = TypeInt;
		leftValue.data = NULL;
		leftBufferLength = 0;

		rightIterator->setIterator();
	}
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();

	for (unsigned i = 0; i < leftAttributeVector.size(); i++) {
		attrs.push_back(leftAttributeVector.at(i));
	}

	for (unsigned i = 0; i < rightAttributeVector.size(); i++) {
		attrs.push_back(rightAttributeVector.at(i));
	}
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition, const unsigned numPages) {
	leftIterator = leftIn;
	rightIterator = rightIn;
	joinCondition = condition;

	leftIterator->getAttributes(leftAttributeVector);
	rightIterator->getAttributes(rightAttributeVector);

	leftBuffer = NULL;
	leftValue.type = TypeInt;
	leftValue.data = NULL;
	leftBufferLength = 0;
}

INLJoin::~INLJoin() {
	/*if (leftBuffer != NULL) {
	 free(leftBuffer);
	 }

	 if (leftValue.data != NULL) {
	 free(leftValue.data);
	 }*/
}

RC INLJoin::getNextTuple(void *data) {
	if (!joinCondition.bRhsIsAttr) {
		return FAILURE;
	}

	while (true) {
		if (leftBuffer == NULL) {
			leftBuffer = malloc(TUPLE_BUFFER_SIZE);
			memset((char *) leftBuffer, 0, TUPLE_BUFFER_SIZE);

			leftValue.data = malloc(TUPLE_BUFFER_SIZE);
			memset((char *) leftValue.data, 0, TUPLE_BUFFER_SIZE);

			if (leftIterator->getNextTuple(leftBuffer) != QE_EOF) {
				leftBufferLength = 0;

				for (unsigned i = 0; i < leftAttributeVector.size(); i++) {
					int attributeLength = sizeof(int);

					if (leftAttributeVector.at(i).type == TypeReal) {
						attributeLength = sizeof(float);

					} else if (leftAttributeVector.at(i).type == TypeVarChar) {
						memcpy(&attributeLength, (char *) leftBuffer + leftBufferLength, sizeof(int));
						attributeLength += sizeof(int);
					}

					if (leftAttributeVector.at(i).name == joinCondition.lhsAttr) {
						leftValue.type = leftAttributeVector.at(i).type;
						memcpy((char *) leftValue.data, (char *) leftBuffer + leftBufferLength, attributeLength);
					}

					leftBufferLength += attributeLength;
				}

				CompOp compOp = joinCondition.op;
				if (compOp == LT_OP) {
					compOp = GT_OP;
				} else if (compOp == GT_OP) {
					compOp = LT_OP;
				} else if (compOp == LE_OP) {
					compOp = GE_OP;
				} else if (compOp == GE_OP) {
					compOp = LE_OP;
				}

				rightIterator->setIterator(compOp, leftValue.data);

			} else {
				free(leftBuffer);
				free(leftValue.data);

				return QE_EOF;
			}
		}

		void *rightBuffer = malloc(TUPLE_BUFFER_SIZE);
		memset((char *) rightBuffer, 0, TUPLE_BUFFER_SIZE);

		while (rightIterator->getNextTuple(rightBuffer) != QE_EOF) {
			int bufferOffset = 0;

			for (unsigned i = 0; i < rightAttributeVector.size(); i++) {
				int attributeLength = sizeof(int);

				if (rightAttributeVector.at(i).type == TypeReal) {
					attributeLength = sizeof(float);

				} else if (rightAttributeVector.at(i).type == TypeVarChar) {
					memcpy(&attributeLength, (char *) rightBuffer + bufferOffset, sizeof(int));
					attributeLength += sizeof(int);
				}

				bufferOffset += attributeLength;
			}

			memcpy((char *) data, (char *) leftBuffer, leftBufferLength);
			memcpy((char *) data + leftBufferLength, (char *) rightBuffer, bufferOffset);

			free(rightBuffer);

			return SUCCESS;
		}

		free(leftBuffer);
		free(leftValue.data);
		free(rightBuffer);

		leftBuffer = NULL;
		leftValue.type = TypeInt;
		leftValue.data = NULL;
		leftBufferLength = 0;
	}
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();

	for (unsigned i = 0; i < leftAttributeVector.size(); i++) {
		attrs.push_back(leftAttributeVector.at(i));
	}

	for (unsigned i = 0; i < rightAttributeVector.size(); i++) {
		attrs.push_back(rightAttributeVector.at(i));
	}
}

HashJoin::HashJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPages) {
	leftIterator = leftIn;
	rightIterator = rightIn;
	joinCondition = condition;
	numberOfPartitions = numPages - 1;

	leftIterator->getAttributes(leftAttributeVector);
	rightIterator->getAttributes(rightAttributeVector);

	vector<PF_FileHandle> pf_FileHandleVector;
	vector<void *> pageBufferVector;
	vector<int> pageBufferOffsetVector;

	partitionTuplesByHash(joinCondition.lhsAttr, pf_FileHandleVector, leftPartitionPageCountVector, pageBufferVector,
			pageBufferOffsetVector, leftIterator, leftAttributeVector);

	partitionTuplesByHash(joinCondition.rhsAttr, pf_FileHandleVector, rightPartitionPageCountVector, pageBufferVector,
			pageBufferOffsetVector, rightIterator, rightAttributeVector);

	currentPartitionNumber = 0;
	rightIndex = 0;
	leftPageNumber = 0;
	leftOffset = 0;
	bool firstLeftFound = false;

	while (!firstLeftFound && currentPartitionNumber < numberOfPartitions) {
		PF_FileHandle leftFileHandle;
		PF_Manager::Instance()->OpenFile(generatePartitionName(joinCondition.lhsAttr, currentPartitionNumber).c_str(),
				leftFileHandle);

		void *pageBuffer = malloc(PF_PAGE_SIZE);

		for (; leftPageNumber < leftPartitionPageCountVector.at(currentPartitionNumber); leftPageNumber++) {
			memset((char *) pageBuffer, 0, PF_PAGE_SIZE);

			leftFileHandle.ReadPage(leftPageNumber, pageBuffer);

			char delimiter;
			memcpy(&delimiter, (char *) pageBuffer, sizeof(char));

			if (delimiter != ENO_OF_PAGE_DELIMITER) {
				firstLeftFound = true;
				leftPageBuffer = pageBuffer;

				break;
			}
		}

		PF_Manager::Instance()->CloseFile(leftFileHandle);

		if (!firstLeftFound) {
			free(pageBuffer);

			leftPageNumber = 0;
			currentPartitionNumber++;

			continue;
		}

		buildRightPartitionBuffer();
	}
}

HashJoin::~HashJoin() {
	PF_Manager *pf_Manager = PF_Manager::Instance();

	for (int i = 0; i < numberOfPartitions; i++) {
		pf_Manager->DestroyFile(generatePartitionName(joinCondition.lhsAttr, i).c_str());
		pf_Manager->DestroyFile(generatePartitionName(joinCondition.rhsAttr, i).c_str());
	}
}

RC HashJoin::getNextTuple(void *data) {
	if (joinCondition.op != EQ_OP || !joinCondition.bRhsIsAttr || rightPartitionBuffer.size() == 0) {
		return FAILURE;
	}

	PF_Manager *pf_Manager = PF_Manager::Instance();

	while (true) {
		while (true) {
			while (true) {

				int leftTupleLength = 0;
				void *leftTupleData = malloc(TUPLE_BUFFER_SIZE);
				memset((char *) leftTupleData, 0, TUPLE_BUFFER_SIZE);

				memcpy(&leftTupleLength, (char *) leftPageBuffer + leftOffset, sizeof(int));
				memcpy((char *) leftTupleData, (char *) leftPageBuffer + leftOffset + sizeof(int), leftTupleLength);

				int leftBufferOffset = 0;
				int leftValueLength = 0;

				for (unsigned i = 0; i < leftAttributeVector.size(); i++) {
					int attributeLength = sizeof(int);

					if (leftAttributeVector.at(i).type == TypeReal) {
						attributeLength = sizeof(float);

					} else if (leftAttributeVector.at(i).type == TypeVarChar) {
						memcpy(&attributeLength, (char *) leftTupleData + leftBufferOffset, sizeof(int));
						attributeLength += sizeof(int);
					}

					if (leftAttributeVector.at(i).name == joinCondition.lhsAttr) {
						leftValueLength = attributeLength;
						break;
					}

					leftBufferOffset += attributeLength;
				}

				while (rightIndex < (int) rightPartitionBuffer.size()) {
					void *rightTupleData = rightPartitionBuffer.at(rightIndex);

					int rightBufferOffset = 0;
					int rightValueLength = 0;
					int rightTupleLength = 0;

					for (unsigned i = 0; i < rightAttributeVector.size(); i++) {
						int attributeLength = sizeof(int);

						if (rightAttributeVector.at(i).type == TypeReal) {
							attributeLength = sizeof(float);

						} else if (rightAttributeVector.at(i).type == TypeVarChar) {
							memcpy(&attributeLength, (char *) rightTupleData + rightTupleLength, sizeof(int));
							attributeLength += sizeof(int);
						}

						if (rightAttributeVector.at(i).name == joinCondition.rhsAttr) {
							rightValueLength = attributeLength;
							rightBufferOffset = rightTupleLength;
						}

						rightTupleLength += attributeLength;
					}

					rightIndex++;

					if (leftValueLength == rightValueLength && memcmp((char *) leftTupleData + leftBufferOffset,
							(char *) rightTupleData + rightBufferOffset, leftValueLength) == 0) {

						memcpy((char *) data, (char *) leftTupleData, leftTupleLength);
						memcpy((char *) data + leftTupleLength, (char *) rightTupleData, rightTupleLength);

						free(leftTupleData);

						return SUCCESS;
					}
				}

				leftOffset += leftTupleLength + sizeof(int);
				rightIndex = 0;

				free(leftTupleData);

				char delimiter;
				memcpy(&delimiter, (char *) leftPageBuffer + leftOffset, sizeof(char));

				if (delimiter == ENO_OF_PAGE_DELIMITER) {
					break;
				}
			}

			leftPageNumber++;
			leftOffset = 0;

			if (leftPageNumber < leftPartitionPageCountVector.at(currentPartitionNumber)) {
				PF_FileHandle leftFileHandle;
				pf_Manager->OpenFile(generatePartitionName(joinCondition.lhsAttr, currentPartitionNumber).c_str(),
						leftFileHandle);

				memset((char *) leftPageBuffer, 0, PF_PAGE_SIZE);

				leftFileHandle.ReadPage(leftPageNumber, leftPageBuffer);

				pf_Manager->CloseFile(leftFileHandle);

			} else {
				break;
			}
		}

		currentPartitionNumber++;
		leftPageNumber = 0;

		while (currentPartitionNumber < numberOfPartitions) {
			PF_FileHandle leftFileHandle;
			pf_Manager->OpenFile(generatePartitionName(joinCondition.lhsAttr, currentPartitionNumber).c_str(),
					leftFileHandle);

			memset((char *) leftPageBuffer, 0, PF_PAGE_SIZE);

			leftFileHandle.ReadPage(leftPageNumber, leftPageBuffer);

			pf_Manager->CloseFile(leftFileHandle);

			char delimiter;
			memcpy(&delimiter, (char *) leftPageBuffer, sizeof(char));

			if (delimiter == ENO_OF_PAGE_DELIMITER) {
				currentPartitionNumber++;

				continue;
			}

			clearRightPartitionBuffer();

			buildRightPartitionBuffer();

			break;
		}

		if (currentPartitionNumber >= numberOfPartitions) {
			break;
		}
	}

	clearRightPartitionBuffer();

	free(leftPageBuffer);

	return QE_EOF;
}

void HashJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();

	for (unsigned i = 0; i < leftAttributeVector.size(); i++) {
		attrs.push_back(leftAttributeVector.at(i));
	}

	for (unsigned i = 0; i < rightAttributeVector.size(); i++) {
		attrs.push_back(rightAttributeVector.at(i));
	}
}

void HashJoin::partitionTuplesByHash(const string attributeName, vector<PF_FileHandle> &pf_FileHandleVector,
		vector<int> &pageCountVector, vector<void *> &pageBufferVector, vector<int> &pageBufferOffsetVector,
		Iterator *tupleIterator, vector<Attribute> &attributeVector) {

	PF_Manager *pf_Manager = PF_Manager::Instance();

	for (int i = 0; i < numberOfPartitions; i++) {
		string partitionFileName = generatePartitionName(attributeName, i);
		pf_Manager->CreateFile(partitionFileName.c_str());

		PF_FileHandle pf_FileHandle;
		pf_Manager->OpenFile(partitionFileName.c_str(), pf_FileHandle);

		pf_FileHandleVector.push_back(pf_FileHandle);

		void *pageBuffer = malloc(PF_PAGE_SIZE);
		memset((char *) pageBuffer, 0, PF_PAGE_SIZE);
		pageBufferVector.push_back(pageBuffer);

		pf_FileHandle.AppendPage(pageBuffer);

		pageCountVector.push_back(1);
		pageBufferOffsetVector.push_back(0);
	}

	void *leftBuffer = malloc(TUPLE_BUFFER_SIZE);
	memset((char *) leftBuffer, 0, TUPLE_BUFFER_SIZE);

	while (tupleIterator->getNextTuple(leftBuffer) != QE_EOF) {
		int bufferOffset = 0;
		int hashValue = 0;

		for (unsigned i = 0; i < attributeVector.size(); i++) {
			int attributeLength = sizeof(int);

			if (attributeVector.at(i).type == TypeReal) {
				attributeLength = sizeof(float);

			} else if (attributeVector.at(i).type == TypeVarChar) {
				memcpy(&attributeLength, (char *) leftBuffer + bufferOffset, sizeof(int));
				attributeLength += sizeof(int);
			}

			if (attributeVector.at(i).name == attributeName) {
				switch (attributeVector.at(i).type) {
					case TypeInt: {
						int leftInt = 0;
						memcpy(&leftInt, (char *) leftBuffer + bufferOffset, sizeof(int));

						hashValue = abs(leftInt) % numberOfPartitions;

						break;
					}

					case TypeReal: {
						float leftFloat = 0.0;
						memcpy(&leftFloat, (char *) leftBuffer + bufferOffset, sizeof(float));

						hashValue = abs((int) leftFloat) % numberOfPartitions;

						break;
					}

					case TypeVarChar: {
						int varCharLength = 0;
						memcpy(&varCharLength, (char *) leftBuffer + bufferOffset, sizeof(int));

						char *leftVarChar = new char[varCharLength];
						memcpy(leftVarChar, (char *) leftBuffer + bufferOffset + sizeof(int), varCharLength);

						int accumulatedInt = 0;
						for (int i = 0; i < min(varCharLength, 10); i++) {
							accumulatedInt += (int) leftVarChar[i];
						}

						hashValue = abs(accumulatedInt) % numberOfPartitions;

						delete[] leftVarChar;

						break;
					}
				}
			}

			bufferOffset += attributeLength;
		}

		if (pageBufferOffsetVector.at(hashValue) + bufferOffset + sizeof(int) + sizeof(char) > PF_PAGE_SIZE) {
			memcpy((char *) pageBufferVector.at(hashValue) + pageBufferOffsetVector.at(hashValue),
					&ENO_OF_PAGE_DELIMITER, sizeof(char));

			pf_FileHandleVector.at(hashValue).WritePage(pageCountVector.at(hashValue) - 1, pageBufferVector.at(
					hashValue));

			free(pageBufferVector.at(hashValue));
			pageBufferVector[hashValue] = malloc(PF_PAGE_SIZE);
			memset((char *) pageBufferVector.at(hashValue), 0, PF_PAGE_SIZE);
			memcpy((char *) pageBufferVector.at(hashValue), &bufferOffset, sizeof(int));
			memcpy((char *) pageBufferVector.at(hashValue) + sizeof(int), (char *) leftBuffer, bufferOffset);

			pf_FileHandleVector.at(hashValue).AppendPage(pageBufferVector.at(hashValue));

			pageCountVector[hashValue] += 1;
			pageBufferOffsetVector[hashValue] = sizeof(int) + bufferOffset;

		} else {
			memcpy((char *) pageBufferVector.at(hashValue) + pageBufferOffsetVector.at(hashValue), &bufferOffset,
					sizeof(int));
			memcpy((char *) pageBufferVector.at(hashValue) + pageBufferOffsetVector.at(hashValue) + sizeof(int),
					(char *) leftBuffer, bufferOffset);

			pf_FileHandleVector.at(hashValue).WritePage(pageCountVector.at(hashValue) - 1, pageBufferVector.at(
					hashValue));

			pageBufferOffsetVector[hashValue] += sizeof(int) + bufferOffset;
		}
	}

	for (int i = 0; i < numberOfPartitions; i++) {
		memcpy((char *) pageBufferVector.at(i) + pageBufferOffsetVector.at(i), &ENO_OF_PAGE_DELIMITER, sizeof(char));

		pf_FileHandleVector.at(i).WritePage(pageCountVector.at(i) - 1, pageBufferVector.at(i));

		free(pageBufferVector.at(i));

		pf_Manager->CloseFile(pf_FileHandleVector.at(i));
	}

	pf_FileHandleVector.clear();
	pageBufferVector.clear();
	pageBufferOffsetVector.clear();
}

string HashJoin::generatePartitionName(const string attributeName, const int partitionNumber) {
	stringstream sstrm;
	sstrm << partitionNumber;

	return attributeName + "_" + sstrm.str();
}

void HashJoin::buildRightPartitionBuffer() {
	PF_FileHandle rightFileHandle;
	PF_Manager::Instance()->OpenFile(generatePartitionName(joinCondition.rhsAttr, currentPartitionNumber).c_str(),
			rightFileHandle);

	void *pageBuffer = malloc(PF_PAGE_SIZE);

	for (int i = 0; i < rightPartitionPageCountVector.at(currentPartitionNumber); i++) {
		memset((char *) pageBuffer, 0, PF_PAGE_SIZE);

		rightFileHandle.ReadPage(i, pageBuffer);

		int pageBufferOffset = 0;

		while (pageBufferOffset < PF_PAGE_SIZE) {
			char delimiter;
			memcpy(&delimiter, (char *) pageBuffer + pageBufferOffset, sizeof(char));

			if (delimiter != ENO_OF_PAGE_DELIMITER) {
				int tupleLength = 0;
				void *tupleData = malloc(TUPLE_BUFFER_SIZE);
				memset((char *) tupleData, 0, TUPLE_BUFFER_SIZE);

				memcpy(&tupleLength, (char *) pageBuffer + pageBufferOffset, sizeof(int));
				memcpy((char *) tupleData, (char *) pageBuffer + pageBufferOffset + sizeof(int), tupleLength);

				rightPartitionBuffer.push_back(tupleData);

				pageBufferOffset += tupleLength + sizeof(int);

			} else {
				break;
			}
		}
	}

	free(pageBuffer);

	PF_Manager::Instance()->CloseFile(rightFileHandle);
}

void HashJoin::clearRightPartitionBuffer() {
	for (unsigned i = 0; i < rightPartitionBuffer.size(); i++) {
		free(rightPartitionBuffer.at(i));
	}

	rightPartitionBuffer.clear();
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
	inputIterator = input;
	aggregateAttribute = aggAttr;
	aggregateOp = op;
	usingGroupBy = false;
	firstInvocation = true;

	inputIterator->getAttributes(attributeVector);
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute gAttr, AggregateOp op) {
	inputIterator = input;
	aggregateAttribute = aggAttr;
	groupAttribute = gAttr;
	aggregateOp = op;
	usingGroupBy = true;
	firstInvocation = true;

	inputIterator->getAttributes(attributeVector);
}

Aggregate::~Aggregate() {
}

RC Aggregate::getNextTuple(void *data) {
	if (aggregateAttribute.type == TypeVarChar) {
		return FAILURE;
	}

	if (firstInvocation) {
		void *tupleBuffer = malloc(TUPLE_BUFFER_SIZE);
		memset((char *) tupleBuffer, 0, TUPLE_BUFFER_SIZE);

		int aggregateIntValue = 0;
		float aggregateFloatValue = 0.0;
		int count = 0;
		bool firstCalculation = true;

		map<int, AggregateValue>::iterator intGroupMapIterator;
		map<float, AggregateValue>::iterator floatGroupMapIterator;
		map<string, AggregateValue>::iterator stringGroupMapIterator;

		while (inputIterator->getNextTuple(tupleBuffer) != QE_EOF) {
			int bufferOffset = 0;
			Value groupValue;
			Value aggregateValue;
			groupValue.data = NULL;
			aggregateValue.data = NULL;

			for (unsigned i = 0; i < attributeVector.size(); i++) {
				int attributeLength = sizeof(int);

				if (attributeVector.at(i).type == TypeReal) {
					attributeLength = sizeof(float);

				} else if (attributeVector.at(i).type == TypeVarChar) {
					memcpy(&attributeLength, (char *) tupleBuffer + bufferOffset, sizeof(int));
					attributeLength += sizeof(int);
				}

				if (usingGroupBy && groupValue.data == NULL && attributeVector.at(i).name == groupAttribute.name
						&& attributeVector.at(i).type == groupAttribute.type) {

					groupValue.type = groupAttribute.type;
					groupValue.data = malloc(TUPLE_BUFFER_SIZE);
					memset((char *) groupValue.data, 0, TUPLE_BUFFER_SIZE);
					memcpy((char *) groupValue.data, (char *) tupleBuffer + bufferOffset, attributeLength);
				}

				if (aggregateValue.data == NULL && attributeVector.at(i).name == aggregateAttribute.name
						&& attributeVector.at(i).type == aggregateAttribute.type) {

					aggregateValue.type = aggregateAttribute.type;
					aggregateValue.data = malloc(TUPLE_BUFFER_SIZE);
					memset((char *) aggregateValue.data, 0, TUPLE_BUFFER_SIZE);
					memcpy((char *) aggregateValue.data, (char *) tupleBuffer + bufferOffset, attributeLength);
				}

				bufferOffset += attributeLength;
			}

			if (aggregateValue.data != NULL) {
				if (usingGroupBy) {
					if (groupValue.data != NULL) {

						switch (groupValue.type) {
							case TypeInt: {
								int groupIntKey = *(int *) groupValue.data;
								intGroupMapIterator = intGroupMap.find(groupIntKey);

								if (intGroupMapIterator == intGroupMap.end()) {
									AggregateValue finalAggregateValue;
									finalAggregateValue.intValue = 0;
									finalAggregateValue.floatValue = 0.0;
									finalAggregateValue.count = 0;

									calculateAggregateValue(aggregateValue, finalAggregateValue.intValue,
											finalAggregateValue.floatValue, finalAggregateValue.count, true);

									intGroupMap[groupIntKey] = finalAggregateValue;

								} else {
									AggregateValue finalAggregateValue = intGroupMapIterator->second;

									calculateAggregateValue(aggregateValue, finalAggregateValue.intValue,
											finalAggregateValue.floatValue, finalAggregateValue.count, false);

									intGroupMap[groupIntKey] = finalAggregateValue;
								}

								break;
							}

							case TypeReal: {
								float groupFloatKey = *(float *) groupValue.data;
								floatGroupMapIterator = floatGroupMap.find(groupFloatKey);

								if (floatGroupMapIterator == floatGroupMap.end()) {
									AggregateValue finalAggregateValue;
									finalAggregateValue.intValue = 0;
									finalAggregateValue.floatValue = 0.0;
									finalAggregateValue.count = 0;

									calculateAggregateValue(aggregateValue, finalAggregateValue.intValue,
											finalAggregateValue.floatValue, finalAggregateValue.count, true);

									floatGroupMap[groupFloatKey] = finalAggregateValue;

								} else {
									AggregateValue finalAggregateValue = floatGroupMapIterator->second;

									calculateAggregateValue(aggregateValue, finalAggregateValue.intValue,
											finalAggregateValue.floatValue, finalAggregateValue.count, false);

									floatGroupMap[groupFloatKey] = finalAggregateValue;
								}

								break;
							}

							case TypeVarChar: {
								int varCharLength = 0;
								memcpy(&varCharLength, (char *) groupValue.data, sizeof(int));

								char *groupVarCharValue = new char[varCharLength + 1];
								memcpy(groupVarCharValue, (char *) groupValue.data + sizeof(int), varCharLength);
								groupVarCharValue[varCharLength] = '\0';
								string groupStringKey = string(groupVarCharValue);

								stringGroupMapIterator = stringGroupMap.find(groupStringKey);

								if (stringGroupMapIterator == stringGroupMap.end()) {
									AggregateValue finalAggregateValue;
									finalAggregateValue.intValue = 0;
									finalAggregateValue.floatValue = 0.0;
									finalAggregateValue.count = 0;

									calculateAggregateValue(aggregateValue, finalAggregateValue.intValue,
											finalAggregateValue.floatValue, finalAggregateValue.count, true);

									stringGroupMap[groupStringKey] = finalAggregateValue;

								} else {
									AggregateValue finalAggregateValue = stringGroupMapIterator->second;

									calculateAggregateValue(aggregateValue, finalAggregateValue.intValue,
											finalAggregateValue.floatValue, finalAggregateValue.count, false);

									stringGroupMap[groupStringKey] = finalAggregateValue;
								}

								delete[] groupVarCharValue;

								break;
							}
						}

						free(groupValue.data);
					}

				} else {
					calculateAggregateValue(aggregateValue, aggregateIntValue, aggregateFloatValue, count,
							firstCalculation);

					if (firstCalculation) {
						firstCalculation = false;
					}
				}

				free(aggregateValue.data);
			}
		}

		firstInvocation = false;

		if (usingGroupBy) {
			switch (groupAttribute.type) {
				case TypeInt: {
					intGroupResultMapIterator = intGroupMap.begin();
					break;
				}

				case TypeReal: {
					floatGroupResultMapIterator = floatGroupMap.begin();
					break;
				}

				case TypeVarChar: {
					stringGroupResultMapIterator = stringGroupMap.begin();
					break;
				}
			}

		} else {
			populateAggregateData(data, 0, aggregateIntValue, aggregateFloatValue, count);

			return SUCCESS;
		}
	}

	if (usingGroupBy) {
		switch (groupAttribute.type) {
			case TypeInt: {
				if (intGroupResultMapIterator != intGroupMap.end()) {
					memcpy((char *) data, &intGroupResultMapIterator->first, sizeof(int));

					populateAggregateData(data, sizeof(int), intGroupResultMapIterator->second.intValue,
							intGroupResultMapIterator->second.floatValue, intGroupResultMapIterator->second.count);

					intGroupResultMapIterator++;

					return SUCCESS;
				}

				break;
			}

			case TypeReal: {
				if (floatGroupResultMapIterator != floatGroupMap.end()) {
					memcpy((char *) data, &floatGroupResultMapIterator->first, sizeof(float));

					populateAggregateData(data, sizeof(float), floatGroupResultMapIterator->second.intValue,
							floatGroupResultMapIterator->second.floatValue, floatGroupResultMapIterator->second.count);

					floatGroupResultMapIterator++;

					return SUCCESS;
				}

				break;
			}

			case TypeVarChar: {
				if (stringGroupResultMapIterator != stringGroupMap.end()) {
					int varCharLength = stringGroupResultMapIterator->first.length();
					memcpy((char *) data, &varCharLength, sizeof(int));
					memcpy((char *) data + sizeof(int), stringGroupResultMapIterator->first.c_str(), varCharLength);

					populateAggregateData(data, varCharLength + sizeof(int),
							stringGroupResultMapIterator->second.intValue,
							stringGroupResultMapIterator->second.floatValue, stringGroupResultMapIterator->second.count);

					stringGroupResultMapIterator++;

					return SUCCESS;
				}

				break;
			}
		}
	}

	return QE_EOF;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();

	if (usingGroupBy) {
		attrs.push_back(groupAttribute);
	}

	Attribute attribute = aggregateAttribute;
	string attregateOpString = "";

	switch (aggregateOp) {
		case MIN: {
			attregateOpString = "MIN";
			break;
		}

		case MAX: {
			attregateOpString = "MAX";
			break;
		}

		case SUM: {
			attregateOpString = "SUM";
			break;
		}

		case AVG: {
			attregateOpString = "AVG";
			break;
		}

		case COUNT: {
			attregateOpString = "COUNT";
			break;
		}
	}

	attribute.name = attregateOpString + "(" + attribute.name + ")";
	attrs.push_back(attribute);
}

void Aggregate::calculateAggregateValue(Value &aggregateValue, int &aggregateIntValue, float &aggregateFloatValue,
		int &count, const bool firstCalculation) {

	switch (aggregateOp) {
		case MIN: {
			if (aggregateValue.type == TypeInt) {
				if (firstCalculation) {
					aggregateIntValue = *(int *) aggregateValue.data;
				} else {
					aggregateIntValue = min(aggregateIntValue, *(int *) aggregateValue.data);
				}

			} else if (aggregateValue.type == TypeReal) {
				if (firstCalculation) {
					aggregateFloatValue = *(float *) aggregateValue.data;
				} else {
					aggregateFloatValue = min(aggregateFloatValue, *(float *) aggregateValue.data);
				}
			}

			break;
		}

		case MAX: {
			if (aggregateValue.type == TypeInt) {
				if (firstCalculation) {
					aggregateIntValue = *(int *) aggregateValue.data;
				} else {
					aggregateIntValue = max(aggregateIntValue, *(int *) aggregateValue.data);
				}

			} else if (aggregateValue.type == TypeReal) {
				if (firstCalculation) {
					aggregateFloatValue = *(float *) aggregateValue.data;
				} else {
					aggregateFloatValue = max(aggregateFloatValue, *(float *) aggregateValue.data);
				}
			}

			break;
		}

		case SUM: {
			if (aggregateValue.type == TypeInt) {
				aggregateIntValue += *(int *) aggregateValue.data;

			} else if (aggregateValue.type == TypeReal) {
				aggregateFloatValue += *(float *) aggregateValue.data;
			}

			break;
		}

		case AVG: {
			if (aggregateValue.type == TypeInt) {
				aggregateIntValue += *(int *) aggregateValue.data;

			} else if (aggregateValue.type == TypeReal) {
				aggregateFloatValue += *(float *) aggregateValue.data;
			}

			count++;
			break;
		}

		case COUNT: {
			count++;
			break;
		}
	}
}

void Aggregate::populateAggregateData(const void *data, const int dataOffset, const int aggregateIntValue,
		const float aggregateFloatValue, const int count) {

	if (aggregateAttribute.type == TypeInt) {
		if (aggregateOp == COUNT) {
			memcpy((char *) data + dataOffset, &count, sizeof(int));

		} else if (aggregateOp == AVG) {
			int result = aggregateIntValue / count;
			memcpy((char *) data + dataOffset, &result, sizeof(int));

		} else {
			memcpy((char *) data + dataOffset, &aggregateIntValue, sizeof(int));
		}

	} else if (aggregateAttribute.type == TypeReal) {
		if (aggregateOp == COUNT) {
			memcpy((char *) data + dataOffset, &count, sizeof(int));

		} else if (aggregateOp == AVG) {
			float result = aggregateFloatValue / count;
			memcpy((char *) data + dataOffset, &result, sizeof(float));

		} else {
			memcpy((char *) data + dataOffset, &aggregateFloatValue, sizeof(float));
		}
	}
}
