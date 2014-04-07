#include <fstream>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "pf.h"

PF_Manager *PF_Manager::_pf_manager = 0;

PF_Manager * PF_Manager::Instance() {
	if (!_pf_manager) {
		_pf_manager = new PF_Manager();
	}

	return _pf_manager;
}

PF_Manager::PF_Manager() {
}

PF_Manager::~PF_Manager() {
}

bool PF_Manager::IsFileExisting(const char *fileName) {
	struct stat stFileInfo;

	return stat(fileName, &stFileInfo) == 0;
}

RC PF_Manager::CreateFile(const char *fileName) {
	if (!IsFileExisting(fileName)) {
		ofstream pfFile;
		pfFile.open(fileName);
		pfFile.close();

		return SUCCESS;

	} else {
		return FAILURE;
	}
}

RC PF_Manager::DestroyFile(const char *fileName) {
	if (IsFileExisting(fileName)) {
		return remove(fileName);
	}

	return SUCCESS;
}

RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle) {
	if (IsFileExisting(fileName) && fileHandle.IsUsable(fileName)) {
		return fileHandle.OpenFileStream(fileName);

	} else {
		return FAILURE;
	}
}

RC PF_Manager::CloseFile(PF_FileHandle &fileHandle) {
	return fileHandle.CloseFileStream();
}

PF_FileHandle::PF_FileHandle() {
	pfFileName = "";
	pfFileStream = NULL;
}

PF_FileHandle::~PF_FileHandle() {
	// delete pfFileStream;
}

bool PF_FileHandle::IsFileStreamOpen() {
	return pfFileStream != NULL && pfFileStream->is_open();
}

RC PF_FileHandle::ReadPage(PageNum pageNum, void *data) {
	if (pageNum + 1 > GetNumberOfPages()) {
		return FAILURE;
	}

	if (IsFileStreamOpen()) {
		pfFileStream->seekg(pageNum * PF_PAGE_SIZE, ios::beg);
		pfFileStream->read((char *) data, PF_PAGE_SIZE);

		return SUCCESS;

	} else {
		return FAILURE;
	}
}

RC PF_FileHandle::WritePage(PageNum pageNum, const void *data) {
	unsigned numberOfPages = GetNumberOfPages();
	if (pageNum > numberOfPages) {
		return FAILURE;
	}

	if (pageNum == numberOfPages) {
		return AppendPage(data);
	}

	if (IsFileStreamOpen()) {
		pfFileStream->seekp(pageNum * PF_PAGE_SIZE, ios::beg);
		pfFileStream->write((char *) data, PF_PAGE_SIZE);
		pfFileStream->flush();

		return SUCCESS;

	} else {
		return FAILURE;
	}
}

RC PF_FileHandle::AppendPage(const void *data) {
	if (IsFileStreamOpen()) {
		pfFileStream->seekp(0, ios::end);
		pfFileStream->write((char *) data, PF_PAGE_SIZE);
		pfFileStream->flush();

		return SUCCESS;

	} else {
		return FAILURE;
	}
}

unsigned PF_FileHandle::GetNumberOfPages() {
	if (IsFileStreamOpen()) {
		long begin, end;
		pfFileStream->seekg(0, ios::beg);
		begin = pfFileStream->tellg();
		pfFileStream->seekg(0, ios::end);
		end = pfFileStream->tellg();

		return (end - begin) / PF_PAGE_SIZE;

	} else {
		return 0;
	}
}

bool PF_FileHandle::IsUsable(const char *fileName) {
	return !IsFileStreamOpen() || strcmp(fileName, pfFileName.c_str()) == 0;
}

RC PF_FileHandle::OpenFileStream(const char *fileName) {
	if (pfFileStream == NULL) {
		pfFileStream = new fstream(fileName, ios::in | ios::out | ios::binary);

	} else {
		if (pfFileStream->is_open()) {
			pfFileStream->close();
		}
		pfFileStream->open(fileName, ios::in | ios::out | ios::binary);
	}

	pfFileName = string(fileName);

	return SUCCESS;
}

RC PF_FileHandle::CloseFileStream() {
	if (IsFileStreamOpen()) {
		pfFileStream->close();
	}

	return SUCCESS;
}
