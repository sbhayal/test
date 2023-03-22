// $Header: /usr/local/cvsroot/RIDE/Datagen/Linux/JGSort/jgsort.c,v 1.3 2008/11/24 21:23:40 dnassirp Exp $
//
//********************************************************************
//                    Description
// 
//   Retranslation of JGSort for Portability.
//   This sort routine will work on Linux, DGUX, and MSDOS
// 
//********************************************************************
//
//
//  The following things will need to be done for a correct sorting function.
//   1. Determine limits and resources (# of files, Amt of memory,temp space)
//   2. Parse Command line.
//   3. Sort the file.
//   4. All necessary cleanup.
//   
//  The following things will be done for sanity:
//    1. No Global Variables.
//    2. Portability between Linux/DGUX/SCO
//    3. Portability between DOS/WINDOWS
//    4. Statistical output via Callback functions
//    5. No memory Leaks.
//    6. Sanity checking all public functions.
//    7. Temp space management on UNIX platform.
//    8. Correct and detailed error reporting.
//

#include <btport.h>

#include "jgsort.h"
#include "stgentyp.h"      // Generic type definitions
#include "stconst.h"
#include "ststruct.h"

#include <errno.h>
#include <btstring.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include BTCPPINCLUDE(strstream)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <btstopwatch.h>

#ifdef HAVE_TMPSPACE_MGR
#  include <tmp_request.h>
#endif

#ifdef BTUNIX
#  include  <unistd.h>
#endif

#ifdef BTDOSBASED
#  include <io.h>
#  include <alloc.h>
#  define W_OK  2
#endif
static char tmptmp[] = ".tmptmp";

class JGdBaseFld : public BTObject
{
	public:
		JGdBaseFld(const char *FldName,size_t StartPos,size_t Length);
		virtual ~JGdBaseFld() { if (fieldName_) delete [] fieldName_; };
		
		size_t fldStartPos_;
		size_t fldLength_;
		char *fieldName_;
};
 
JGdBaseFld::JGdBaseFld(const char *FldName,size_t StartPos,size_t Length):
  fldStartPos_(StartPos),fldLength_(Length)
{
	fieldName_ = new char [strlen(FldName)+1];
	if (fieldName_) 
		strcpy(fieldName_,FldName);
}

class JGSortedFieldList : public BTSortedCollection
{
	public:
		JGSortedFieldList(ccIndex aLimit,ccIndex aDelta) :
		BTSortedCollection(aLimit,aDelta) {};
		virtual ~JGSortedFieldList() { };
		virtual void *keyOf(void *item);
		virtual ccIndex insert(void *item);

	private:
		virtual int compare(void *key1, void *key2);
};

void *JGSortedFieldList::keyOf(void *item)
{
	return ((JGdBaseFld*)item)->fieldName_;
}

int JGSortedFieldList::compare(void *key1,void *key2)
{
	return strcmp((char*)key1,(char*)key2);
}

ccIndex JGSortedFieldList::insert(void *item)
{
	if (item && ((JGdBaseFld*)item)->fieldName_)
		return BTSortedCollection::insert(item);
	else if (item)
		delete (BTObject*)item;

	return (ccIndex)-1;
}

JGSorter::JGSorter(BTStatus *SortStatus) :
  jgsortStatus_(SortStatus),fileCollection_(NULL),sorter_(NULL),
  compares_(NULL),memoryToAlloc_(0),filesToOpen_(0),
  tempSpaceNeeded_(0),maxSections_(0),
  mainBuffer_(NULL),tempFilesDir_(NULL),
  usingTempSpace_(0),ctrlZEnd_(0),dBaseSort_(0),
  noTempSpace_(0),dBaseFields_(NULL)
{
	maxSwaps_ = 0;
	useShellSort_ = 0;
}

JGSorter::~JGSorter()
{
	cleanupResources();
	releaseTempSpace();
}


int JGSorter::ensureInputSanity()
{
// Start with initial sanity checking.
//  I will consider it an error if the input or output files are listed
//  as null or blank, if the sort criteria is null or blank,
//  if the record length is zero or if, in MSDOS, a single record will
//  not fit into memory.

	if (!inputFile_ || !inputFile_[0])
		return jgsortStatus_->setStatus(ecdInvalidInputFile,
                             "Program Bug: Invalid Input File Parameter"
                             " to JGSort(), Blank or NULL",1);
	if (!outputFile_ || !outputFile_[0])
		return jgsortStatus_->setStatus(ecdInvalidOutputFile,
                             "Program Bug: Invalid Output File Parameter"
                             " to JGSort(), Blank or NULL",1);

	// Check the sanity of the input file.
	struct stat64 statbuf;
	if (stat64(inputFile_,&statbuf)) {
		if (errno == ENOENT) 
			return jgsortStatus_->setStatus(ecdInputFileNotThere,
                                      "Input File does not exist",1);
		return jgsortStatus_->setErrnoStatus(ecdInputFileBase,errno,
                                      "opening",inputFile_);
	}    

	if (!statbuf.st_size)
		return jgsortStatus_->setStatusf(ecdInputFileZeroLength,
                                    "Input File %s is Zero Length",inputFile_);

	// If we are doing a "dBase" file sort, get the header and
	// record length from the dBase header.
	if (dBaseSort_ && getDBaseHeaderInfo())
		return (*jgsortStatus_)();

	if (recordLength_ == 0)
		return jgsortStatus_->setStatus(ecdRecLenIsZero,
                     "Invalid Record Length, Record Length cannot be Zero",1);

#ifdef BTDOSBASED
	if (recordLength_ > 65534)
		return jgsortStatus_->setStatus(ecdRecLenTooBig,
                "Invalid Record Length, Must fit in 64K Boundary in MSDOS",1);
#endif

	if (statbuf.st_size < headerLength_)
		return jgsortStatus_->setStatusf(ecdInputFileLessHeader,
                      "Input File %s Size is Less than Header Size",inputFile_);

	if (statbuf.st_size == headerLength_)
		return jgsortStatus_->setStatusf(ecdInputFileEqHeader,
                   "Input File %s Size Equals Header Size",inputFile_);

	if ((statbuf.st_size - headerLength_) % recordLength_) {
		// check for CtrlZ at the end.
		if (((statbuf.st_size - headerLength_ - 1) % recordLength_) || !ctrlZAtEnd())
			return jgsortStatus_->setStatusf(ecdInputFileSizeMismatch,
               "Input File %s Size Does not match Header/Record Size",inputFile_);
		else
			ctrlZEnd_ = 1;
	}

    // trunk the output file base on the requirement from Robert Findlay --cxj
	if (!access(tmpOutputFile_, W_OK)) 
		unlink(tmpOutputFile_);

	// Check the sanity of the output file.
	if (access(outputFile_,W_OK)) {
		if (errno == EACCES)
			return jgsortStatus_->setStatusf(ecdOutputPermissionDenied,
                    "Incorrect Permissions to write Output File %s",outputFile_);
	 	if (errno == ENOENT) {
			char *dname = new char [strlen(outputFile_)+1];
			strcpy(dname,outputFile_);

			if (access(BTDirName(dname),W_OK)) {
				if (errno == EACCES) { 
					delete [] dname;
	
				return jgsortStatus_->setStatusf(ecdOutputPermissionDenied,
	                      "Incorrect Permissions to write Output File %s",outputFile_);
	        }
	        delete [] dname;
	        return jgsortStatus_->setErrnoStatus(ecdOutfileBase,errno,
	                                             "creating",outputFile_);
	       }
	       delete [] dname;
		}
	}
	return ecdNoError;
}

int JGSorter::ctrlZAtEnd()
{
	int infile = open(inputFile_, O_RDONLY);
	if (infile < 0)
		return 0;

	lseek(infile, -1, SEEK_END);
	char ch[1];
	read(infile, ch, 1);
	close(infile);

	if (ch[0] == 26)
		return 1;
	return 0;
}
  
// The format of a compare string is 
// ws#ws,ws#ws,ws#ws [, Another one]
int JGSorter::checkCompares(char *SortCriteria,int *NumComps)
{
	char *traversal = SortCriteria;
	*NumComps = 0;
	int Commas = 0;
	while (*traversal) {
		if (*traversal == ',') {
			*traversal = ' ';
			Commas++;
		}
		traversal++;
	}

	if ((Commas + 1) % 4)
		return jgsortStatus_->setStatus(ecdInvalidNumOfCommas,
                    "Sort Criteria Error, Invalid number of parameters",1);

	*NumComps = (Commas + 1) / 4;
	return ecdNoError;
}

int JGSorter::outOfMemory(const char *DoingWhat)
{
	return jgsortStatus_->setMemoryStatus(ecdOutOfMemory,DoingWhat);
}

int JGSorter::buildCriteriaList(const char *SortCriteria,
                                const char *ExcludeCriteria,
                                int *NumberOfCompares)
{
	if (!SortCriteria || !SortCriteria[0])
		return jgsortStatus_->setStatus(ecdSortCriteria,
             "Program Bug: Invalid Sort Criteria Parameter to JGSort(), "
             "Blank or NULL",1);

	if (ExcludeCriteria && ExcludeCriteria[0])
		return jgsortStatus_->setStatus(ecdExclusionNotImplemented,
              "Exclusion Feature Not yet implemented",1);

	int NumComps;
	char *duplicateSort;
	if (!dBaseSort_) {
		duplicateSort = new char [strlen(SortCriteria)+1];
		if (!duplicateSort) return outOfMemory("Duplicating sort criteria");
			strcpy(duplicateSort,SortCriteria);
	} else {
		duplicateSort = getCriteriaFromFields(SortCriteria);
		if (!duplicateSort) return (*jgsortStatus_)();
	}
  
	if (checkCompares(duplicateSort,&NumComps)) {
		delete [] duplicateSort;
		return (*jgsortStatus_)();
	}

	BRCompareType *WorkingCmps =  new BRCompareType[NumComps];
	if (!WorkingCmps) {
		delete [] duplicateSort;
		return outOfMemory("Allocating compare keys");
	}
  
	strstream strm;
	strm << duplicateSort;
  
	delete [] duplicateSort;
 
	int ReadComps = 0;
	int ErrorStat = ecdNoError;

	while (!ErrorStat && (ReadComps < NumComps) && strm.good()) {
		char ctype;
		char dtype;
		size_t start;
		size_t length;
		strm >> start >> length >> dtype >> ctype;

		if (!strm.good())
			ErrorStat = jgsortStatus_->setStatus(ecdInvalidSortParams,
				"Sort Criteria Error, Invalid characters in sort parameters",1);
		else {
			ctype = toupper(ctype);
			dtype = toupper(dtype);

			if (start >= recordLength_)
				ErrorStat = jgsortStatus_->setStatus(ecdStartExceedsRecLen,
					"Sort Criteria Error, Start Position is greater "
					"than Record Length",1);
			else if (!length)
				ErrorStat = jgsortStatus_->setStatus(ecdLenIsZero,
					"Sort Criteria Error, Sort Length Cannot be zero",1);
			else if (length + start > recordLength_)
				ErrorStat = jgsortStatus_->setStatus(ecdStartLenExceedsRecLen,
					"Sort Criteria Error, Start Position + Length is greater "
					"than Record Length",1);
			else if (ctype != 'D' && ctype != 'A')
				ErrorStat = jgsortStatus_->setStatus(ecdInvalidDirection,
					"Sort Criteria Error, Invalid Ascending/Descending Direction",1);
			else if (dtype != 'C')
				ErrorStat = jgsortStatus_->setStatus(ecdDataTypeNotSupported,
					"Sort Criteria Error, Data Type not yet supported",1);
			else {
				WorkingCmps[ReadComps].type = (ctype == 'D')?-1:1;
				WorkingCmps[ReadComps].start = start;
				WorkingCmps[ReadComps].len = length;
				WorkingCmps[ReadComps].data = dtype;
				ReadComps++;
			}
		}
	}

	if (ErrorStat != ecdNoError) {
		delete [] WorkingCmps;
		return ErrorStat;
	}

	compares_ = WorkingCmps; 
	*NumberOfCompares = NumComps;
	return ecdNoError;
}

// This function will attempt to compute the total amount of
// file handles that are available.
// This is a two step process:
//  1. Easy Step -- Look for Environment variable "JGSORT_FILES"
//  2. Open files until failure.
//  return the minimum of the two values.
int JGSorter::fileHandlesAvailable(const char *testfile,int UserFiles)
{

  int totalMax = 1024; // Never go beyond this number
  int FileCount = sysconf(_SC_OPEN_MAX);
  if (FileCount < 0 || FileCount > totalMax)
     FileCount = totalMax;

#if 0
	BTCollection FileList(700,50);
	int Finished = 0;
	struct stat64 info;

	while (!Finished) {
		int *teststream = new int;
		*teststream = open(testfile, O_RDONLY);

		if (teststream < 0) {
		// I will assume, however incorrectly, that if there is a memory
		// allocation error the sort will have other things to worry about
		// rather than file handles.  If there is more than BRFileOverhead
		// files I will consider it not a problem with memory, just a 
		// problem with file handles.
			if (FileList.count > BRFileOverhead)
				Finished = 1;
			else 
				Finished = 2;
		} else if (fstat64(*teststream, &info) < 0) {
			delete teststream;
			Finished = 1;
		} else {
			FileList.insert(teststream);
			FileCount++;
        }
	} // end of while (!Finished)


	while (FileList.count) {
		int iVal = close( *(int *)FileList.at(FileList.count-1) );
		delete (int *)FileList.at(FileList.count-1);
		FileList.atRemove(FileList.count-1);
	}

	if (Finished == 2) 
		return BRFileMemoryError;

#endif

	char *envp=getenv("JGSORT_FILES");
	if (envp) {
		int MaximumFiles;
		strstream str;
		str << envp;
		str >> MaximumFiles;
		if (MaximumFiles < FileCount)
			FileCount = MaximumFiles; 
	}
	if ((UserFiles != BRSysFileLimit) && (UserFiles < FileCount))
		return UserFiles;

	return FileCount;
}

// This is somewhat platform dependant.
// If we are in DOS, Use coreleft()
// If we are in UNIX, use 100Megs unless JGSORT_MEM is defined.
// JGSORT_MEM is the number of 1K blocks that can be allocated:
//  10 Megs == 10000
long JGSorter::memoryToAllocate(long UserMemory)
{
	long MemoryLimit;
#ifdef BTUNIX
	char *envp = getenv("JGSORT_MEM");

	if (envp) {
		strstream str;
		str << envp;
		str >> MemoryLimit;
		MemoryLimit *= 1000;
	} else
		MemoryLimit = BRUnixMemoryLimit;

#else
	#ifdef BTMSDOS
		MemoryLimit = coreleft() - BRDosMemoryBuffer;
	#  else
		#     error Do not know how to get memory usage on this platform
	#  endif
#endif

	if ((UserMemory != BRSysMemoryLimit) && (UserMemory < MemoryLimit))
		return UserMemory;

	return MemoryLimit;
}

int JGSorter::getResourceLimits(int UserFiles,long UserMemory,
                                const char *TempSpaceMessage,
                                const char *tempspacedir)
{
	struct stat64 statbuf;
	if (stat64(inputFile_,&statbuf)) {
		if (errno == ENOENT)
			return jgsortStatus_->setStatus(ecdInputFileNotThere,
					"Input File does not exist",1);

		return jgsortStatus_->setErrnoStatus(ecdInputFileBase,errno,
                                          "opening",inputFile_);
	}    

	//  The Sorting needs this number of file handles:
	//    1. Input File.
	//    2. MergeFile (final output file)
	//    3. TmpFiles.
	//  Files to open only refer to TmpFiles.
	int FilesAvailable = fileHandlesAvailable(inputFile_,UserFiles);

	if (FilesAvailable == BRFileMemoryError) 
		return outOfMemory("Testing available file handles");

	if (FilesAvailable <= BRFileOverhead)
		return jgsortStatus_->setStatus(ecdNotEnoughFiles,
           "Not Enough files handles available for sorting",1);

	filesToOpen_ = FilesAvailable - BRFileOverhead;

	// This kinda has to be in two passes to ensure that the collection
	// size is taken into consideration as part of the sort.
	long ResourceOverhead = (filesToOpen_ * (recordLength_+50));
	long Allocateable = memoryToAllocate(UserMemory);

        // Try to account for large record lengths and many open files.
        while ((filesToOpen_ > 10) && (Allocateable < ResourceOverhead)) {
           if (filesToOpen_ > 500)
  	     filesToOpen_ -= 100; 
           else
             filesToOpen_ -= 5;
           ResourceOverhead = (filesToOpen_ * (recordLength_+50));
        }
        if (Allocateable < ResourceOverhead) {
          return jgsortStatus_->setStatus(ecdNotEnoughMemory,
              "Allocateable memory is less than base resource memory, try increasing the "
              "JGSORT_MEM environment variable",1);
        }

	memoryToAlloc_ = Allocateable - ResourceOverhead;
     if (recordLength_ < ( sizeof(void*) * 3)) 
	maxSections_ = memoryToAlloc_ / (sizeof(void*) * 3);
     else
	maxSections_ = memoryToAlloc_ / recordLength_;
	ResourceOverhead += (maxSections_ * sizeof (void *));

	if (Allocateable < ResourceOverhead) {
		jgsortStatus_->setStatus(ecdNotEnoughMemory,
              "Allocateable memory is less than base resource memory, ",0);
		return jgsortStatus_->catStatus("Free more memory in the system or "
                          "increase the JGSORT_MEM environment variable",1);
	}

	if (maxSections_ > maxBTCollectionSize)
		maxSections_ = maxBTCollectionSize;

	off_t TmpFileSize = maxSections_ * recordLength_;

	maxSwaps_ = (statbuf.st_size - headerLength_) / TmpFileSize;

	if (maxSwaps_ <= filesToOpen_)
		tempSpaceNeeded_ = maxSwaps_ * TmpFileSize;
	else {
		off_t BigBuf = 0;
		off_t MinorBufSize = TmpFileSize * filesToOpen_;
		off_t NewSize = 0;
		while (maxSwaps_ > filesToOpen_) {
			NewSize = MinorBufSize * 2 + BigBuf * 2 + TmpFileSize;
			BigBuf = BigBuf + MinorBufSize + TmpFileSize;
			// One Extra Swap doesn't really count since it is an in-memory swap
			maxSwaps_ -= (filesToOpen_ + 1);
		}
		tempSpaceNeeded_ =  NewSize;
	}
	usingTempSpace_ = 0;
  
	if (tempspacedir) {
		if (stat64(tempspacedir,&statbuf)) 
			return jgsortStatus_->setErrnoStatus(ecdTempSpaceBase,errno,
                                           "examining",tempspacedir);
		tempFilesDir_ = new char [strlen(tempspacedir)+1];

		if (!tempFilesDir_) return outOfMemory("Duplicating temp directory name");
			strcpy(tempFilesDir_,tempspacedir);
	} else {
#ifdef HAVE_TMPSPACE_MGR
		long tmpAmount = (long) (tempSpaceNeeded_ / 4000000) + 1;
		if (noTempSpace_)
			tmpAmount = 0;

		tempFilesDir_ = tmp_request(getpid(), tmpAmount, getenv("LOGNAME"),TempSpaceMessage,1); 

		if (tempFilesDir_[0] != '/') {
			jgsortStatus_->setStatus(ecdTempSpaceBase + errno, "Error with Temp Space Manager--",0);
			return jgsortStatus_->catStatus(tempFilesDir_,1);
		}
		usingTempSpace_ = 1;
#else
		//  Fix this to get an enviromental directory!!!
		tempFilesDir_ = new char [50];
		strcpy(tempFilesDir_,"/tmp");
#endif
	}
	return ecdNoError;
}

int JGSorter::releaseTempSpace()
{
#ifdef HAVE_TMPSPACE_MGR
	if (usingTempSpace_) 
		tmp_release(getpid());
	else
#endif
		if (tempFilesDir_)
			delete [] tempFilesDir_;
	return ecdNoError;
}


// MergeFiles will merge the current sorted buffer with the
// collection of temp files to create the output file.
// This is done for intermediate merges as well as the final merge.
// Bear in mind that for small sorts, the final buffer may not
// be merged with anything else.
int JGSorter::mergeFiles(const char *outfilename, int writeCtrlZ,
                    BRStatusCallBack StatusFunction,long Freq)
{

	int mergedfile;

	//	O_RDWR|O_CREAT|O_TRUNC
	mergedfile = open(outfilename, O_RDWR|O_CREAT, 0666);
	if (mergedfile < 0)
		return jgsortStatus_->setErrnoStatus(ecdOutfileBase,errno,
                                         "Opening or Creating",outfilename);
	if (headerLength_) 
		lseek(mergedfile, 0, SEEK_END);

	// Open all associated Merge files.
	if (fileCollection_->count) {
		for (int loop = 0; loop < fileCollection_->count; loop++) {
			if (((BRFileObject*)(fileCollection_->at(loop)))->OpenFile() ) {
				jgsortStatus_->setErrnoStatus(ecdTmpFileBase,errno,"re-opening",
					((BRFileObject*)(fileCollection_->at(loop)))->FileName);

				close(mergedfile);
				return (*jgsortStatus_)();
			}
		}
	}

	long WrittenAmt = 0;
	int SortPos = 0;


	while (fileCollection_->count) {
		int NextPos = 0; 
		const char *NextRec;
		NextRec = ((BRFileObject*)(fileCollection_->at(0)))->recordBuffer;
		NextPos = 1;

		for (int loop = 1; loop < fileCollection_->count; loop++) {
			BRFileObject *tmpFO = (BRFileObject*)fileCollection_->at(loop);
			int compareCode = sorter_->compare(NextRec,tmpFO->recordBuffer);

			if (compareCode > 0) {
				NextRec = tmpFO->recordBuffer;
				NextPos = loop + 1;
			} else if (deleteDuplicates_ && compareCode == 0) {
				// Delete all the records that match this one.
				while (compareCode == 0 && tmpFO->ReadRecord())
					compareCode = sorter_->compare(NextRec,tmpFO->recordBuffer);

				if (compareCode == 0) {
					fileCollection_->atFree(loop); 
					loop--;
				}
			}
		} // end of for (int loop = 1; loop < fileCollection_->count; loop++) 

		if (SortPos < sorter_->bufCount()) {
			const char *tmpRec = sorter_->at(SortPos);
			int compareCode = sorter_->compare(NextRec,tmpRec);

			if (compareCode > 0) {
				NextRec = tmpRec;
				NextPos = 0;
				++SortPos;

				if (deleteDuplicates_) {
					const char *tempNext;
					int finished = 0;

					while ((sorter_->bufCount() > (SortPos)) && !finished) {
						tempNext = sorter_->at(SortPos);
						if (sorter_->compare(NextRec,tempNext) == 0) {
							SortPos++;
						} else
						finished = 1;
					}
				}
			} else if (deleteDuplicates_ && compareCode == 0) {
				++SortPos;
				const char *tempNext;
				int finished = 0;

				while ((sorter_->bufCount() > (SortPos)) && !finished) {
					tempNext = sorter_->at(SortPos);

					if (sorter_->compare(NextRec,tempNext) == 0)
						SortPos++;
					else
						finished = 1;
				}
			}
		} // end of if (SortPos < sorter_->bufCount())

		if ( write(mergedfile, NextRec, recordLength_) != recordLength_ ) {
			jgsortStatus_->setErrnoStatus(ecdOutfileBase,errno,
                                    "writing to",outfilename);
			close(mergedfile);
			return (*jgsortStatus_)();
		}

		WrittenAmt++;
		if (StatusFunction && (!(WrittenAmt % Freq)))
			StatusFunction("Merging",WrittenAmt,WrittenAmt);

		if (NextPos) {
			BRFileObject *tmpFO = (BRFileObject*)fileCollection_->at(NextPos-1);

			if (!tmpFO->ReadRecord())
				fileCollection_->atFree(NextPos-1); 
		}
	} // end of while (fileCollection_->count)

	while (SortPos < sorter_->bufCount()) {
		const char *tmpRec = sorter_->at(SortPos);

		if ( write(mergedfile, tmpRec, recordLength_) != recordLength_) {
			jgsortStatus_->setErrnoStatus(ecdOutfileBase,errno, "writing to",outfilename);
			close(mergedfile);
			return (*jgsortStatus_)();
		}

		WrittenAmt++;
		if (StatusFunction && (!(WrittenAmt % Freq)))
			StatusFunction("Merging",WrittenAmt,WrittenAmt);

		++SortPos;

		if (deleteDuplicates_) {
			const char *nextRec;
			int compCode = 1;

			do {
				if (SortPos < sorter_->bufCount()) {
					nextRec = sorter_->at(SortPos);
					compCode = sorter_->compare(nextRec,tmpRec);

				if (!compCode)
					++SortPos;
				} else
					compCode = 1;
			} while (compCode == 0);
		}
	} // end of while (SortPos < sorter_->bufCount())

	if (writeCtrlZ) {
		char EOF_CHAR[1];
		EOF_CHAR[0] = 26;
		write(mergedfile, EOF_CHAR, 1);
	}

	close(mergedfile); 
	return ecdNoError;
}



BTBufferSort *JGSorter::getSorter(const char *Buffer,size_t RecordSize,
                                  int Cmps, BRCompareType *CompareList)
{
	return new BTBufferSort(Buffer,RecordSize, Cmps,CompareList,useShellSort_);
}


// The sort will have to do the following:
//   1. Open the input file.
//   2. While not at the end of the file
//      a. Load a buffer
//      b. Sort the buffer.
//      c. Write the buffer.
//   3. ...... As needed.
int JGSorter::performSort(int NumberOfCompares,
                          long StatusFrequency, 
                          BRStatusCallBack StatusFunction)
{
	int infile;
	infile = open(inputFile_, O_RDWR, 0666);

	if (infile < 0)
		return jgsortStatus_->setErrnoStatus(ecdInputFileBase,errno,
                                         "opening",inputFile_);

	int allocAmt = maxSections_ * recordLength_;
	mainBuffer_ = new char [allocAmt];

	if (!mainBuffer_) {
		close(infile);
		return outOfMemory("Creating Sort Buffer");
	}

	sorter_ = getSorter(mainBuffer_,recordLength_,NumberOfCompares,compares_);

	if (!sorter_) {
		close(infile);
		return outOfMemory("Creating Buffer Sort Object");
	}

	fileCollection_ = new BTCollection(filesToOpen_,1);

	if (!fileCollection_) {
		close(infile);
		return outOfMemory("Creating Internal Sort Collection");
	}

	off_t fSize = lseek(infile, 0, SEEK_END);
	long TotalNumberOfRecords = (long)((fSize - headerLength_) / recordLength_);

	lseek(infile, headerLength_, SEEK_SET);

	off_t ChunkSize = maxSections_;
	off_t TotalRecordsLeft = TotalNumberOfRecords;
	off_t TotalRead = 0;
	int Finished = 0;
	int IntMergeNumber = filesToOpen_ + 2;

	while (!Finished) {
		long RecsToRead;
		long RecsRead = 0;

		if (StatusFunction)
			StatusFunction("Reading",(long)TotalRead, TotalNumberOfRecords);

		if (TotalRecordsLeft >= ChunkSize)
			RecsToRead = (long)ChunkSize;
		else 
			RecsToRead = (long)TotalRecordsLeft; 

		if ( read(infile, mainBuffer_, RecsToRead * recordLength_) < 0) {
			jgsortStatus_->setErrnoStatus(ecdInputFileBase,errno,
                                    "reading",inputFile_);
			close(infile); 
			return (*jgsortStatus_)();
		}

		RecsRead += RecsToRead;
		TotalRead +=RecsToRead;


		if (StatusFunction)
			StatusFunction("Sorting",(long)TotalRead, TotalNumberOfRecords);

		sorter_->sort(RecsToRead);

		if (TotalRecordsLeft == RecsToRead) {
			if (headerLength_) {
				char *hdr = new char [headerLength_];

				if (!hdr) {
					close(infile);
					return outOfMemory("Creating file header buffer");
				}

				lseek(infile, 0, SEEK_SET);
		
		        if ( read(infile, hdr, headerLength_) < 0 ) {
		          jgsortStatus_->setErrnoStatus(ecdInputFileBase,errno,"reading",
		                                        inputFile_);
		          delete [] hdr;
		          close(infile);
		          return (*jgsortStatus_)(); 
		        }
		
		        close(infile);
		        int outfile = open(tmpOutputFile_, O_RDWR|O_CREAT|O_TRUNC, 0666);
		
		        if ( outfile < 0 ) {
					jgsortStatus_->setErrnoStatus(ecdOutfileBase,errno,
		                               "opening or creating", tmpOutputFile_);
					delete [] hdr;
					close(infile);
					return (*jgsortStatus_)(); 
				}
		
		        if ( write(outfile, hdr, headerLength_) < 0 ) {
					jgsortStatus_->setErrnoStatus(ecdOutfileBase,errno,
		                                        "writing to", tmpOutputFile_);
					delete [] hdr;
					close(infile);
					return (*jgsortStatus_)(); 
				}
		
				delete [] hdr;
		        close(outfile);
			} else
				close(infile);

			// if (mergeFiles(outputFile_, ctrlZEnd_, StatusFunction,StatusFrequency))
			if (mergeFiles(tmpOutputFile_, 0, StatusFunction,StatusFrequency))
				return (*jgsortStatus_)(); 

		} else if (fileCollection_->count == filesToOpen_) {
			BRFileObject *TmpFile = new BRFileObject(tempFilesDir_,
                                           IntMergeNumber++,recordLength_);
			if (!TmpFile) {
				close(infile);
				return outOfMemory("Creating temporary merge file");
			}

			if (mergeFiles(TmpFile->FileName, 0, StatusFunction, StatusFrequency))
				return (*jgsortStatus_)();

			fileCollection_->insert(TmpFile);

		} else {

			BRFileObject *TmpFile = new BRFileObject(tempFilesDir_,
                                       fileCollection_->count,recordLength_);
			if (!TmpFile) {
				close(infile);
				return outOfMemory("Creating temporary sort file");
			}

			fileCollection_->insert(TmpFile); 

			int tmpfile = open(TmpFile->FileName, O_RDWR|O_CREAT|O_TRUNC, 0666);

			if ( tmpfile < 0 ) {
				jgsortStatus_->setErrnoStatus(ecdTmpFileBase,errno,
                                     "creating",TmpFile->FileName);
				close(infile);
				return (*jgsortStatus_)();
			}
 
			for (int loop = 0; loop < (int)RecsToRead; loop++) {
				if ( write(tmpfile, sorter_->at(loop), recordLength_) < 0 ) {
					jgsortStatus_->setErrnoStatus(ecdTmpFileBase,errno,
                                        "writing to",TmpFile->FileName);
					close(infile);
					return (*jgsortStatus_)();
				}

				if (deleteDuplicates_) {
					const char *thisRec = sorter_->at(loop);
					const char *nextRec;
					int finished = 0;
	
					while (!finished && (loop+1 < RecsToRead)) {
						nextRec = sorter_->at(loop+1);
						if (sorter_->compare(thisRec,nextRec) == 0)
							loop++;
						else
							finished = 1;
					} // end of while (!finished && (loop+1 < RecsToRead))
				} // end of if (deleteDuplicates_)
	
				if (StatusFunction && (!(loop % StatusFrequency)))
					StatusFunction("Writing", (long)TotalRead+loop-RecsToRead,
	                         TotalNumberOfRecords);
			} // end of for (int loop = 0; loop < (int)RecsToRead; loop++)
			close(tmpfile);
		} // end of if (TotalRecordsLeft == RecsToRead)

		sorter_->ReleaseBuffer();
		TotalRecordsLeft -= RecsToRead;

		if (!TotalRecordsLeft)   
			Finished = 1;
	} // end of while (!Finished)
  
	delete [] mainBuffer_;
	mainBuffer_ = NULL;
	return ecdNoError;
}

  
int JGSorter::internalSort(const char *inputfile, const char *outputfile,
                        const char *SortCriteria, const char *ExcludeCriteria,
                        size_t RecordLength, size_t HeaderLength,
                        const char *TempSpaceMessage,
                        long StatusFrequency,
                        BRStatusCallBack StatusFunction,
                        long *TempSpaceRequired,
                        int UserFiles, long UserMemory,
                        const char *tempspacedir,
                        int DeleteDuplicates,
                        int usingDBaseSort,
                        int Control)
{
  jgsortStatus_->clearStatus();

  inputFile_ = inputfile;
 
  outputFile_ = outputfile; 
  tmpOutputFile_ = new char[strlen(outputFile_) + strlen(tmptmp) + 1];
  strcpy(tmpOutputFile_, outputfile);
  strcat(tmpOutputFile_, tmptmp);
  recordLength_ = RecordLength;
  headerLength_ = HeaderLength;

  deleteDuplicates_ = DeleteDuplicates;
  dBaseSort_ = usingDBaseSort;
  
  if (ensureInputSanity()) return (*jgsortStatus_)();

  int NumberOfCompares;
  if (buildCriteriaList(SortCriteria,ExcludeCriteria,
                        &NumberOfCompares)) return (*jgsortStatus_)();

  if (getResourceLimits(UserFiles,UserMemory,
                        TempSpaceMessage,tempspacedir)) {
    cleanupResources();
    return (*jgsortStatus_)();
  }
  if (TempSpaceRequired)
     *TempSpaceRequired = (long)tempSpaceNeeded_;


  if (Control == BRSortFile)
    performSort(NumberOfCompares, StatusFrequency, StatusFunction);

// Cleanup
  cleanupResources();
  releaseTempSpace();
  return (*jgsortStatus_)();
}

int JGSorter::JGSort(const char *inputfile, const char *outputfile,
                         const char *SortCriteria, const char *ExcludeCriteria,
                         size_t RecordLength, size_t HeaderLength,
                         const char *TempSpaceMessage,
                         int DeleteDuplicates,
                         long StatusFrequency,
                         BRStatusCallBack StatusFunction)
{
  return internalSort(inputfile,outputfile,SortCriteria,ExcludeCriteria,
                        RecordLength,HeaderLength,TempSpaceMessage,
                        StatusFrequency,StatusFunction,NULL,
                        BRSysFileLimit,BRSysMemoryLimit,NULL,DeleteDuplicates,
                        0,BRSortFile);
}

int JGSorter::dBaseSort(const char *inputfile, const char *outputfile,
                         const char *SortCriteria, const char *ExcludeCriteria,
                         const char *TempSpaceMessage,
                         int DeleteDuplicates,
                         long StatusFrequency,
                         BRStatusCallBack StatusFunction)
{
  return internalSort(inputfile,outputfile,SortCriteria,ExcludeCriteria,
                      0,0,TempSpaceMessage,
                      StatusFrequency,StatusFunction,NULL,
                      BRSysFileLimit,BRSysMemoryLimit,NULL,DeleteDuplicates,
                      1, BRSortFile);
}

int JGSorter::JGSort(const char *inputfile,const char *outputfile,
                          const char *SortCriteria, const char *ExcludeCriteria,
                          size_t RecordLength, size_t HeaderLength,
                          int FileLimit,long MemoryLimit,
                          const char *tempspacedir,
                          int DeleteDuplicates,
                          long StatusFrequency,
                          BRStatusCallBack StatusFunction)
{
  return internalSort(inputfile,outputfile,SortCriteria,ExcludeCriteria,
                      RecordLength,HeaderLength,NULL,
                      StatusFrequency,StatusFunction,NULL,
                      FileLimit,MemoryLimit,tempspacedir,DeleteDuplicates,
                      0,BRSortFile);
}


int JGSorter::JGTestParams(const char *inputfile, const char *outputfile,
                           const char *SortCriteria, 
                           const char *ExcludeCriteria,
                           size_t RecordLength, size_t HeaderLength,
                           long *TmpSpaceRequired)
{
  return internalSort(inputfile,outputfile,SortCriteria,ExcludeCriteria,
                        RecordLength, HeaderLength, NULL,0,NULL,
                        TmpSpaceRequired, BRSysFileLimit,
                        BRSysMemoryLimit,NULL,0,0,BRJustTest);
}

int JGSorter::JGTestParams(const char *inputfile,const char *outputfile,
                           const char *SortCriteria, 
                           const char *ExcludeCriteria,
                           size_t RecordLength, size_t HeaderLength,
                           int FileLimit,long MemoryLimit,
                           long *TmpSpaceRequired)
{
  return internalSort(inputfile,outputfile,SortCriteria,ExcludeCriteria,
                        RecordLength, HeaderLength, NULL,0,NULL,
                        TmpSpaceRequired, FileLimit,
                        MemoryLimit,NULL,0,0,BRJustTest);
}

void JGSorter::cleanupResources()
{

  if (fileCollection_) {
    delete fileCollection_;  
    fileCollection_ = NULL;
  }
  if (sorter_) {
    sorter_->ReleaseBuffer();
    delete sorter_;
    sorter_ = NULL;
  }
  if (mainBuffer_) {
    delete [] mainBuffer_;
    mainBuffer_ = NULL;
  }
  if (compares_) {
    delete [] compares_;
    compares_ = NULL;
  }
  if (dBaseFields_) {
    delete dBaseFields_;
    dBaseFields_ = NULL;
  }

  errno = 0;
  if (!access(tmpOutputFile_, W_OK)) {
	if (!access(outputFile_, W_OK))
		unlink(outputFile_);

	if (rename(tmpOutputFile_, outputFile_) != 0)
 	  cout << "Error: Rename " << tmpOutputFile_ << " to " << outputFile_ << 
		"(" << strerror(errno) << ")" << endl;

    unlink(tmpOutputFile_);
  }
  return;
}

int JGSorter::getDBaseHeaderInfo()
{
  int dbfFile = open(inputFile_, O_RDWR);
  if ( dbfFile < 0 )
    return jgsortStatus_->setErrnoStatus(ecdInputFileBase,errno,"opening",inputFile_);

  // At position 8 in the dBase file is the header/record.
  lseek(dbfFile, 8, SEEK_SET);
  char headerRecBuf[4];
  if ( read(dbfFile, headerRecBuf, 4) < 0 ) 
  {
    jgsortStatus_->setErrnoStatus(ecdInputFileBase,errno,
                                  "opening",inputFile_);
    close(dbfFile);
    return (*jgsortStatus_)();
  }

  unsigned short TempHdr,TempRec;
#ifdef BTBIG_ENDIAN
  *((char *)&TempHdr) = headerRecBuf[1];
  *(((char *)&TempHdr) + 1) = headerRecBuf[0];
  *((char *)&TempRec) = headerRecBuf[3];
  *(((char *)&TempRec) + 1) = headerRecBuf[2];
#else
  *((char *)&TempHdr) = headerRecBuf[0];
  *(((char *)&TempHdr) + 1) = headerRecBuf[1];
  *((char *)&TempRec) = headerRecBuf[2];
  *(((char *)&TempRec) + 1) = headerRecBuf[3];
#endif
  headerLength_ = TempHdr;
  recordLength_ = TempRec;

  if ((headerLength_ - 1) % 32) {
    close(dbfFile);
    return jgsortStatus_->setStatus(1,
                     "Invalid dBase Header Size, Not dbase format?",1);
  }
  // Start of field descriptions
  lseek(dbfFile, 32, SEEK_SET);

  TempHdr -= 33;
  // Position just after the deleted flag.
  long PositionCount = 1;

  struct DbfRecord {
    char fldname[11];
    char type;
    char index[2];
    char reserved1[2];
    char length;
    char decimal;
    char reserved2[14];
  } TempRecord;

  dBaseFields_ = new JGSortedFieldList(10,10);
  if (!dBaseFields_) {
    close(dbfFile);
    return outOfMemory("Creating internal DBase Field List");
  }

  while (TempHdr) {
    if ( read(dbfFile, (char *)&TempRecord, sizeof(TempRecord)) < 0) 
    {
      jgsortStatus_->setErrnoStatus(ecdInputFileBase,errno,"opening",inputFile_);
      close(dbfFile);
      return (*jgsortStatus_)();
    }
    if (TempRecord.type == 'C') {
      TempRecord.type = 0;
      dBaseFields_->insert(new JGdBaseFld(TempRecord.fldname,PositionCount,
                                          TempRecord.length));
    }
    PositionCount += TempRecord.length;
    TempHdr -= 32;
  }
  close(dbfFile);

  return 0;
}

char *JGSorter::getCriteriaFromFields(const char *SortCriteria)
{
  strstream CriteriaBuffer;
  if (!dBaseFields_) {
    jgsortStatus_->setStatus(1,"Program Bug, dBase Fields not allocated",1);
    return NULL;
  }
  char fieldBuffer[40];
  int bufpos = 0;
  while(*SortCriteria) {
   if (*SortCriteria == ',') {
     if (!bufpos) {
       jgsortStatus_->setStatus(1,
                   "DBase Field List Criteria Error, misplaced comma",1);
       return NULL;
     } 
     fieldBuffer[bufpos] = 0;
     ccIndex ind;
     if (!dBaseFields_->search(fieldBuffer,ind)) {
       jgsortStatus_->setStatus(1,
              "DBase Field List Criteria Error, undefined field name '",0);
       jgsortStatus_->catStatus(fieldBuffer,0);
       jgsortStatus_->catStatus("'",1);
       return NULL;
     }
     JGdBaseFld *TempFld = (JGdBaseFld*)dBaseFields_->at(ind);
     CriteriaBuffer << TempFld->fldStartPos_ << "," 
                    << TempFld->fldLength_ << ",C,";
     SortCriteria++;
     while (*SortCriteria == ' ') SortCriteria++;
     if (*SortCriteria != 'A' && *SortCriteria != 'D') {
       if (*SortCriteria == ',' || !(*SortCriteria))
         jgsortStatus_->setStatus(1,
              "DBase Field List Criteria Error, Sort direction missing",1);
       else
         jgsortStatus_->setStatus(1,"DBase Field List Criteria Error, "
                                  "Invalid sort direction",1);
       return NULL;
     }
     CriteriaBuffer << (*SortCriteria); 
     SortCriteria++;
     while (*SortCriteria == ' ') SortCriteria++;
     if (*SortCriteria == ',') {
       CriteriaBuffer << ",";
       SortCriteria++;
       while (*SortCriteria == ' ') SortCriteria++;
     } else if (*SortCriteria) {
       jgsortStatus_->setStatus(1,"DBase Field List Criteria Error, "
                                "Invalid sort direction",1);
       return NULL;
     }
     bufpos = 0;
   } else {
     fieldBuffer[bufpos++] = *SortCriteria;
     SortCriteria++;
   }
  }
  if (bufpos) {
    jgsortStatus_->setStatus(1,"DBase Field List Criteria Error, "
                             "Sort direction missing",1);
    return NULL; 
  }

  delete dBaseFields_;
  dBaseFields_ = NULL;

  CriteriaBuffer << ends;
  char *newCrit = BTDuplicateString(CriteriaBuffer.str());
  if (!newCrit) {
    outOfMemory("Allocating absolute criteria specification");
    return NULL;
  }
  return newCrit; 
}

////////////////////////////////////////////////////////////////////////
//
//           Profiling sorter
//
////////////////////////////////////////////////////////////////////////


JGProfilingSorter::JGProfilingSorter(BTStatus *SortStatus) :
  JGSorter(SortStatus)
{
}

JGProfilingSorter::~JGProfilingSorter()
{
}

void JGProfilingSorter::outputProfile()
{
  cout << "JGSort Sort profiler" << endl;
  cout << "  Input file is " << inputFile_ << endl
       << "  Output file is " << outputFile_ << endl
       << "  Temp space directory " << tempFilesDir_ << endl
       << "  Record length is " << recordLength_ << endl
       << "  Header length is " << headerLength_ << endl;
  if (dBaseSort_)
    cout << "  Performing dBase sort" << endl;
  else
    cout << "  Performing flat file sort" << endl;
  cout << "Resource Limits:" << endl
       << "  Memory to Allocate " << memoryToAlloc_ << endl
       << "  Maximum Intermediate Files to open " << filesToOpen_ << endl
       << "  Temp file space needed " << tempSpaceNeeded_ << endl 
       << "  Records per section " << maxSections_ << endl
       << "  Max Swaps " << maxSwaps_ << endl;
}

int JGProfilingSorter::mergeFiles(const char *outfilename, int WriteCtrlZ,
                                  BRStatusCallBack StatusFunction,long Freq)
{
  cout << "Merge Statistics:" << endl 
       << "  Files to merge " << fileCollection_->count << endl
       << "  In Memory Records " << sorter_->bufCount() << endl;
  BTStopWatch stpWatch;
  int retvalue = JGSorter::mergeFiles(outfilename,WriteCtrlZ,
                                      StatusFunction,Freq);
  stpWatch.stop();
  cout << "  Merge times " << stpWatch << endl;
  return retvalue;
}

int JGProfilingSorter::performSort(int NumberOfCompares,
                                   long StatusFrequency, 
                                   BRStatusCallBack StatusFunction)
{
  outputProfile();
  return JGSorter::performSort(NumberOfCompares,StatusFrequency,StatusFunction);
}

BTBufferSort *JGProfilingSorter::getSorter(const char *Buffer,
                                           size_t RecordSize,
                                           int Cmps,
                                           BRCompareType *CompareList)
{
  return new BTProfilingSort(Buffer,RecordSize, Cmps,CompareList,
                             useShellSort_);
}
