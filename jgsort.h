//
//  Module Name : $RCSfile: jgsort.h,v $
//  RCS Name    : $Source: /usr/local/cvsroot/RIDE/Datagen/Linux/JGSort/jgsort.h,v $
//  Author      : $Author: dnassirp $
//  RCS Version : $Revision: 1.2 $
//  Last Delta  : $Date: 2008/02/05 17:56:56 $
//
//********************************************************************
//                    Description
// 
//  Function Declarations for JGSort generic sorting routines.
// 
//********************************************************************
//                     Change Log
//
/*
 * $Log: jgsort.h,v $
 * Revision 1.2  2008/02/05 17:56:56  dnassirp
 * Updated from rchen's dir on eclipse. Should fix issue we were having with
 * quicksort
 *
 * Revision 1.6  1995/04/22  22:01:03  brian
 * Added support for dBase type files
 *
 * Revision 1.5  1995/04/17  00:30:17  brian
 * Added out of memory status reporting.
 * Changed function names to conform with Clines' C++ FAQ book.
 *
 * Revision 1.4  1995/04/15  23:18:39  brian
 * Modified to work within a class structure rather then individual functions.
 * Moved all functions into the class.
 *
 * Revision 1.3  1995/01/03  01:14:08  brian
 * Changed the BRGetStatus function to BRDecodeStatus
 *
 * Revision 1.2  1995/01/02  23:28:31  brian
 * Added function calls for testing
 * Added function for sorting with user supplied memory and file limits
 *
 * Revision 1.1  1995/01/02  19:25:23  brian
 * Initial revision
 *
 */
//********************************************************************
//
//
//
//
// 
//
//
//   In the SortCriteria
//      C - Character
//      II - Intel 16 Bit Integer
//      IU - Intel 16 Bit Unsigned Integer
//      IL - Intel 32 Bit Integer
//      IT - Intel 32 Bit Unsigned Integer
//      MI - Motorola 16 Bit Integer
//      MU - Motorola 16 Bit Unsigned Integer
//      ML - Motorola 32 Bit Integer
//      MT - Motorola 32 Bit Unsigned Integer
//

#ifndef JG_SORT_DEF
#define JG_SORT_DEF

#include <sys/types.h>
#include "stgentyp.h"
#include "ststruct.h"
#include <btstatus.h>

static char jgsort_h_id[]="@(#) JGSort/jgsort.h 2.1 (09/04/98) by Brian G. Reid (TIMS) Build: 02/19/99";

class JGSortedFieldList;

class JGSorter 
{
  public:
    JGSorter(BTStatus *SortStatus);
    virtual ~JGSorter();

// This variation will sort the file using system default memory,
// file, and Temporary space parameters.
    int JGSort(const char *inputfile, const char *outputfile,
               const char *SortCriteria, 
               const char *ExcludeCriteria,
               size_t RecordLength, size_t HeaderLength,
               const char *TempSpaceMessage,
               int DeleteDuplicates,
               long StatusFrequency,
               BRStatusCallBack StatusFunction);

// This variation will sort the file using system default memory,
// file, and Temporary space parameters.
    int dBaseSort(const char *inputfile, const char *outputfile,
                  const char *SortCriteria, 
                  const char *ExcludeCriteria,
                  const char *TempSpaceMessage,
                  int DeleteDuplicates,
                  long StatusFrequency,
                  BRStatusCallBack StatusFunction);

// This variation will sort the file using the provided FileLimit number
// of files and Memory Limit amount of memory only if they are less than
// the system defaults.
    int JGSort(const char *inputfile,const char *outputfile,
               const char *SortCriteria, 
               const char *ExcludeCriteria,
               size_t RecordLength, size_t HeaderLength,
               int FileLimit,long MemoryLimit,
               const char *tempspacedir,
               int DeleteDuplicates,
               long StatusFrequency,
               BRStatusCallBack StatusFunction);

// This function will test the given parameters to make sure the sort
// will work.  It will return the total amount of Temporary file space
// required for the sort.
    int JGTestParams(const char *inputfile, 
                     const char *outputfile,
                     const char *SortCriteria, 
                     const char *ExcludeCriteria,
                     size_t RecordLength, 
                     size_t HeaderLength,
                     long *TmpSpaceRequired);

// This function will test the given parameters to make sure the sort
// will work.  It will return the total amount of Temporary file space
// required for the sort.
    int JGTestParams(const char *inputfile,
                     const char *outputfile,
                     const char *SortCriteria, 
                     const char *ExcludeCriteria,
                     size_t RecordLength, 
                     size_t HeaderLength,
                     int FileLimit,
                     long MemoryLimit,
                     long *TmpSpaceRequired);

   // This will make the sort not allocate any tempspace from the 
   // Tempspace manager.  It is assumed that the parent process
   // has already allocated enough space.
   void forceNoTmpSpace() { noTempSpace_ = 1; }

   void useShellSort() { useShellSort_ = 1; }

  protected:
    BTStatus *jgsortStatus_;

    int ensureInputSanity();
    int checkCompares(char *SortCriteria,int *NumComps);
    int outOfMemory(const char *DoingWhat);
    int buildCriteriaList(const char *SortCriteria,
                          const char *ExcludeCriteria,
                          int *NumberOfCompares);
    int fileHandlesAvailable(const char *testfile,int UserFiles);
    long memoryToAllocate(long UserMemory);
    int getResourceLimits(int UserFiles,long UserMemory,
                          const char *TempSpaceMessage,
                          const char *tempspacedir);
    int releaseTempSpace();
    virtual int mergeFiles(const char *outfilename, int WriteCtrlZ,
                           BRStatusCallBack StatusFunction,long Freq);
    virtual int performSort(int NumberOfCompares,
                            long StatusFrequency, 
                            BRStatusCallBack StatusFunction);
    virtual BTBufferSort *getSorter(const char *Buffer,size_t RecordSize,
                                    int Cmps, BRCompareType *CompareList);
    int internalSort(const char *inputfile, const char *outputfile,
                     const char *SortCriteria, const char *ExcludeCriteria,
                     size_t RecordLength, size_t HeaderLength,
                     const char *TempSpaceMessage,
                     long StatusFrequency,
                     BRStatusCallBack StatusFunction,
                     long *TempSpaceRequired,
                     int UserFiles, long UserMemory,
                     const char *tempspacedir,
                     int DeleteDuplicates,
                     int UsingDbaseHeader,
                     int Control);
  
  int ctrlZAtEnd();
  // For use with DBase Files
  int getDBaseHeaderInfo();
  char *getCriteriaFromFields(const char *SortCriteria);

  const char *inputFile_;
  const char *outputFile_;
  char *tmpOutputFile_;
  size_t recordLength_;
  size_t headerLength_;

  void cleanupResources();

  BTCollection *fileCollection_;
  BTBufferSort *sorter_;
  BRCompareType *compares_;

  long memoryToAlloc_;
  off_t maxSwaps_;
  int  filesToOpen_;
  off_t tempSpaceNeeded_;
  long maxSections_;
  char *mainBuffer_;
  char *tempFilesDir_;
  int usingTempSpace_;
  int useShellSort_;
  int deleteDuplicates_;
  int ctrlZEnd_;
  int dBaseSort_;
  int noTempSpace_;
  JGSortedFieldList *dBaseFields_;
};

class JGProfilingSorter : public JGSorter
{
  public:
    JGProfilingSorter(BTStatus *SortStatus);
    virtual ~JGProfilingSorter();

  protected:

    virtual int mergeFiles(const char *outfilename, int WriteCtrlZ,
                           BRStatusCallBack StatusFunction,long Freq);
    virtual int performSort(int NumberOfCompares,
                            long StatusFrequency, 
                            BRStatusCallBack StatusFunction);
    virtual BTBufferSort *getSorter(const char *Buffer,size_t RecordSize,
                                    int Cmps, BRCompareType *CompareList);
  
    void outputProfile();
};

#endif

