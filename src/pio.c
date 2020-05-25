/* $Id$ */
#include "pio.h"
#define _FILE_OFFSET_BITS 64
#define _LARGEFILE_SOURCE 1
#define  _LARGE_FILE
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <assert.h>
#include <mpi.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>
#include "error.h"
#include "utilities.h"
#include "object.h"
#include "hardwareInfo.h"
#include "ddcMalloc.h"
#include "heap.h"
#include "tagServer.h"
#include "tar.h"

#define MIN(A,B) ((A) < (B) ? (A) : (B))
#define MAX(A,B) ((A) > (B) ? (A) : (B))

// This defintion needs to be kept in sync with PIO_ENUMS
char* PioNames [] = {"NONE" , "SPLIT", "FIXRECORDASCII", "FIXRECORDBINARY",
							"VARRECORDASCII", "VARRECORDBINARY", "CRC32"};

/** Amount of extra space in read buffer for bytes that couldn't be used
 *  by the previous task. */
static const unsigned _bufferExcess = 10*1024;
static const size_t _maxMpiCount = INT_MAX;

static int    nWriteFilesDefault(int nTasks);
static int    Pio_groupSetup(PFILE* file);
PFILE* Pfile_init(const char *filename, const char *mode, MPI_Comm comm);
static void   Pfile_free(PFILE*);
void   Popen_forRead(PFILE*);
static int    Pclose_forRead(PFILE*);
static int    Pclose_forWrite(PFILE*);
static char*  readPheader(PFILE * pfile, int* rawHeaderLength, int *strippedHeaderLength);
static int64_t fillBuffer(char* buf, int tid, const PFILE* file);
static int    setupIoIds(PFILE* file);
static int    groupId(int tid, const PFILE* file);
static int    groupBegin(int gid, const PFILE* file);
static int    groupEnd(int gid, const PFILE* file);
static void   formName(char* filename, char* basename, int fid);
static int64_t computeMaxReadBuf(const PFILE* file);
static int    groupForFile(int iFile, const PFILE*);
static int    nAssignedFiles(int tid, const PFILE* file);
static FILE*  fileHandle(int tid, int iFile, const PFILE* file);
static off_t  readStart(int iFile, int tid, int64_t* readLen, const PFILE*);
static void   bufferAppend(PFILE* file, const void* data, size_t n);
static void   internalSelfTest(const PFILE* file);
static pio_long64 statPfile(PFILE *pfile,pio_long64 *datasize,pio_long64 *startOfFile);
char *getMasterName(const char *filename , int *fileType);


static char string[1024];
static int error_global; 
static int _nWriteFiles = 0;
enum FILETYPE { PIO, SIMPLE, TAR }; 

PFILE *Popen(const char *filename, const char *mode, MPI_Comm comm)
{
   PFILE* file = Pfile_init(filename, mode, comm);
   if (strcmp(mode, "w") == 0)
	{
		PioReserve(file, 1024); // ensure pio_buf_blk is created
      return file;
	}
   if (strcmp(mode, "r") == 0)
   {
      Popen_forRead(file);
      return file;
   }

   // unrecognized mode
   Pfile_free(file);
   file = NULL;
   return file;
}
void PopenNext(PFILE *pfile, const char *mode, MPI_Comm comm)
{
   assert(strcmp(mode, "r") == 0);
   Popen_forRead(pfile);
   pfile->start = pfile->next; 
   pfile->bufpos =0; 
   pfile->recordLength = -1;
   pfile->numberRecords = -1;
}


PFILE* Pfile_init(const char *filename, const char *mode, MPI_Comm comm)
{
   PFILE *file;
   file = (PFILE *) ddcMalloc(sizeof(PFILE));
   file->comm = comm;
   MPI_Comm_rank(file->comm, &file->id);
   MPI_Comm_size(file->comm, &file->size);

   file->file = NULL;
   file->readFile = NULL;
   file->nBytesInFile = NULL;
   file->buf = NULL;
   file->nfields = 0; 
   file->beanCounter = -1;
   file->doneTag = getUniqueTag(comm);
   file->goTag   = getUniqueTag(comm);
   file->msgTag  = getUniqueTag(comm);
   file->field_names = NULL;
   file->field_types = NULL;
   file->field_units = NULL;
   file->field_format = NULL;
   file->bufsize = 0;
   file->bufpos = 0;
   file->bufcapacity = 0;
   file->recordLength = -1;
   file->numberRecords = -1;
   file->datatype = PIO_NONE;
   file->checksum=0; 
   file->name = strdup(filename);
   file->masterName = getMasterName(filename,&file->type);
   file->start =0; 
   file->next =0; 
   file->mode = strdup(mode);
   file->misc_info = strdup(""); 
   file->headerObject = NULL;
   file->helper = NULL;
   // These are default values for write.  Values for read are set after
   // file header is read.
   if (_nWriteFiles == 0)
      _nWriteFiles = nWriteFilesDefault(file->size);
   file->nfiles = _nWriteFiles;
   file->ngroup = MIN(file->size, file->nfiles);
   int nIoTasks = hi_nIoTasks(file->comm);
   if (nIoTasks > 511)
      file->ngroup = MIN(file->ngroup, nIoTasks);
   Pio_groupSetup(file);
   internalSelfTest(file);
   return file;
}

char *getMasterName(const char *filename , int *fileType)
{
   char *masterName = NULL; 
   int len = strlen(filename); 
   if (filename[len-1] == '#')
   {
      masterName = ddcMalloc(len+7);
      sprintf(masterName, "%s%6.6d", filename, 0);
      *fileType = PIO;
   }
   char *offset; 
   if (masterName == NULL && (offset = strstr(filename,".tar")) != NULL ) 
   {
      int length = strlen(offset); 
      assert (length == 4 || (length > 4 && offset[4] == ':' ));
      char *archive = strdup(filename);
      masterName = archive; 
      offset[length]='\0';
      char *memberName = NULL; 
      if (length > 4)  memberName = offset+length+1; 
      *fileType = TAR; 
   }
   if (masterName == NULL )
   {
      masterName = strdup(filename);
      *fileType = SIMPLE; 
   }
   return masterName; 
}
/** Returns the number of tasks that will perform I/O. */
int Pio_groupSetup(PFILE* file)
{
   file->sizegroup = file->size/file->ngroup;
   if (file->size%file->ngroup != 0 && strcmp(file->mode, "w") == 0)
   {
      file->sizegroup ++;
      file->ngroup = (file->size/file->sizegroup);
      if (file->size % file->sizegroup != 0)
         ++file->ngroup;
   }
   return setupIoIds(file);
}


void PioSet(PFILE*file, const char *string, ... )
{
   va_list ap;
   va_start(ap, string);
   if (strcmp(string, "ngroup") == 0)
   {
      file->ngroup = va_arg(ap, int);
      file->ngroup = MIN(file->size, file->ngroup);
      Pio_groupSetup(file);
   }
   if (strcmp(string, "recordLength") == 0) file->recordLength = va_arg(ap, int);
   if (strcmp(string, "numberRecords") == 0) file->numberRecords = va_arg(ap, pio_long64);
   if (strcmp(string, "datatype") == 0) file->datatype = va_arg(ap, int);
   if (strcmp(string, "checksum") == 0) file->checksum = va_arg(ap, int);
   if (strcmp(string, "nfields") == 0) file->nfields = va_arg(ap, int);
   if (strcmp(string, "field_names") == 0) file->field_names = strdup(va_arg(ap, char *));
   if (strcmp(string, "field_types") == 0) file->field_types = strdup(va_arg(ap, char *));
   if (strcmp(string, "field_units") == 0) file->field_units = strdup(va_arg(ap, char *));
   if (strcmp(string, "field_format") == 0) file->field_format = strdup(va_arg(ap, char *));
   if (strcmp(string, "misc_info") == 0) 
   {
      char *new_info; 
      new_info = va_arg(ap, char *);
      file->misc_info=ddcRealloc(file->misc_info,strlen(file->misc_info)+strlen(new_info)+2);
      strcat(file->misc_info,new_info);
      strcat(file->misc_info," ");
   }
}

void Pget(PFILE*file, const char *string, void *ptr)
{
   if (strcmp(string, "sizegroup") == 0) *(int *)ptr = file->sizegroup;
   if (strcmp(string, "ngroup") == 0) *(int *)ptr = file->ngroup;
   if (strcmp(string, "recordLength") == 0) *(int *)ptr = file->recordLength;
   if (strcmp(string, "numberRecords") == 0) *(pio_long64 *)ptr = file->numberRecords;
   if (strcmp(string, "datatypeName") == 0) *(char **)ptr = PioNames[file->datatype];
   if (strcmp(string, "checksumName") == 0) *(char **)ptr = PioNames[file->checksum];
   if (strcmp(string, "nfields") == 0) *(int *)ptr = file->nfields;
   if (strcmp(string, "field_names") == 0) *(char **)ptr = file->field_names;
   if (strcmp(string, "field_types") == 0) *(char **)ptr = file->field_types;
   if (strcmp(string, "field_units") == 0) *(char **)ptr = file->field_units;
   if (strcmp(string, "field_format") == 0) *(char **)ptr = file->field_format;
   if (strcmp(string, "misc_info") == 0) *(char **)ptr = file->misc_info;

}

int Pprintf(PFILE* file, const char* fmt, ...)
{
   const int size=1024;
   char buffer[size]; 
   va_list ap;
   va_start(ap, fmt);
   int n = vsnprintf(buffer, size, fmt, ap);
   assert(n<size-1); 
   va_end(ap);
   bufferAppend(file, buffer, n);
   return n;
}

char* Pfgets(char* string, int size, PFILE* file)
{
   assert(size >= 1);

   string[0] = '\0';
   if (file->bufpos >= file->bufsize)
   {
      return NULL;
   }

   size_t ii;
   size_t maxRead = size-1;
   for (ii=file->bufpos; ii<file->bufsize; ++ii)
   {
      if (file->buf[ii] == '\n' || ii - file->bufpos == maxRead)
         break;
   }

   strncat(string, file->buf+file->bufpos, ii-file->bufpos);
   file->bufpos = ii+1;
   return string;
}

size_t Pread(void* ptr, size_t size, size_t nitems, PFILE* file)
{
   if (file->bufpos >= file->bufsize)
   {
      return 0;
   }
   size_t readsize = size*nitems;
   if (file->bufpos+readsize > file->bufsize)
      readsize = file->bufsize-file->bufpos;
   memcpy(ptr, file->buf+file->bufpos, readsize);
   file->bufpos += readsize;
   return readsize;
}


size_t Pwrite(const void *ptr, size_t size, size_t nmemb, PFILE*file)
{
   bufferAppend(file, ptr, size*nmemb);
   return nmemb;
}

void bufferAppend(PFILE* file, const void* data, size_t n)
{
   if (file->bufsize+n > file->bufcapacity)
   {
      int increment = MAX(n, 1024*1024);
      PioReserve(file, file->bufcapacity + increment);
   }
   memcpy(file->buf + file->bufsize, data, n);
   file->bufsize += n;
}


int Pclose(PFILE* file)
{
   if (strcmp(file->mode, "w") == 0)
      return Pclose_forWrite(file);
   if (strcmp(file->mode, "r") == 0)
      return Pclose_forRead(file);

   // Reach here only if mode is unrecognized.
   assert(1==0);
   return -1;
}

int Pclose_forRead(PFILE* file)
{
   Pfile_free(file);
   return 0;
}


/** We require that data be written to the files in strict task order.
 * I.e., the first task in file#000000 must be task 0, followed by task
 * 1 and so on.  If all files were concatenated the tasks would be in
 * order.  The internalSelfTest function ensures that the groups contain
 * the tasks in strict sequential order so the group iteration we
 * perform below satifies this requirement.  */
int Pclose_forWrite(PFILE*file)
{
   unsigned pio_msg_blk;
   heapEndBlock(file->pio_buf_blk, file->bufsize);
   char* buffer = (char*) heapGet(&pio_msg_blk);
   size_t bufMax = heapBlockSize(pio_msg_blk);
   heapEndBlock(pio_msg_blk, bufMax);

   int flag = 1;
   int error = 0;
   int dataWritten = 0;
   if (file->groupToHandle >= 0)
   {
      sprintf(string, "%s#%6.6d", file->name, file->groupToHandle);
      file->file = fopen(string, file->mode);

      for (int id = groupBegin(file->groupToHandle, file); id < groupEnd(file->groupToHandle, file); id++)
      {
         // Check another writer is asking for this task's data.  If so, send it.
         MPI_Iprobe(file->io_id, file->goTag, file->comm, &flag, MPI_STATUS_IGNORE);
         if (flag == 1)
         {
            MPI_Recv(&flag, 1, MPI_INT, file->io_id, file->goTag, file->comm, MPI_STATUS_IGNORE);
            MPI_Send(&file->bufsize, sizeof(size_t), MPI_BYTE, file->io_id, file->msgTag, file->comm);
            assert(file->bufsize <= _maxMpiCount);
            MPI_Send(file->buf, (int)file->bufsize, MPI_BYTE, file->io_id, file->msgTag, file->comm);
            dataWritten = 1;
         }

         if (id == file->id)
         {
            int cnt = fwrite(file->buf, file->bufsize, 1, file->file); fflush(file->file); 
            if (cnt == 0) error &= ferror(file->file);
            dataWritten = 1;
         }
         else
         {
            size_t bufsize;
            MPI_Send(&flag, 1, MPI_INT, id, file->goTag, file->comm);
            MPI_Recv(&bufsize, sizeof(size_t), MPI_BYTE, id, file->msgTag, file->comm, MPI_STATUS_IGNORE);
            if (bufsize > bufMax)
            {
               printf("ERROR:  Buffer size exceeded.  Size = %zu\n"
                     "Task %d attempting to send %zu bytes to I/O task %d\n",
                     bufMax, id, bufsize, file->id);
               MPI_Abort(file->comm, 11);
            }
            assert(bufsize < _maxMpiCount);
            MPI_Recv(buffer, (int)bufsize, MPI_BYTE, id, file->msgTag, file->comm, MPI_STATUS_IGNORE);
            int cnt = fwrite(buffer, bufsize, 1, file->file); fflush(file->file); 
            if (cnt == 0) error &= ferror(file->file);
         }
      }
      fclose(file->file);
   }

   //  Any non-writer task waits here for the signal to send data to the
   //  writer.  This also applies to writer tasks that don't write their
   //  own data and might have finished writing before they were asked
   //  to send their data elsewhere.
   if (dataWritten == 0)
   {
      MPI_Recv(&flag, 1, MPI_INT, file->io_id, file->goTag, file->comm, MPI_STATUS_IGNORE);
      MPI_Send(&file->bufsize, sizeof(size_t), MPI_BYTE, file->io_id, file->msgTag, file->comm);
      assert(file->bufsize <= _maxMpiCount);
      MPI_Send(file->buf, (int)file->bufsize, MPI_BYTE, file->io_id, file->msgTag, file->comm);
   }
   MPI_Reduce(&error,&error_global,1,MPI_INT,MPI_BAND,0,file->comm);
   heapFree(pio_msg_blk);
   heapFree(file->pio_buf_blk);
   // Since write gets file->buf from the scratch heap instead of malloc
   // we must NULL the pointer before calling Pfile_free to ensure that
   // Pfile_free doesn't free the scratch heap.
   file->buf=NULL;
   Pfile_free(file);
   return error_global; 
}

void slave_Pio_setNumWriteFiles(int* nWriteFiles)
{
   Pio_setNumWriteFiles(*nWriteFiles);
}



void Pio_setNumWriteFiles(int nWriteFiles)
{
   _nWriteFiles = nWriteFiles;
}

void PioReserve(PFILE* file, size_t capacity)
{
   if (strcmp(file->mode, "w") != 0)
      return;

   if (file->buf == NULL)
   {
      file->buf = heapGet(&file->pio_buf_blk);
      file->bufcapacity = heapBlockSize(file->pio_buf_blk);
   }

   if (capacity > file->bufcapacity)
   {
      float b2mb = 1.0/(1024.0*1024.0);
      printf("ERROR:  Heap too small for requested buffer capacity in PioReserve.\n"
            "  Task %d:\n"
            "  Requested buffer capacity = %10.2f MB\n"
            "  Total Heap Size = %10.2f MB\n"
            "  Size of Available block = %10.2f MB\n"
            "  Block number = %d\n",
            file->id, capacity*b2mb, heapSize()*b2mb, file->bufcapacity*b2mb,
            file->pio_buf_blk);

      MPI_Abort(file->comm, 11);
   }
}


int nWriteFilesDefault(int nTasks)
{
   if (nTasks < 4096)
      return 16;
   if (nTasks < 8192)
      return 64;
   return 256;
}

void Pfile_free(PFILE* file)
{
   if (file->helper != NULL) file->helper->destroy(file->helper);
   object_free(file->headerObject);
   file->headerObject= NULL; 
   ddcFree(file->field_names);
   ddcFree(file->field_types);
   ddcFree(file->field_units);
   ddcFree(file->field_format);
   ddcFree(file->misc_info);
   ddcFree(file->buf);
   ddcFree(file->name);
   ddcFree(file->mode);

   if (file->readFile != NULL)
      for (int ii=0; ii<file->nfiles; ++ii) if (file->readFile[ii] != NULL) fclose(file->readFile[ii]);
   ddcFree(file->nBytesInFile);
   ddcFree(file->readFile);
   ddcFree(file);
}

void Popen_forRead(PFILE* pfile)
{
   char* header=NULL;
   int strippedHeaderLength, rawHeaderLength;
   pio_long64 datasize0,startOfFile0; 
   OBJECTFILE ofile; 
   if (pfile->id == 0)
   {
      pfile->file= fopen(pfile->masterName, "r");
      int rc = fseeko(pfile->file,pfile->start,SEEK_CUR);
      if (pfile->file == NULL)
      {
         printf("Popen for read failed for file name: %s\n" "Can't open master file:  %s\n" , pfile->name, pfile->masterName);
         MPI_Abort(MPI_COMM_WORLD, 1);
      }
      statPfile(pfile,&datasize0,&startOfFile0);
      header = readPheader(pfile, &rawHeaderLength,&strippedHeaderLength);
      fclose(pfile->file);
      pfile->file=NULL; 
   }
   MPI_Bcast(&rawHeaderLength, 1, MPI_INT, 0, pfile->comm);
   MPI_Bcast(&strippedHeaderLength, 1, MPI_INT, 0, pfile->comm);
   MPI_Bcast(&datasize0, 1, MPI_UNSIGNED_LONG_LONG, 0, pfile->comm);
   MPI_Bcast(&startOfFile0, 1, MPI_UNSIGNED_LONG_LONG, 0, pfile->comm);
   if (pfile->id != 0) header = (char*) ddcMalloc(strippedHeaderLength);
   MPI_Bcast(header, strippedHeaderLength, MPI_CHAR, 0, pfile->comm);
   OBJECT *hobj=pfile->headerObject =  (OBJECT*) ddcMalloc(sizeof(OBJECT));
   object_lineparse(header, hobj);
   ddcFree(header);

   pfile->helper = pioHelperFactory(pfile->headerObject);

   char* string;
   object_get(hobj, "nfiles",   &pfile->nfiles,        INT,    1, "0");
   object_get(hobj, "lrec",     &pfile->recordLength,  INT,    1, "0");
   object_get(hobj, "nrecord",  &pfile->numberRecords, U64,    1, "0");
   object_get(hobj, "nfields",  &pfile->nfields,       INT,    1, "0");
   object_get(hobj, "checksum", &string,              STRING, 1, "NONE");
   int checksum=0; 
   if (strcmp(string,"CRC32")==0) checksum=CRC32;
   object_get(hobj, "datatype", &string,              STRING, 1, "NONE");
   if      (strcmp(string, "FIXRECORDASCII") == 0)  pfile->datatype = FIXRECORDASCII;
   else if (strcmp(string, "FIXRECORDBINARY") == 0) pfile->datatype = FIXRECORDBINARY;
   else if (strcmp(string, "VARRECORDASCII") == 0)  pfile->datatype = VARRECORDASCII;
   else if (strcmp(string, "VARRECORDBINARY") == 0) pfile->datatype = VARRECORDBINARY;

   if ( (pfile->nfiles == 0 ) && pfile->id == 0)
   {
      printf("Popen for read failed.\n"
            "Ensure header of file %s specifies nfiles.\n", pfile->name);
      MPI_Abort(pfile->comm, 1);
   }

   pfile->headerLength = rawHeaderLength;
   pfile->checksum = checksum;
   pfile->ngroup = MIN(pfile->size, pfile->nfiles);
   int nReaders = Pio_groupSetup(pfile);
   if (pfile->id ==0)
      printf("Using %d tasks for reading.\n", nReaders);

   if (pfile->groupToHandle >= 0)
   {
      // This task will read one or more files.  Figure out which files
      // it is assigned to read.  We open and stat only those files.
      // The other file handles in the array will remain NULL.
      pfile->readFile     = ddcMalloc(pfile->nfiles*sizeof(FILE*));
      pfile->nBytesInFile = ddcMalloc(pfile->nfiles*sizeof(pio_long64));
      pfile->startOfFile = ddcMalloc(pfile->nfiles*sizeof(pio_long64));
      for (int ii=0; ii<pfile->nfiles; ++ii)
      {
         pfile->readFile[ii] = NULL;
         pfile->nBytesInFile[ii] = 0;
         pfile->startOfFile[ii] = 0;
      }

      /*     open file  */
      for (int fid=0; fid<pfile->nfiles; ++fid)
         if (groupForFile(fid, pfile) == pfile->groupToHandle)
         {
            char filename[1024];
            formName(filename, pfile->name, fid);
            pfile->readFile[fid] = fopen(filename, "r");
            assert(pfile->readFile[fid] != NULL);
            if (fid == 0)     //nBytesInfile and startOfFile are already set for fid == 0
            {
               datasize0 -= pfile->headerLength;
               pfile->nBytesInFile[fid] = datasize0;
               pfile->startOfFile[fid] = startOfFile0; 
            }
            else
            {
               struct stat statbuf;
               int rc = stat(filename, &statbuf);
               assert(rc == 0);
               pio_long64 datasize = statbuf.st_size;
               pfile->nBytesInFile[fid] = datasize;
               pfile->startOfFile[fid] =0; 
            }

         }
   }

   int dataReceived = 0;
   int flag = 0;
   if (pfile->groupToHandle >= 0)
   {
      int64_t maxReadBuf = computeMaxReadBuf(pfile);
      char* buf = ddcMalloc(maxReadBuf+_bufferExcess);
      int gBegin = groupBegin(pfile->groupToHandle, pfile);
      int gEnd = groupEnd(pfile->groupToHandle, pfile);
      size_t leftOver = 0;
      for (int ii=gBegin; ii<gEnd; ++ii)
      {
         size_t buf_len = fillBuffer(buf+leftOver, ii, pfile);
         buf_len += leftOver;
         size_t bufEnd = pfile->helper->endOfRecords(buf, buf_len, pfile->helper);

         if (ii == pfile->id)
         {
            pfile->buf = ddcMalloc(bufEnd);
            pfile->bufsize = bufEnd;
            memcpy(pfile->buf, buf, bufEnd);
            dataReceived = 1;
         }
         else
         {
            MPI_Send(&bufEnd, sizeof(size_t), MPI_BYTE, ii, pfile->msgTag, pfile->comm);
            MPI_Send(buf, bufEnd, MPI_BYTE, ii, pfile->msgTag, pfile->comm);
         }
         MPI_Iprobe(pfile->io_id, pfile->msgTag, pfile->comm, &flag, MPI_STATUS_IGNORE);
         if (flag == 1)
         {
            MPI_Recv(&pfile->bufsize, sizeof(size_t), MPI_BYTE, pfile->io_id, pfile->msgTag, pfile->comm, MPI_STATUS_IGNORE);
            pfile->buf = ddcMalloc(pfile->bufsize);
            assert(pfile->bufsize <= _maxMpiCount);
            MPI_Recv(pfile->buf, (int)pfile->bufsize, MPI_BYTE, pfile->io_id, pfile->msgTag, pfile->comm, MPI_STATUS_IGNORE);
            dataReceived = 1;
         }

         leftOver = buf_len - bufEnd;
         assert(leftOver < _bufferExcess);
         memcpy(buf, buf+bufEnd, leftOver);
      }
      assert(leftOver == 0);
      ddcFree(buf);
   }

   if (dataReceived == 0)
   {
      MPI_Recv(&pfile->bufsize, sizeof(size_t), MPI_BYTE, pfile->io_id, pfile->msgTag, pfile->comm, MPI_STATUS_IGNORE);
      pfile->buf = ddcMalloc(pfile->bufsize);
      assert(pfile->bufsize <= _maxMpiCount);
      MPI_Recv(pfile->buf, (int)pfile->bufsize, MPI_BYTE, pfile->io_id, pfile->msgTag, pfile->comm, MPI_STATUS_IGNORE);
   }

   int dummy;
   if (pfile->beanCounter == pfile->id)
   {
      int nMsgs = MIN(20, pfile->size/2);
      assert(nMsgs>0 || pfile->size == 1);
      for (int ii=1; ii<pfile->size; ++ii)
      {
         MPI_Recv(&dummy, 1, MPI_INT, MPI_ANY_SOURCE, pfile->doneTag, pfile->comm, MPI_STATUS_IGNORE);
         if (ii%(pfile->size/nMsgs) == 0)
         {
            char foo[1024];
            sprintf(foo, "Popen(%s) %2.0f%% complete (%d).",
                  pfile->name, 100.0*ii/(1.0*pfile->size), ii);
            timestamp_anyTask(foo);
         }
      }
   }
   else
   {
      MPI_Send(&dummy, 1, MPI_INT, pfile->beanCounter, pfile->doneTag, pfile->comm);
   }

   /* Gather statistics on opened files... */ 
   {
      struct { long long int zerolencount,totalsize,headerlen; } local = { 0 , 0 , 0 },global;

      /* Initialize local data */ 
      {
         for (int fid=0; fid<pfile->nfiles; ++fid)
            if (groupForFile(fid, pfile) == pfile->groupToHandle) {
               if(fid == 0) local.headerlen = pfile->headerLength;
               local.totalsize += pfile->nBytesInFile[fid];
               if(pfile->nBytesInFile[fid] == 0)
                  local.zerolencount++;
            }
      }
      assert(sizeof(local) == 3*sizeof(local.totalsize));
      MPI_Allreduce(&local,&global,3,MPI_LONG_LONG,MPI_SUM,pfile->comm);
      if(pfile->id == 0) 
      {
         long long int esz = 
            ((long long int) pfile->recordLength) *
            ((long long int) pfile->numberRecords);
         printf("PIO file loaded/read:\n"
               "    number of files       = %15lld\n"
               "    number of empty files = %15lld\n"
               "    header size           = %15lld\n"
               "    record length         = %15lld\n"
               "    nrecords              = %15lld\n"
               "    expected total size   = %15lld  (%.5e)\n"
               "    total data size       = %15lld  (%.5e)\n",
               (long long int) pfile->nfiles,
               global.zerolencount,
               global.headerlen,
               (long long int) pfile->recordLength,
               (long long int) pfile->numberRecords,
               esz,(double) esz,
               global.totalsize,(double) global.totalsize
               );
      }
      MPI_Barrier(pfile->comm);
   }

}

static pio_long64 statPfile(PFILE *pfile,pio_long64* datasize,pio_long64 *startOfFile)
{
   char *offset;
   char *filename = pfile->name;
   pio_long64 size=0;
   struct stat statbuf;
   int rc;
   pio_long64 nblocks=0;
   switch (pfile->type)
   {
      case PIO:
      case SIMPLE:
         rc = stat(pfile->masterName, &statbuf);
         assert(rc == 0);
         size = statbuf.st_size;
         pfile->next = 0; 
         pfile->start = 0; 
         *startOfFile=ftello(pfile->file); 
          assert(pfile->start == *startOfFile); 
         break;
      case TAR:
         {
            char block[BLOCKSIZE]; 
            TARHEADER *tarHeader = (TARHEADER *)&block[0];
            int nn = fread(block,BLOCKSIZE,1,pfile->file);
            size =0; 
            size = strtoll(tarHeader->size,NULL,8);
            int nBlocks = size/BLOCKSIZE ;
            if (size%BLOCKSIZE != 0)  nBlocks++;
            if (nBlocks == 0) break;
            pfile->next = pfile->start + (nBlocks+1)*BLOCKSIZE; 
            *startOfFile=ftello(pfile->file); 
            assert(pfile->start+BLOCKSIZE == *startOfFile); 
            break;
         }
      default:
        break;
   }
   *datasize = size; 
   return size; 
}

/** Creates a new string containing the header of the specified file.
 *  The caller is responsible to free the newly created string.  If the
 *  specified filename ends in "#" then file filename+"000000" will be
 *  opened to read the header.
 *
 *  Note that the rawHeaderLength returned through the argument list is
 *  not necessarily the same as the length of the returned char*.  This
 *  is because the char* will have had comments stripped. We need to
 *  know the actual raw header length because it will be used as an
 *  offset to find the start of the data in the #000000 file. */

char* readPheader(PFILE *pfile, int* rawHeaderLength,int *strippedHeaderLength)
{

   OBJECTFILE ofile; 
   ofile.file = pfile->file; 
   ofile.name = pfile->masterName; 
   off_t start = ftello(pfile->file); 
   char *header = strdup(object_read(ofile)); 
   *strippedHeaderLength = strlen(header) + 1;
   for (int ii=0; ii<*strippedHeaderLength-1; ++ii) if (header[ii] == '\n') header[ii] = ' ';
   for (int ii=*strippedHeaderLength-2; ii > 0; --ii) {if (header[ii] == '}') break; header[ii]= ' '; }
   char *string = strstr(header,";  ;"); 
   if (string != NULL) string[3] = ' ' ;  // hack for a bad header in position.tar files. 
   char line[1025];
   line[0]='\0'; 
   fgets(line, 1025, pfile->file);
   while (!feof(pfile->file)) 
   {
      int n = strlen(line); 
      if (line[n-1] == '\n') 
      {
         char c = fgetc(pfile->file);
         if (c=='\n') break; 
      }
      fgets(line, 1025, pfile->file);
   } 
   off_t end = ftello(pfile->file); 
   *rawHeaderLength=(end-start); 

   return header;
}


int64_t fillBuffer(char* buf, int tid, const PFILE* file)
{
   unsigned nReadFiles = nAssignedFiles(tid, file);
   int64_t buf_len = 0;
   int64_t nn=0; 
   for (unsigned ifile=0; ifile<nReadFiles; ++ifile)
   {
      FILE* fhandle = fileHandle(tid, ifile, file);
      int64_t read_len;
      off_t read_start = readStart(ifile, tid, &read_len, file);
      assert(read_len >= 0);
      fseeko(fhandle, read_start, SEEK_SET);
      if(read_len > 0) nn = fread(buf+buf_len, read_len, 1, fhandle);
      buf_len += read_len;
    }
   return buf_len;
}

/** Establishes file->io_id for the current task, taking into account
 *  BG/L I/O nodes.  Works for read and write.  Sets file->groupToHandle
 *  to -1 for tasks not assigned to do I/O and to a group number for any
 *  task assigned to do I/O.  Finally, sets file->beanCounter.
 *
 *  If there are fewer I/O tasks than groups then we allow any task to do I/O.
 *
 *  Returns the number of tasks that will perform I/O.
 */
static int setupIoIds(PFILE* file)
{
   file->groupToHandle = -1;
   file->beanCounter = -1;

   int myGroup = groupId(file->id, file);
   assert(myGroup < file->ngroup);
   int nIoTasks = hi_nIoTasks(file->comm);
   const int* ioTaskList = hi_ioTaskList(file->comm);

   // handle special case.
   if (nIoTasks < file->ngroup)
   {
      file->io_id = groupBegin(myGroup, file);
      if (file->id == file->io_id)
         file->groupToHandle=myGroup;

      for (int iGroup=0; iGroup<file->ngroup; ++iGroup) 
      {
         int gBegin = groupBegin(iGroup, file);
         int gEnd = groupEnd(iGroup, file);
         if (gBegin+1 != gEnd)
         {
            file->beanCounter = gBegin+1;
            break;
         }
      }
      if (file->beanCounter == -1)
         file->beanCounter = 0;
      return file->ngroup;
   }

   int* handlerForGroup = ddcMalloc(file->ngroup * sizeof(int));
   for (int ii=0; ii<file->ngroup; ++ii)
      handlerForGroup[ii] = -1;
   int* ioTaskAssignment = ddcMalloc(nIoTasks * sizeof(int));
   for (int ii=0; ii<nIoTasks; ++ii)
      ioTaskAssignment[ii] = -1;

   // Assigns first ioTask in group to handle that group.
   for (int ii=0; ii<nIoTasks; ++ii)
   {
      unsigned group = groupId(ioTaskList[ii], file);
      if (handlerForGroup[group] == -1)
      {
         handlerForGroup[group] = ioTaskList[ii];
         ioTaskAssignment[ii] = group;
      }
   }

   // For groups with no ioTasks in group assigns "nearest" uncommitted
   // ioTask to handle group.  (Nearest means closest task number to
   // groupBegin.  Does not take network mapping into account.)
   for (int ii=0; ii<file->ngroup; ++ii)
   {
      if (handlerForGroup[ii] >= 0)
         continue;
      unsigned maxDist = file->size+1;
      int bestjj = -1;
      for (int jj=0; jj<nIoTasks; ++jj)
      {
         if (ioTaskAssignment[jj] >= 0)
            continue;
         unsigned dist = abs(groupBegin(ii, file) - ioTaskList[jj]);
         if (dist < maxDist)
         {
            maxDist = dist;
            bestjj = jj;
         }
      }
      assert(bestjj >= 0 );
      handlerForGroup[ii] = ioTaskList[bestjj];
      ioTaskAssignment[bestjj] = ii;
   }

   for (int ii=0; ii<file->ngroup; ++ii)
   {
      if (file->id == handlerForGroup[ii])
         file->groupToHandle = ii;
   }

   int iTask = 0;
   while (file->beanCounter < 0 && iTask < file->size)
   {
      int busyTask = 0;
      for (int jj=0; jj<nIoTasks; ++jj)
      {
         if (ioTaskList[jj] == iTask && ioTaskAssignment[jj] >= 0)
         {
            busyTask = 1;
            break;
         }
      }
      if (busyTask == 0)
         file->beanCounter = iTask;
      ++iTask;
   }
   if (file->beanCounter < 0)
      file->beanCounter = 0;

   assert(handlerForGroup[myGroup] >= 0);
   file->io_id = handlerForGroup[myGroup];
   ddcFree(handlerForGroup);
   ddcFree(ioTaskAssignment);
   return file->ngroup;
}

/** This formula good for read and write. */
static int groupId(int tid, const PFILE* file)
{
   return MIN(tid/file->sizegroup, file->ngroup -1);
}

static int groupBegin(int gid, const PFILE* file)
{
   return MIN(file->size, gid * file->sizegroup);
}

static int groupEnd(int gid, const PFILE* file)
{
   if (gid == file->ngroup-1)
      return file->size;
   return MIN(file->size, (gid+1) * file->sizegroup);
}

static void formName(char* filename, char* basename, int fid)
{

   if (basename[strlen(basename)-1] == '#' ) 
   {
      sprintf(filename, "%s%6.6d", basename, fid);
      return; 
   }
   else 
   { 
      if (fid == 0) 
      {
         strcpy(filename,basename); 
         return; 
      }
      printf("Unsupported filename style in COLLECTION object.\n"
            "Please use a filename that ends with the # character.\n"
            "filename=%s if more than one file\n", basename);
      MPI_Abort(MPI_COMM_WORLD, 1);
   }

}

/** Computes the buffer size that will be needed for reading files by
 *  finding the size of the largest chunk we will need to read.
 */
static int64_t computeMaxReadBuf(const PFILE* file)
{
   int64_t maxBuf = 0;
   int gBegin = groupBegin(file->groupToHandle, file);
   int gEnd = groupEnd(file->groupToHandle, file);
   for (int tid=gBegin; tid<gEnd; ++tid)
   {
      unsigned nReadFiles = nAssignedFiles(tid, file);
      int64_t bufLen = 0;
      for (unsigned ifile=0; ifile<nReadFiles; ++ifile)
      {
         int64_t readLen;
         readStart(ifile, tid, &readLen, file);
         bufLen += readLen;
      }
      maxBuf = MAX(maxBuf, bufLen);
   }
   return maxBuf;
}

static int groupForFile(int iFile, const PFILE* file)
{
   return iFile%file->ngroup;
}

static int nAssignedFiles(int tid, const PFILE* file)
{
   int nfiles = file->nfiles;
   int ngroups = file->ngroup;

   if (nfiles <= ngroups)
      return 1;

   int gid = groupId(tid, file);
   if (nfiles%ngroups > gid)
      return nfiles/ngroups +1;
   return nfiles/ngroups;
}

static FILE* fileHandle(int tid, int iFile, const PFILE* file)
{
   int gid = groupId(tid, file);
   FILE* f = file->readFile[gid + iFile*file->ngroup];
   assert(f != NULL);
   return f;
}

static off_t readStart(int iFile, int tid, int64_t* readLen, const PFILE* file)
{
   int gid = groupId(tid, file);
   int fid = gid + iFile*file->ngroup;
   pio_long64 nBytes = file->nBytesInFile[fid];
   int groupSize = groupEnd(gid, file) - groupBegin(gid, file);

   size_t size = nBytes/ ( (pio_long64)groupSize );
   off_t start = (off_t) (tid - groupBegin(gid, file)) * (off_t) size+ (off_t)file->startOfFile[fid];
   if (gid == 0 && fid == 0)
      start += (off_t) file->headerLength;

   // compute different size for last task of group
   if (groupEnd(gid, file) - tid == 1)
      size = nBytes - size*(groupSize-1);

   *readLen = size;
   return start;
}

/** Perform tests on pio internal structures to ensure they satisfy
 * requirements.  Expect pio functions to break and/or misbehave if any
 * of these requirements are relaxed. */
static void internalSelfTest(const PFILE* file)
{
   unsigned status = 0;
   int myGroup = groupId(file->id, file);
   int count = 0;

   // Test 1:  nGroups > 0
   if (file->ngroup <= 0)
      status |= 1;

   // Test 2:  groupId function returns value in range
   if (myGroup >= file->ngroup)
      status |= 2;

   // Test 3:  groupId function agrees with groupBegin, groupEnd
   if (file->id < groupBegin(myGroup, file) ||
         file->id >= groupEnd(myGroup, file) )
      status |= 4;

   // Test 4:  Groups contain tasks in contiguous, consecutive order.
   for (int ii=0; ii<file->ngroup; ++ii)
   {
      int gEnd = groupEnd(ii, file);
      for (int jj=groupBegin(ii, file); jj<gEnd; ++jj)
      {
         if (jj != count)
            status |= 8;
         ++count;
      }
   }

   // Test 5:  All tasks in a group
   if (count != file->size)
      status |= 16;

   if (status != 0)
   {
      printf("ERROR: pio failed internal self test.  status = %d\n", status);
      MPI_Abort(file->comm, 10);
   }
}

/** Notes:
 *
 *  PFILE::recordLength <= 0 -> not a fixed recordLength file.
 *
 *  OR, we could pass the header object through to the record
 *  interpreter function so that it can extract whatever parameters it
 *  needs.
 */

/* Local Variables: */
/* tab-width: 3 */
/* End: */
