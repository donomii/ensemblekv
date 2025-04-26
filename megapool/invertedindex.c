/** Inverted indexes
*
*Create and manage high speed database tables.
*Store and retrieve numbers and strings in an efficient and fast format.
*Transparently access terabytes of data.
*
*II is aimed towards Write Once, Read Many data (WORM data).  It is perfect for
*storing map data, but has also been used for time series data, and text indexing.
*
*II does not provide any nice features like SQL syntax or type checking.  It simply
*takes chunks of bytes and stores them, then hands them back to you later.
*
*It fully supports Windows, MacOSX and Linux.
*/

/* Compile notes:
*
* Compile with -DII_WINDOWS for Microsoft Windows.  No options should be needed for *nix systems.
*
* Compile with -DII_DEBUG for vast amounts of debugging information.
*
* I recommend compiling with all optimisations switched on.  i.e. -O3 for gcc
*
*/

#ifndef II_INCLUDED
#define II_INCLUDED


#ifndef SPLINT
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdint.h>
#include <errno.h>
#endif

#ifdef II_DEBUG
int ii_debug=1;
#else
int ii_debug=0;
#endif

/** II works fine with int32_t and uint32_t, with the obvious warning that you won't be able to make files greater than 2Gb.
typedef int32_t ii_int;
typedef uint32_t ii_uint;
*/

typedef int64_t ii_int;
typedef uint64_t ii_uint;


void  assume(int truth, char * message) {
	if (truth==0) {
		printf("Assumption failed: %s\n", message);
		abort();
	}
}

//Debug printf
void ii_d(const char *fmt, ...)
{
	if (ii_debug>0) {
		va_list args;
		int wrote;
		va_start(args, fmt);
		//vfprintf(stderr, fmt, args);
		printf("[ii] ");
		wrote = vfprintf(stdout, fmt, args);
		if (wrote<0) {
			printf("Failure writing debug message!\n");
		}
		va_end(args);
	}
}


ii_int c_mmap=0, c_extend=0, c_addrow=0, c_addstring=0, c_delrow=0;
#ifdef ENT_WINDOWS
#define II_WINDOWS 1
#endif

#ifndef SPLINT
#ifdef II_WINDOWS
#include <windows.h>
#include <shlobj.h>
#else
#include <sys/mman.h>
#endif
#endif //SPLINT

#define II_MAX_FILENAME_LENGTH 4096
#define II_MAX_PATH_LENGTH 4096

#ifdef MACOSX
#include "CoreFoundation/CFBundle.h"

CFBundleRef mainBundle;

#endif //MACOSX


void ii_free(void * ptr){
	free(ptr);
}

#define ii_calloc(elements, size) \
calloc(elements, size)
#define ii_realloc(ptr, size) \
realloc(ptr, size);
#define ii_chk_ptr(ptr, name) \
if ( ptr == NULL || (intptr_t )ptr < 1024 ) \
{ \
	printf ("I got a bad pointer(%p) for data \"%s\" when I shouldn't have.\n", ptr, name); \
	printf ("Bad coder error in %s at %s, line %i in process %d\n", __FILE__, __FUNCTION__, __LINE__, getpid()); \
	abort(); \
}

#define ii_chk_ptr_warn(ptr, name) \
if ( ptr == NULL || (intptr_t )ptr < 1024 ) \
{ \
	printf ("I got a bad pointer(%p) for data \"%s\" when I shouldn't have.\n", ptr, name); \
	printf ("Bad coder error in %s at %s, line %i in process %d\n", __FILE__, __FUNCTION__, __LINE__, getpid()); \
}

static void ii_chkerr () {
	if ( errno>0 ) {
		#if II_DEBUG
		printf ("An error occurred: %s\n", strerror(errno));
		#endif
	}
}



#ifdef II_WINDOWS
void
ii_close_file(HANDLE fd) {
                                CloseHandle(fd);
}
#else
void
ii_close_file(int fd)
{
                                close(fd);
}
#endif




#ifdef II_WINDOWS
HANDLE ii_get_readwrite_fd(char * filename) /*@*/
{

	HANDLE fd = CreateFile(filename, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_WRITE | FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
	if ( fd == INVALID_HANDLE_VALUE )
	{printf("Unable to open ->|%s|<- for writing.  Check file permissions\n", filename);}
	return fd;
}
#else
int
ii_get_readwrite_fd( char *filename) /*@*/
{
	int fd = open( filename, O_RDWR | O_CREAT , S_IRUSR | S_IWUSR );
	if ( fd < 0 )
	{ printf("An error occured opening file: ->|%s|<- for writing.\n", filename);}
	return fd;
}
#endif

#ifdef II_WINDOWS
ii_int random()
{return 50;}   // 50 is a random number
#endif


/** Attempts to get a read only file handle for the given file.
*
*\param filename	A file name, absolute or relative
*/
#ifdef II_WINDOWS
HANDLE
ii_get_readonly_fd(const char * filename) /*@*/
{
	HANDLE ret=INVALID_HANDLE_VALUE ;
	ii_int i = 0;
	ii_chk_ptr(filename, "File name argument to function");
	ii_d("Getting filehandle for file '%s' at %s, %d\n", filename, __FILE__, __LINE__);

	while ( ++i < 3 ) {
		ret = CreateFile(filename, GENERIC_READ, FILE_SHARE_WRITE | FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
		if ( ret == INVALID_HANDLE_VALUE ) {
			printf("Unable to get handle for ->|%s|<- in %s, %d.  Retrying.\n", filename, __FILE__, __LINE__);
			if ( i < 2 ) {
				//FIXME sleep briefly
			} else {
				printf("Unable to get handle for ->|%s|<- in %s, %d.  Check file permissions\n", filename, __FILE__, __LINE__);
				//MessageBox(NULL, "Unable to get handle for returning a handle.  Check file permissions\n", filename, MB_OK | MB_ICONINFORMATION );
				return 0;
			}
		} else
		{ return ret;}
	}
	return ret;
}

#else

int
ii_get_readonly_fd( char *filename) /*@*/
{
	int ret = -69;
	ii_d("Getting filehandle for file '%s'\n", filename);
	ret = open(filename, O_RDONLY);
	/* These paths allow the program to work correctly from
	* within the source tree */
	if ( ret < 0 )
	{
		ii_d("Unable to get file handle for %s\n", filename);
		/*exit(EXIT_FAILURE);*/
	}

	ii_d("Got file descriptor %d for file: ->|%s|<-\n", ret, filename);
	return ret;
}

#endif

#ifdef II_WINDOWS
HANDLE
ii_get_fd(const char * filename, int rw) /*@*/
{
	if (rw) {
		return ii_get_readwrite_fd(filename);
	} else {
		return ii_get_readonly_fd(filename);
	}
}
#else
int
ii_get_fd( char *filename, int rw) /*@*/
{
	if (rw) {
		return ii_get_readwrite_fd(filename);
	} else {
		return ii_get_readonly_fd(filename);
	}
}
#endif


/** Get size of file.
*
*\param filename	A file name, absolute or relative
*/
#ifdef II_WINDOWS
ii_int ii_get_file_length(const char *filename)
{
	HANDLE fd;
	LARGE_INTEGER size;
	ii_int retsize;
	BOOL ok;
	fd = ii_get_readonly_fd(filename);
	ok = GetFileSizeEx(fd, &size);
	if (ok==0) {
		printf("Error retrieving filesize, aborting\n");
		return 0;
		abort();
		exit(1);
	}
	retsize = size.QuadPart;
	//printf("file length is  %lld \n", size.QuadPart);
	CloseHandle(fd);
	//printf("Length of %s is %d(%d) bytes\n", filename, retsize,size.QuadPart);
	return retsize;
}
#else

size_t ii_get_file_length( char *filename)
{
	int ret = -69, fd = -69;
	size_t size = (size_t ) 0;
	struct stat file_stat;
	ii_d("Fetching filessize for file '%s'\n", filename);
	ii_chk_ptr(filename, "filename");
	fd = ii_get_readonly_fd(filename);
	ret = fstat(fd, &file_stat);
	(void ) close(fd);
	if ( ret > -1 )
	{size = (size_t ) file_stat.st_size;}
	else
	{return 0;printf("Unable to get file size for ->|%s|<-, aborting\n", filename); abort();}
	ii_d("Got file size of %zu for file %s\n", size, filename);
	return size;
}

#endif





/** The control struct for a table */
typedef struct
{
	ii_int front_bumper; /**< Front guard value */
	ii_int last_index; /**< The last tag that has data we consider valid */
	ii_int max_index;  /**< The last 'empty' tag that we can access before we run out of allocated memory and segfault */
	ii_int row_width; /**< The width of each row, in bytes */
	int type;	   /** MMapped=1 or malloced=0 */
	int synced;  /**< The start of the data array */
	int sorted;  /**< Is the table sorted? */
	char file_name[II_MAX_FILENAME_LENGTH]; /**< The full name of the map description file */
	char string_file_name[II_MAX_FILENAME_LENGTH]; /**< The full name of the string data file */
	void * data; /**< Pointer to the data segment */
	ii_int data_bytes; /**< The length of data section, empty and used components */
	void * string_bank; /**< Pointer to the string data segment */
	ii_int string_bytes; /**< The length of string data section, empty and used components */
	ii_int string_used_bytes; /**< The length of string data section, that is actually used to store data */
	char identifier[20]; /**< Safety check for table */
	ii_int rear_bumper; /**< Rear guard value */
} ii_control;


/** Check ii table for corruption.
*
*\param table	An ii table
*/
void check_table(ii_control * table) {
	if (strcmp("II TABLE", table->identifier)!=0) {
		const char *s = NULL;
		printf("Not a table, identifier is %s!\n", table->identifier);
		printf( "%c\n", s[0] );
		abort();
	}
	if (table->front_bumper != ~table ->rear_bumper) {
		printf("Table corrupt or wrong type!\n");
		abort();
	}
}

/** Given a table, and an index , return a pointer to the row.
*
* Returns a pointer to the row.  Altering the row directly will alter the database.
*
*\param table an ii_control struct
*\param index The array index of the row you want
*/
void * ii_fetch_row (ii_control * table, ii_int index) {
	void * ret=NULL;
	check_table(table);
	ret = table->data + (index * table->row_width);
	//printf("row %p = tabledata %p + index %llu * width %llu\n", ret, table->data, index, table->row_width);
	//ii_d("Fetchptr: table: %p + index:  %lld  * bytewidth: %d = %p\n", table, index, byte_width, ret);
	return ret;
}



#ifdef II_WINDOWS
void * ii_mmap_rw( ii_int length, HANDLE file_descriptor, int rw, int cow, void* address );
#else
void * ii_mmap_rw( size_t length, int file_descriptor, int rw, int cow, void* address );
#endif



/** Cross-platform unmap
*
* Unmaps a previously mmapped chunk of memory.  Bad things happen if it has already been unmapped
*
*\param ptr    The start of the memory to be unmapped
*\param length How much memory needs to be unmapped (only needed for Unix)
*/
int
ii_munmap( void * ptr, ssize_t length)
{
	int ret;
	#ifndef II_WINDOWS
	ret = munmap(ptr, length);
	#else
	ret = UnmapViewOfFile(ptr);
	#endif
	return ret;
}





/** Shrink file on disk.
*
* Shrink filename to length.
*
* The filename must include a complete pathname (preferably an absolute pathname), and the length is the desired
* length for the file, in bytes.
*
* The file must already exist, and be writable.
*
*\param filename 	The complete path and filename
*\param length		The desired length for the file
*/
void ii_truncate(char * filename, ii_int length) {
	#ifdef II_WINDOWS
	HANDLE fd;
	#else
	int fd;
	#endif


	#ifdef II_WINDOWS
	fd = CreateFile(filename, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_WRITE | FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
	if ( fd == INVALID_HANDLE_VALUE )
	{printf("Unable to open ->|%s|<- for resizing.  Check file permissions\n", filename);}
	LARGE_INTEGER tmp;
	tmp.QuadPart = 0;
	SetFilePointerEx( fd, tmp, 0, FILE_END);
	#else
	fd = open(filename, O_RDWR | O_CREAT | O_APPEND, S_IRWXU);
	if ( ! (fd>0) )
	{
		printf("Open failed for file ->|%s|<-\n", filename);
		abort();
	}
	#endif

	#ifdef II_WINDOWS
	tmp.QuadPart = length;
	SetFilePointerEx(fd, tmp, 0, FILE_BEGIN); SetEndOfFile(fd);
	ii_d("Resized %s to  %lld \n", filename, length);
	#else
	ftruncate(fd, length);
	#endif


	#ifdef II_WINDOWS
	CloseHandle(fd);
	#else
	close(fd);
	#endif
}


/** Resize a file (and create it first, if needed), and then mmap it.
*
* The current contents of the file are not affected.  If file exists and is smaller than requested, it will be extended with zeros.  If the file doesn't exist it will be created and filled with zeros.
*
* If the file is already larger than the requested size, nothing will be done, and a warning will be printed out.
*
* The filename must include a complete pathname (preferably an absolute pathname), and the length is the desired
* length for the file, in bytes.
*
* The contents of the file will be mapped to address.  If any part of the target address is already allocated, then this function will fail.
*
*\param filename 	The complete path and filename
*\param length		The desired length for the file
*\param address		The destination address, in memory, for this map
*/
void *
ii_size_and_map_file_to_address (char * filename, ii_int requested_length, void* address )
{
	ii_int tofill = 0, file_length=0;
	char * mapped ;

	#ifdef II_WINDOWS
	HANDLE fd;
	#else
	int fd;
	#endif


	ii_d("Resizing file ->|%s|<- to length  %lld \n", filename, requested_length);

	if ( strlen(filename) < 2 )
	{
		printf("Warning: Attempt to open a one-letter or null map file called '%s'.  Aborting immediately\n", filename);
		abort(); }

		file_length = ii_get_file_length(filename);
		/* Shrinking files is not supported */
		if ( requested_length <= file_length )
		{
			ii_d("Attempt to shrink file %s from  %lld  to  %lld  not allowed\n", filename, file_length, requested_length);
		}

		else
		{
			ii_d("Attempt to expand file %s from  %lld  to  %lld \n", filename, file_length, requested_length);


			tofill = requested_length - file_length;
			if (tofill>0) {
				ii_truncate(filename, requested_length);
			}


		}
		file_length = ii_get_file_length(filename);
		if (file_length==0) {return NULL;}
		#ifdef II_WINDOWS
		fd = CreateFile(filename, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_WRITE | FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
		if ( fd == INVALID_HANDLE_VALUE )
		{printf("Unable to get handle for ->|%s|<- for resize and memory mapping.  Check file permissions\n", filename);}
		#else
		fd = open(filename, O_RDWR);
		if (fd < 0)
		{
			ii_chkerr();
			abort();
		}
		#endif

		ii_d("Mapping %s of length  %lld , into memory\n", filename, file_length);
		mapped = (char *) ii_mmap_rw(file_length, fd, 1, 0, address);  //Always request rw, because we can't remap the size otherwise
		ii_d("Mapping %s into memory at %p\n", filename, mapped);
		#ifndef II_WINDOWS
		if ( mapped == MAP_FAILED )
		{
			printf("Failed to map file ->|%s|<- in ii_size_and_map_file() in invertedindex.c\n", filename);
			ii_chkerr();
			abort();
			exit(1);
		}
		#endif
		ii_chk_ptr_warn(mapped, "Freshly mapped file");
		if (mapped==NULL){return NULL;}
		#ifdef II_WINDOWS
		CloseHandle(fd);
		#else
		close(fd);
		#endif
		#ifdef II_WINDOWS
		#ifdef II_DEBUG
			MEMORY_BASIC_INFORMATION lpBuffer;
			SIZE_T x = VirtualQuery( mapped, &lpBuffer, file_length);
			if (x == 0) { printf("Last error %lld\n", GetLastError());}
			printf("Vquery says mapped size is (lpBuffer) %lld  (system file length) %lld \n",lpBuffer.RegionSize, file_length);
		#endif
		#endif


	if (tofill>0){ //If the file was extended
		#ifdef II_DEBUG
		printf("Mmapped file was extended by  %lld  bytes\n", tofill);
		#endif
		char * startzeros = mapped + (file_length-tofill)+1;
		char * endzeros = mapped + file_length;
		char * i;
		#ifdef II_DEBUG
		printf("Zeroing file %s from %p to %p\n", filename, startzeros, endzeros);
		#endif
		for (i=startzeros;i<endzeros;i++) {
			*i = 0;
		}
	}
	ii_d("Size and map complete\n");
	return mapped;
}


/** Resize a file (and create it first, if needed), and then mmap it.
*
* The current contents of the file are not affected.  If file exists and is smaller than requested, it will be extended with zeros.  If the file doesn't exist it will be created and filled with zeros.
*
* If the file on disk is already larger than the requested size, nothing will be done, and a warning will be printed out.
*
* The filename must include a complete pathname (preferably an absolute pathname), and the length is the desired
* length for the file, in bytes.
*
*\param filename 	The complete path and filename
*\param length		The desired length for the file
*/
void *
ii_size_and_map_file (char * filename, ii_int requested_length )
{
        return ii_size_and_map_file_to_address(filename, requested_length, 0);
}
#ifdef II_WINDOWS
void *
ii_mmap_rw( ii_int length, HANDLE file_descriptor, int rw, int cow, void* address )
{
	c_mmap++;
	HANDLE hMap;
	LPVOID lpMsgBuf;
	void * ptr;
	
	ii_d("ii_mmap_rw: Attempting mmap of  %lld  bytes from handle %p to %p, %s, %s.\n", 
	length, file_descriptor, address, rw?"read/write":"read-only", cow?"copy-on-write":"no-copy-on-write");

	
	if ( rw ) {
		
		ii_d("ii_mmap_rw: create file mapping rw\n");
		
		hMap = CreateFileMapping( file_descriptor, NULL, PAGE_READWRITE, 0,0, NULL);
	} else {
		
		ii_d("ii_mmap_rw: create file mapping ro\n");
		

		hMap = CreateFileMapping( file_descriptor, NULL, PAGE_READONLY, 0,0, NULL);
	}
	 if ( hMap == INVALID_HANDLE_VALUE) { //} && GetLastError() == ERROR_ALREADY_EXISTS )
		printf("Failed to get filehandle! check permissions\n");
		CloseHandle(hMap);
		hMap = NULL;
		return NULL;
	}

	if ( rw ) {
		#if II_DEBUG
		printf("ii_mmap_rw: mapview rw\n");
		#endif
		if (address>0) {
			printf("ii_mmap_rw: Mapping file to %p\n", address);
			ptr = MapViewOfFileEx (hMap, FILE_MAP_ALL_ACCESS, 0, 0, 0, address);
		} else {
			ptr = MapViewOfFile (hMap, FILE_MAP_ALL_ACCESS, 0, 0, 0);
		}
	} else {
		
		ii_d("ii_mmap_rw: mapview ro\n");
		
		int flags =0;
		if ( cow ) {
			ii_d("ii_mmap_rw: mapview with cow\n");
			flags= FILE_MAP_COPY;
		} else {
			flags = FILE_MAP_READ;
		}
		if (address>0) {
			ii_d("ii_mmap_rw: Forcing map to address %p\n", address);
			ptr = MapViewOfFileEx (hMap, flags, 0, 0, 0, address);
		} else {
			ptr = MapViewOfFile (hMap, flags, 0, 0, 0);
		}
		
	}
	if ( ptr == NULL )
	{
		printf("ii_mmap_rw: Cannot get a mmap view at %s, %d (error %lld)\n", __FILE__, __LINE__, GetLastError());
		if (! FormatMessage(
			FORMAT_MESSAGE_ALLOCATE_BUFFER |
			FORMAT_MESSAGE_FROM_SYSTEM |
			FORMAT_MESSAGE_IGNORE_INSERTS ,
			NULL,
			GetLastError(),
			MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
			(LPSTR) &lpMsgBuf,
			0,
			NULL)) {
				/*Do stuff*/


			}
			MessageBox(NULL, (LPCTSTR)lpMsgBuf, "Error", MB_OK | MB_ICONINFORMATION );
			LocalFree ( lpMsgBuf );
	}
	CloseHandle(hMap);
	ii_chk_ptr_warn(ptr, "Windows MMapped file");
	ii_d("ii_mmap_rw: Successfully mmapped file to address %llu(%p)\n", ptr, ptr);
	
	return ptr;
}

#else

void *
ii_mmap_rw( size_t length, int file_descriptor, int rw, int cow, void* address )
{
	void * ptr;
	int flags = MAP_SHARED;
	c_mmap++;

	if (length == 0) {
		#ifdef II_DEBUG
		printf("mmap will probably fail - most kernels will not create a map with length 0\n");
		printf("Changing length to 1 so mmap will succeed\n");
		#endif
		length = 1;
	}
    
    //flags = flags | MAP_FIXED;
	ii_d("Mmapping file of %zu bytes, rw mode: %d from file descriptor: %d\n", length, rw, file_descriptor);
	ptr = mmap(NULL, length, PROT_READ | (rw ? PROT_WRITE : 0), flags, file_descriptor, 0);
	if ( ptr == MAP_FAILED )
	{
		printf("mmap failed in %s, at line %d in file %s (errno %d)\n", __FUNCTION__, __LINE__, __FILE__, errno);
		abort();
		exit(EXIT_FAILURE);
	}

	ii_d("mmap complete!\n");
	return ptr;
}

#endif


/** Extend the files holding a mmapped handle.
*
* Tries to extend the files controlled by handle.  It unmaps and remaps the files,
* using the filenames stored in the handle.
*
* Note that the mapped address will probably change.
*
*\param handle	The mapped area to be extended
*/
int
ii_extend_mmapped_table(ii_control *handle, ii_int extension_rows)
{
	ii_int old = -1;
	check_table(handle);
	old = handle->data_bytes;
	c_extend++;
	ii_munmap(handle->data, handle->data_bytes);

	handle->data_bytes = handle->data_bytes + extension_rows*handle->row_width;
	ii_d("Extend from  %lld  to  %lld !\n", old, handle->data_bytes);
	handle->data = ii_size_and_map_file(handle->file_name, handle->data_bytes);

	handle->max_index += extension_rows;
	return 0;
}


/** Add a string to the stringbank.
*
* Works on both mmapped and malloced tables.
* It will attempt to extend the on-disc files
* and remap them if a mmapped table runs out of room.
*
* It will not automatically synchronise the mmapped file.
*
*\param handle control struct for a table
*\param row  row to be added.  Will be copied.
*\param byte_width width of the row, in bytes
*/

ii_int
ii_add_string(ii_control *handle, void * row, ii_int copylen)
{
	void *ret_ptr = NULL;
	char * target = NULL;
	ii_int oldpos = -1;
	c_addstring++;
	ii_d("Starting to add string of length  %lld \n", copylen);
	ii_chk_ptr(handle, "add_string");
	//Adding a string always requires a file extension
	oldpos = handle->string_used_bytes;
	handle->string_used_bytes = handle->string_used_bytes+copylen;
	if (handle->string_used_bytes>handle->string_bytes) {
		handle->string_bytes = handle->string_bytes+copylen*10000;
		ii_d("Resizing table!\n");
		if ( handle->type == 1 )  //Memory mapped
		{
			//size and map here
			handle->string_bank = ii_size_and_map_file(handle->string_file_name, handle->string_bytes );

		}
		else
		{
			//Memory only table
			ii_d("Reallocating in-memory table\n");

			ret_ptr = realloc(handle->string_bank, (handle->string_bytes));
			if( ret_ptr == NULL ) { printf("Failed to allocate %lld bytes for table, dieing\n", handle->string_bytes);abort();}
		}
	}


	target = handle->string_bank + oldpos;
	#ifdef II_DEBUG
	printf("Adding string at index  %lld , string used bytes is  %lld \n", oldpos, handle->string_used_bytes);
	printf("Source %p, target %p, length  %lld \n", row, target, copylen);
	#endif
	memcpy( target, row, copylen);
	ii_d("Add string completed, returning string position  %lld \n", oldpos);
	return oldpos;
}

int ii_bytes_equal(int size, void * aa, void * bb) {
	int res = memcmp(aa, bb, size);
	return !res;
}

/** Inplace swap two data rows
*
*\param a,b    pointers to the data to be swapped
*\param l      length of the data to be swapped, in bytes */
void xorSwap(char *aa, char *bb, ii_int l) {
	ii_int i;
	if (ii_bytes_equal(l, aa, bb)){return;}

	ii_d("Swapping  %lld  bytes between %p and %p\n", l, aa, bb);
	for (i=0;i<l;i++) {
		char * a = aa + i;
		char * b = bb + i;
		*a ^= *b; // a = A XOR B
		*b ^= *a; // b = B XOR ( A XOR B ) = A
		*a ^= *b; // a = ( A XOR B ) XOR A = B
	}

}


/** Delete a row.
*
* Works on both mmapped and malloced tables.
*
* We swap the deleted row
* with the last row, then shrink the active region by one.
*
* It will not automatically synchronise the mmapped file.
*
*\param handle control struct for a table
*\param row  row to be added.  Will be copied.
*\param byte_width width of the row, in bytes
*/

ii_control *
ii_delete_row(ii_control *handle, void * row)
{
	void * lastRow = NULL;
	ii_int byte_width = handle->row_width;
	handle->sorted=0;
	c_delrow++;
	//ii_d("Deleting row %p\n", row);
	ii_chk_ptr(handle, "Table");
	//Swap the deleted row with the last row
	lastRow = ii_fetch_row(handle, handle->last_index);
	ii_d("Swapping %p to end position %p, width  %lld , in table %p\n", row, lastRow, byte_width, handle->data);
	xorSwap(row, lastRow, byte_width);
	//Reduce the used space pointer by one
	handle->last_index--;
	return handle;
}

/** Add a blank row.
*
* Returns a pointer to an empty row for you to add data to.
*
* Allocates space in the table for a new row,
* but does not copy any data into it, and returns a pointer
* to that row.
*
* If you are building the row structure yourself,
* then you should use this function.
*
* If you have the row structure in memory already,
* perhaps from another routine, then you can use
* ii_add_row to allocate a row and copy the data into the table.
*
* Works on both mmapped and malloced tables.
* It will attempt to extend the on-disc files
* and remap them if a mmapped table runs out of room.
*
* It will not automatically synchronise the mmapped file.
*
*\param handle control struct for a table
*/

void *
ii_add_blank_row(ii_control *handle)
{
	void *ret_ptr=NULL;
	void *target = NULL;
#ifndef SPLINT
	check_table(handle);
#endif
	ii_int  byte_width = handle->row_width;
	handle->sorted = 0;
	c_addrow++;
	ii_d("Starting to add row\n");
	ii_chk_ptr(handle, "Table");
	handle->last_index++;
	if ( handle->type == 1 )  //Memory mapped
	{
		if ( !(handle->last_index<handle->max_index) )
		{
			ii_d("mmapped table is full.  I will now attempt to extend the file on disc.  This will fail if there isn't enough free disc space.\n");
			ii_extend_mmapped_table(handle, 1024*1024*10);
		}

	}
	else
	{
		//Memory only table
		if ( handle->last_index >= handle->max_index )
		{
			ii_int memstep = 20;
			ii_d("Reallocating in-memory table\n");
			ret_ptr = realloc(handle->data, (handle->data_bytes+memstep*byte_width));
			if( ret_ptr == NULL ) { printf("Failed to allocate %lld bytes for table, dieing\n", 2*handle->data_bytes+memstep*byte_width);abort();}


			handle->max_index+=memstep;
		}
	}


	//printf("Adding blank row at index %llu\n", handle->last_index);
	target = ii_fetch_row(handle, handle->last_index);
	if (target<handle->data){printf("New row out of bounds!\n"); abort();}
	if (!(target < handle->data + handle->data_bytes)){printf("New row out of bounds!  New row (%p) larger than or equal to end of row data space(%p).\n", target, handle->data + handle->data_bytes);abort();}
	ii_d("Adding row at index  %lld , max index is  %lld \n", handle->last_index, handle->max_index);
	return target;
}

/** Add a row.
*
* Copies data from memory into a new row in the table.
*
* Works on both mmapped and malloced tables.
* It will attempt to extend the on-disc files
* and remap them if a mmapped table runs out of room.
*
* It will not automatically synchronise the mmapped file.
*
*\param handle control struct for a table
*\param row  row to be added.  Will be copied.
*\param byte_width length of the row, in bytes
*/

ii_control *
ii_add_row(ii_control *handle, void * row) {
#ifndef SPLINT
	check_table(handle);
	assume(handle != NULL, "handle != NULL");
#endif
	void * target = ii_add_blank_row(handle);
	memcpy( target, row, handle->row_width);
	ii_d("Add row completed\n");
	return handle;
}


/** Search a subset of a table.
*
* Uses a binary search to find the search term in the subset.
* The subset must be sorted (with ii_sort) before this search will work.
*
* This is the recursive part of the search, so you must also provide the start and end indices of the section you want to search.
*
*\param handle The ii_control table descriptor.
*\param search  A pointer to the search data.  Can be anything, will be passed to your function.
*\param lower   The lower index to search in.  Must be more than 0, as zero is reserved in the table.
*\param upper   The upper index of the search space.  Usually the last row in the array.
*\param compareFunc The user-supplied comparison function.
*\param partial boolean,  If true, return the closest match when the search fails.  When false, return -1 on search failure.
*\param user_data A pointer to any data that you want to access during the search.
*/

long long
ii_sub_search (ii_control* handle, const void * search, ii_int lower, ii_int upper, int (*compareFunc)(ii_control *, const void *, const void *, void *), int partial, void * user_data) {
	ii_int res = -1;
	ii_int h,half;
	check_table(handle);
	assume(lower>0, "lower>0");
	assume(upper<=handle->last_index, "upper<=handel->last_index");

	if (!handle->sorted){printf("Invalid state!  Table has not been sorted, so it cannot be searched!\n"); abort(); exit(1);}
	ii_d("Starting  bsearch on %p, lower  %lld , upper  %lld \n", handle->data, lower,  upper);
	if (upper<1) {
		ii_d("bsearch failed: Leaving bsearch as upper bound < 1\n");
		return -1;
	}
	if (lower>upper) {
		ii_d("bsearch failed: Leaving bsearch as lower bound > upper bound\n");
		return -1;
	}
	if (lower == upper) {
		ii_d("bsearch comparing search and upper  %lld \n", upper);
		res = compareFunc(handle, search, ii_fetch_row(handle, upper), user_data);
		if (res==0) {
			ii_d("bsearch succeeded: Located row:  %lld \n", upper);
			return upper;
		}
		else {
			ii_d("Leaving bsearch as lower bound = upper bound, but search string does not match found string\n");
			if (partial) {
				return upper;
			} else {
				return -1;
			}
		}
	}

	h = (upper-lower)/2;
	half = lower+h;

	res = compareFunc(handle, search, ii_fetch_row(handle, half), user_data);
	if (res==0) { return half;}
	if (res<0) {
		return ii_sub_search(handle, search, lower, half, compareFunc, partial, user_data);
	} else {
		if (h==0) {
			return ii_sub_search(handle, search, half+1, upper, compareFunc, partial, user_data);
		} else {
			return ii_sub_search(handle, search, half, upper, compareFunc, partial, user_data);
		}
	}

}

/** Search a table.
*
* Uses a binary search to find the search term in the table.
* The table must be sorted (with ii_sort) before this search will work.
*
*\param handle The ii_control table descriptor.
*\param search  A pointer to the search data.  Can be anything, will be passed to your function.
*\param compareFunc The user-supplied comparison function.
*\param partial boolean,  If true, return the closest match when the search fails.  When false, return -1 on search failure.
*\param user_data A pointer to any data that you want to access during the search.
*/



long long
ii_search (ii_control* handle, const void * search, int (*compareFunc)(ii_control *, const void *, const void *, void *), int partial, void * user_data) {
	check_table(handle);
	return ii_sub_search(handle, search, 1, handle->last_index, compareFunc, partial, user_data);
}

/**  The recursive sort routine.  Call ii_sort instead.
*
* Sorts the data table using the user provided comparison function.
*
*\param handle The table handle
*\param l The lower index to search from
*\param r The upper index to search to
*\param compareFunc A function that receives pointers to the rows and returns -1,0,1 based on whether a<b,a==b,a>b
*\param user_data A pointer to any data that you want to access during the search.
*
*Sorting is required to do searches on the table.
*/
void ii_sub_sort ( ii_control *handle, ii_int l, ii_int r, int (*compareFunc)(ii_control * handle, void *, void *, void *), void * user_data) {
#ifndef SPLINT
	check_table(handle);
#endif
	void * pivot = NULL;
	ii_int byte_width = handle->row_width;
	//ii_debug && dumpTable(handle);
	if(r==l+1) {
			if(compareFunc(handle, ii_fetch_row(handle, l), ii_fetch_row(handle, r), user_data) < 0) {
				xorSwap(ii_fetch_row(handle, l), ii_fetch_row(handle, r), byte_width);
			}

		return;
	}
	if (r-l>0) {
		ii_int left = l-1;
		ii_int right = -1;
		ii_int pivot_index=r; //(ii_uint)(l) + (r-l)/2 ;
		ii_d("Start: Sorting from  %lld  to  %lld \n", l, r);
		xorSwap(ii_fetch_row(handle, pivot_index), ii_fetch_row(handle, r), byte_width);
		pivot = ii_fetch_row(handle, r);
		ii_d("Chose pivot %p index  %lld \n", pivot, pivot_index);

			//ii_d("Comparing left %p and pivot %p\n", ii_fetch_row(handle, left), pivot);
				//left++;
			//ii_d("3Left:  %lld , Right:  %lld \n", left, right);
			//ii_d("Comparing right %p and pivot %p\n", ii_fetch_row(handle, right), pivot);
			//right--;
			right=left;
			while (++right <r) {
			ii_d("1Left:  %lld , Right:  %lld \n", left, right);
			if (compareFunc(handle, ii_fetch_row(handle, right), pivot, user_data) > 0 ) {
				left++;
				ii_d("4Left:  %lld , Right:  %lld \n", left, right);
				if (right<left) {printf("oops\n");exit(1);}
				//right--;

				ii_d("Want to swap items at  %lld ,  %lld \n", left, right);
				ii_d("Want to swap  %lld  bytes at pointers at %p, %p\n", byte_width, ii_fetch_row(handle, left), ii_fetch_row(handle, right));
				xorSwap(ii_fetch_row(handle, left), ii_fetch_row(handle, right), byte_width);
				//left++;
				//right--;
			}
			}
		xorSwap(ii_fetch_row(handle, left+1), ii_fetch_row(handle, right), byte_width);
		left++;
		ii_d("Recursing from  %lld  to  %lld , and  %lld  to  %lld \n", l, left-1, left+1, r);
		ii_sub_sort(handle, l, left-1, compareFunc, user_data);
		ii_sub_sort(handle, left+1, r, compareFunc, user_data);
		}
	}


/**  Sorts the table using compareFunc.
*
*You must provide compareFunc(handle, *a, *b, *user_data).  compareFunc must take two pointers(to two rows), and return an integer, where:
*
*-1  a < b
*0  a = b
*1  a > b
*
*i.e. whether a sorts before or after b
*
* Sorting is required before you can do searches on a table.
*
*\param handle The table handle
*\param l The lower index to search from
*\param r The upper index to search to
*\param compareFunc A function that receives pointers to the rows and returns -1,0,1 based on whether a<b,a==b,a>b
*\param user_data A pointer to any data that you want to access during the search.
*
*/

void ii_sort ( ii_control *handle, int (*compareFunc)(ii_control * handle, void *, void *, void *), void * user_data) {
	int ooo = 0;
	ii_int i=-1;
	check_table(handle);
	//Usually, if the user calls sort, they want it to run
	//if(handle->sorted) return;
	ii_sub_sort(handle, 1, handle->last_index, compareFunc, user_data);
	//Check that the sort worked, because sometimes it doesn't, apparently
	for (i=1;i<handle->last_index; i++) {
		void * row1 = ii_fetch_row(handle, i);
		void * row2 = ii_fetch_row(handle, i+1);
		int res = compareFunc(handle, row1, row2, user_data);
		if (res>0) {
			ooo=1;
		}
	}
	if(ooo) printf("Some rows are out of order!\n");
	handle->sorted = 1;
}


//BUG: this isn't fast
void ii_fast_zero(int fd, ii_int size) {
       char * temp_buff = NULL;
       ii_int i=0;
       if (size<1) { return; }
       temp_buff = calloc(1, 1);
       for (i=0;i<size;i++) {
	       write(fd, temp_buff, size);
       }
       ii_free(temp_buff);
}

/** Opens a table
*
* Takes the full path name of a database file and mmaps it and builds a
* table.
*
*\param dataname		File path holding the data for this table
*\param descriptor_name	File path holding the metadata for this table
*\param readwrite 		1 - Readwrite, 0 - read only
*\param byte_width		The size of an element in the table.  Usually sizeof(your struct), where struct is a row.
*/

ii_control *
ii_do_open_table( char *dataname, char *descriptor_name, char *string_name,  int readwrite, int byte_width)
{
	ii_control *handle;
	#ifdef II_WINDOWS
	HANDLE DESC;
	#else
	#ifdef MACOSX
	int DATA, DESC, STRINGS;   			//Must ust int or llvm emits code that causes a misaligned access exception
	printf("Using int for file descriptors\n");
	#else
	ii_int DESC;
	#endif
	#endif
	void * ret;

	ii_d("Opening %s, %s, %s\n", dataname, descriptor_name, string_name);

	if (readwrite) {
		handle = (ii_control *)  ii_size_and_map_file(descriptor_name, sizeof(ii_control));
		DESC = ii_get_readwrite_fd(descriptor_name);

	} else {
		DESC = ii_get_readonly_fd(descriptor_name);
		ii_d("Read-only!\n");
		handle = (ii_control *)  ii_calloc(1,sizeof(ii_control));  //No point mmaping this since we need to write to it
	}
	ii_chk_ptr(handle, "ii_control");
	handle->front_bumper = random();
	handle->rear_bumper = ~handle->front_bumper;
	ii_chk_ptr(handle, "handle");
	ii_close_file(DESC);

	strncpy((char *) &handle->file_name, dataname, II_MAX_PATH_LENGTH-1);
	handle->data_bytes = ii_get_file_length(dataname);
	if (handle->data_bytes<1){

		printf("File %s does not exist, creating\n", dataname);
		handle->data_bytes = byte_width;
		handle->max_index = 0;
		handle->last_index=0;
		handle->synced=1;
		handle->sorted=1;
	}

	ii_d("File %s has size  %lld \n", dataname, handle->data_bytes);

	ret = ii_size_and_map_file(dataname,handle->data_bytes);
	ii_chk_ptr_warn(ret, "ii_control");
	if (ret==NULL){return NULL;}
	handle->data = ret;

	strncpy((char *) &handle->string_file_name, string_name, II_MAX_PATH_LENGTH-1);
	handle->string_bytes = ii_get_file_length(string_name);
	if (handle->string_bytes<1){

		printf("File %s does not exist, creating\n", string_name);
		handle->string_bytes = 1;
		handle->synced=1;
		handle->sorted=0;
	}


	ii_d("File %s has size  %lld \n", string_name, handle->string_bytes);


	ret = ii_size_and_map_file(string_name,handle->string_bytes);
	ii_chk_ptr(ret, "stringbank");
	if (ret==NULL){return NULL;}
	handle->string_bank = ret;

	//When II exits properly, it sets synced to 1
	if (handle->synced == 1 ) {
		ii_d("Table %s is synced - using cached data!\n", dataname);
	} else {
		/* Figure out the number of rows in the table */
		ii_int siz = byte_width;
		ii_int i=-1;
#ifndef SPLINT
		ii_d("Dividing  %lld  by  %lld  to get number of entries\n", handle->data_bytes, siz);
#endif
		ii_int numitems = handle->data_bytes/siz;
		#ifdef II_DEBUG
		printf("Calculated number of items in table %s to be  %lld \n",dataname,  numitems);
		#endif
		handle->max_index = numitems-1;
		ii_d("Table max index is  %lld \n", handle->max_index);


		i=handle->max_index;
		/* We want to scan the file and look for empty rows, which should signal the end of the user entries
		FIXME
		if (ent_search_command_line_switch("--rebuild-all")) {
		printf("Rebuilding file %s\n", pointsname);
		for (i=handle->max_index;i>-1; i--)
		{
		double absx, absy;
		absx = fabs(handle->points[i].x);
		absy = fabs(handle->points[i].y);
		if (  !(absx < DBL_EPSILON) || !(absy < DBL_EPSILON) )
		{ break; }
	}
}
*/


if (i<0) { i=0;}
if (i>handle->max_index) { i=handle->max_index;}

handle->last_index = i;
handle->string_used_bytes=handle->string_bytes; //FIXME



}

#ifdef II_DEBUG
printf("Finished loading table\n");
printf("Loaded table with  %lld  items out of a possible  %lld \n", handle->last_index+1, handle->max_index+1);
#endif
handle->type = 1;
handle->synced=0;
handle->row_width = byte_width;

//FIXME check all pointers etc before returning
return handle;
}

/** The default comparison function
 * 
 * Does a byte-by-byte comparision on two chunks of memory to decide how to sort them.
 * 
 * \param handle	The table being sorted
 * \param a			Pointer to the data in row 1
 * \param b			Pointer to the data in row 2
 * \param user_data	A pointer that you provide.  Not used here
 */

int ii_default_row_equal(ii_control * handle, void * a, void * b, void * user_data){
	int same = 1;
	ii_int i=-1;
	for (i=0; i<handle->row_width; i++) {
		//printf("Comparing %d and %d\n", aa[i], bb[i]);
		if (ii_bytes_equal(handle->row_width, a, b) != 0){
			same = 0;
		}
	}
	//ii_d("Rows are %s\n", same ? "same": "different");
	return same;
}
/** Remove duplicate entries, when adjacent.
*
* Scans the entire table looking for duplicate rows and then removing them.
* Only adjacent rows are compared, so sort the table first.
*
*\param table The table control struct
*\param compareFunc The same compareFunc used for sorts.
*
* Returns the number of deleted rows.
*/
ii_uint ii_weed(ii_control * table, int (*compareFunc)(ii_control * handle, void *, void *, void *), void * user_data) {
	ii_uint discards = 0;
	ii_int i;
	table->sorted=0;
	for (i=table->last_index; i>1; i--) {
		void * a =ii_fetch_row(table, i);
		void * b =ii_fetch_row(table, i-1);
		ii_d("Comparing row %llu and %llu\n", i, i-1);
		if (compareFunc(table, a, b, user_data)) {
			ii_d("Rows are the same, deleting %llu\n", i);
			ii_delete_row(table, a);
			discards++;
		}
	}
	return discards;
}


/** Create or open a tables
*
* Open a table in the given directory.  The table will be created if it doesn't exist
* already.
*
* The table consists of multiple files - a metadata (control) file, a "rows" file
* and a "stringbank".
*
* The files are memory mapped, so they appear as normal memory, but any changes are
* persisted to disk.
*/
ii_control * ii_open_table(char * directory, char * table, int byte_width) {
#ifndef SPLINT
	ii_d("Opening table %s in directory %s\n", table, directory);

#endif
	ii_uint buff_length = II_MAX_PATH_LENGTH;
	ii_control *handle=NULL;
	char datafile[buff_length], descriptorfile[buff_length], stringsfile[buff_length];
	snprintf((char *)&datafile, II_MAX_PATH_LENGTH-1, "%s/%s.table", directory,table);
	snprintf((char *)&descriptorfile, II_MAX_PATH_LENGTH-1, "%s/%s.descriptor", directory,table);
	snprintf((char *)&stringsfile, II_MAX_PATH_LENGTH-1, "%s/%s.strings", directory,table);

	handle =  ii_do_open_table( (char *) &datafile, (char *) &descriptorfile, (char *) &stringsfile,1, byte_width);
	strcpy((char *)&(handle->identifier),"II TABLE");

	#ifdef II_DEBUG
	printf("Stringbank starts at %p\n", handle->string_bank);
	printf("Stringbank used size at %llu\n", handle->string_used_bytes);
	printf("Stringbank total size %llu\n", handle->string_bytes);
	printf("Databank starts at %p\n", handle->data);
	printf("Databank last index: %llu\n", handle->last_index);
	printf("Databank max index: %llu\n", handle->max_index);
	printf("Databank total size: %llu\n", handle->data_bytes);
	printf("Row width in bytes: %llu\n", handle->row_width);
	#endif
	check_table(handle);
	return handle;
}

/** Print out msync errors */
void
check_msync(int res) {
		if(res==EBUSY) { printf("Memory is locked, cannot unmap!\n"); }
		if(res==EINVAL) { printf("Memory is not aligned, or bad flags.\n"); }
		if(res==ENOMEM) { printf("Attempt to unmap memory that is not mapped.\n"); }
}

/** Closes a table
 * 
 * After closing, any attempt to access that table will segfault.
 * 
 * \param handle Table to close
 */

int ii_close_table (ii_control * handle)
{
	int res;
	handle->synced=1;
	#ifndef II_WINDOWS
	if (0!=(res=msync(handle->data, handle->data_bytes, MS_SYNC))) {
		check_msync(res);
		printf("Sync failed, data loss likely!\n");	
	};
	if (0!=(res=msync(handle->string_bank, handle->string_bytes, MS_SYNC))) {
		check_msync(res);
		printf("Sync failed, data loss likely!\n");	
	};
	if(0!=(res=msync(handle, sizeof(ii_control), MS_SYNC))) {
		check_msync(res);
		printf("Sync failed, data loss likely!\n");	
	};
	#endif
	//FIXME flush windows as well
	//Or do we not want to flush?
	ii_munmap(handle->data, handle->data_bytes);
	handle->max_index=handle->last_index;
	ii_truncate(handle->file_name, (handle->last_index+1)*handle->row_width);

	ii_munmap(handle->string_bank, handle->string_bytes);
	ii_truncate(handle->string_file_name, (handle->string_used_bytes));

	ii_munmap(handle, sizeof(ii_control));


	return 0;
}

/** Search the command line parameters for a string
 * 
 * \param searchString	The text to search for.  Note that you must include the leading "--" if you are looking for a switch.
 */

int
ii_search_command_line_switch (char * searchString, int argc, char *argv[]) {
	int i;
	#ifdef DEBUG
	//printf("Searching for %s in arglist\n", searchString);
	#endif
	for (i = 1; i < argc; i++)  /* Skip argv[0] (program name). */
	{

		#ifdef DEBUG
		//printf("Comparing %s, %s\n", ent_argv[i], searchString);
		#endif
		if (strcmp(argv[i], searchString) == 0)  /* Process optional arguments. */
		{
			return(i);
		}
	}
	return(0);
}

#endif // II_INCLUDED