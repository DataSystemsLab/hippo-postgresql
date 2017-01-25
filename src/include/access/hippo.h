/*
 * hippo.h
 * Author: jiayu2@asu.edu
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "access/relscan.h"

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/bufpage.h"
#include "storage/buf.h"
#include "utils/relcache.h"
#include "access/itup.h"

#include "fmgr.h"
#include "nodes/execnodes.h"
#ifndef HIPPO_H
#define HIPPO_H


/*
 * Storage type for HIPPO's reloptions
 */
typedef struct HippoOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	BlockNumber density;
} HippoOptions;



/*
 * Histogram binary search result
 */
typedef struct searchResult {
	    int index;
	    int numberOfGuesses;
}searchResult;

#define CLEAR_INDEX_TUPLE 0
//#define DIRTY_INDEX_TUPLE 1
#define LAST_INDEX_TUPLE 2
#define SORTED_LIST_PAGES 100
#define SORTED_LIST_TUPLES_PER_PAGE 650
#define ITEM_POINTER_MEM_UNIT 1000
#define ItemPointerSize (sizeof(uint16)*2+sizeof(OffsetNumber))

#define HISTOGRAM_PER_PAGE 650

#define HIPPO_DEFAULT_DENSITY 20
#define HippoGetMaxPagesPerRange(relation) \
	((relation)->rd_options ? \
	 ((HippoOptions *) (relation)->rd_options)->density : \
	 HIPPO_DEFAULT_DENSITY)
#define HISTOGRAM_OUT_OF_BOUNDARY -9999


/*
 * This is Hippo's own definition of ItemPointer
 */
typedef struct HippoItemPointer
{
	uint16		bi_hi;
	uint16		bi_lo;
	BlockNumber blockNumber;
	OffsetNumber ip_posid;
}HippoItemPointer;
/*
 * We use a HippoBuildState during initial construction of a HIPPO index.
 *
 */
typedef struct GridList
{
	int16 length;
	int16 grids[FLEXIBLE_ARRAY_MEMBER];
} GridList;

/*
 * HippoTupleLong is an intermediate structure which contains the in memory index entry. This data is stored in memory temporarily. This list will be dropped later.
 */
typedef struct HippoTupleLong
{
	BlockNumber hp_PageStart;
	BlockNumber hp_PageNum;/* The last page covered by this tuple */
	int16 length;
	int16 deleteFlag;
	int16 *grids;
	/*
	 * This attribute is only valid when we do scanning.
	 */
	struct ewah_bitmap *compressedBitset;
	struct bitmap *originalBitset;
} HippoTupleLong;


typedef struct HippoBuildState
{
	Relation	hp_irel;
	int			hp_numtuples;
	int 		hp_indexnumtuples;
	int 		hp_scanpage;
	AttrNumber attrNum;
	BlockNumber hp_MaxPages;
	BlockNumber hp_PageStart;
	BlockNumber hp_PageNum; /* The last page we have */
	BlockNumber hp_currentPage; /* Which page we are working on now */
	int16 hp_length; /* This length records the real length of a current grid list */
//	int16 hp_grids[10000]; /* This array pre-allocates a large enough size (10000) to accomodate all possible grids */
	Buffer		hp_currentInsertBuf;/* The new index tuple is to be inserted into this buffer */
	Page hp_currentInsertPage;
	Datum *histogramBounds;
	int histogramBoundsNum;
	int lengthcounter;
	struct bitmap *originalBitset;
/*
 * These parameters are used to control the page merging
 */
	int differentTuples;
	double differenceThreshold;
	bool stopMergeFlag;
	bool stopMergeFlagPageTrigger;
	bool *gridCheckFlag;
	struct bitmap *pageBitmap;
	int16 deleteFlag;
	bool dirtyFlag;
/*
 * The following parameters are used to control the sorted lists
 */
	HippoItemPointer hippoItemPointer[10000000];
	int itemPointerMemSize;
	BlockNumber sorted_list_pages;

} HippoBuildState;

typedef struct HippoScanState
{

	Buffer currentLookupBuffer; /* Which buffer we are  */
	BlockNumber currentLookupPageNum;
	//Page currentLookupPagePointer;
	OffsetNumber currentOffset;
	OffsetNumber maxOffset;
	HippoTupleLong hippoTupleLong;
	IndexTuple currentDiskTuple;
	BlockNumber nBlocks;/* Total blocks in this relation */
	bool scanHasNext;
	int16 length;
	/*
	 * This attribute is only valid when we do scanning.
	 */
	struct ewah_bitmap *compressedBitset;
	Datum *histogramBounds;
	int histogramBoundsNum;
} HippoScanState;





/*
 * Standard system calls for functions in hippo.c (external entry points for PG Kernel)
 */
//extern Datum hippobeginscan(PG_FUNCTION_ARGS);
//extern Datum hippobuild(PG_FUNCTION_ARGS);
//extern Datum hippobuildempty(PG_FUNCTION_ARGS);
//extern Datum hippogetbitmap(PG_FUNCTION_ARGS);
//extern Datum hippoendscan(PG_FUNCTION_ARGS);
//extern Datum hipporescan(PG_FUNCTION_ARGS);
//extern Datum hippooptions(PG_FUNCTION_ARGS);
//extern void hippo_desc(StringInfo buf, XLogReaderState *record);
//extern const char *hippo_identify(uint8 info);
//extern void hippo_redo(XLogReaderState *record);
//extern Datum hippoinsert(PG_FUNCTION_ARGS);
//extern Datum hippobulkdelete(PG_FUNCTION_ARGS);
//extern Datum hippovacuumcleanup(PG_FUNCTION_ARGS);

extern Datum hippohandler(PG_FUNCTION_ARGS);

extern IndexBuildResult *hippobuild(Relation heap, Relation index,
		  struct IndexInfo *indexInfo);
extern void hippobuildempty(Relation index);
extern bool hippoinsert(Relation idxRel, Datum *values, bool *nulls,
		   ItemPointer heaptid, Relation heapRel,
		   IndexUniqueCheck checkUnique);
extern IndexScanDesc hippobeginscan(Relation r, int nkeys, int norderbys);
extern int64 hippogetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void hipporescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		   ScanKey orderbys, int norderbys);
extern void hippoendscan(IndexScanDesc scan);
extern IndexBulkDeleteResult *hippobulkdelete(IndexVacuumInfo *info,
			   IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback,
			   void *callback_state);
extern IndexBulkDeleteResult *hippovacuumcleanup(IndexVacuumInfo *info,
				  IndexBulkDeleteResult *stats);
extern bytea *hippooptions(Datum reloptions, bool validate);

/*
 * Hippo utils functions in hippo_utils.c
 */


/*
 * Buffer and page operations
 */
Buffer hippo_getinsertbuffer(Relation irel);
void hippoinit_special(Buffer buffer);
OffsetNumber hippo_doinsert(HippoBuildState *buildstate);

/*
 * Index entry operations
 */
void hippoGetNextIndexTuple(IndexScanDesc scan);
IndexTupleData * hippo_form_indextuple(HippoTupleLong *memTuple, Size *memlen, int histogramBoundsNum);
void hippo_form_memtuple(HippoTupleLong *hippoTupleLong,IndexTuple diskTuple,Size *memlen);
bool hippo_can_do_samepage_update(Buffer buffer, Size origsz, Size newsz);
HippoTupleLong* build_real_hippo_tuplelong(HippoBuildState* buildstate);
void copy_hippo_mem_tuple(HippoTupleLong *newTuple,HippoTupleLong* oldTuple);


/*
 * Complete histogram operations
 */
void binary_search_histogram_ondisk(searchResult *histogramMatchData, Relation idxrel, Datum value,BlockNumber startBlock,int totalNumber);
void binary_search_histogram(searchResult *histogramMatchData,int histogramBoundsNum,Datum *histogramBounds, Datum value);
void put_histogram(Relation idxrel, BlockNumber startBlock, int histogramBoundsNum,Datum *histogramBounds);
int get_histogram_totalNumber(Relation idxrel,BlockNumber startBlock);
//int get_histogram(Relation idxrel,BlockNumber startblock,int histogramPosition)

/*
 *Index entries sorted list operations
 */
void SortedListPerIndexPage(HippoBuildState *hippoBuildState,BlockNumber currentBlock,BlockNumber maxBlock,int remainder,BlockNumber startBlock);
void SortedListInialize(Relation index,HippoBuildState *hippoBuildState,BlockNumber startBlock);
int GetTotalIndexTupleNumber(Relation idxrel,BlockNumber startBlock);
void AddTotalIndexTupleNumber(Relation idxrel,BlockNumber startBlock);
void update_sorted_list_tuple(Relation idxrel,int index_tuple_id,BlockNumber diskBlock,OffsetNumber diskOffset,BlockNumber startBlock);
bool binary_search_sorted_list(Relation heaprel,Relation idxrel,int totalIndexTupleNumber,BlockNumber targetHeapBlock,int *resultPosition,HippoTupleLong *hippoTupleLong,BlockNumber* diskBlock, OffsetNumber* diskOffset,BlockNumber startBlock);
void add_new_sorted_list_tuple(Relation idxrel,int totalIndexTupleNumber,BlockNumber diskBlock,OffsetNumber diskOffset,BlockNumber startBlock);
int check_index_position(Relation idxrel,int index_tuple_id,BlockNumber targetHeapBlock,HippoTupleLong *hippoTupleLong,BlockNumber* diskBlock, OffsetNumber* diskOffset,BlockNumber startBlock);
void serializeSortedListTuple(HippoItemPointer *hippoItemPointer,char *diskTuple);
void deserializeSortedListTuple(HippoItemPointer *hippoItemPointer,char *diskTuple);
void get_sorted_list_pages(Relation idxrel,BlockNumber *sorted_list_pages,BlockNumber startBlock);

#endif /* HIPPO_H */

