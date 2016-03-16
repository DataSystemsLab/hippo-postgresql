/*
 * gsin.h
 *
 *  Created on: Aug 28, 2015
 *      Author: sparkadmin
 */
#include "postgres.h"
#include "fmgr.h"
#include "nodes/execnodes.h"
//#include "access/xlogreader.h"
//#include "lib/stringinfo.h"
//#include "storage/bufpage.h"
//#include "storage/buf.h"
#include "utils/relcache.h"
//#include "access/itup.h"

#ifndef GSIN_H
#define GSIN_H

/*
 * Storage type for GSIN's reloptions
 */
typedef struct GsinOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	BlockNumber maxPagesPerRange;
} GsinOptions;



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

#define GSIN_DEFAULT_MAX_PAGES_PER_RANGE 20
#define GsinGetMaxPagesPerRange(relation) \
	((relation)->rd_options ? \
	 ((GsinOptions *) (relation)->rd_options)->maxPagesPerRange : \
	 GSIN_DEFAULT_MAX_PAGES_PER_RANGE)
#define HISTOGRAM_OUT_OF_BOUNDARY -9999


/*
 * This is GSIN's own definition of ItemPointer
 */
typedef struct GsinItemPointer
{
	uint16		bi_hi;
	uint16		bi_lo;
	BlockNumber blockNumber;
	OffsetNumber ip_posid;
}GsinItemPointer;
/*
 * We use a GsinBuildState during initial construction of a GSIN index.
 *
 */
typedef struct GridList
{
	int16 length;
	int16 grids[FLEXIBLE_ARRAY_MEMBER];
} GridList;

/*
 * GsinTupleLong is an intermediate structure which contains the gs_gridList. This data is stored on disk temporarily. This list will be dropped later.
 */
typedef struct GsinTupleLong
{
	BlockNumber gs_PageStart;
	BlockNumber gs_PageNum;/* The last page covered by this tuple */
	//GridList gs_gridList;
	int16 length;
	int16 deleteFlag;
	int16 *grids;//[FLEXIBLE_ARRAY_MEMBER];
	//uint32_t *grids;
	/*
	 * This attribute is only valid when we do scanning.
	 */
	struct ewah_bitmap *compressedBitset;
	struct bitmap *originalBitset;
} GsinTupleLong;
// GsinTupleLong;// *GsinTupleLong_Pointer;
/*
 * GsinTuple is a final structure which stores GSIN tuple information. This data is stored on disk permanently.
 */
typedef struct GsinTuple
{
	BlockNumber gs_PageStart;
	BlockNumber gs_PageNum;
} GsinTuple;
/*
 * GsinLookUpTable is a structure consists of histogram buckets.Each entry is this table
 */
typedef struct HistogramBucket
{
	double LowerBound;
	double UpperBound;
} HistogramBucket;

typedef struct GsinLookupTable
{
	HistogramBucket histogramBuckets;
	GridList gridList;
} GinsLookupTable;

typedef struct GsinBuildState
{
	Relation	gs_irel;
	int			gs_numtuples;
	int 		gs_indexnumtuples;
	int 		gs_scanpage;
	AttrNumber attrNum;
	BlockNumber gs_MaxPages;
	BlockNumber gs_PageStart;
	BlockNumber gs_PageNum; /* The last page we have */
	BlockNumber gs_currentPage; /* Which parent page we are working on now */
	//GridList gs_gridList;
	int16 gs_length; /* This length records the real length of a current grid list */
	int16 gs_grids[10000]; /* This array pre-allocates a large enough size (10000) to accomodate all possible grids */
//	GsinTupleLong gs_state_tuplelong; /* This pointer points to the GsinTuple with grid list*/
//	GsinTuple *gs_state_tuple; /* This pointer points to the GsinTuple without grid list */
//	Buffer		gs_currentInsertBuf;/* The new index tuple is to be inserted into this buffer */
	BlockNumber gs_currentInsertBlock;
	Page gs_currentInsertPage;
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
	GsinItemPointer gsinItemPointer[1000000];
	int itemPointerMemSize;
	BlockNumber sorted_list_pages;
	//uint32_t gs_length;
	//uint32_t gs_grids[10000];
} GsinBuildState;

typedef struct GsinScanState
{

	Buffer currentLookupBuffer; /* Which buffer we are  */
	BlockNumber currentLookupPageNum;
	//Page currentLookupPagePointer;
	OffsetNumber currentOffset;
	OffsetNumber maxOffset;
	GsinTupleLong gsinTupleLong;
	IndexTuple currentDiskTuple;
	BlockNumber nBlocks;/* Total blocks in this relation */
	bool scanHasNext;
	int16 length;
	//int16 grids[1000];
	/*
	 * This attribute is only valid when we do scanning.
	 */
	struct ewah_bitmap *compressedBitset;
	Datum *histogramBounds;
	int histogramBoundsNum;
} GsinScanState;


#undef GSIN_DEBUG

#ifdef GSIN_DEBUG
#define GSIN_elog(args)			elog args
#else
#define GSIN_elog(args)			((void) 0)
#endif


/*
 * prototypes for functions in gsin.c (external entry points for BRIN)
 */
extern Datum gsinbuild(PG_FUNCTION_ARGS);
extern Datum gsinbuildempty(PG_FUNCTION_ARGS);
extern Datum gsinbeginscan(PG_FUNCTION_ARGS);
extern Datum gsingetbitmap(PG_FUNCTION_ARGS);
extern Datum gsinendscan(PG_FUNCTION_ARGS);
extern Datum gsinrescan(PG_FUNCTION_ARGS);
extern Datum gsinoptions(PG_FUNCTION_ARGS);
//extern void gsin_desc(StringInfo buf, XLogReaderState *record);
//extern const char *gsin_identify(uint8 info);
//extern void gsin_redo(XLogReaderState *record);
extern Datum gsininsert(PG_FUNCTION_ARGS);
extern Datum gsinbulkdelete(PG_FUNCTION_ARGS);
extern Datum gsinvacuumcleanup(PG_FUNCTION_ARGS);
#endif /* GSIN_H */

