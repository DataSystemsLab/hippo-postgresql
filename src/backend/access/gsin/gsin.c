#include <stdio.h>
#include <stdlib.h>
#include "postgres.h"
#include "commands/vacuum.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "catalog/pg_statistic.h"
#include "fmgr.h"
#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "access/gsin.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/elog.h"
#include "catalog/pg_attribute.h"
#include "access/xlogreader.h"
#include "catalog/pg_index.h"
#include "access/htup_details.h"
//#include "trle.h"
#include "ewok.h"

//#include "izenelib/include/am/bitmap/RoaringBitmap.h"
//#include "access/bitset.h"
#define SORT_TYPE int16
#define SORT_NAME mine
#include "access/sort.h"



/*
 * Claim the functions in thisfile
 */
void gsininit(Buffer b);
void gsinGetNextIndexTuple(IndexScanDesc scan);
Datum gsinbulkdelete(PG_FUNCTION_ARGS);
IndexTupleData * gsin_form_indextuple(GsinTupleLong *memTuple, Size *memlen, int histogramBoundsNum);
void gsin_form_memtuple(GsinTupleLong *gsinTupleLong,IndexTuple diskTuple,Size *memlen);
bool gsin_can_do_samepage_update(Buffer buffer, Size origsz, Size newsz);
void binary_search_histogram(searchResult *histogramMatchData,int histogramBoundsNum,Datum *histogramBounds, Datum value);
void SortedListFormDiskTuple(GsinBuildState *gsinBuildState,BlockNumber currentBlock,BlockNumber maxBlock,int remainder,BlockNumber startBlock);
void SortedListInialize(Relation index,GsinBuildState *gsinBuildState,BlockNumber startBlock);
//void SerializeItemPointer(ItemPointer itemPointer,char *diskItemPointer);
//void AppendItemPointer(GsinBuildState *gsinBuildState,ItemPointer itemPointer);
//void CheckItemPointerMem(GsinBuildState *gsinBuildState);
int GetTotalIndexTupleNumber(Relation idxrel,BlockNumber startBlock);
void AddTotalIndexTupleNumber(Relation idxrel,BlockNumber startBlock);
void update_sorted_list_tuple(Relation idxrel,int index_tuple_id,BlockNumber diskBlock,OffsetNumber diskOffset,BlockNumber startBlock);
bool binary_search_sorted_list(Relation heaprel,Relation idxrel,int totalIndexTupleNumber,BlockNumber targetHeapBlock,int *resultPosition,GsinTupleLong *gsinTupleLong,BlockNumber* diskBlock, OffsetNumber* diskOffset,BlockNumber startBlock);
void add_new_sorted_list_tuple(Relation idxrel,int totalIndexTupleNumber,BlockNumber diskBlock,OffsetNumber diskOffset,BlockNumber startBlock);
int check_index_position(Relation idxrel,int index_tuple_id,BlockNumber targetHeapBlock,GsinTupleLong *gsinTupleLong,BlockNumber* diskBlock, OffsetNumber* diskOffset,BlockNumber startBlock);
void gsininit_special(Buffer buffer);
void serializeSortedListTuple(GsinItemPointer *gsinItemPointer,char *diskTuple);
void deserializeSortedListTuple(GsinItemPointer *gsinItemPointer,char *diskTuple);
/*
 * Histogram_bounds information from pg_statistics
 */


/*
 * Form a real size GsinTupleLong (long version)
 */
static GsinTupleLong* build_real_gsin_tuplelong(GsinBuildState* buildstate)
{
//	elog(LOG,"Start to build real gsintuplelong");
	int i;
	GsinTupleLong *gsinTupleLong=palloc(sizeof(GsinTupleLong));
	gsinTupleLong->length=buildstate->gs_length;
	//elog(LOG,"Just about to form real tuple, length is %d",gsinTupleLong->length);
//	gsinTupleLong->grids=palloc(sizeof(int16)*gsinTupleLong->length);
	//elog(LOG,"Alloc memory for real gsin tuple long is done");
	//gridList.grids=buildstate->gs_grids;
/*
	for(i=0;i<gsinTupleLong->length;i++)
	{
		gsinTupleLong->grids[i]=buildstate->gs_grids[i];
	}
*/
		//Sort the grid list for each index tuple
//		mine_quick_sort(gsinTupleLong->grids,gsinTupleLong->length);
		gsinTupleLong->gs_PageStart=buildstate->gs_PageStart;
		gsinTupleLong->gs_PageNum=buildstate->gs_PageNum;
		buildstate->lengthcounter+=gsinTupleLong->length;
		gsinTupleLong->originalBitset=buildstate->originalBitset;
		gsinTupleLong->deleteFlag=buildstate->deleteFlag;
		//gsinTupleLong.gs_gridList=gridList;
		//elog(LOG,"Start to print one mem tuple");
		//for(i=0;i<gsinTupleLong->length;i++)
		//{
		//	elog(LOG,"Grid %d Value %d",i,gsinTupleLong->grids[i]);
		//}
//		elog(LOG,"End to print one mem tuple");
	return gsinTupleLong;
}

/*
 * Copy a GsinTupleLong
 */
static void copy_gsin_mem_tuple(GsinTupleLong *newTuple,GsinTupleLong* oldTuple)
{
	newTuple->gs_PageStart=oldTuple->gs_PageStart;
	newTuple->gs_PageNum=oldTuple->gs_PageNum;
	newTuple->deleteFlag=oldTuple->deleteFlag;
	newTuple->originalBitset=bitmap_copy(oldTuple->originalBitset);
}


/*
 * Get new buffer if the old buffer is not large enough
 */


static Buffer
gsin_getinsertbuffer(Relation irel)
{
	Buffer buffer;
	Page page;
	Size pageSize;
	buffer = ReadBuffer(irel, P_NEW);
	//elog(LOG, "Initialized buffer at subfunc");
	if(buffer==NULL)
	{
		elog(LOG, "Initialized buffer NULL at subfunc");
	}
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	//elog(LOG, "Locked buffer");
	pageSize = BufferGetPageSize(buffer);
	page = BufferGetPage(buffer);
	if(page==NULL)
	{
		elog(LOG, "Initialized page NULL at subfunc");
	}
	PageInit(page, pageSize, sizeof(BlockNumber)*2+sizeof(GridList));
	//gsininit(buffer);
	MarkBufferDirty(buffer);
	//elog(LOG, "Marked this buffer dirty at subfunc");
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
	//elog(LOG, "Release buffer at subfunc");
	return buffer;

}
/*
 * Form an index tuple
 */
IndexTupleData * gsin_form_indextuple(GsinTupleLong *memTuple, Size *memlen, int histogramBoundsNum)
{
//	elog(LOG,"Start to form index tuple");
	char	   *ptr,
			   *ret,
			   *diskGridSet,
			   *diskGridSetAccumulator;
	//bitset_t *compressedBitset;
	struct ewah_bitmap *compressedBitset;
//	struct bitmap *gridBitset;
	int			i;
	Size gridMemLength,bitmapLength;
	/*
	 * The deleteFlag is a flag for distinguishing whether this GSIN tuple is "dirty" or not. And also it is also used to mark whether this is the last index tuple. It is used in bulkdelete.
	 */
	int16 deleteFlag=0;
	*memlen = 0;

//	elog(LOG,"Mem tuple start %d",memTuple->gs_PageStart);
//	elog(LOG,"Mem tuple last %d",memTuple->gs_PageNum);
//	elog(LOG,"Mem tuple last %d",memTuple->gs_PageStart+memTuple->gs_PageNum-1);
	deleteFlag=memTuple->deleteFlag;
	//gridMemLength=sizeof((memTuple->grids[0])&INDEX_SIZE_MASK)*memTuple->length;
	//gridMemLength=sizeof(int16)*memTuple->length;
	//elog(LOG,"Tuple Length %d",memTuple->length);
//	gridBitset=bitmap_new();
	//elog(LOG,"Initialization is done");
/*	for(i=memTuple->length-1;i>=0;i--)
	{
		//elog(LOG,"memtuple grid %d is %d",i,memTuple->grids[i]);
		bitmap_set(gridBitset,memTuple->grids[i]);
	}*/
	//bitmap_set(gridBitset,histogramBoundsNum-1);
	//elog(LOG,"Set value is done");

	compressedBitset=bitmap_compress(memTuple->originalBitset);
//	elog(LOG,"Compressing is done");
	bitmapLength=estimate_ewah_size(compressedBitset);
//	elog(LOG,"Compression size estimation is done %d",bitmapLength);
	//diskGridSet=diskGridSetAccumulator=palloc(bitmapLength);
	//elog(LOG,"Assign memory for serialization");
	//ewah_serialize(compressedBitset,diskGridSetAccumulator);

	//*memlen=sizeof((memTuple->gs_PageStart)&INDEX_SIZE_MASK)+sizeof((memTuple->gs_PageNum)&INDEX_SIZE_MASK)+sizeof((memTuple->length)&INDEX_SIZE_MASK)+bitmapLength;
	*memlen=sizeof((memTuple->gs_PageStart)&INDEX_SIZE_MASK)+sizeof((memTuple->gs_PageNum)&INDEX_SIZE_MASK)+sizeof(deleteFlag)+bitmapLength;
	ptr = ret = palloc(*memlen);
	//gridBitset=palloc(sizeof((memTuple->grids[0])&INDEX_SIZE_MASK)*memTuple->length);
	memcpy(ptr,&(memTuple->gs_PageStart),sizeof((memTuple->gs_PageStart)&INDEX_SIZE_MASK));
	ptr+=sizeof((memTuple->gs_PageStart)&INDEX_SIZE_MASK);
	memcpy(ptr,&(memTuple->gs_PageNum),sizeof((memTuple->gs_PageNum)&INDEX_SIZE_MASK));
	ptr+=sizeof((memTuple->gs_PageNum)&INDEX_SIZE_MASK);
	memcpy(ptr,&(deleteFlag),sizeof((deleteFlag)));
	ptr+=sizeof((deleteFlag));
	//memcpy(ptr,&(memTuple->length),sizeof(memTuple->length));
	//ptr+=sizeof(memTuple->length);
	//memcpy(ptr,diskGridSet,bitmapLength);
	//ptr+=bitmapLength;
	ewah_serialize(compressedBitset,ptr);
//	elog(LOG,"Serialization is done");
	//elog(LOG,"Append bitmap to disktuple");
	//pfree(diskGridSet);
	//elog(LOG,"free diskGridSet");
//	pfree(memTuple->grids);
	//elog(LOG,"free memTuple->grids");

	//elog(LOG,"free memTuple");
//	bitmap_free(memTuple->originalBitset);

//	elog(LOG,"free gridBitset");
	ewah_free(compressedBitset);
//	elog(LOG,"free compressedBitset");
//	pfree(memTuple);
//	elog(LOG,"End to build one index tuple");
	return (IndexTupleData *) ret;
}
/*
 * Insert an index tuple into the index relation
 */

static OffsetNumber
gsin_doinsert(GsinBuildState *buildstate)//, Relation idxrel, BlockNumber pagesPerRange, Buffer *buffer, BlockNumber heapBlk,
			 // GsinTupleLong tup, Size itemsz)
{
//	elog(LOG, "Start to insert, this tuple volume is %d",buildstate->gs_PageNum);
	Page		page;
	OffsetNumber off,maxoff;
	BlockIdData blkId;
//	ItemPointer itemPointer=palloc(SizeOfIptrData);
	Buffer buffer=buildstate->gs_currentInsertBuf;
//=buildstate->gs_currentPage;

	GsinTupleLong *memTuple=build_real_gsin_tuplelong(buildstate);


	Size itemsz;//=sizeof(BlockNumber)*2+sizeof(int16)+sizeof(int16)*gsinTupleLong.length;
	char *data=(char *)gsin_form_indextuple(memTuple,&itemsz,buildstate->histogramBoundsNum);
	//elog(LOG,"Start insert");

	/*
	 * Acquire lock on buffer supplied by caller, if any.  If it doesn't have
	 * enough space, unpin it to obtain a new one below.
	 */
	if (BufferIsValid(buffer))
	{
		/*
		 * It's possible that another backend (or ourselves!) extended the
		 * revmap over the page we held a pin on, so we cannot assume that
		 * it's still a regular page.
		 */
		//LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		if (PageGetFreeSpace(BufferGetPage(buffer)) < itemsz)
		{
			buffer = InvalidBuffer;
			//elog(LOG, "page size in this buffer is not enough~~");
		}
		//LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	}

	/*
	 * If we still don't have a usable buffer, have brin_getinsertbuffer
	 * obtain one for us.
	 */
	if (!BufferIsValid(buffer))
	{
		//elog(LOG, "This buffer is invalid~~");
		//LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		buffer = gsin_getinsertbuffer(buildstate->gs_irel);//, InvalidBuffer, itemsz, &extended);
		buildstate->gs_currentInsertBuf=buffer;
		if(buffer==NULL)
		{
			elog(ERROR, "Try to get valid buffer but failed!");
		}
	}

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);
//	blk = BufferGetBlockNumber(*buffer);
	//elog(LOG, "Start insert");
	/* Execute the actual insertion */
	START_CRIT_SECTION();
	//elog(LOG, "Start critical section");
	//elog(LOG,"Just before insert, content: start %d PageNum %d grid length %d second grid %d",gsinTupleLong.gs_PageStart,gsinTupleLong.gs_PageNum,gsinTupleLong.length,gsinTupleLong.grids[1]);
//	elog(LOG,"Put gsin index on disk at disk page %d",BufferGetBlockNumber(buffer));
	off = PageAddItem(page, (Item) data, itemsz, InvalidOffsetNumber,
					  false, false);
//	ItemPointerSetBlockNumber(itemPointer,BufferGetBlockNumber(buffer));
//	ItemPointerSetOffsetNumber(itemPointer,off);
//	blkId=itemPointer->ip_blkid;
//	elog(LOG,"Block hi %d lo %d",blkId.bi_hi,blkId.bi_lo);
	//buildstate->gsinItemPointer[buildstate->gs_indexnumtuples].bi_lo=blkId.bi_lo;
//	elog(LOG,"This is index tuple number %d",buildstate->gs_indexnumtuples);
	//buildstate->gsinItemPointer[buildstate->gs_indexnumtuples].bi_hi=blkId.bi_hi;
	buildstate->gsinItemPointer[buildstate->gs_indexnumtuples].blockNumber=BufferGetBlockNumber(buffer);
	//elog(LOG,"Just put blockNumber %d to a itemPointer",buildstate->gsinItemPointer[buildstate->gs_indexnumtuples].blockNumber);
	buildstate->gsinItemPointer[buildstate->gs_indexnumtuples].ip_posid=off;
	//AppendItemPointer(buildstate,itemPointer);
	maxoff=PageGetMaxOffsetNumber(page);
	if (off == InvalidOffsetNumber)
		{elog(ERROR, "could not insert new index tuple to page~~~");}
//	else {elog(LOG, "Insert one index tuple, offset is %d maxoff is %d size is %d",off,maxoff,itemsz);}

//	elog(LOG, "Insert new index tuple to page successfully");
	MarkBufferDirty(buffer);

	//GSIN_elog((LOG, "inserted tuple (%u,%u) for range starting at %u",
	//		   blk, off, heapBlk));
	//elog(LOG, "End critical section");
	END_CRIT_SECTION();

	/* Tuple is firmly on buffer; we can release our locks */
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	/*
	 * Free serialized tuple
	 */
	pfree((char *)data);
	//ReleaseBuffer(buffer);
//	elog(LOG, "One insertion is done");
	return off;
}


/*
 * Per-tuple callback from IndexBuildHeapScan
 */
static void
gsinbuildCallback(Relation index,
		HeapTuple htup,
		Datum *values,
		bool *isnull,
		bool tupleIsAlive,
		void *state)
{

	BlockNumber thisblock;
	GsinBuildState *buildstate = (GsinBuildState *) state;
	Datum *histogramBounds=buildstate->histogramBounds;
	int histogramBoundsNum=buildstate->histogramBoundsNum;
	int i,j;
	AttrNumber attrNum;
//	elog(LOG, "BuildCallback is called");
	thisblock = ItemPointerGetBlockNumber(&htup->t_self);
//	if(buildstate->gs_currentPage!=thisblock && buildstate->gs_currentPage!=(thisblock-1))
//	{
		/*
		 * Should return error because the table tuple is not pop up by page order
		 */
//		elog(LOG, "index cannot get ordered table tuples");
		//return;
//	}
	attrNum=buildstate->attrNum;
//	elog(LOG,"BuildCallBack AttrNum is %d",attrNum);
	if(buildstate->gs_PageNum==0)
	{
		buildstate->gs_PageStart=thisblock;
		buildstate->gs_currentPage=thisblock;
		//buildstate->gs_PageNum++;
		buildstate->gs_PageNum=thisblock;
		buildstate->gs_scanpage++;
		buildstate->gs_length=0;
		buildstate->dirtyFlag=true;
	}
	else
	{
		if(buildstate->gs_currentPage!=thisblock)
		{
			if(buildstate->gs_currentPage!=(thisblock-1))
			{
				elog(LOG,"HeapTuple is not ordered");
			}
			//buildstate->gs_PageNum++;
			buildstate->gs_PageNum=thisblock;
			buildstate->dirtyFlag=true;
			buildstate->gs_scanpage++;
			//elog(LOG,"Grid list density is %f",(buildstate->differentTuples*1.00)/(buildstate->histogramBoundsNum-1));
			if(buildstate->differentTuples*100>=buildstate->differenceThreshold*(buildstate->histogramBoundsNum-1))
			{
				buildstate->differentTuples=0;
				buildstate->stopMergeFlag=true;
				//elog(LOG,"StopMergeFlag is set to true");
			}



		}
	}
	/*
	 * Check whether buildstate triggers the threshold. If so, insert a new index tuple into gsin.
	 */
	if(buildstate->stopMergeFlag==true)//)buildstate->gs_MaxPages)
	{
		/*
		 * Extract information from buildstate to a new real gsin tuple long
		 */
		//buildstate->gs_state_tuplelong=


			//elog(LOG, "Stop merge Insert is called");
			buildstate->stopMergeFlag=false;
			//bitmap_free(buildstate->originalBitset);
			buildstate->deleteFlag=CLEAR_INDEX_TUPLE;
			/*
			 * Insert the gsin tuple long into gsin index relation
			 */
			//elog(LOG, "Stop merge Insert is going to insert");
			buildstate->gs_PageNum=buildstate->gs_PageNum-1;
			//CheckItemPointerMem(buildstate);
			gsin_doinsert(buildstate);//, buildstate->gs_irel, buildstate->gs_MaxPages, buildstate->gs_currentInsertBuf,buildstate->gs_PageStart ,
						  //buildstate->gs_state_tuplelong, gsin_tuple_size(buildstate));
			buildstate->gs_indexnumtuples++;
			//buildstate->gs_PageNum=1;
			//elog(LOG, "Insert is called");
			buildstate->gs_PageStart=thisblock;
			buildstate->gs_PageNum=thisblock;
			buildstate->dirtyFlag=false;
			buildstate->gs_length=0;
			buildstate->gs_grids[0]=-1;
			buildstate->originalBitset=bitmap_new();


		//elog(LOG,"Buildcallback doinsert is done");
	}
	buildstate->gs_currentPage=thisblock;

	if (tupleIsAlive)
	{


		/* Cost estimation info */
		buildstate->gs_numtuples++;
		/* Go through histogram one by one */
		searchResult histogramMatchData;
		/*
		 * Binary search histogram
		 */

				int min=0,max=histogramBoundsNum-1,equalFlag,equalPosition,guess;
				histogramMatchData.index=-9999;
				histogramMatchData.numberOfGuesses=0;
				equalFlag=-1;

				binary_search_histogram(&histogramMatchData,histogramBoundsNum,histogramBounds, values[0]);
/*				while (max>=min) {

				        guess = (min + max) / 2;
				        histogramMatchData.numberOfGuesses++;


				        if((int)histogramBounds[min]==DatumGetInt32(values[0]))
				        {
				        	equalFlag=min;
				        	break;
				        }
				        if((int)histogramBounds[max]==DatumGetInt32(values[0]))
				        {
				        	equalFlag=max;
				        	break;
				        }
				        if((int)histogramBounds[guess]==DatumGetInt32(values[0]))
				        {
				        	equalFlag=guess;
				        	break;
				        }
				        if((int)histogramBounds[guess]>DatumGetInt32(values[0]))
				        {
				        	max=guess-1;
				        }
				        else
				        {
				        	min=guess+1;
				        }

				    }

				if(equalFlag<0)
				{
					histogramMatchData.index=min-1;

					if(DatumGetInt32(values[0])<(int)histogramBounds[0])
					{
						histogramMatchData.index=0;
					}
					else if(DatumGetInt32(values[0])>(int)histogramBounds[histogramBoundsNum-1])
					{
						histogramMatchData.index=histogramBoundsNum-2;
					}

				}
				else
				{
					histogramMatchData.index=equalFlag;
				}
*/

				if(histogramMatchData.index<0)
				{
					elog(LOG,"Got one bitmap set postition < 0 %d",histogramMatchData.index);
				}
				if(bitmap_get(buildstate->originalBitset,histogramMatchData.index)==false)
				{
					//log(LOG,"Set one bit");
					buildstate->differentTuples++;
					//elog(LOG,"Differebt tuple number is %d",buildstate->differentTuples);

					bitmap_set(buildstate->originalBitset,histogramMatchData.index);
					//elog(LOG,"Set is done");
				}


	}

	else
	{
		elog(LOG,"Got one dead tuple");
	}


}
Datum
gsinbuild(PG_FUNCTION_ARGS)
{
	/*
	 * This is some routine declarations for variables
	 */
		Relation	heap = (Relation) PG_GETARG_POINTER(0);
		Relation	index = (Relation) PG_GETARG_POINTER(1);
		IndexInfo  *indexInfo = (IndexInfo *) PG_GETARG_POINTER(2);
		IndexBuildResult *result;
		double		reltuples;
		GsinBuildState buildstate;
		Buffer		buffer;
		Datum *histogramBounds;
		int histogramBoundsNum;
		HeapTuple heapTuple;
		Page page;/* Initial page */
		Size		pageSize;
		AttrNumber attrNum;


		int i;

		/*
		 * GSIN option: max pages per range
		 */
		buildstate.gs_MaxPages=GsinGetMaxPagesPerRange(index);
		BlockNumber sorted_list_pages=((RelationGetNumberOfBlocks(heap)/((1)*SORTED_LIST_TUPLES_PER_PAGE))+1);
//		elog(LOG,"Max pages per range is %d",buildstate.gs_MaxPages);
		/*
		 * GSIN Only supports one column as the key
		 */
		attrNum=indexInfo->ii_KeyAttrNumbers[0];
//		elog(LOG, "KeyAttrNum is \"%d\" ", attrNum);
		/*
		 * Search cache for pg_statistic info
		 */
//		elog(LOG, "indexid is \"%d\" ", index->rd_id);
		heapTuple=SearchSysCache2(STATRELATTINH,ObjectIdGetDatum(heap->rd_id),Int16GetDatum(attrNum));
//		elog(LOG, "Got stattuple");
		if (HeapTupleIsValid(heapTuple))
		{

			get_attstatsslot(heapTuple,
					get_atttype(heap->rd_id, attrNum),get_atttypmod(heap->rd_id,attrNum),
							STATISTIC_KIND_HISTOGRAM, InvalidOid,
							NULL,
							&histogramBounds, &histogramBoundsNum,
							NULL, NULL);
//			elog(LOG, "Got histogramdata");
		}
		/*
		 * If didn't release the stattuple, it will be locked.
		 */
//		elog(LOG, "Release heaptuple");
		ReleaseSysCache(heapTuple);
		if(histogramBounds==NULL)
		{
			elog(LOG, "Got histogram NULL");
		}
		BlockNumber histogramPages=histogramBoundsNum/HISTOGRAM_PER_PAGE;

//		elog(LOG,"sorted_list_pages is %d",sorted_list_pages);
		/* Initialize pages for sorted list */
		for(i=0;i<sorted_list_pages+histogramPages+1;i++)
		{

			buffer=ReadBuffer(index,P_NEW);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			START_CRIT_SECTION();
			gsininit_special(buffer);
//			elog(LOG,"I inited block %d",BufferGetBlockNumber(buffer));
			MarkBufferDirty(buffer);
			END_CRIT_SECTION();
			UnlockReleaseBuffer(buffer);
		}

		/*
		 * We expect to be called exactly once for any index relation.
		 */
		//if (RelationGetNumberOfBlocks(index) != 0)
		/*	elog(ERROR, "index \"%s\" already contains data",
				 RelationGetRelationName(index));*/
		/*
		 *	Init GSIN again. Not sure whether it is repeated in BuildEmpty.
		 */
		buffer = ReadBuffer(index, P_NEW);
//		elog(LOG,"Start to put index on  block %d",BufferGetBlockNumber(buffer));
//		elog(LOG, "Initialized buffer");
		if(buffer==NULL)
		{
			elog(LOG, "Initialized buffer NULL");
		}
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
//		elog(LOG, "Locked buffer");
		pageSize = BufferGetPageSize(buffer);
		page = BufferGetPage(buffer);
		if(page==NULL)
		{
			elog(LOG, "Initialized page NULL");
		}
		PageInit(page, pageSize, sizeof(BlockNumber)*2+sizeof(GridList));
		//gsininit(buffer);
//		elog(LOG, "GsinInitialized");
		MarkBufferDirty(buffer);
//		elog(LOG, "Marked this buffer dirty");
		END_CRIT_SECTION();
		UnlockReleaseBuffer(buffer);
//		elog(LOG, "Release buffer");
		/*
		 * Initialize the build state
		 */
//		initialize_gsin_buildstate(buildstate.gs_irel,GsinGetMaxPagesPerBlock(index),buffer);
//		initialize_gsin_buildstate(buildstate.gs_irel,GSIN_DEFAULT_MAX_PAGES_PER_BLOCK,&buffer);

		buildstate.gs_irel= index;
		buildstate.attrNum=attrNum;
		buildstate.gs_PageStart=0;
		buildstate.gs_PageNum=0;
		buildstate.dirtyFlag=false;
		buildstate.gs_currentPage=0;
		buildstate.gs_length=0;
		buildstate.gs_numtuples=0;
		buildstate.gs_indexnumtuples=0;
		buildstate.gs_scanpage=0;
//		buildstate.gs_state_tuplelong=build_gsin_tuplelong();
//		buildstate.gs_state_tuple=build_gsin_tuple();
		buildstate.gs_currentInsertBuf=buffer;
		buildstate.gs_currentInsertPage=page;
		buildstate.lengthcounter=0;
		//elog(LOG, "Initialize gsin_buildstate");
		buildstate.histogramBounds=histogramBounds;
		buildstate.histogramBoundsNum=histogramBoundsNum;
		buildstate.originalBitset=bitmap_new();
		buildstate.pageBitmap=bitmap_new();
		/*
		 * Initial some parameters for merging
		 */
		//buildstate.differenceThreshold=(buildstate.gs_MaxPages*1.0)/100;
		buildstate.differenceThreshold=buildstate.gs_MaxPages;
		buildstate.differentTuples=0;
		buildstate.stopMergeFlag=false;
		/*
		 * Initialize sorted list parameters
		 */
		buildstate.sorted_list_pages=sorted_list_pages;
//		buildstate.itemPointerMemSize=1;
//		buildstate.itemPointers=palloc(buildstate.itemPointerMemSize*ITEM_POINTER_MEM_UNIT*ItemPointerSize);
		//buildstate.gridCheckFlag=palloc(sizeof(bool)*(histogramBoundsNum-1));
//		elog(LOG, "Assign histogram value %d",histogramBoundsNum);
		/* build the index */
		reltuples=IndexBuildHeapScan(heap, index, indexInfo, false,
				   gsinbuildCallback, (void *) &buildstate);


//		elog(LOG,"Buildcallback is done");
		/*
		 * Finish the last index tuple whose volume is less than the maximum page
		 */
		if(buildstate.dirtyFlag==true)
		{
			//buildstate.gs_state_tuplelong=build_real_gsin_tuplelong(&buildstate);
			buildstate.deleteFlag=buildstate.differentTuples;
			buildstate.gs_PageNum=buildstate.gs_currentPage;
			//CheckItemPointerMem(&buildstate);
			gsin_doinsert(&buildstate);
			buildstate.gs_indexnumtuples++;
			elog(LOG,"Grid list density is %f",(buildstate.differentTuples*1.00)/(buildstate.histogramBoundsNum-1));
			//elog(LOG,"Last index tuple starts %d volume %d grids length %d",buildstate.gs_state_tuplelong.gs_PageStart,buildstate.gs_state_tuplelong.gs_PageNum,buildstate.gs_state_tuplelong.gs_gridList.length);
			//for(i=0;i<buildstate.gs_state_tuplelong.gs_gridList.length;i++)
			//{
				//elog(LOG,"Grid is %d",buildstate.gs_state_tuplelong.gs_gridList.grids[i]);
			//}
		}

		page=PageGetContents(BufferGetPage(buildstate.gs_currentInsertBuf));
//		elog(LOG, "Build is about to end, last page offset is %d",PageGetMaxOffsetNumber(page));
		put_histogram(index,0,histogramBoundsNum,histogramBounds);
		SortedListInialize(index,&buildstate,histogramPages+1);

		/*
		 * Release the statistic data from system cache
		 */
//		free_attstatsslot(get_atttype(heap->rd_id, attrNum),histogramBounds,histogramBoundsNum,NULL,0);
//		elog(LOG, "Free stat slot");
		/*
		 * Return statistics
		 */
		result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
		//elog(LOG, "Initial IndexBuildResult");
		result->heap_tuples = buildstate.gs_numtuples;
		result->index_tuples = buildstate.gs_indexnumtuples;
//		elog(LOG,"Scan heap tuple %d , create index tuple %d, scaned page %d",buildstate.gs_numtuples,buildstate.gs_indexnumtuples,buildstate.gs_scanpage);

		elog(LOG,"Scan heap tuple %d , create index tuple %d, average grid list length %f grid list density %f scaned page %d",buildstate.gs_numtuples,buildstate.gs_indexnumtuples,buildstate.lengthcounter*1.0/buildstate.gs_indexnumtuples,(buildstate.lengthcounter*1.0/buildstate.gs_indexnumtuples)/(buildstate.histogramBoundsNum-1),RelationGetNumberOfBlocks(heap));
		PG_RETURN_POINTER(result);
}
/*
 *	Build an empty GSIN index in the initialization phase
 */
Datum
gsinbuildempty(PG_FUNCTION_ARGS)
{
	Relation	index = (Relation) PG_GETARG_POINTER(0);
	int i;
	Buffer		buffer;
	/* Initialize pages for sorted list */
	for(i=0;i<SORTED_LIST_PAGES;i++)
	{
		buffer=ReadBuffer(index,P_NEW);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		START_CRIT_SECTION();
		gsininit_special(buffer);
		elog(LOG,"I inited block %d",BufferGetBlockNumber(buffer));
		MarkBufferDirty(buffer);
		END_CRIT_SECTION();
		UnlockReleaseBuffer(buffer);
	}
	//log_newpage_buffer(buffer, true);
	PG_RETURN_VOID();
}

/*
 * Check itemPointerMemSize
 */
/*void CheckItemPointerMem(GsinBuildState *gsinBuildState)
{
	if(gsinBuildState->gs_indexnumtuples>=gsinBuildState->itemPointerMemSize*ITEM_POINTER_MEM_UNIT)
	{
		gsinBuildState->itemPointerMemSize++;
		gsinBuildState->itemPointers=repalloc(gsinBuildState->itemPointers,gsinBuildState->itemPointerMemSize*ITEM_POINTER_MEM_UNIT*ItemPointerSize);
	}
}*/

/*
 * Append one item pointer to current serialize tuple
 */
/*void AppendItemPointer(GsinBuildState *gsinBuildState,ItemPointer itemPointer)
{
	Size currentSize;
	BlockIdData blockIdData=itemPointer->ip_blkid;
	currentSize=(gsinBuildState->gs_indexnumtuples-1)*ItemPointerSize;
	elog(LOG,"blockID lo %d",blockIdData.bi_lo);
	memcpy(gsinBuildState->itemPointers+currentSize,&(blockIdData.bi_lo),sizeof(uint16));
	currentSize+=sizeof(uint16);
	elog(LOG,"blockID hi %d",blockIdData.bi_hi);
	memcpy(gsinBuildState->itemPointers+currentSize,&(blockIdData.bi_hi),sizeof(uint16));
	currentSize+=sizeof(uint16);
	elog(LOG,"pointer offset %d",itemPointer->ip_posid);
	memcpy(gsinBuildState->itemPointers+currentSize,&(itemPointer->ip_posid),sizeof(OffsetNumber));
	currentSize+=sizeof(OffsetNumber);

}*/

/*
 * Create sorted list
 */
void SortedListInialize(Relation index,GsinBuildState *gsinBuildState,BlockNumber startBlock)
{
//	elog(LOG,"Start to put sorted list");
//	char* diskTuple;
	Size itemSize;
	BlockNumber maxBlock,currentBlock;
	Offset off;
	int totalIndexTuples,remainder;
	maxBlock=gsinBuildState->gs_indexnumtuples/SORTED_LIST_TUPLES_PER_PAGE;
	remainder=gsinBuildState->gs_indexnumtuples%SORTED_LIST_TUPLES_PER_PAGE;
	totalIndexTuples=gsinBuildState->gs_indexnumtuples;
//	elog(LOG,"Got basic numbers SORTED TUPLES PER PAGE %d",SORTED_LIST_TUPLES_PER_PAGE);
	/*
	 * Start to initialize the list and put them on disk
	 */
	for(currentBlock=startBlock;currentBlock<=maxBlock+startBlock;currentBlock++){
	//off = PageAddItem(page, (Item) diskTuple, itemSize, InvalidOffsetNumber,false, false);
	SortedListFormDiskTuple(gsinBuildState,currentBlock,maxBlock,remainder,startBlock);
//	elog(LOG,"Put sort list on block %d",currentBlock);
	}
	/*if(off==InvalidOffsetNumber)
	{
		elog(LOG,"Fail to put one sorted list tuple");
	}

	}*/

}
void SortedListFormDiskTuple(GsinBuildState *gsinBuildState,BlockNumber currentBlock,BlockNumber maxBlock,int remainder,BlockNumber startBlock)
{
//	elog(LOG,"Serialize disk tuple");
	char *diskTuple;
	Size len=0;
	int i;
	Buffer currentBuffer;
	Page page;
	int iterationTimes;

	currentBuffer=ReadBuffer(gsinBuildState->gs_irel,currentBlock);
	page=BufferGetPage(currentBuffer);
	LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	if(page==NULL)
	{
		elog(LOG,"I got NULL page for sorted list");
	}
	if(currentBlock==startBlock)
	{
		/*
		 * This is the first tuple
		 */
		elog(LOG,"Current list block is %d",currentBlock);
		/*
		 * Put the first element on the first page
		 */
		OffsetNumber tempOff;



		diskTuple=palloc(sizeof(int));
		memcpy(diskTuple,&(gsinBuildState->gs_indexnumtuples),sizeof(int));
		elog(LOG,"Going to put total index tuples number %d on disk",gsinBuildState->gs_indexnumtuples);
		tempOff=PageAddItem(page, (Item) diskTuple, sizeof(int), InvalidOffsetNumber,false, false);
//		elog(LOG,"I have put the total tuple number on disk");
		tempOff=PageAddItem(page, (Item) (&gsinBuildState->sorted_list_pages), sizeof(BlockNumber), InvalidOffsetNumber,false, false);
		elog(LOG,"Succeed to put it on disk offset %d",tempOff);
//		pfree(diskTuple);
//		MarkBufferDirty(currentBuffer);
//		END_CRIT_SECTION();
//		UnlockReleaseBuffer(currentBuffer);
		//put_sorted_list_pages(gsinBuildState->gs_irel,gsinBuildState->sorted_list_pages);
	}
//	currentBuffer=ReadBuffer(gsinBuildState->gs_irel,currentBlock);
//	LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
//	START_CRIT_SECTION();
		if(currentBlock<maxBlock+startBlock)
		{
			iterationTimes=SORTED_LIST_TUPLES_PER_PAGE;
//			elog(LOG,"IterationTimes is max %d",iterationTimes);
		}
		else
		{
			iterationTimes=remainder;
//		elog(LOG,"IterationTimes is remainder %d",iterationTimes);
		}

			for(i=0;i<iterationTimes;i++){
			int testOffsetNumber;
			len=0;
			diskTuple=palloc(ItemPointerSize);
			/*memcpy(diskTuple,&(gsinBuildState->gsinItemPointer[currentBlock*SORTED_LIST_TUPLES_PER_PAGE+i].bi_hi),sizeof(uint16));
			len+=sizeof(uint16);
			memcpy(diskTuple+len,&(gsinBuildState->gsinItemPointer[currentBlock*SORTED_LIST_TUPLES_PER_PAGE+i].bi_lo),sizeof(uint16));
			len+=sizeof(uint16);
			memcpy(diskTuple+len,&(gsinBuildState->gsinItemPointer[currentBlock*SORTED_LIST_TUPLES_PER_PAGE+i].ip_posid),sizeof(OffsetNumber));
			len+=sizeof(OffsetNumber);*/
			serializeSortedListTuple(&(gsinBuildState->gsinItemPointer[(currentBlock-startBlock)*SORTED_LIST_TUPLES_PER_PAGE+i]),diskTuple);
//			elog(LOG,"this page has free space %d",PageGetFreeSpace(page));
			testOffsetNumber=PageAddItem(page, (Item) diskTuple, ItemPointerSize, InvalidOffsetNumber,false, false);

//			elog(LOG,"I am putting sorted list %d on block %d offset%d its value block %d off %d",i,currentBlock,testOffsetNumber,gsinBuildState->gsinItemPointer[(currentBlock-startBlock)*SORTED_LIST_TUPLES_PER_PAGE+i].blockNumber,gsinBuildState->gsinItemPointer[(currentBlock-startBlock)*SORTED_LIST_TUPLES_PER_PAGE+i].ip_posid);

//			pfree(diskTuple);
			}
			MarkBufferDirty(currentBuffer);
			END_CRIT_SECTION();
			UnlockReleaseBuffer(currentBuffer);

}
void deserializeSortedListTuple(GsinItemPointer *gsinItemPointer,char *diskTuple)
{
	int len=0;
	memcpy(&(gsinItemPointer->blockNumber),diskTuple,sizeof(uint32));
	len+=sizeof(uint32);
//	elog(LOG,"Just retrieve block number %d",gsinItemPointer->blockNumber);
	memcpy(&(gsinItemPointer->ip_posid),diskTuple+len,sizeof(OffsetNumber));
	len+=sizeof(OffsetNumber);
//	elog(LOG,"Just retrieve offset %d",gsinItemPointer->ip_posid);
}
void serializeSortedListTuple(GsinItemPointer *gsinItemPointer,char *diskTuple)
{
	int len=0;
	memcpy(diskTuple,&(gsinItemPointer->blockNumber),sizeof(uint32));
	len+=sizeof(uint32);
//	elog(LOG,"~~~Just put block number %d",gsinItemPointer->blockNumber);
	memcpy(diskTuple+len,&(gsinItemPointer->ip_posid),sizeof(OffsetNumber));
//	elog(LOG,"~~~Just put offset number %d",gsinItemPointer->ip_posid);
	len+=sizeof(OffsetNumber);
}
void put_histogram(Relation idxrel, BlockNumber startBlock, int histogramBoundsNum,Datum *histogramBounds)
{
	Buffer buffer;
	Page page;
	int totalNumber=histogramBoundsNum;
	int histogramIterator=0;
	int histogramBound;
	int maxBlock=histogramBoundsNum/HISTOGRAM_PER_PAGE;
	int i,j,off;
	for(i=startBlock;i<=startBlock+maxBlock;i++)
	{

	buffer=ReadBuffer(idxrel,i);
//	elog(LOG,"Put histogram on block %d",BufferGetBlockNumber(buffer));
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	if(i==startBlock){
	PageAddItem(page, (Item) (&totalNumber), sizeof(int), InvalidOffsetNumber,false, false);
	}
	for(j=0;j<HISTOGRAM_PER_PAGE;j++)
	{
		histogramIterator=i*HISTOGRAM_PER_PAGE+j;
		if(histogramIterator>=totalNumber)
		{
			break;
		}
		histogramBound=DatumGetInt32(histogramBounds[histogramIterator]);
		off=PageAddItem(page, (Item) (&histogramBound), sizeof(int), InvalidOffsetNumber,false, false);
	//	elog(LOG,"I am putting histogram %d at %d freespace %d",histogramBound,off,PageGetFreeSpace(page));
	}
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
	}

}
int get_histogram_totalNumber(Relation idxrel,BlockNumber startBlock)
{
//	elog(LOG,"Start to get histogram totalNumber");
	Buffer buffer;
	Page page;
	char* diskTuple;
	int totalNumber;
	buffer=ReadBuffer(idxrel,startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	diskTuple=(Item)PageGetItem(page,PageGetItemId(page,1));
	memcpy(&totalNumber,diskTuple,sizeof(int));
//	elog(LOG,"Got histogram totalNumber %d",totalNumber);
	UnlockReleaseBuffer(buffer);
	return totalNumber;
}
int get_histogram(Relation idxrel,BlockNumber startblock,int histogramPosition)
{
	//elog(LOG,"Start to get one histogram at position %d",histogramPosition);
	Buffer buffer;
	Page page;
	char* diskTuple;
	int value;
	BlockNumber blockNumber=histogramPosition/HISTOGRAM_PER_PAGE+startblock;
	Offset off=histogramPosition%HISTOGRAM_PER_PAGE+1;
	if(blockNumber==startblock)
	{
		off++;
	}
	buffer=ReadBuffer(idxrel,blockNumber);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
//	elog(LOG,"I am reading block %d offset %d",blockNumber,off);
	diskTuple=(Item)PageGetItem(page,PageGetItemId(page,off));
	memcpy(&value,diskTuple,sizeof(int));
//	elog(LOG,"Got histogram value %d",value);
	UnlockReleaseBuffer(buffer);
	return value;
}
void gsininit(Buffer b)
{
	Page		page;
	Size		pageSize;
	GsinTupleLong *gsinTupleLong;
	pageSize = BufferGetPageSize(b);
	page = BufferGetPage(b);
	//sizeof(BlockNumber*2)

	/* Initialize a page using the size of GsinTupleLong */
	PageInit(page, pageSize, sizeof(BlockNumber)*2+offsetof(GridList,grids));
//	elog(LOG, "Initialized Page");
	gsinTupleLong=(GsinTupleLong *)PageGetContents(page);
	//gsinTupleLong->gs_PageStart=0;
	//gsinTupleLong->gs_PageNum=0;
}

void gsininit_special(Buffer b)
{
	Page		page;
	Size		pageSize;
	//GsinTupleLong *gsinTupleLong;
	pageSize = BufferGetPageSize(b);
	page = BufferGetPage(b);
	//sizeof(BlockNumber*2)

	/* Initialize a page using the size of GsinTupleLong */
	PageInit(page, pageSize, ItemPointerSize);
//	elog(LOG, "Initialized Page");
	//gsinTupleLong=(GsinTupleLong *)PageGetContents(page);
	//gsinTupleLong->gs_PageStart=0;
	//gsinTupleLong->gs_PageNum=0;
}
void binary_search_histogram_ondisk(searchResult *histogramMatchData, Relation idxrel, Datum value,BlockNumber startBlock,int totalNumber)
{
	int histogramBoundsNum=totalNumber;
	int min=0,max=histogramBoundsNum-1,equalFlag,equalPosition,guess;
						histogramMatchData->index=-9999;
						histogramMatchData->numberOfGuesses=0;
						equalFlag=-1;
	int maxHistogramBound=get_histogram(idxrel,startBlock,max);
	int minHistogramBound=get_histogram(idxrel,startBlock,min);
						if(DatumGetInt32(value)>maxHistogramBound)
						{
							histogramMatchData->index=HISTOGRAM_OUT_OF_BOUNDARY;
							return;
						}
						if(DatumGetInt32(value)<minHistogramBound)
						{
							histogramMatchData->index=HISTOGRAM_OUT_OF_BOUNDARY;
							return;
						}
						while (min<=max) {

						        guess = (min + max) / 2;
						        histogramMatchData->numberOfGuesses++;
						        int result=get_histogram(idxrel,startBlock,guess);

						        if(result>DatumGetInt32(value))
						        {
						        	max=guess-1;
						        }
						        else
						        {
						        	min=guess+1;
						        }

						    }
						if(min==0)
						{
							histogramMatchData->index=0;
						}
						else
						{
						histogramMatchData->index=min-1;
						}
/*
						if(histogramMatchData->index<histogramBoundsNum-1)
						{
							if(DatumGetInt32(value)>=get_histogram(idxrel,startBlock,histogramMatchData->index+1)||DatumGetInt32(value)<get_histogram(idxrel,startBlock,histogramMatchData->index))
							{
								elog(ERROR,"Out of histogram bounds");
							}

						}
						else
						{
							if(histogramMatchData->index>histogramBoundsNum-1)
							{
								elog(ERROR,"Didn't find histogram");
							}
						}
						if(DatumGetInt32(value)==2000)
						{
							elog(LOG,"1000 Search histogram %d",histogramMatchData->index);
						}*/
	//					elog(LOG,"Binary search disk histogram is done");
						return;
}

void binary_search_histogram(searchResult *histogramMatchData,int histogramBoundsNum,Datum *histogramBounds, Datum value)
{
	int min=0,max=histogramBoundsNum-1,equalFlag,equalPosition,guess;
						histogramMatchData->index=-9999;
						histogramMatchData->numberOfGuesses=0;
						equalFlag=-1;
				        if((int)histogramBounds[min]==DatumGetInt32(value))
				        {
				        	equalFlag=min;

				        }
				        if((int)histogramBounds[max]==DatumGetInt32(value))
				        {
				        	equalFlag=max;

				        }

						while (min<=max) {

						        guess = (min + max) / 2;
						        histogramMatchData->numberOfGuesses++;



						      /*  if((int)histogramBounds[guess]==DatumGetInt32(value))
						        {
						        	equalFlag=guess;
						        	break;
						        }*/
						        if((int)histogramBounds[guess]>DatumGetInt32(value))
						        {
						        	max=guess-1;
						        }
						        else
						        {
						        	min=guess+1;
						        }

						    }
						if(min==0)
						{
							histogramMatchData->index=0;
						}
						else
						{
						histogramMatchData->index=min-1;
						}
						/*
						if(histogramMatchData->index<histogramBoundsNum-1)
						{
							if(DatumGetInt32(value)>=(int)histogramBounds[histogramMatchData->index+1]||DatumGetInt32(value)<(int)histogramBounds[histogramMatchData->index])
							{
								elog(ERROR,"Out of histogram bounds");
							}
							if(histogramMatchData->index>histogramBoundsNum-1)
							{
								elog(ERROR,"Didn't find histogram");
							}
						}
						if(DatumGetInt32(value)==2000)
						{
							elog(LOG,"1000 histogram %d",histogramMatchData->index);
						}*/
						//elog(LOG,"Histogram %d lower %d higher %d value %d",histogramMatchData->index,(int)histogramBounds[histogramMatchData->index],(int)histogramBounds[histogramMatchData->index+1],DatumGetInt32(value));

}

/*
 * Return whether gsin can do a samepage update.
 */
bool
gsin_can_do_samepage_update(Buffer buffer, Size origsz, Size newsz)
{
	return
		((newsz <= origsz) ||
				PageGetFreeSpace(BufferGetPage(buffer)) >= (newsz - origsz));
}
void update_sorted_list_tuple(Relation idxrel,int index_tuple_id,BlockNumber diskBlock,OffsetNumber diskOffset,BlockNumber startBlock)
{
	int listBlock=index_tuple_id/SORTED_LIST_TUPLES_PER_PAGE;
	OffsetNumber listOffset=index_tuple_id%SORTED_LIST_TUPLES_PER_PAGE;
	Buffer buffer;
	Page page;
	OffsetNumber tempOff;
	GsinItemPointer gsinItemPointer;
	int len=0;
	char *sortedListDiskTuple=palloc(ItemPointerSize);
//	gsinItemPointer.bi_hi=diskBlock >> 16;
//	gsinItemPointer.bi_lo=diskBlock & 0xffff;
	gsinItemPointer.blockNumber=diskBlock;
	gsinItemPointer.ip_posid=diskOffset;

	buffer=ReadBuffer(idxrel,listBlock+startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	if((listBlock+startBlock)==startBlock)
	{
		listOffset+=3;
	}
	else
	{
		listOffset+=1;
	}
	PageIndexDeleteNoCompact(page,&listOffset,1);
	/*memcpy(sortedListDiskTuple,&(gsinItemPointer.bi_hi),sizeof(uint16));
	len+=sizeof(uint16);
	memcpy(sortedListDiskTuple+len,&(gsinItemPointer.bi_lo),sizeof(uint16));
	len+=sizeof(uint16);
	memcpy(sortedListDiskTuple+len,&(gsinItemPointer.ip_posid),sizeof(OffsetNumber));
	len+=sizeof(OffsetNumber);*/
	serializeSortedListTuple(&gsinItemPointer,sortedListDiskTuple);
	elog(LOG,"update list block %d off %d new pointer point block %d off %d",listBlock+startBlock,listOffset,gsinItemPointer.blockNumber,gsinItemPointer.ip_posid);
	tempOff=PageAddItem(page, (Item) sortedListDiskTuple, ItemPointerSize, listOffset,true, false);
	pfree(sortedListDiskTuple);
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
}
void put_sorted_list_pages(Relation idxrel,BlockNumber sorted_list_pages)
{
	Buffer buffer=ReadBuffer(idxrel,0);
	Page page=BufferGetPage(buffer);
	BlockNumber list_pages=sorted_list_pages&MaxBlockNumber;
	char* diskTuple=palloc(sizeof(BlockNumber));
	OffsetNumber tempOff;
	memcpy(diskTuple,&sorted_list_pages,sizeof(BlockNumber));
	LockBuffer(buffer,BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
//	elog(LOG,"Going to put index pages number %d on disk, free space %d",list_pages,PageGetFreeSpace(page));
	tempOff=PageAddItem(page, (Item) (&list_pages), sizeof(BlockNumber), InvalidOffsetNumber,false, false);
//	elog("Put sorted list pages on disk off %d",tempOff);
	pfree(diskTuple);
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
}
void get_sorted_list_pages(Relation idxrel,BlockNumber *sorted_list_pages,BlockNumber startBlock)
{
	Buffer buffer=ReadBuffer(idxrel,startBlock);
	Page page=BufferGetPage(buffer);
	char* diskTuple;
	LockBuffer(buffer,BUFFER_LOCK_SHARE);
	diskTuple=(Item)PageGetItem(page,PageGetItemId(page,2));
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
	memcpy(sorted_list_pages,diskTuple,sizeof(BlockNumber));
}
void add_new_sorted_list_tuple(Relation idxrel,int totalIndexTupleNumber,BlockNumber diskBlock,OffsetNumber diskOffset,BlockNumber startBlock)
{
	int listBlock=totalIndexTupleNumber/SORTED_LIST_TUPLES_PER_PAGE;
//	int listOffset=totalIndexTupleNumber%SORTED_LIST_TUPLES_PER_PAGE;
	Buffer buffer;
	Page page;
	GsinItemPointer gsinItemPointer;
	int len=0;
	char *sortedListDiskTuple=palloc(ItemPointerSize);
//	gsinItemPointer.bi_hi=diskBlock >> 16;
//	gsinItemPointer.bi_lo=diskBlock & 0xffff;
	gsinItemPointer.blockNumber=diskBlock;
	gsinItemPointer.ip_posid=diskOffset;
//	elog(LOG,"I am going to add a new item pinter it has offset %d",gsinItemPointer.ip_posid);
	buffer=ReadBuffer(idxrel,listBlock+startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
/*
  	if(listBlock==0)
	{
		listOffset+=1;
	}
*/
//	PageIndexDeleteNoCompact(page,&listOffset,1);
/*	memcpy(sortedListDiskTuple,&(gsinItemPointer.bi_hi),sizeof(uint16));
	len+=sizeof(uint16);
	memcpy(sortedListDiskTuple+len,&(gsinItemPointer.bi_lo),sizeof(uint16));
	len+=sizeof(uint16);
	memcpy(sortedListDiskTuple+len,&(gsinItemPointer.ip_posid),sizeof(OffsetNumber));
	len+=sizeof(OffsetNumber);*/
	serializeSortedListTuple(&gsinItemPointer,sortedListDiskTuple);
	PageAddItem(page, (Item) sortedListDiskTuple, ItemPointerSize, InvalidOffsetNumber,false, false);
	pfree(sortedListDiskTuple);
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
	AddTotalIndexTupleNumber(idxrel,startBlock);
}
/*
 * Return 1 means targetHeapBlock is larger than this position
 * Return -1 means targetHeapBlock is smaller than this position
 * Return 0 means targetHeapBlock belongs to this position
 */
int check_index_position(Relation idxrel,int index_tuple_id,BlockNumber targetHeapBlock,GsinTupleLong *gsinTupleLong,BlockNumber* diskBlock, OffsetNumber* diskOffset,BlockNumber startBlock)
{
//	elog(LOG, "start to check index tuple  %d",index_tuple_id);
	int listBlock=index_tuple_id/SORTED_LIST_TUPLES_PER_PAGE;
	OffsetNumber listOffset=index_tuple_id%SORTED_LIST_TUPLES_PER_PAGE;
	GsinItemPointer gsinItemPointer;
//	uint16 blk_hi;
//	uint16 blk_lo;
	Buffer buffer;
	Page page;
	Size itemsz;
	char* listDiskTuple;
	char* indexDiskTuple;
//	elog(LOG, "check listBlock %d",listBlock+startBlock);
//	elog(LOG, "check listOffset %d",listOffset);
	buffer=ReadBuffer(idxrel,listBlock+startBlock);
//	elog(LOG, "2 check listBlock %d",listBlock);
	page=BufferGetPage(buffer);
//	elog(LOG, "check_index_position 2");
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	if(listBlock==0)
	{
		listDiskTuple=(Item)PageGetItem(page,PageGetItemId(page,listOffset+3));
//		elog(LOG, "check_index_position listBlock %d listOffset %d",listBlock+startBlock,listOffset+3);
	}
	else
	{
		listDiskTuple=(Item)PageGetItem(page,PageGetItemId(page,listOffset+1));
//		elog(LOG, "check_index_position listBlock %d listOffset %d",listBlock+startBlock,listOffset+1);
	}
//	elog(LOG, "check_index_position 3");
	UnlockReleaseBuffer(buffer);
/*	memcpy(&blk_hi,listDiskTuple,sizeof(uint16));
	memcpy(&blk_lo,listDiskTuple+sizeof(uint16),sizeof(uint16));
	memcpy(diskOffset,listDiskTuple+sizeof(uint16)*2,sizeof(OffsetNumber));
*/
	deserializeSortedListTuple(&gsinItemPointer,listDiskTuple);
//	elog(LOG, "check_index_position 4");
	/*
	 * Deserialize the BlockNumber
	 */
	//*diskBlock=(BlockNumber)((blk_hi << 16) | ((uint16) blk_lo));
	*diskBlock=gsinItemPointer.blockNumber;
	*diskOffset=gsinItemPointer.ip_posid;
//	elog(LOG, "check_index_position  disk block %d",*diskBlock);
	buffer=ReadBuffer(idxrel,*diskBlock);
	page=BufferGetPage(buffer);
//	elog(LOG, "check_index_position 6");
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	indexDiskTuple=(Item)PageGetItem(page,PageGetItemId(page,*diskOffset));
	UnlockReleaseBuffer(buffer);
//	elog(LOG, "check_index_position 7");
	gsin_form_memtuple(gsinTupleLong,indexDiskTuple,&itemsz);

//	elog(LOG, "check_index_position 8");
//	elog(LOG, "end to check_index_position for %d",targetHeapBlock);
	if(targetHeapBlock>=gsinTupleLong->gs_PageStart&&targetHeapBlock<=gsinTupleLong->gs_PageNum)
	{

//		elog(LOG, "Return 0");
		return 0;
	}
	else if (targetHeapBlock<gsinTupleLong->gs_PageStart)
	{
//		elog(LOG, "Return -1");
		return -1;
	}
	else
	{
//		elog(LOG, "end to check_index_position return 1");
//		elog(LOG, "Return 1");
		return 1;
	}

}
/*
 * Return true means find this heap block. Return false means cannot find this heap block
 */
int GetTotalIndexTupleNumber(Relation idxrel,BlockNumber startBlock)
{
	Buffer buffer;
	Page page;
	char *listDiskTuple;
	int totalIndexTupleNumber;
	buffer=ReadBuffer(idxrel,startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	listDiskTuple=PageGetItem(page,PageGetItemId(page,1));
	memcpy(&totalIndexTupleNumber,listDiskTuple,sizeof(int));
	UnlockReleaseBuffer(buffer);
	return totalIndexTupleNumber;
}
void AddTotalIndexTupleNumber(Relation idxrel,BlockNumber startBlock)
{
	Buffer buffer;
	Page page;
	char *listDiskTuple;
	char *newListDiskTuple=palloc(sizeof(int));
	OffsetNumber FirstOffset=1;
	int totalIndexTupleNumber;
	buffer=ReadBuffer(idxrel,startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	listDiskTuple=PageGetItem(page,PageGetItemId(page,1));
	UnlockReleaseBuffer(buffer);
	memcpy(&totalIndexTupleNumber,listDiskTuple,sizeof(int));
	totalIndexTupleNumber+=1;
	memcpy(newListDiskTuple,&totalIndexTupleNumber,sizeof(int));
	buffer=ReadBuffer(idxrel,startBlock);
	page=BufferGetPage(buffer);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	PageIndexDeleteNoCompact(page,&FirstOffset,1);
	PageAddItem(page,(Item)newListDiskTuple,sizeof(int),1,true,false);
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
}
bool binary_search_sorted_list(Relation heaprel,Relation idxrel,int totalIndexTupleNumber,BlockNumber targetHeapBlock,int *resultPosition,GsinTupleLong *gsinTupleLong,BlockNumber* diskBlock, OffsetNumber* diskOffset,BlockNumber startBlock)
{
//	int totalIndexTupleNumber=GetTotalIndexTupleNumber(idxrel);
//	elog(LOG, "start to binary search sorted list");
	int min=0,max=totalIndexTupleNumber-1,equalFlag,equalPosition,guess;
	int guessTimes=0;
	int checkResult;
	int i;
	equalFlag=-1;
//	elog(LOG, "total number heap blocks %d target %d",RelationGetNumberOfBlocks(heaprel),targetHeapBlock);
	if(check_index_position(idxrel,max,targetHeapBlock,gsinTupleLong,diskBlock,diskOffset,startBlock)==0)
	{
		*resultPosition=max;
	//	elog(LOG, "Use last index tuple");
		return true;
	}
	else if(targetHeapBlock==(RelationGetNumberOfBlocks(heaprel)-1))
	{
	//	elog(LOG, "Create new index tuple");
		return false;
	}
	if(check_index_position(idxrel,min,targetHeapBlock,gsinTupleLong,diskBlock,diskOffset,startBlock)==0)
	{
		*resultPosition=min;
		return true;
	}
	while (max>=min) {
		guessTimes+=1;
		guess = (min + max) / 2;
		checkResult=check_index_position(idxrel,guess,targetHeapBlock,gsinTupleLong,diskBlock,diskOffset,startBlock);
		if(checkResult==0)
		{
			equalFlag=1;
			*resultPosition=guess;
			break;
		}
		if(checkResult==-1)
		{
			max=guess-1;
		}
		else
		{
			min=guess+1;
		}
	}
	elog(LOG,"I guessed %d times and find %d",guessTimes,equalFlag);
	if(equalFlag<0)
	{
		return false;
	}
	else
	{
		return true;
	}
//	elog(LOG, "end to binary search sorted list");
}

/*
 * GSIN insert
 * A tuple in the heap is being inserted.  To keep a gsin index up to date,
 * we need to obtain the relevant index tuple and compare its stored values
 * with those of the new tuple.  If the tuple values are not consistent with
 * the summary tuple, we need to update the index tuple.
 *
 * If the range is not currently summarized, there's nothing to do.
 */
Datum
gsininsert(PG_FUNCTION_ARGS)
{
	elog(LOG,"gsin insert start");
	Relation	idxRel = (Relation) PG_GETARG_POINTER(0);
	Datum	   *values = (Datum *) PG_GETARG_POINTER(1);
	bool	   *nulls = (bool *) PG_GETARG_POINTER(2);
	ItemPointer heaptid = (ItemPointer) PG_GETARG_POINTER(3);
	Relation heapRelation=PG_GETARG_POINTER(4);
	IndexInfo *indexInfo=BuildIndexInfo(idxRel);
	struct FormData_pg_index *heapInfo=idxRel->rd_index;
	/*
	 * Parameters defined by gsin itself
	 */
	BlockNumber startBlk;
	BlockNumber volume;
	BlockNumber heapBlk,totalblocks;
	Buffer currentBuffer,buffer;
	Page page;
	OffsetNumber currentOffset,lastIndexTupleOffset;
	OffsetNumber maxOffset;
	BlockNumber i=0;
	IndexTuple diskTuple, lastDiskTuple;
	GsinTupleLong gsinTupleLong,lastGsinTupleLong;
	Size itemsz;
	HeapTuple histogramTuple;
	AttrNumber attrNum=indexInfo->ii_KeyAttrNumbers[0];
	bool seekFlag=false;
	int j,gridNumber,lastIndexTupleBlockNumber;
	Datum *histogramBounds;
	BlockNumber indexDiskBlock;
	int histogramBoundsNum=get_histogram_totalNumber(idxRel,0);
	BlockNumber histogramPages=get_histogram_totalNumber(idxRel,0)/HISTOGRAM_PER_PAGE;
	OffsetNumber indexDiskOffset;
	int resultPosition;
	int totalIndexTupleNumber=GetTotalIndexTupleNumber(idxRel,histogramPages+1);

	heapBlk = ItemPointerGetBlockNumber(heaptid);
//	elog(LOG, "Heap block numbers is %d",heapBlk);
	/*
	 * Check histograms to see which one contains this heap tuple
	 */
//	histogramTuple=SearchSysCache2(STATRELATTINH,ObjectIdGetDatum(heapInfo->indrelid),Int16GetDatum(attrNum));
//	if (HeapTupleIsValid(histogramTuple))
//	{
//		elog(LOG, "Insert Got stattuple");
//		get_attstatsslot(histogramTuple,get_atttype(heapInfo->indrelid, attrNum),get_atttypmod(heapInfo->indrelid,attrNum),STATISTIC_KIND_HISTOGRAM, InvalidOid,NULL,&histogramBounds, &histogramBoundsNum,NULL, NULL);
//	}
//	histogramBoundsNum=10000;
//	ReleaseSysCache(histogramTuple);

			/*
			 * Binary search histogram
			 */
					searchResult histogramMatchData;
					binary_search_histogram_ondisk(&histogramMatchData,idxRel,values[0],0,histogramBoundsNum);
	//				elog(LOG, "Receive histogram match data %d",histogramMatchData.index);
	/*
	 * This nested loop is for seeking the disk tuple which contains the heap tuple
	 */
					//histogramMatchData.index=10;
	lastIndexTupleBlockNumber=-1;
	totalblocks=RelationGetNumberOfBlocks(idxRel);
//	elog(LOG, "Initial index block numbers is %d, I am checking for heapblock %d",totalblocks,heapBlk);

	if(binary_search_sorted_list(heapRelation,idxRel,totalIndexTupleNumber,heapBlk,&resultPosition,&gsinTupleLong,&indexDiskBlock,&indexDiskOffset,histogramPages+1)==true)
	{
		seekFlag=true;
	}
	else
	{
		seekFlag=false;
		check_index_position(idxRel,totalIndexTupleNumber-1,heapBlk,&gsinTupleLong,&indexDiskBlock,&indexDiskOffset,histogramPages+1);
	}
//	elog(LOG,"Result position is %d",resultPosition);
//	elog(LOG, "Traverse index tuple is done");
	/*
	 * Update the memory tuple
	 */

	if(seekFlag==true){
		/*
		 * The inserted heap tuple belongs to one index tuple
		 */
	//	elog(LOG, "Seek flag is true, match grid is %d",histogramMatchData.index);

		if(bitmap_get(gsinTupleLong.originalBitset,histogramMatchData.index)==false)
		{
			/*
			 * Before update the memory tuple,copy the old memory tuple
			 */
		//	elog(LOG, "We need to update index");
			IndexTupleData *newDiskTuple;

			Size oldsize,newsize;
			bool samePageUpdate=true;
			GsinTupleLong newGsinTupleLong;
			copy_gsin_mem_tuple(&newGsinTupleLong,&gsinTupleLong);
	//		elog(LOG, "Old mem tuple start %d volume %d delete flag %d",gsinTupleLong.gs_PageStart,gsinTupleLong.gs_PageNum,gsinTupleLong.deleteFlag);
			gsin_form_indextuple(&gsinTupleLong,&oldsize,histogramBoundsNum);
	//		elog(LOG, "form one old index disk tuple");
			bitmap_set(newGsinTupleLong.originalBitset,histogramMatchData.index);
	//		elog(LOG, "update mem tuple");
			/*
			 *If gsin can do samepage update, go ahead and do it.If not, get new buffer, new page and insert them.
			 */
			newDiskTuple=gsin_form_indextuple(&newGsinTupleLong,&newsize,histogramBoundsNum);
	//		elog(LOG, "form one new index disk tuple");
			/*
			 * Check whether current index page has enough space. If so, go ahead and put it on disk.
			 * Otherwise, ask for new buffer and page.
			 */
			buffer=ReadBuffer(idxRel,indexDiskBlock);
			if(gsin_can_do_samepage_update(buffer,oldsize,newsize))
			{
				samePageUpdate=true;

			}
			else
			{
				samePageUpdate=false;
				ReleaseBuffer(buffer);
			}

			/*
			 *	Delete the old index tuple from disk
			 */

			if(samePageUpdate==true)
			{
				/*
				 * Do samepage update
				 */
				//elog(LOG, "do same page update");
				OffsetNumber testOff;
				page=BufferGetPage(buffer);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

				START_CRIT_SECTION();
//				elog(LOG,"We are going to delete %d and update at the same page",indexDiskOffset);
				PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
//				elog(LOG,"Delete is done");
//				elog(LOG, "old index tuple is deleted at %d",indexDiskOffset);
				testOff=PageAddItem(page,(Item)newDiskTuple,newsize,indexDiskOffset,true,false);
				if(testOff==InvalidOffsetNumber)
				{
					elog(ERROR, "failed to add GSIN tuple same page");
				}
//				elog(LOG, "new index tuple has been placed at off %d",testOff);
				MarkBufferDirty(buffer);
				END_CRIT_SECTION();
				UnlockReleaseBuffer(buffer);
			}
			else
			{
				/*
				 * Create a new page and insert disk tuple
				 */
	//			elog(LOG, "Ask for new pages");

				Buffer newInsertBuffer;
				Page newInsertPage;
				BlockNumber newBlockNumber;
				OffsetNumber newOffsetNumber;
				if(indexDiskBlock<totalblocks-1){
	//			elog(LOG, "Check whether the last index page has space");
				currentBuffer=ReadBuffer(idxRel,indexDiskBlock);
				page=BufferGetPage(currentBuffer);
				LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
				START_CRIT_SECTION();
//				elog(LOG,"We are going to delete %d and update at a different page",indexDiskOffset);
				PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
//				elog(LOG,"Done");
				MarkBufferDirty(currentBuffer);
				END_CRIT_SECTION();
				UnlockReleaseBuffer(currentBuffer);
				newInsertBuffer=ReadBuffer(idxRel,totalblocks-1);
				newInsertPage = BufferGetPage(newInsertBuffer);
				if(PageGetFreeSpace(newInsertPage)<newsize)
				{
					ReleaseBuffer(newInsertBuffer);
					gsin_getinsertbuffer(idxRel);
					newInsertBuffer=ReadBuffer(idxRel,totalblocks);
					newInsertPage = BufferGetPage(newInsertBuffer);

				}
				newBlockNumber=BufferGetBlockNumber(newInsertBuffer);
				LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
				START_CRIT_SECTION();
				newOffsetNumber=PageAddItem(newInsertPage, (Item) newDiskTuple, newsize, InvalidOffsetNumber,false, false);
	//			elog(LOG,"New offset is  %d",newOffsetNumber);
				if(newOffsetNumber==InvalidOffsetNumber)
				{
					elog(ERROR, "failed to add GSIN tuple new page");
				}
				MarkBufferDirty(newInsertBuffer);
				END_CRIT_SECTION();
				UnlockReleaseBuffer(newInsertBuffer);
				update_sorted_list_tuple(idxRel,resultPosition,newBlockNumber,newOffsetNumber,histogramPages+1);
				}
				else
				{
	//				elog(LOG, "Last index page has no space. Get a new page");
					currentBuffer=ReadBuffer(idxRel,indexDiskBlock);

					page=BufferGetPage(currentBuffer);
					LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
	//				elog(LOG,"We are going to delete %d and update at a different page",indexDiskOffset);
					PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
	//				elog(LOG,"Done");
					MarkBufferDirty(currentBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(currentBuffer);
					gsin_getinsertbuffer(idxRel);
					newInsertBuffer=ReadBuffer(idxRel,totalblocks);
					newBlockNumber=totalblocks;
					newInsertPage = BufferGetPage(newInsertBuffer);
					LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
					newOffsetNumber=PageAddItem(newInsertPage, (Item) newDiskTuple, newsize, InvalidOffsetNumber,
											  false, false);
		//			elog(LOG,"New block~~ is  %d",newBlockNumber);
		//			elog(LOG,"New offset~~ is  %d",newOffsetNumber);
		//			elog(LOG,"New offset is  %d",newOffsetNumber);
					if(newOffsetNumber==InvalidOffsetNumber)
					{
						elog(ERROR, "failed to add GSIN tuple new page");
					}
					MarkBufferDirty(newInsertBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(newInsertBuffer);
		//			elog(LOG,"I am updating the pointer at Result position is %d",resultPosition);
					update_sorted_list_tuple(idxRel,resultPosition,newBlockNumber,newOffsetNumber,histogramPages+1);
				}


			}


		}
		else
		{
			/*
			 * The new inserted value doesn't change the original index tuple, thus do nothing.
			 */
		//	elog(LOG, "We need to do nothing");
			//UnlockReleaseBuffer(currentBuffer);
		}
		//bitmap_free(gsinTupleLong.originalBitset);
		ewah_free(gsinTupleLong.compressedBitset);
	}
	else
	{
	//	elog(LOG, "Seek flag is false");
		/*
		 * The inserted heap tuple doesn't belong to one index tuple. First check whether the
		 * last index (the index contains the last heap page) is full. If full, create a new index tuple. If not full, go ahead and merge
		 * its page into this tuple.
		 */
		/*
		 * Get grid list density
		 */
/*
		gridNumber=0;
		for(j=0;j<histogramBoundsNum-1;j++)
		{
			if(bitmap_get(gsinTupleLong.originalBitset,j)==true)
			{
				gridNumber++;
			}
		}
*/
		/*
		 *Check whether this tuple is full. If full, ask for new buffer and page. If not,delete the old one and insert new one.
		 */
//		elog(LOG,"The volume of the last tuple is %d histogramnum %d percent %d",gsinTupleLong.deleteFlag,histogramBoundsNum,GsinGetMaxPagesPerRange(idxRel));
		if((gsinTupleLong.deleteFlag*1.0/histogramBoundsNum-1)>=(GsinGetMaxPagesPerRange(idxRel)*1.00/100))
		{
			elog(LOG, "The last index tuple is full. We need to insert one more");
			/*
			 * Full. Keep the last index tuple there and create new buffer, page and last index tuple.
			 */
			GsinTupleLong newLastIndexTuple;
			char *newLastIndexDiskTuple;
			Size itemsize;
			Buffer newInsertBuffer;
			Page newInsertPage;
			BlockNumber newBlock;
			OffsetNumber newOffsetNumber;
			newLastIndexTuple.gs_PageStart=heapBlk;
			newLastIndexTuple.gs_PageNum=heapBlk;
			newLastIndexTuple.deleteFlag=1;
//			newLastIndexTuple.deleteFlag=CLEAR_INDEX_TUPLE;
			newLastIndexTuple.originalBitset=bitmap_new();
			bitmap_set(newLastIndexTuple.originalBitset,histogramMatchData.index);
			newLastIndexDiskTuple=gsin_form_indextuple(&newLastIndexTuple,&itemsize,histogramBoundsNum);
			newInsertBuffer=ReadBuffer(idxRel,totalblocks-1);
			newInsertPage=BufferGetPage(newInsertBuffer);
		//	elog(LOG,"Last page free space is %d",PageGetFreeSpace(newInsertPage));
			if(PageGetFreeSpace(newInsertPage)<itemsize)
			{
				ReleaseBuffer(newInsertBuffer);
				gsin_getinsertbuffer(idxRel);
				newInsertBuffer=ReadBuffer(idxRel,totalblocks);
				newInsertPage=BufferGetPage(newInsertBuffer);
			}
			newBlock=BufferGetBlockNumber(newInsertBuffer);
			LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
			START_CRIT_SECTION();
			newOffsetNumber=PageAddItem(newInsertPage,(Item)newLastIndexDiskTuple,itemsize,InvalidOffsetNumber,false,false);
		//	elog(LOG, "We insert the last index tuple at block %d offset %d",BufferGetBlockNumber(newInsertBuffer),newOffsetNumber);
			if(newOffsetNumber==InvalidOffsetNumber)
			{
				elog(ERROR, "failed to update GSIN last index tuple same page");
			}
			MarkBufferDirty(newInsertBuffer);
			END_CRIT_SECTION();
			UnlockReleaseBuffer(newInsertBuffer);
			add_new_sorted_list_tuple(idxRel,totalIndexTupleNumber,newBlock,newOffsetNumber,histogramPages+1);
			//bitmap_free(gsinTupleLong.originalBitset);
			ewah_free(gsinTupleLong.compressedBitset);
		}
		else
		{
			/*
			 * Not full. Update last index tuple. Check whether we can do samepage insert. If not, ask for new buffer and page.
			 */
//			elog(LOG, "The last tuple is not full.  Lets update it");
			IndexTupleData *newDiskTuple;
			Size oldsize,newsize;
			GsinTupleLong newLastGsinTupleLong;
			bool samePageUpdate=true;
			copy_gsin_mem_tuple(&newLastGsinTupleLong,&gsinTupleLong);
			gsin_form_indextuple(&gsinTupleLong,&oldsize,histogramBoundsNum);
			newLastGsinTupleLong.gs_PageNum=heapBlk;;
			newLastGsinTupleLong.deleteFlag++;
			bitmap_set(newLastGsinTupleLong.originalBitset,histogramMatchData.index);
			newDiskTuple=gsin_form_indextuple(&newLastGsinTupleLong,&newsize,histogramBoundsNum);;
			currentBuffer=ReadBuffer(idxRel,indexDiskBlock);
			page=BufferGetPage(currentBuffer);
			if(gsin_can_do_samepage_update(currentBuffer,oldsize,newsize))
			{
				samePageUpdate=true;
			}
			else
			{
				samePageUpdate=false;
				ReleaseBuffer(currentBuffer);
			}
			/*
			 *	Delete the old index tuple from disk
			 */

			if(samePageUpdate==true)
			{
				/*
				 * Do samepage update
				 */
				LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
				START_CRIT_SECTION();
				PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
				if(PageAddItem(page,(Item)newDiskTuple,newsize,indexDiskOffset,true,false)==InvalidOffsetNumber)
				{
					elog(ERROR, "failed to add GSIN last index tuple same page");
				}
				MarkBufferDirty(currentBuffer);
				END_CRIT_SECTION();
				UnlockReleaseBuffer(currentBuffer);
			}
			else
			{
				/*
				 * Create a new page and insert disk tuple
				 */
				Buffer newInsertBuffer;
				Page newInsertPage;
				BlockNumber newBlockNumber;
				OffsetNumber newOffsetNumber;

				if(indexDiskBlock<totalblocks-1){
					currentBuffer=ReadBuffer(idxRel,indexDiskBlock);
					page=BufferGetPage(currentBuffer);
					LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
					PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
					MarkBufferDirty(currentBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(currentBuffer);
					newInsertBuffer=ReadBuffer(idxRel,totalblocks-1);
					if(PageGetFreeSpace(newInsertPage)<newsize)
					{
						ReleaseBuffer(newInsertBuffer);
						newInsertBuffer=gsin_getinsertbuffer(idxRel);
						newInsertPage = BufferGetPage(newInsertBuffer);
					}
					newBlockNumber=BufferGetBlockNumber(newInsertBuffer);
					LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
					newOffsetNumber=PageAddItem(newInsertPage, (Item) newDiskTuple, newsize, InvalidOffsetNumber,
												  false, false);
					MarkBufferDirty(newInsertBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(newInsertBuffer);
				}
				else
				{
					currentBuffer=ReadBuffer(idxRel,indexDiskBlock);
					page=BufferGetPage(currentBuffer);
					LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);

					START_CRIT_SECTION();
					PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
					MarkBufferDirty(currentBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(currentBuffer);
					newInsertBuffer=gsin_getinsertbuffer(idxRel);
					newInsertPage = BufferGetPage(newInsertBuffer);
					newBlockNumber=BufferGetBlockNumber(newInsertBuffer);
					LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
					newOffsetNumber=PageAddItem(newInsertPage, (Item) newDiskTuple, newsize, InvalidOffsetNumber,
							  false, false);
					MarkBufferDirty(newInsertBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(newInsertBuffer);
				}




				update_sorted_list_tuple(idxRel,totalIndexTupleNumber-1,newBlockNumber,newOffsetNumber,histogramPages+1);
			}
			//bitmap_free(gsinTupleLong.originalBitset);
			ewah_free(gsinTupleLong.compressedBitset);
		}


	}
	/*
	 *	Update the disk tuple
	 */
	free_attstatsslot(get_atttype(heapInfo->indrelid, attrNum),histogramBounds,histogramBoundsNum,NULL,0);
	elog(LOG,"gsin insert stop");
	return BoolGetDatum(false);
}


/*
 * gsinbulkdelete
 *	GSIN will update itself after each deletion. Note that: the deletion here is not per-tuple deletion. In this deletion, Postgres may delete millions of records in one time.
 * we could mark item tuples as "dirty" (when a minimum or maximum heap
 * tuple is deleted), meaning the need to re-run summarization on the affected
 * range.  Would need to add an extra flag in gsintuple for that.
 */
Datum
gsinbulkdelete(PG_FUNCTION_ARGS)
{
	elog(LOG,"Start one bulkdelete");
	/* other arguments are not currently used */
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void	   *callback_state = (void *) PG_GETARG_POINTER(3);
	BlockNumber startblock;
	BlockNumber sorted_list_pages;
	BlockNumber histogramPages;
	BlockNumber totalHeapBlocks;
	/* allocate stats if first time through, else re-use existing struct */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));


	BlockNumber idxBlkNum=0,heapBlkNum=0;
	Relation idxRelation,heapRelation;
	Oid heapRelationOid;
	int histogramBoundsNum;
	Datum *histogramBounds;
	IndexInfo *indexInfo=BuildIndexInfo(info->index);
	AttrNumber attrNum=indexInfo->ii_KeyAttrNumbers[0];
	HeapTuple histogramTuple;
	idxRelation=info->index;
	heapRelationOid=IndexGetRelation(RelationGetRelid(idxRelation), false);
	heapRelation=relation_open(heapRelationOid, AccessShareLock);
	totalHeapBlocks=RelationGetNumberOfBlocks(heapRelation);
//	heapRelation=heap_open(heapRelationOid, AccessShareLock);
	struct FormData_pg_index *heapInfo=idxRelation->rd_index;
	histogramTuple=SearchSysCache2(STATRELATTINH,ObjectIdGetDatum(heapInfo->indrelid),Int16GetDatum(attrNum));
	if (HeapTupleIsValid(histogramTuple))
	{
		elog(LOG, "Got stattuple");
		get_attstatsslot(histogramTuple,
				get_atttype(heapInfo->indrelid, attrNum),get_atttypmod(heapInfo->indrelid,attrNum),
						STATISTIC_KIND_HISTOGRAM, InvalidOid,
						NULL,
						&histogramBounds, &histogramBoundsNum,
						NULL, NULL);
	}
	ReleaseSysCache(histogramTuple);
/*
 * Set the startblock to bypass the histogram and sorted list
 */
	histogramPages=histogramBoundsNum/HISTOGRAM_PER_PAGE;
	get_sorted_list_pages(idxRelation,&sorted_list_pages,histogramPages+1);
	startblock=sorted_list_pages+histogramPages+1;
	/* allocate stats if first time through, else re-use existing struct */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
/*
 * First loop, traverse index
 */
	elog(LOG,"Start to traverse %d",RelationGetNumberOfBlocks(idxRelation));

	for(idxBlkNum=startblock;idxBlkNum<RelationGetNumberOfBlocks(idxRelation);idxBlkNum++)
	{
		/*
		 * Second loop, traverse the heap tuple inside
		 */
	//	elog(LOG,"Start to traverse one index page %d",idxBlkNum);
		Buffer currentBuffer=ReadBuffer(idxRelation,idxBlkNum);
		Page currentPage=BufferGetPage(currentBuffer);
		OffsetNumber idxTupleOffset,maxOffset;

		GsinTupleLong gsinTupleLong;
		LockBuffer(currentBuffer, BUFFER_LOCK_SHARE);
		Size itemsz;
		bool indexDeleteFlag;

		maxOffset=PageGetMaxOffsetNumber(currentPage);

		IndexTuple currentDiskTuple;
		for(idxTupleOffset=1;idxTupleOffset<=maxOffset;idxTupleOffset++)
		{

		//	elog(LOG,"Start to traverse one index tuple %d",idxTupleOffset);

			currentDiskTuple=(IndexTuple)PageGetItem(currentPage,PageGetItemId(currentPage,idxTupleOffset));
			gsin_form_memtuple(&gsinTupleLong,currentDiskTuple,&itemsz);
			//pfree(currentDiskTuple);
			indexDeleteFlag=false;
			/*
			 * The index tuple will be checked
			 */
		//	elog(LOG,"I am checking one new index tuple");

			for(heapBlkNum=gsinTupleLong.gs_PageStart;heapBlkNum<=gsinTupleLong.gs_PageNum;heapBlkNum++)
			{
				Buffer currentHeapBuffer=ReadBuffer(heapRelation,heapBlkNum);
/*				if(currentHeapBuffer==NULL)
				{
					elog(LOG,"Got NULL heap buffer");
				}*/
				//elog(LOG,"Got buffer %d",currentHeapBuffer);
				Page currentHeapPage=BufferGetPage(currentHeapBuffer);
				Size recordedFreeSpace=GetRecordedFreeSpace(heapRelation,heapBlkNum);
				Size pageFreeSpace=PageGetFreeSpace(currentHeapPage);
				if(pageFreeSpace-recordedFreeSpace>24&&recordedFreeSpace==0)
				{
					indexDeleteFlag=true;
		//			elog(LOG,"This block %d is dirty! by FSM %d and %d",heapBlkNum,pageFreeSpace,recordedFreeSpace);
					ReleaseBuffer(currentHeapBuffer);
					break;
				}

				ReleaseBuffer(currentHeapBuffer);
			}
//			elog(LOG,"I am here");
			if(indexDeleteFlag==true)
			{
			/*
			 * DeleteFalg is true. This index tuple should be updated.
			 */
		//		elog(LOG,"I re-summarize one index tuple");
				UnlockReleaseBuffer(currentBuffer);
				currentBuffer=ReadBuffer(idxRelation,idxBlkNum);
				LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
				bitmap_free(gsinTupleLong.originalBitset);
				gsinTupleLong.originalBitset=bitmap_new();
				Size diskSize;
			//	IndexTuple diskTuple;//=palloc(sizeof);;

		//		elog(LOG,"Go through the heap pages");
			for(heapBlkNum=gsinTupleLong.gs_PageStart;heapBlkNum<=gsinTupleLong.gs_PageNum;heapBlkNum++)
			{
				/*
				 * Traverse each index tuple.
				 */
				Buffer currentHeapBuffer=ReadBuffer(heapRelation,heapBlkNum);
				Page currentHeapPage=BufferGetPage(currentHeapBuffer);

				ItemId lp;
				ItemPointerData itmPtr;

				OffsetNumber maxOffset=PageGetMaxOffsetNumber(currentHeapPage),heapTupleOffset;
//				elog(LOG,"Max off is that %d",maxOffset);
				Datum value;
				bool isnull;//=false;

		//		elog(LOG,"Lock this heap buffer");
				LockBuffer(currentHeapBuffer, BUFFER_LOCK_SHARE);
				for(heapTupleOffset=1;heapTupleOffset<=maxOffset;heapTupleOffset++)
				{
					lp = PageGetItemId(currentHeapPage,heapTupleOffset);
					ItemPointerSet(&itmPtr, heapBlkNum, heapTupleOffset);
					HeapTupleData currentHeapTuple;//=palloc(sizeof(HeapTuple));
					currentHeapTuple.t_data=(HeapTupleHeader)PageGetItem(currentHeapPage,lp);
					currentHeapTuple.t_len=ItemIdGetLength(lp);
					currentHeapTuple.t_tableOid = RelationGetRelid(heapRelation);
					currentHeapTuple.t_self=itmPtr;
					if(HeapTupleIsValid(&currentHeapTuple))
					{
						//elog(LOG,"Got heap tuple att %d",attrNum);
					}
					else{elog(LOG,"Got NULL heap tuple");continue;}

					value=fastgetattr(&currentHeapTuple,attrNum,RelationGetDescr(heapRelation),&isnull);
					//pfree(currentHeapTuple->t_data);
					//pfree(currentHeapTuple);
					searchResult histogramMatchData;
			//		elog(LOG,"Got attribute value %d",DatumGetInt32(value));
					binary_search_histogram(&histogramMatchData,histogramBoundsNum,histogramBounds,value);
					//elog(LOG,"Histogram grid is %d",histogramMatchData.index);
					bitmap_set(gsinTupleLong.originalBitset,histogramMatchData.index);
				}
				UnlockReleaseBuffer(currentHeapBuffer);
			//	elog(LOG,"Unlock this heap buffer");
			}
		//	elog(LOG,"End to traverse");
			/*
							 * Update the index tuple on disk
							 */
							IndexTupleData *diskTuple;
							diskTuple=gsin_form_indextuple(&gsinTupleLong,&diskSize,histogramBoundsNum);
				//			elog(LOG,"Generate one index tuple");
							START_CRIT_SECTION();
							/*PageIndexDeleteNoCompact(currentPage,&idxTupleOffset,1);
							if(PageAddItem(currentPage,(Item)diskTuple,diskSize,idxTupleOffset,true,false)==InvalidOffsetNumber)
							{
									elog(ERROR, "failed to add GSIN last index tuple same page");
							}*/
			//				elog(LOG,"Update one index tuple");
							//MarkBufferDirty(currentBuffer);
							END_CRIT_SECTION();
							UnlockReleaseBuffer(currentBuffer);
							pfree(diskTuple);
							pfree(gsinTupleLong.originalBitset);
							pfree(gsinTupleLong.compressedBitset);
							currentBuffer=ReadBuffer(idxRelation,idxBlkNum);

							LockBuffer(currentBuffer, BUFFER_LOCK_SHARE);
							//relation_close(heapRelation, AccessShareLock);
			//				elog(LOG,"Read buffer again for share lock");
							continue;
			}else
			{

				pfree(gsinTupleLong.originalBitset);
				pfree(gsinTupleLong.compressedBitset);

				continue;
			}
			elog(LOG,"I should not reach this pointer");
		}
		UnlockReleaseBuffer(currentBuffer);
	}
	relation_close(heapRelation, AccessShareLock);
	elog(LOG,"Finish one bulkdelete");
	PG_RETURN_POINTER(stats);
}

/*
 * This routine is in charge of "vacuuming" a GSIN index: we just summarize
 * ranges that are currently unsummarized.
 */
Datum
gsinvacuumcleanup(PG_FUNCTION_ARGS)
{
	/* other arguments are not currently used */
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);

/*
 * We don't actually delete gsin tuple. There is almost nothing to do.
 */


	PG_RETURN_POINTER(stats);
}
Datum
gsinbeginscan(PG_FUNCTION_ARGS)
{
//	elog(LOG,"Scan start");
	Relation	r = (Relation) PG_GETARG_POINTER(0);
	int			nkeys = PG_GETARG_INT32(1);
	int			norderbys = PG_GETARG_INT32(2);
	IndexScanDesc scan = RelationGetIndexScan(r, nkeys, norderbys);

	//elog(LOG, "beginscan is ended");
	PG_RETURN_POINTER(scan);
}

/*
 * Form an memory format tuple from a disk tuple. Decoding...
 */

/*
 * Form an index tuple
 */
void gsin_form_memtuple(GsinTupleLong *gsinTupleLong,IndexTuple diskTuple,Size *memlen)
{
	char	   *ptr=(char *)diskTuple;
	char	    *diskGridSet;
	struct ewah_bitmap *compressedBitset;
	BlockNumber start;
	BlockNumber volume;
	int16 length;
	int			i;
	struct bitmap *originalBitset;
	int16 deleteFlag=0;
	*memlen = 0;
	compressedBitset=ewah_new();
	//elog(LOG,"Start form memory tuple");
	if(diskTuple==NULL)
	{
		elog(LOG,"Transfer NULL diskTuple");
	}
	memcpy(&start,ptr,sizeof(BlockNumber));
	*memlen+=sizeof(BlockNumber);
//	elog(LOG,"start pass %d",start);
	memcpy(&volume,ptr+*memlen,sizeof(BlockNumber));
	*memlen+=sizeof(BlockNumber);
	memcpy(&deleteFlag,ptr+*memlen,sizeof(int16));
	*memlen+=sizeof(int16);
//	elog(LOG,"last page pass %d",volume);
	//memcpy(&length,ptr+*memlen,sizeof(int16));
	//*memlen+=sizeof(int16);
	//elog(LOG,"length pass %d",deleteFlag);
//	gsinTupleLong->grids=palloc(sizeof(int16)*length);
	//elog(LOG,"grid mem assign pass");
/*	for (i = 0; i < length; i++)
	{
		memcpy(&(gsinTupleLong->grids[i]),ptr+*memlen,sizeof(int16));
		*memlen+=sizeof(int16);
		//elog(LOG,"grid pass %d",gsinTupleLong->grids[i]);
	}*/

	ewah_deserialize(compressedBitset,ptr+*memlen);

	//elog(LOG,"Deserialization is done");
	gsinTupleLong->gs_PageStart=start;
	gsinTupleLong->gs_PageNum=volume;
	gsinTupleLong->deleteFlag=deleteFlag;
	//gsinTupleLong->length=length;
	gsinTupleLong->compressedBitset=compressedBitset;
	/*
	 * Test decompressed bitset
	 */
	originalBitset=ewah_to_bitmap(compressedBitset);
	gsinTupleLong->originalBitset=originalBitset;
	//elog(LOG,"Start to print one decompressed tuple");
	//for(i=0;i<100;i++)
	//{
	//	elog(LOG,"grid %d value %d",i,bitmap_get(originalBitset,i));
	//}
	//elog(LOG,"End to print one decompressed tuple");
	//elog(LOG,"mem tuple mem start %d volume %d",start,volume);

}


Datum gsingetbitmap(PG_FUNCTION_ARGS)
{
//	elog(LOG,"GSIN scan start");
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	TIDBitmap  *tbm = (TIDBitmap *) PG_GETARG_POINTER(1);
	Relation	idxRel = scan->indexRelation;
	GsinTupleLong gsinTupleLong;
	Oid			heapOid;
	Relation	heapRel;
	GsinScanState scanstate;//=(GsinScanState *) scan->opaque;
	BlockNumber sorted_list_pages;

	BlockNumber nblocks;
	Size itemsz;
	IndexInfo *indexInfo;
	//int			totalpages = 0;
	bool matchFlag=false;
	bool scankeymatchflag=false;
	int totalPages=0;
	int i,j,counter,k;
	int histogramBoundsNum;//=scanstate->histogramBoundsNum;
	Datum *histogramBounds;//=scanstate->histogramBounds;
	ScanKey keys=scan->keyData;
	HeapTuple heapTuple; /* pg_statistic data */
	Page page;
	IndexTuple diskTuple;
	BlockNumber histogramPages;
	/*
	 * Compressed query bitmap
	 */
	struct ewah_bitmap *compressedBitset;
	struct bitmap *gridBitset;
	/*
	 * Store qualified grids
	 */
	int predicateLen=0;
	int predicateGrids[10000];

	Size queryItemsz;
	//struct ewah_bitmap *ANDresult;
	struct ewah_iterator	scankeyIterator;
	eword_t nextWordScankey;
	eword_t nextWordTuple;
	scan->opaque=&scanstate;
	/*
	 * Currently GSIN supports the numerOfKeys is no larger than 1
	 */
	int nkeys=scan->numberOfKeys;
	//pgstat_count_index_scan(idxRel);


	AttrNumber attrNum;
	struct FormData_pg_index *heapInfo=idxRel->rd_index;

	//elog(LOG,"Scan start to retrieve histogram");
	//scanstate=palloc(sizeof(GsinScanState));
	indexInfo=BuildIndexInfo(idxRel);
	//elog(LOG, "scan indexid is \"%d\" ", idxRel->rd_id);
	scan->opaque=&scanstate;


	scanstate.length=0;
	/*
	 	 *Retrieve histogram information from pg_statistic table
	 	 */



	attrNum=indexInfo->ii_KeyAttrNumbers[0];
	//attrNum=r->ii_KeyAttrNumbers[0];
	//		elog(LOG, "KeyAttrNum is \"%d\" ", attrNum);
	/*
	 * Search cache for pg_statistic info
	 */
			//checkHeap=index_fetch_heap(scan);
			if(heapInfo==NULL)
			{
				elog(LOG, "Heap is null");
			}
			//elog(LOG, "heapid is \"%d\" ", checkHeap->t_tableOid);
/*	heapTuple=SearchSysCache2(STATRELATTINH,ObjectIdGetDatum(heapInfo->indrelid),Int16GetDatum(attrNum));
	elog(LOG, "Got heaptuple");
	if (HeapTupleIsValid(heapTuple))
	{
		elog(LOG, "Got stattuple");
		get_attstatsslot(heapTuple,
				get_atttype(heapInfo->indrelid, attrNum),get_atttypmod(heapInfo->indrelid,attrNum),
						STATISTIC_KIND_HISTOGRAM, InvalidOid,
						NULL,
						&histogramBounds, &histogramBoundsNum,
						NULL, NULL);
	}
	ReleaseSysCache(heapTuple);
	*/
	histogramBoundsNum=get_histogram_totalNumber(idxRel,0);
//	scanstate.histogramBounds=histogramBounds;
	scanstate.histogramBoundsNum=histogramBoundsNum;
	histogramPages=histogramBoundsNum/HISTOGRAM_PER_PAGE;
	get_sorted_list_pages(idxRel,&sorted_list_pages,histogramPages+1);
//	elog(LOG,"Scan got histogram %d",histogramBoundsNum);


	scanstate.currentLookupBuffer=ReadBuffer(scan->indexRelation,sorted_list_pages+histogramPages+1);
//	elog(LOG,"Start to read index on block %d",BufferGetBlockNumber(sorted_list_pages+histogramPages+1));
	if(scanstate.currentLookupBuffer==NULL)
	{
		elog(LOG,"Read NULL buffer");
	}
	LockBuffer(scanstate.currentLookupBuffer, BUFFER_LOCK_SHARE);
	//page=PageGetContents(BufferGetPage(scanstate.currentLookupBuffer));
	page=BufferGetPage(scanstate.currentLookupBuffer);
	if(page==NULL)
	{
		elog(LOG,"Read NULL page");
	}
	diskTuple=(IndexTuple)PageGetItem(page,PageGetItemId(page,1));
	scanstate.maxOffset=PageGetMaxOffsetNumber(page);
	//elog(LOG,"Max offset number is %d",scanstate.maxOffset);
	scanstate.currentLookupPageNum=sorted_list_pages+histogramPages+1;
	scanstate.currentOffset=1;
	scanstate.currentDiskTuple=diskTuple;
	scanstate.length=0;
	//scanstate.currentLookupPagePointer=page;
	//scanstate->gsinTupleLong=*gsinTupleLong_Pointer;
	scanstate.scanHasNext=true;
//	free_attstatsslot(get_atttype(heapInfo->indrelid, attrNum),histogramBounds,histogramBoundsNum,NULL,0);




	bool test[nkeys][histogramBoundsNum];
	bool gridtest[nkeys][histogramBoundsNum];
	/*
	 * Check the scankey with histograms
	 */

		/*
		 * Check whether one histogram bound satisfies scankeys. It is true only under this circumstance that this bound satisfies all the keys.
		 */
	//elog(LOG,"bitmap got histogram %d",histogramBoundsNum);

	/*
	 * Iterate scan key to find matched grids
	 */
	for(k=0;k<nkeys;k++){
	if(keys[k].sk_strategy!=3){
		/*
		 * Handle all the operators <, >, <=, >=, except =
		 */
//		elog(LOG,"Strategy is %d sizeofint %d",keys[k].sk_strategy,sizeof(int));

			//elog(LOG,"Start to match");
			//elog(LOG,"nkeys %d",nkeys);
			//elog(LOG,"strategy %d",keys->sk_strategy);

			//elog(LOG,"Comparasion starts");
			//elog(LOG,"argument %d",keys->sk_argument);
			searchResult histogramMatchData;
			binary_search_histogram_ondisk(&histogramMatchData,idxRel,DatumGetInt32(keys[k].sk_argument),0,histogramBoundsNum);
			if(histogramMatchData.index==HISTOGRAM_OUT_OF_BOUNDARY)
			{
				for(i=0;i<=histogramBoundsNum-1;i++)
				{
					gridtest[k][i]=false;
				}
			}
			else
			{
			if(keys[k].sk_strategy==1 || keys[k].sk_strategy==2)
			{//test[k][i]=histogramBounds[i]<=DatumGetInt32(keys[k].sk_argument)?true:false;//FunctionCall2Coll(&(keys->sk_func),keys->sk_collation,histogramBounds[i],keys->sk_argument);
			//elog(LOG,"Comparasion is done %d",test[k][i]);


				for(i=0;i<=histogramMatchData.index;i++)
				{
					gridtest[k][i]=true;
				}
				for(i=histogramMatchData.index+1;i<=histogramBoundsNum-1;i++)
				{
					gridtest[k][i]=false;
				}

			}
			else if(keys[k].sk_strategy==5 || keys[k].sk_strategy==4)
			{//test[k][i]=histogramBounds[i]>=DatumGetInt32(keys[k].sk_argument)?true:false;//FunctionCall2Coll(&(keys->sk_func),keys->sk_collation,histogramBounds[i],keys->sk_argument);
			//elog(LOG,"Comparasion is done %d",test[k][i]);


				for(i=0;i<histogramMatchData.index;i++)
				{
					gridtest[k][i]=false;
				}
				for(i=histogramMatchData.index;i<=histogramBoundsNum-1;i++)
				{
					gridtest[k][i]=true;
				}
			}
			}
		}


/*		for(i=0;i<histogramBoundsNum-1;i++)
		{
			if(test[k][i]==true||test[k][i+1]==true)
			{
				gridtest[k][i]=true;
			}
			else{gridtest[k][i]=false;}
		}*/

	else
	{
		/*
		 * Handle the case that strategy number is 3 which means "equal" like id = 100
		 */
		int data=DatumGetInt32(keys[k].sk_argument);
//		elog(LOG,"= is being handled");
		searchResult histogramMatchData;
		binary_search_histogram_ondisk(&histogramMatchData,idxRel,DatumGetInt32(keys[k].sk_argument),0,histogramBoundsNum);
		gridtest[k][histogramMatchData.index]=true;
		for(i=0;i<=histogramBoundsNum-1;i++)
		{
			gridtest[k][i]=false;
		}
		if(histogramMatchData.index!=HISTOGRAM_OUT_OF_BOUNDARY)
		{
			gridtest[k][histogramMatchData.index]=true;
		}

	//	elog(LOG,"Equal match is done. Grid %d is found value %d",histogramMatchData.index,data);
	}
	}

	gridBitset=bitmap_new();
//	elog(LOG,"Initialization is done");



	for(i=0;i<=histogramBoundsNum-1;i++)
	{
		scankeymatchflag=gridtest[0][i];
		for(k=0;k<nkeys;k++)
		{
			scankeymatchflag=scankeymatchflag&&gridtest[k][i];
		}
		if(scankeymatchflag==true)
		{
			//scanstate.grids[scanstate.length]=i;
			//elog(LOG,"Scankey bitmap set %d",i);

			predicateGrids[predicateLen]=i;
			predicateLen++;

			bitmap_set(gridBitset,i);
			//scanstate.length++;
		}

	}
	//bitmap_set(gridBitset,histogramBoundsNum-1);
//	elog(LOG,"Set value is done");
//	compressedBitset=bitmap_compress(gridBitset);
//	elog(LOG,"Compressing is done");
//	scanstate.compressedBitset=compressedBitset;
//	elog(LOG,"Scankey bitmap is done");
//	ewah_iterator_init(&scankeyIterator,compressedBitset);
//	elog(LOG,"Scankey iterator is inited");
	/*
	 * We need to know the size of the table so that we know how long to
	 * iterate on the gsin.
	 */
	heapOid =idxRel->rd_id; //IndexGetRelation(RelationGetRelid(idxRel), false);
	heapRel = index_open(heapOid, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(idxRel);
	scanstate.nBlocks=nblocks;
	index_close(heapRel, AccessShareLock);
	//scan=
	gsinGetNextIndexTuple(scan);

//	elog(LOG,"Get total blocks %d",nblocks);
	do{
//	elog(LOG,"Scan index tuple");

	gsinTupleLong.gs_PageStart=0;
	gsinTupleLong.gs_PageNum=0;
	gsinTupleLong.length=0;
	gsin_form_memtuple(&gsinTupleLong,scanstate.currentDiskTuple,&itemsz);
	matchFlag=false;
//	elog(LOG,"form one memtuple is done");
	/*
	 * Traverse two gridlists to find one match. If so, break the loop.
	 */
/*
	for(i=0;i<gsinTupleLong.length;i++)
	{
		for(j=0;j<scanstate.length;j++)
		{
			if(gsinTupleLong.grids[i]==scanstate.grids[j])
			{
				matchFlag=true;
				break;
			}
		}
		if(matchFlag==true)
		{
			break;
		}
	}
	if(matchFlag==true){
		for(i=gsinTupleLong.gs_PageStart;i<gsinTupleLong.gs_PageStart+gsinTupleLong.gs_PageNum;i++)
		{
			//elog(LOG,"Find one qualified page");
			tbm_add_page(tbm,i);
			totalPages++;
		}
	}
*/
	//ANDresult=ewah_new();
	//elog(LOG,"Declare ANDresult");
	//ewah_and(compressedBitset,gsinTupleLong.compressedBitset,ANDresult);
	//elog(LOG,"And operation is done");

	//elog(LOG,"resultIterator initialization is done");
//	struct ewah_iterator	tupleIterator;
//	ewah_iterator_init(&tupleIterator,gsinTupleLong.compressedBitset);


/*	while(ewah_iterator_next(&nextWordScankey,&scankeyIterator)&&ewah_iterator_next(&nextWordTuple,&tupleIterator))
	{
		if((nextWordScankey&nextWordTuple)!=0)
		{
			//elog(LOG,"Find one qualified tuple");
			for(i=gsinTupleLong.gs_PageStart;i<=gsinTupleLong.gs_PageStart+gsinTupleLong.gs_PageNum;i++)
			{

				tbm_add_page(tbm,i);
				totalPages++;
			}
			break;
		}
	}
*/
/*	for(i=0;i<histogramBoundsNum-1;i++)
	{
		if((bitmap_get(gridBitset,i)==true)&&(bitmap_get(gsinTupleLong.originalBitset,i)==true))
		{
			for(j=gsinTupleLong.gs_PageStart;j<gsinTupleLong.gs_PageStart+gsinTupleLong.gs_PageNum;j++)
			{

				tbm_add_page(tbm,j);
				totalPages++;
//				elog(LOG,"Add page %d",j);
			}
			break;
		}
	}
	*/



	/*
	int iterator=gridBitset->word_alloc<gsinTupleLong.originalBitset->word_alloc?gridBitset->word_alloc:gsinTupleLong.originalBitset->word_alloc;
	for(i=0;i<iterator-1;i++)
	{
		if((*(gridBitset->words+i)&*(gsinTupleLong.originalBitset->words+i))!=0)
		{
			for(j=gsinTupleLong.gs_PageStart;j<=gsinTupleLong.gs_PageNum;j++)
			{

				tbm_add_page(tbm,j);
				totalPages++;
				//elog(LOG,"Add page %d",j);
			}
			break;
		}
	}*/


	for(i=0;i<predicateLen;i++)
	{
		if(bitmap_get(gsinTupleLong.originalBitset,predicateGrids[i])==true)
		{
			//elog(LOG,"One index tuple is qualified",j);
			for(j=gsinTupleLong.gs_PageStart;j<=gsinTupleLong.gs_PageNum;j++)
						{

							tbm_add_page(tbm,j);
							totalPages++;
						//	elog(LOG,"Add page %d",j);
						}
						break;
		}
	}

//	elog(LOG,"Matching index is done");
	//scan=
	//pfree(scanstate.currentDiskTuple);
	ewah_free(gsinTupleLong.compressedBitset);
	bitmap_free(gsinTupleLong.originalBitset);
	//ewah_free(ANDresult);
	gsinGetNextIndexTuple(scan);
//	elog(LOG,"Get next index");
	counter++;
	}
while(scanstate.scanHasNext==true);
	//LockBuffer(scanstate->currentLookupBuffer, BUFFER_LOCK_UNLOCK);
	elog(LOG,"Return bitmap %d pages",totalPages);
	//UnlockReleaseBuffer(scanstate.currentLookupBuffer);
	PG_RETURN_INT64(totalPages * 10);
}
void gsinGetNextIndexTuple(IndexScanDesc scan)
{
	Buffer		buffer;
	Page		page;
	Item diskTuple;
	GsinScanState *scanstate = (GsinScanState *) scan->opaque;
	//GsinTupleLong gsinTupleLong;
	OffsetNumber maxOffset;
	maxOffset=scanstate->maxOffset;
	scanstate->scanHasNext=false;
//	elog(LOG,"Just in case, getNextIndexTuple max offset num is %d",maxOffset);
	if(scanstate->currentOffset<=maxOffset)
	{

		//buffer=ReadBuffer(scan->indexRelation,scanstate->currentLookupPageNum);
		page=BufferGetPage(scanstate->currentLookupBuffer);
//		elog(LOG,"Just before decode disktuple, offset is %d",scanstate->currentOffset);
		//elog(LOG,"item usage %d lenth %d",disktest->lp_flags,disktest->lp_len);
		diskTuple=PageGetItem(page,PageGetItemId(page,scanstate->currentOffset));
		//itemsz=ItemIdGetLength(PageGetItemId(page,1));

		//elog(LOG,"Retrieve indextuple abc off size %d",PageGetMaxOffsetNumber(page));
		//gsin_form_memtuple(&gsinTupleLong,diskTuple,&itemsz);
		if(diskTuple==NULL)
		{
			elog(LOG,"Got disktuple NULL");
		}
		//scanstate->maxOffset=PageGetMaxOffsetNumber(page);
		scanstate->currentDiskTuple=diskTuple;
		scanstate->currentOffset++;
		scanstate->scanHasNext=true;
	}
	else{
	LockBuffer(scanstate->currentLookupBuffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(scanstate->currentLookupBuffer);
	scanstate->currentLookupPageNum++;
	//elog(LOG,"current block is %d",scanstate->currentLookupPageNum);
	if(scanstate->currentLookupPageNum<scanstate->nBlocks)
	{


		buffer=ReadBuffer(scan->indexRelation,scanstate->currentLookupPageNum);
		LockBuffer(buffer,BUFFER_LOCK_SHARE);
		page=BufferGetPage(buffer);
		//elog(LOG,"Just before decode disktuple, offset is %d",scanstate->currentOffset);
		diskTuple=(IndexTuple)PageGetItem(page,PageGetItemId(page,1));
		//elog(LOG,"Retrieve indextuple def size %d",IndexTupleSize(diskTuple));
		//gsin_form_memtuple(&gsinTupleLong,diskTuple,&itemsz);
		if(diskTuple==NULL)
		{
			elog(LOG,"Got disktuple NULL");
		}
		scanstate->maxOffset=PageGetMaxOffsetNumber(page);
		scanstate->currentLookupBuffer=buffer;
		scanstate->currentOffset=2;
		scanstate->scanHasNext=true;
		//scanstate->currentLookupPageNum=page;

		scanstate->currentDiskTuple=diskTuple;
	}
	else
	{
		scanstate->scanHasNext=false;
	}
	}
	scan->opaque=scanstate;
	//return scan;
}
Datum
gsinendscan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	//UnlockReleaseBuffer(scanstate->currentLookupBuffer);
	PG_RETURN_POINTER(scan);
}

void
gsin_redo(XLogReaderState *record)
{
	elog(PANIC, "gsin_redo: unimplemented");
}
/*
 * Re-initialize state for a BRIN index scan
 */
Datum
gsinrescan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	ScanKey		scankey = (ScanKey) PG_GETARG_POINTER(1);
	//elog(LOG,"Rescan got histogram %d",scanstate->histogramBoundsNum);
	/* other arguments ignored */

	/*
	 * Other index AMs preprocess the scan keys at this point, or sometime
	 * early during the scan; this lets them optimize by removing redundant
	 * keys, or doing early returns when they are impossible to satisfy; see
	 * _bt_preprocess_keys for an example.  Something like that could be added
	 * here someday, too.
	 */

	if (scankey && scan->numberOfKeys > 0){
		memmove(scan->keyData, scankey,scan->numberOfKeys * sizeof(ScanKeyData));}








	//PG_RETURN_VOID();
	return 0;
}
/*
 * reloptions processor for GSIN indexes
 */
Datum
gsinoptions(PG_FUNCTION_ARGS)
{
	Datum		reloptions = PG_GETARG_DATUM(0);
	bool		validate = PG_GETARG_BOOL(1);
	relopt_value *options;
	GsinOptions *rdopts;
	int			numoptions;
	static const relopt_parse_elt tab[] = {
		{"density", RELOPT_TYPE_INT, offsetof(GsinOptions, maxPagesPerRange)}
	};

	options = parseRelOptions(reloptions, validate, RELOPT_KIND_GSIN,
							  &numoptions);

	/* if none set, we're done */
	if (numoptions == 0)
		PG_RETURN_NULL();

	rdopts = allocateReloptStruct(sizeof(GsinOptions), options, numoptions);

	fillRelOptions((void *) rdopts, sizeof(GsinOptions), options, numoptions,
				   validate, tab, lengthof(tab));

	pfree(options);

	PG_RETURN_BYTEA_P(rdopts);
}

