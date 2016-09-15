
#include "postgres.h"

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
#include "access/hippo.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/elog.h"
#include "catalog/pg_attribute.h"
#include "access/xlogreader.h"
#include "catalog/pg_index.h"
#include "access/htup_details.h"

#include "ewok.h"

#define SORT_TYPE int16

/*
 * Per-data-tuple callback from IndexBuildHeapScan. You'd better not add log here otherwise the log file will have a crazy size.
 */
static void
hippobuildCallback(Relation index,
		HeapTuple htup,
		Datum *values,
		bool *isnull,
		bool tupleIsAlive,
		void *state)
{

	BlockNumber thisblock;
	HippoBuildState *buildstate = (HippoBuildState *) state;
	Datum *histogramBounds=buildstate->histogramBounds;
	int histogramBoundsNum=buildstate->histogramBoundsNum;
	AttrNumber attrNum;
	thisblock = ItemPointerGetBlockNumber(&htup->t_self);
	attrNum=buildstate->attrNum;
	if(buildstate->hp_PageNum==0)
	{
		/*
		 * We are working on the first index tuple.
		 */
		buildstate->hp_PageStart=thisblock;
		buildstate->hp_currentPage=thisblock;
		buildstate->hp_PageNum=thisblock;
		buildstate->hp_scanpage++;
		buildstate->hp_length=0;
		buildstate->dirtyFlag=true;
	}
	else
	{
		/*
		 * For all incoming page, check current working partial histogram density.
		 */
		if(buildstate->hp_currentPage!=thisblock)
		{
			buildstate->hp_PageNum=thisblock;
			buildstate->dirtyFlag=true;
			buildstate->hp_scanpage++;
			if(buildstate->differentTuples*100>=buildstate->differenceThreshold*(buildstate->histogramBoundsNum-1))
			{
				/*
				 * Hippo density threshold is satisfied. We are going to put one index entry on disk. Note the Hippo SQL parameter is in percentage unit.
				 */
				buildstate->differentTuples=0;
				buildstate->stopMergeFlag=true;
			}
		}
	}
	/*
	 * Check whether the working partial histogram triggers the threshold. If so, put one index entry on disk.
	 */
	if(buildstate->stopMergeFlag==true)
	{
		/*
		 * Extract information from buildstate to a new real hippo tuple long
		 */
		buildstate->stopMergeFlag=false;
		buildstate->deleteFlag=CLEAR_INDEX_TUPLE;
		/*
		 * Put one Hippo index entry on disk
		 */
		buildstate->hp_PageNum=buildstate->hp_PageNum-1;
		hippo_doinsert(buildstate);
		buildstate->hp_indexnumtuples++;
		buildstate->hp_PageStart=thisblock;
		buildstate->hp_PageNum=thisblock;
		buildstate->dirtyFlag=false;
		buildstate->hp_length=0;
		buildstate->hp_grids[0]=-1;
		buildstate->originalBitset=bitmap_new();
	}
	buildstate->hp_currentPage=thisblock;
	if (tupleIsAlive)
	{

		/* Cost estimation info */
		buildstate->hp_numtuples++;
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
		if(histogramMatchData.index<0)
		{
			/*
			 *Got an overflow data tuple
			 */
		}
		if(bitmap_get(buildstate->originalBitset,histogramMatchData.index)==false)
		{
			buildstate->differentTuples++;
			bitmap_set(buildstate->originalBitset,histogramMatchData.index);
		}
	}
	else
	{
		/*
		 *Got an deleted tuple. Don't care about it.
		 */
	}


}
/*
 *This function initializes the entire Hippo. It will call buildcallback many times.
 */
Datum
hippobuild(PG_FUNCTION_ARGS)
{
	/*
	 * This is some routine declarations for variables
	 */
		Relation	heap = (Relation) PG_GETARG_POINTER(0);
		Relation	index = (Relation) PG_GETARG_POINTER(1);
		IndexInfo  *indexInfo = (IndexInfo *) PG_GETARG_POINTER(2);
		IndexBuildResult *result;
		double		reltuples;
		HippoBuildState buildstate;
		Buffer		buffer;
		Datum *histogramBounds;
		int histogramBoundsNum,i;
		HeapTuple heapTuple;
		Page page;/* Initial page */
		Size		pageSize;
		AttrNumber attrNum;
		BlockNumber sorted_list_pages=((RelationGetNumberOfBlocks(heap)/((1)*SORTED_LIST_TUPLES_PER_PAGE))+1);
		/*
		 * HIPPO Only supports one column as the key
		 */
		attrNum=indexInfo->ii_KeyAttrNumbers[0];
		/*
		 * Search PG kernel cache for pg_statistic info
		 */
		heapTuple=SearchSysCache2(STATRELATTINH,ObjectIdGetDatum(heap->rd_id),Int16GetDatum(attrNum));
		if (HeapTupleIsValid(heapTuple))
		{

			get_attstatsslot(heapTuple,
					get_atttype(heap->rd_id, attrNum),get_atttypmod(heap->rd_id,attrNum),
							STATISTIC_KIND_HISTOGRAM, InvalidOid,
							NULL,
							&histogramBounds, &histogramBoundsNum,
							NULL, NULL);
		}
		/*
		 * If didn't release the stattuple, it will be locked.
		 */
		ReleaseSysCache(heapTuple);
		if(histogramBounds==NULL)
		{
			elog(LOG, "Got histogram NULL");
		}
		BlockNumber histogramPages=histogramBoundsNum/HISTOGRAM_PER_PAGE;
		/* Initialize pages for sorted list */
		for(i=0;i<sorted_list_pages+histogramPages+1;i++)
		{

			buffer=ReadBuffer(index,P_NEW);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			START_CRIT_SECTION();
			hippoinit_special(buffer);
			MarkBufferDirty(buffer);
			END_CRIT_SECTION();
			UnlockReleaseBuffer(buffer);
		}

		/*
		 *	Init HIPPO.
		 */
		buffer = ReadBuffer(index, P_NEW);

		if(buffer==NULL)
		{
			elog(ERROR, "[hippobuild] Initialized buffer NULL");
		}
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		START_CRIT_SECTION();
		pageSize = BufferGetPageSize(buffer);
		page = BufferGetPage(buffer);
		if(page==NULL)
		{
			elog(ERROR, "[hippobuild] Initialized page NULL");
		}
		PageInit(page, pageSize, sizeof(BlockNumber)*2+sizeof(GridList));
		MarkBufferDirty(buffer);
		END_CRIT_SECTION();
		LockBuffer(buffer,BUFFER_LOCK_UNLOCK);
		/*
		 * Initialize the build state
		 */
		buildstate.hp_irel= index;
		buildstate.attrNum=attrNum;
		buildstate.hp_PageStart=0;
		buildstate.hp_PageNum=0;
		buildstate.dirtyFlag=false;
		buildstate.hp_currentPage=0;
		buildstate.hp_length=0;
		buildstate.hp_numtuples=0;
		buildstate.hp_indexnumtuples=0;
		buildstate.hp_scanpage=0;
		buildstate.hp_currentInsertBuf=buffer;
		buildstate.hp_currentInsertPage=page;
		buildstate.lengthcounter=0;
		buildstate.histogramBounds=histogramBounds;
		buildstate.histogramBoundsNum=histogramBoundsNum;
		buildstate.originalBitset=bitmap_new();
		buildstate.pageBitmap=bitmap_new();
		buildstate.differenceThreshold=HippoGetMaxPagesPerRange(index);/* Hippo option: partial histogram density */
		buildstate.differentTuples=0;
		buildstate.stopMergeFlag=false;
		/*
		 * Initialize sorted list parameters
		 */
		buildstate.sorted_list_pages=sorted_list_pages;
		/* build the index */
		reltuples=IndexBuildHeapScan(heap, index, indexInfo, false, hippobuildCallback, (void *) &buildstate);
		/*
		 * Finish the last index tuple.
		 */
		if(buildstate.dirtyFlag==true)
		{
			/*
			 * This is to summarize the last few data pages which are contained by buildstate but haven't got chances to be put on disk.
			 */
			buildstate.deleteFlag=buildstate.differentTuples;
			buildstate.hp_PageNum=buildstate.hp_currentPage;
			hippo_doinsert(&buildstate);
			buildstate.hp_indexnumtuples++;
		}

		ReleaseBuffer(buildstate.hp_currentInsertBuf);
		put_histogram(index,0,histogramBoundsNum,histogramBounds);
		/*
		 * Stored sorted list
		 */
		SortedListInialize(index,&buildstate,histogramPages+1);
		/*
		 * Return statistics
		 */
		result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
		result->heap_tuples = buildstate.hp_numtuples;
		result->index_tuples = buildstate.hp_indexnumtuples;
		PG_RETURN_POINTER(result);
}
/*
 *	Pre-Build an empty HIPPO index in the initialization phase.
 */
Datum
hippobuildempty(PG_FUNCTION_ARGS)
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
		hippoinit_special(buffer);
		MarkBufferDirty(buffer);
		END_CRIT_SECTION();
		UnlockReleaseBuffer(buffer);
	}
	PG_RETURN_VOID();
}



/*
 * HIPPO insert
 * A tuple in the heap is being inserted.  To keep a hippo index up to date,
 * we need to obtain the relevant index tuple and compare its stored values
 * with those of the new tuple.  If the tuple values are not consistent with
 * the summary tuple, we need to update the index tuple.
 *
 *
 */
Datum
hippoinsert(PG_FUNCTION_ARGS)
{
	Relation	idxRel = (Relation) PG_GETARG_POINTER(0);
	Datum	   *values = (Datum *) PG_GETARG_POINTER(1);
	bool	   *nulls = (bool *) PG_GETARG_POINTER(2);
	ItemPointer heaptid = (ItemPointer) PG_GETARG_POINTER(3);
	Relation heapRelation=PG_GETARG_POINTER(4);
	IndexInfo *indexInfo=BuildIndexInfo(idxRel);
	struct FormData_pg_index *heapInfo=idxRel->rd_index;
	/*
	 * Parameters defined by hippo itself
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
	HippoTupleLong hippoTupleLong,lastHippoTupleLong;
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
	/*
	 * Binary search histogram
	*/
	searchResult histogramMatchData;
	binary_search_histogram_ondisk(&histogramMatchData,idxRel,values[0],0,histogramBoundsNum);
	/*
	 * This nested loop is for seeking the disk tuple which contains the heap tuple
	 */
	lastIndexTupleBlockNumber=-1;
	totalblocks=RelationGetNumberOfBlocks(idxRel);
	if(binary_search_sorted_list(heapRelation,idxRel,totalIndexTupleNumber,heapBlk,&resultPosition,&hippoTupleLong,&indexDiskBlock,&indexDiskOffset,histogramPages+1)==true)
	{
		seekFlag=true;
	}
	else
	{
		seekFlag=false;
		check_index_position(idxRel,totalIndexTupleNumber-1,heapBlk,&hippoTupleLong,&indexDiskBlock,&indexDiskOffset,histogramPages+1);
	}
	/*
	 * Update the memory tuple
	 */

	if(seekFlag==true){
		/*
		 * The inserted heap tuple belongs to one index tuple
		 */
		if(bitmap_get(hippoTupleLong.originalBitset,histogramMatchData.index)==false)
		{
			/*
			 * Before update the memory tuple,copy the old memory tuple
			 */
			IndexTupleData *newDiskTuple;

			Size oldsize,newsize;
			bool samePageUpdate=true;
			HippoTupleLong newHippoTupleLong;
			copy_hippo_mem_tuple(&newHippoTupleLong,&hippoTupleLong);
			hippo_form_indextuple(&hippoTupleLong,&oldsize,histogramBoundsNum);
			bitmap_set(newHippoTupleLong.originalBitset,histogramMatchData.index);
			/*
			 *If hippo can do samepage update, go ahead and do it.If not, get new buffer, new page and insert them.
			 */
			newDiskTuple=hippo_form_indextuple(&newHippoTupleLong,&newsize,histogramBoundsNum);
			/*
			 * Check whether current index page has enough space. If so, go ahead and put it on disk.
			 * Otherwise, ask for new buffer and page.
			 */
			buffer=ReadBuffer(idxRel,indexDiskBlock);
			if(hippo_can_do_samepage_update(buffer,oldsize,newsize))
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
				OffsetNumber testOff;
				page=BufferGetPage(buffer);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

				START_CRIT_SECTION();
				PageIndexDeleteNoCompact(page,&indexDiskOffset,1);
				testOff=PageAddItem(page,(Item)newDiskTuple,newsize,indexDiskOffset,true,false);
				if(testOff==InvalidOffsetNumber)
				{
					elog(ERROR, "[hippoinsert] failed to add HIPPO tuple same page");
				}
				MarkBufferDirty(buffer);
				END_CRIT_SECTION();
				UnlockReleaseBuffer(buffer);
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
				newInsertPage = BufferGetPage(newInsertBuffer);
				if(PageGetFreeSpace(newInsertPage)<newsize)
				{
					ReleaseBuffer(newInsertBuffer);
					newInsertBuffer=ReadBuffer(idxRel,totalblocks);
					newInsertPage = BufferGetPage(newInsertBuffer);

				}
				newBlockNumber=BufferGetBlockNumber(newInsertBuffer);
				LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
				START_CRIT_SECTION();
				newOffsetNumber=PageAddItem(newInsertPage, (Item) newDiskTuple, newsize, InvalidOffsetNumber,false, false);
				if(newOffsetNumber==InvalidOffsetNumber)
				{
					elog(ERROR, "failed to add HIPPO tuple new page");
				}
				MarkBufferDirty(newInsertBuffer);
				END_CRIT_SECTION();
				UnlockReleaseBuffer(newInsertBuffer);
				update_sorted_list_tuple(idxRel,resultPosition,newBlockNumber,newOffsetNumber,histogramPages+1);
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
					hippo_getinsertbuffer(idxRel);
					newInsertBuffer=ReadBuffer(idxRel,totalblocks);
					newBlockNumber=totalblocks;
					newInsertPage = BufferGetPage(newInsertBuffer);
					LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
					START_CRIT_SECTION();
					newOffsetNumber=PageAddItem(newInsertPage, (Item) newDiskTuple, newsize, InvalidOffsetNumber,
											  false, false);
					if(newOffsetNumber==InvalidOffsetNumber)
					{
						elog(ERROR, "[hippoinsert]failed to add HIPPO tuple new page");
					}
					MarkBufferDirty(newInsertBuffer);
					END_CRIT_SECTION();
					UnlockReleaseBuffer(newInsertBuffer);
					update_sorted_list_tuple(idxRel,resultPosition,newBlockNumber,newOffsetNumber,histogramPages+1);
				}

			}
		}
		else
		{
			/*
			 * The new inserted value doesn't change the original index tuple, thus do nothing.
			 */
		}
		ewah_free(hippoTupleLong.compressedBitset);
	}
	else
	{
		/*
		 * The inserted heap tuple doesn't belong to one index tuple. First check whether the
		 * last index (the index contains the last heap page) is full. If full, create a new index tuple. If not full, go ahead and merge
		 * its page into this tuple.
		 */
		/*
		 *Check whether this tuple is full. If full, ask for new buffer and page. If not,delete the old one and insert new one.
		 */
		if((hippoTupleLong.deleteFlag*1.0/histogramBoundsNum-1)>=(HippoGetMaxPagesPerRange(idxRel)*1.00/100))
		{
			/*
			 * Full. Keep the last index tuple there and create new buffer, page and last index tuple.
			 */
			HippoTupleLong newLastIndexTuple;
			char *newLastIndexDiskTuple;
			Size itemsize;
			Buffer newInsertBuffer;
			Page newInsertPage;
			BlockNumber newBlock;
			OffsetNumber newOffsetNumber;
			newLastIndexTuple.hp_PageStart=heapBlk;
			newLastIndexTuple.hp_PageNum=heapBlk;
			newLastIndexTuple.deleteFlag=1;
			newLastIndexTuple.originalBitset=bitmap_new();
			bitmap_set(newLastIndexTuple.originalBitset,histogramMatchData.index);
			newLastIndexDiskTuple=hippo_form_indextuple(&newLastIndexTuple,&itemsize,histogramBoundsNum);
			newInsertBuffer=ReadBuffer(idxRel,totalblocks-1);
			newInsertPage=BufferGetPage(newInsertBuffer);
			if(PageGetFreeSpace(newInsertPage)<itemsize)
			{
				ReleaseBuffer(newInsertBuffer);
				hippo_getinsertbuffer(idxRel);
				newInsertBuffer=ReadBuffer(idxRel,totalblocks);
				newInsertPage=BufferGetPage(newInsertBuffer);
			}
			newBlock=BufferGetBlockNumber(newInsertBuffer);
			LockBuffer(newInsertBuffer, BUFFER_LOCK_EXCLUSIVE);
			START_CRIT_SECTION();
			newOffsetNumber=PageAddItem(newInsertPage,(Item)newLastIndexDiskTuple,itemsize,InvalidOffsetNumber,false,false);
			if(newOffsetNumber==InvalidOffsetNumber)
			{
				elog(ERROR, "failed to update HIPPO last index tuple same page");
			}
			MarkBufferDirty(newInsertBuffer);
			END_CRIT_SECTION();
			UnlockReleaseBuffer(newInsertBuffer);
			add_new_sorted_list_tuple(idxRel,totalIndexTupleNumber,newBlock,newOffsetNumber,histogramPages+1);
			ewah_free(hippoTupleLong.compressedBitset);
		}
		else
		{
			/*
			 * Not full. Update last index tuple. Check whether we can do samepage insert. If not, ask for new buffer and page.
			 */
			IndexTupleData *newDiskTuple;
			Size oldsize,newsize;
			HippoTupleLong newLastHippoTupleLong;
			bool samePageUpdate=true;
			copy_hippo_mem_tuple(&newLastHippoTupleLong,&hippoTupleLong);
			hippo_form_indextuple(&hippoTupleLong,&oldsize,histogramBoundsNum);
			newLastHippoTupleLong.hp_PageNum=heapBlk;;
			newLastHippoTupleLong.deleteFlag++;
			bitmap_set(newLastHippoTupleLong.originalBitset,histogramMatchData.index);
			newDiskTuple=hippo_form_indextuple(&newLastHippoTupleLong,&newsize,histogramBoundsNum);;
			currentBuffer=ReadBuffer(idxRel,indexDiskBlock);
			page=BufferGetPage(currentBuffer);
			if(hippo_can_do_samepage_update(currentBuffer,oldsize,newsize))
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
					elog(ERROR, "failed to add HIPPO last index tuple same page");
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
						newInsertBuffer=hippo_getinsertbuffer(idxRel);
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
					newInsertBuffer=hippo_getinsertbuffer(idxRel);
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
			ewah_free(hippoTupleLong.compressedBitset);
		}


	}
	/*
	 *	Update the disk tuple
	 */
	free_attstatsslot(get_atttype(heapInfo->indrelid, attrNum),histogramBounds,histogramBoundsNum,NULL,0);
	return BoolGetDatum(false);
}


/*
 * hippobulkdelete
 *	HIPPO will update itself after each deletion. Note that: the deletion here is not per-tuple deletion. In this deletion, Postgres may delete millions of records in one time.
 * we could mark item tuples as "dirty" (when a minimum or maximum heap
 * tuple is deleted), meaning the need to re-run summarization on the affected
 * range.  Would need to add an extra flag in hippotuple for that.
 */
Datum
hippobulkdelete(PG_FUNCTION_ARGS)
{
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
	struct FormData_pg_index *heapInfo=idxRelation->rd_index;
	/*
	 *Pre-retrieve histogram from PG kernel
	 */
	histogramTuple=SearchSysCache2(STATRELATTINH,ObjectIdGetDatum(heapInfo->indrelid),Int16GetDatum(attrNum));
	if (HeapTupleIsValid(histogramTuple))
	{
		get_attstatsslot(histogramTuple,
				get_atttype(heapInfo->indrelid, attrNum),get_atttypmod(heapInfo->indrelid,attrNum),
						STATISTIC_KIND_HISTOGRAM, InvalidOid,
						NULL,
						&histogramBounds, &histogramBoundsNum,
						NULL, NULL);
	}
	ReleaseSysCache(histogramTuple);
/*
 * Set the start block to skip the histogram and sorted list
 */
	histogramPages=histogramBoundsNum/HISTOGRAM_PER_PAGE;
	get_sorted_list_pages(idxRelation,&sorted_list_pages,histogramPages+1);
	startblock=sorted_list_pages+histogramPages+1;
/*
 * First loop, traverse each index page
 */
	for(idxBlkNum=startblock;idxBlkNum<RelationGetNumberOfBlocks(idxRelation);idxBlkNum++)
	{

		Buffer currentBuffer=ReadBuffer(idxRelation,idxBlkNum);
		Page currentPage=BufferGetPage(currentBuffer);
		OffsetNumber idxTupleOffset,maxOffset;
		HippoTupleLong hippoTupleLong;
		bool indexDeleteFlag;
		/*
		 *Acquire an exclusive lock in case execute in place index entry update
		 */
		LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
		maxOffset=PageGetMaxOffsetNumber(currentPage);
		Size itemsz;
		IndexTuple currentDiskTuple;
		/*
		 * Second loop, traverse the each index entry inside each index page
		 */
		for(idxTupleOffset=1;idxTupleOffset<=maxOffset;idxTupleOffset++)
		{

			currentDiskTuple=(IndexTuple)PageGetItem(currentPage,PageGetItemId(currentPage,idxTupleOffset));
			hippo_form_memtuple(&hippoTupleLong,currentDiskTuple,&itemsz);
			indexDeleteFlag=false;
			/*
			 * The index tuple will be checked
			 */
			for(heapBlkNum=hippoTupleLong.hp_PageStart;heapBlkNum<=hippoTupleLong.hp_PageNum;heapBlkNum++)
			{
				/*
				 * Check each data page summarized by this index entry
				 */
				Buffer currentHeapBuffer=ReadBuffer(heapRelation,heapBlkNum);
				Page currentHeapPage=BufferGetPage(currentHeapBuffer);
				Size recordedFreeSpace=GetRecordedFreeSpace(heapRelation,heapBlkNum);
				Size pageFreeSpace=PageGetFreeSpace(currentHeapPage);
				if(pageFreeSpace-recordedFreeSpace>24&&recordedFreeSpace==0)
				{
					/*
					 * Hippo detects some data tuples are deleted. It will break the loop and start to re-summarize.
					 */
					indexDeleteFlag=true;
					ReleaseBuffer(currentHeapBuffer);
					break;
				}
				else{
					/*
					 * Hippo doesn't find anything deleted on this data page. Scan the next data page.
					 */
					ReleaseBuffer(currentHeapBuffer);}
			}
			if(indexDeleteFlag==true)
			{
			/*
			 * DeleteFalg is true. Hippo detects something deleted. This index tuple should be updated.
			 */

				bitmap_free(hippoTupleLong.originalBitset);
				hippoTupleLong.originalBitset=bitmap_new();
				Size diskSize;
			for(heapBlkNum=hippoTupleLong.hp_PageStart;heapBlkNum<=hippoTupleLong.hp_PageNum;heapBlkNum++)
			{
				/*
				 * Traverse each data page summarized by this index entry
				 */
				Buffer currentHeapBuffer=ReadBuffer(heapRelation,heapBlkNum);
				Page currentHeapPage=BufferGetPage(currentHeapBuffer);
				ItemId lp;
				ItemPointerData itmPtr;

				OffsetNumber maxOffset=PageGetMaxOffsetNumber(currentHeapPage),heapTupleOffset;
				Datum value;
				bool isnull;//=false;
				LockBuffer(currentHeapBuffer, BUFFER_LOCK_SHARE);
				for(heapTupleOffset=1;heapTupleOffset<=maxOffset;heapTupleOffset++)
				{
					/*
					 *Traverse each data tuple belongs to this data page
					 */
					lp = PageGetItemId(currentHeapPage,heapTupleOffset);
					ItemPointerSet(&itmPtr, heapBlkNum, heapTupleOffset);
					HeapTupleData currentHeapTuple;
					currentHeapTuple.t_data=(HeapTupleHeader)PageGetItem(currentHeapPage,lp);
					currentHeapTuple.t_len=ItemIdGetLength(lp);
					currentHeapTuple.t_tableOid = RelationGetRelid(heapRelation);
					currentHeapTuple.t_self=itmPtr;
					if(!HeapTupleIsValid(&currentHeapTuple))
					{
						elog(LOG,"Got NULL heap tuple");
					}

					value=fastgetattr(&currentHeapTuple,attrNum,RelationGetDescr(heapRelation),&isnull);
					searchResult histogramMatchData;
					binary_search_histogram(&histogramMatchData,histogramBoundsNum,histogramBounds,value);
					bitmap_set(hippoTupleLong.originalBitset,histogramMatchData.index);
				}
				UnlockReleaseBuffer(currentHeapBuffer);
			}
			/*
			 * Update the index tuple on disk
							 */
				IndexTupleData *diskTuple;
				diskTuple=hippo_form_indextuple(&hippoTupleLong,&diskSize,histogramBoundsNum);
				START_CRIT_SECTION();
				/*
				 *Delete the old index entry
				 */
				PageIndexDeleteNoCompact(currentPage,&idxTupleOffset,1);
				/*
				 *Add the new index entry
				 */
				if(PageAddItem(currentPage,(Item)diskTuple,diskSize,idxTupleOffset,true,false)==InvalidOffsetNumber)
				{
					elog(ERROR, "Failed to add HIPPO last index tuple same page");
				}
				MarkBufferDirty(currentBuffer);
				END_CRIT_SECTION();

				pfree(diskTuple);
				pfree(hippoTupleLong.originalBitset);
				pfree(hippoTupleLong.compressedBitset);
				//currentBuffer=ReadBuffer(idxRelation,idxBlkNum);
				//LockBuffer(currentBuffer, BUFFER_LOCK_SHARE);
			}
			else
			{
				/*
				 *This index entry does not need to re-summarize.
				 */
				pfree(hippoTupleLong.originalBitset);
				pfree(hippoTupleLong.compressedBitset);
				//UnlockReleaseBuffer(currentBuffer);
			}

		}

		UnlockReleaseBuffer(currentBuffer);
	}
	relation_close(heapRelation, AccessShareLock);

	PG_RETURN_POINTER(stats);
}

/*
 * This routine is in charge of "vacuuming" a HIPPO index. It doessome statistics for PG.
 */
Datum
hippovacuumcleanup(PG_FUNCTION_ARGS)
{
	/* other arguments are not currently used */
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	PG_RETURN_POINTER(stats);
}
Datum
hippobeginscan(PG_FUNCTION_ARGS)
{
	Relation	r = (Relation) PG_GETARG_POINTER(0);
	int			nkeys = PG_GETARG_INT32(1);
	int			norderbys = PG_GETARG_INT32(2);
	IndexScanDesc scan = RelationGetIndexScan(r, nkeys, norderbys);
	PG_RETURN_POINTER(scan);
}


/*
 * This function does index search.
 */
Datum hippogetbitmap(PG_FUNCTION_ARGS)
{

	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	TIDBitmap  *tbm = (TIDBitmap *) PG_GETARG_POINTER(1);
	Relation	idxRel = scan->indexRelation;
	HippoTupleLong hippoTupleLong;
	Oid			heapOid;
	Relation	heapRel;
	HippoScanState scanstate;//=(HippoScanState *) scan->opaque;
	BlockNumber sorted_list_pages;

	BlockNumber nblocks;
	Size itemsz;
	IndexInfo *indexInfo;
	//int			totalpages = 0;
	bool matchFlag=false;
	bool scankeymatchflag=false;
	int totalPages=0;
	int i,j,counter,k;
	int histogramBoundsNum;
	Datum *histogramBounds;
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
	struct ewah_iterator	scankeyIterator;
	eword_t nextWordScankey;
	eword_t nextWordTuple;
	scan->opaque=&scanstate;
	/*
	 * Currently HIPPO supports the numerOfKeys is no larger than 1
	 */
	int nkeys=scan->numberOfKeys;
	AttrNumber attrNum;
	struct FormData_pg_index *heapInfo=idxRel->rd_index;
	indexInfo=BuildIndexInfo(idxRel);
	scan->opaque=&scanstate;
	scanstate.length=0;
	attrNum=indexInfo->ii_KeyAttrNumbers[0];
	if(heapInfo==NULL)
	{
		elog(ERROR, "[hippogetbitmap] Heap is null");
	}
	histogramBoundsNum=get_histogram_totalNumber(idxRel,0);
	scanstate.histogramBoundsNum=histogramBoundsNum;
	histogramPages=histogramBoundsNum/HISTOGRAM_PER_PAGE;
	get_sorted_list_pages(idxRel,&sorted_list_pages,histogramPages+1);
	scanstate.currentLookupBuffer=ReadBuffer(scan->indexRelation,sorted_list_pages+histogramPages+1);
	if(scanstate.currentLookupBuffer==NULL)
	{
		elog(ERROR,"[hippogetbitmap] Read NULL buffer");
	}
	LockBuffer(scanstate.currentLookupBuffer, BUFFER_LOCK_SHARE);
	page=BufferGetPage(scanstate.currentLookupBuffer);
	if(page==NULL)
	{
		elog(ERROR,"[hippogetbitmap] Read NULL page");
	}
	diskTuple=(IndexTuple)PageGetItem(page,PageGetItemId(page,1));
	scanstate.maxOffset=PageGetMaxOffsetNumber(page);
	scanstate.currentLookupPageNum=sorted_list_pages+histogramPages+1;
	scanstate.currentOffset=1;
	scanstate.currentDiskTuple=diskTuple;
	scanstate.length=0;
	scanstate.scanHasNext=true;
	bool test[nkeys][histogramBoundsNum];
	bool gridtest[nkeys][histogramBoundsNum];

	/*
	 * Check whether one histogram bound satisfies scankeys. It is true only under this circumstance that this bound satisfies all the keys.
	 */
	/*
	 * Iterate scan key to find matched grids
	 */
	for(k=0;k<nkeys;k++){
	if(keys[k].sk_strategy!=3){
		/*
		 * Handle all the operators <, >, <=, >=, except =
		 */
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
			{
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
			{
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
	else
	{
		/*
		 * Handle the case that strategy number is 3 which means "equal" like id = 100
		 */
		int data=DatumGetInt32(keys[k].sk_argument);
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
	}
	}
	gridBitset=bitmap_new();
	for(i=0;i<=histogramBoundsNum-1;i++)
	{
		scankeymatchflag=gridtest[0][i];
		for(k=0;k<nkeys;k++)
		{
			scankeymatchflag=scankeymatchflag&&gridtest[k][i];
		}
		if(scankeymatchflag==true)
		{
			predicateGrids[predicateLen]=i;
			predicateLen++;
			bitmap_set(gridBitset,i);
		}

	}
	/*
	 * We need to know the size of the table so that we know how long to
	 * iterate on the hippo.
	 */
	heapOid =idxRel->rd_id;
	heapRel = index_open(heapOid, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(idxRel);
	scanstate.nBlocks=nblocks;
	index_close(heapRel, AccessShareLock);
	hippoGetNextIndexTuple(scan);
	do{
	hippoTupleLong.hp_PageStart=0;
	hippoTupleLong.hp_PageNum=0;
	hippoTupleLong.length=0;
	hippo_form_memtuple(&hippoTupleLong,scanstate.currentDiskTuple,&itemsz);
	matchFlag=false;
	/*
	 * Traverse two bitmaps (one from index entry and one from query predicate) to find one match. If so, break the loop.
	 */

	for(i=0;i<predicateLen;i++)
	{
		if(bitmap_get(hippoTupleLong.originalBitset,predicateGrids[i])==true)
		{
			for(j=hippoTupleLong.hp_PageStart;j<=hippoTupleLong.hp_PageNum;j++)
						{

							tbm_add_page(tbm,j);
							totalPages++;
						}
						break;
		}
	}
	ewah_free(hippoTupleLong.compressedBitset);
	bitmap_free(hippoTupleLong.originalBitset);
	hippoGetNextIndexTuple(scan);
	counter++;
	}
while(scanstate.scanHasNext==true);
	PG_RETURN_INT64(totalPages * 10);
}


Datum
hippoendscan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	PG_RETURN_POINTER(scan);
}

void
hippo_redo(XLogReaderState *record)
{
	elog(PANIC, "hippo_redo: unimplemented");
}
/*
 * Re-initialize state for a HIPPO index scan
 */
Datum
hipporescan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	ScanKey		scankey = (ScanKey) PG_GETARG_POINTER(1);
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

	return 0;
}
/*
 * reloptions processor for HIPPO indexes
 */
Datum
hippooptions(PG_FUNCTION_ARGS)
{
	Datum		reloptions = PG_GETARG_DATUM(0);
	bool		validate = PG_GETARG_BOOL(1);
	relopt_value *options;
	HippoOptions *rdopts;
	int			numoptions;
	static const relopt_parse_elt tab[] = {
		{"density", RELOPT_TYPE_INT, offsetof(HippoOptions, density)}
	};

	options = parseRelOptions(reloptions, validate, RELOPT_KIND_HIPPO,
							  &numoptions);

	/* if none set, we're done */
	if (numoptions == 0)
		PG_RETURN_NULL();

	rdopts = allocateReloptStruct(sizeof(HippoOptions), options, numoptions);

	fillRelOptions((void *) rdopts, sizeof(HippoOptions), options, numoptions,
				   validate, tab, lengthof(tab));

	pfree(options);

	PG_RETURN_BYTEA_P(rdopts);
}

