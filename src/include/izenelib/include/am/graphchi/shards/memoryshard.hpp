
/**
 * @file
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 
 *
 * @section DESCRIPTION
 *
 * The memory shard. This class should only be accessed internally by the GraphChi engine.
 */

#ifdef DYNAMICEDATA
#include <am/graphchi/shards/dynamicdata/memoryshard.hpp>
#else

#ifndef DEF_GRAPHCHI_MEMSHARD
#define DEF_GRAPHCHI_MEMSHARD


#include <iostream>
#include <cstdio>
#include <sstream>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <string>

#include <am/graphchi/api/graph_objects.hpp>
#include <am/graphchi/metrics/metrics.hpp>
#include <am/graphchi/io/stripedio.hpp>
#include <am/graphchi/graphchi_types.hpp>


namespace graphchi {
    
    
    template <typename VT, typename ET, typename svertex_t = graphchi_vertex<VT, ET> >
    class memory_shard {
        
        stripedio * iomgr;
        
        std::string filename_edata;
        std::string filename_adj;
        
        vid_t range_st;
        vid_t range_end;
        size_t adjfilesize;
        size_t edatafilesize;
        
        size_t edgeptr;
        vid_t streaming_offset_vid;
        size_t streaming_offset; // The offset where streaming should continue
        size_t range_start_offset; // First byte for this range's vertices (used for writing only outedges)
        size_t range_start_edge_ptr;
        size_t streaming_offset_edge_ptr;
        uint8_t * adjdata;
        char ** edgedata;
        int * doneptr;
        std::vector<size_t> blocksizes;
        uint64_t chunkid;
        
        std::vector<int> block_edatasessions;
        int adj_session;
        streaming_task adj_stream_session;
        
        bool async_edata_loading;
        bool is_loaded;
        size_t blocksize;
        metrics &m;
        
    public:
        bool only_adjacency;
        
        memory_shard(stripedio * iomgr,
                     std::string _filename_edata,
                     std::string _filename_adj,
                     vid_t _range_start,
                     vid_t _range_end,
                     size_t _blocksize,
                     metrics &_m) : iomgr(iomgr), filename_edata(_filename_edata),
        filename_adj(_filename_adj),
        range_st(_range_start), range_end(_range_end), blocksize(_blocksize),  m(_m) {
            adjdata = NULL;
            only_adjacency = false;
            is_loaded = false;
            adj_session = -1;
            edgedata = NULL;
            doneptr = NULL;
            async_edata_loading = !svertex_t().computational_edges();
#ifdef SUPPORT_DELETIONS
            async_edata_loading = false; // See comment above for memshard, async_edata_loading = false;
#endif
        }
        
        ~memory_shard() {
            int nblocks = (int) block_edatasessions.size();
            
            for(int i=0; i < nblocks; i++) {
                if (edgedata[i] != NULL) {
                    iomgr->managed_release(block_edatasessions[i], &edgedata[i]);
                    iomgr->close_session(block_edatasessions[i]);
                }
            }
            if (adj_session >= 0) {
                if (adjdata != NULL) iomgr->managed_release(adj_session, &adjdata);
                iomgr->close_session(adj_session);
            }
            if (edgedata != NULL)
                free(edgedata);
            edgedata = NULL;
            if (doneptr != NULL) {
                free(doneptr);
            }
        }
        
        void commit(bool commit_inedges, bool commit_outedges) {
            if (block_edatasessions.size() == 0 || only_adjacency) return;
            assert(is_loaded);
            metrics_entry cm = m.start_time();
            
            /**
             * This is an optimization that is relevant only if memory shard
             * has been used in a case where only out-edges are considered.
             * Out-edges are in a continuous "window", while in-edges are
             * scattered all over the shard
             */
            int nblocks = (int) block_edatasessions.size();
            
            if (commit_inedges) {
                int start_stream_block = (int) (range_start_edge_ptr / blocksize);
                
                for(int i=0; i < nblocks; i++) {
                    /* Write asynchronously blocks that will not be needed by the sliding windows on
                     this iteration. */
                    if (i >= start_stream_block) {
                        iomgr->managed_pwritea_now(block_edatasessions[i], &edgedata[i], blocksizes[i], 0);
                        iomgr->managed_release(block_edatasessions[i], &edgedata[i]);
                        iomgr->close_session(block_edatasessions[i]);
                        
                        edgedata[i] = NULL;
                        
                    } else {
                        iomgr->managed_pwritea_async(block_edatasessions[i], &edgedata[i], blocksizes[i], 0, true, true);
                        edgedata[i] = NULL;
                    }
                }
            } else if (commit_outedges) {
                size_t last = streaming_offset_edge_ptr;
                if (last == 0){
                    // rollback
                    last = edatafilesize;
                }
                //char * bufp = ((char*)edgedata + range_start_edge_ptr);
                int startblock = (int) (range_start_edge_ptr / blocksize);
                int endblock = (int) (last / blocksize);
                for(int i=0; i < nblocks; i++) {
                    if (i >= startblock && i <= endblock) {
                        iomgr->managed_pwritea_now(block_edatasessions[i], &edgedata[i], blocksizes[i], 0);
                    }
                    iomgr->managed_release(block_edatasessions[i], &edgedata[i]);
                    edgedata[i] = NULL;
                    iomgr->close_session(block_edatasessions[i]);
                }
            } else {
              for(int i=0; i < nblocks; i++) {
                  iomgr->close_session(block_edatasessions[i]);
              }
            }
            
            m.stop_time(cm, "memshard_commit");
            
            iomgr->managed_release(adj_session, &adjdata);
            // FIXME: this is duplicated code from destructor
            for(int i=0; i < nblocks; i++) {
                if (edgedata[i] != NULL) {
                    iomgr->managed_release(block_edatasessions[i], &edgedata[i]);
                }
            }
            block_edatasessions.clear();
            is_loaded = false;
        }
        
        bool loaded() {
            return is_loaded;
        }
        
    private:
        
        void load_edata() {
            assert(blocksize % sizeof(ET) == 0);
            int nblocks = (int) (edatafilesize / blocksize + (edatafilesize % blocksize != 0));
            edgedata = (char **) calloc(nblocks, sizeof(char*));
            size_t compressedsize = 0;
            int blockid = 0;
            
            if (!async_edata_loading) {
                doneptr = (int *) malloc(nblocks * sizeof(int));
                for(int i=0; i < nblocks; i++) doneptr[i] = 1;
            }
            
            while(true) {
                std::string block_filename = filename_shard_edata_block(filename_edata, blockid, blocksize);
                if (file_exists(block_filename)) {
                    size_t fsize = std::min(edatafilesize - blocksize * blockid, blocksize);
                    
                    compressedsize += get_filesize(block_filename);
                    int blocksession = iomgr->open_session(block_filename, false, true); // compressed
                    block_edatasessions.push_back(blocksession);
                    blocksizes.push_back(fsize);
                    
                    edgedata[blockid] = NULL;
                    iomgr->managed_malloc(blocksession, &edgedata[blockid], fsize, 0);
                    if (async_edata_loading) {
                        iomgr->managed_preada_async(blocksession, &edgedata[blockid], fsize, 0);
                    } else {
                        iomgr->managed_preada_async(blocksession, &edgedata[blockid], fsize, 0, (volatile int *)&doneptr[blockid]);
                    }
                    blockid++;
                    
                } else {
                    if (blockid == 0) {
                        logstream(LOG_ERROR) << "Shard block file did not exists:" << block_filename << std::endl;
                    }
                    if (blockid < nblocks) {
                        logstream(LOG_ERROR) << "Did not find block " << block_filename << std::endl;
                        logstream(LOG_ERROR) << "Going to exit..." << std::endl;
                    }
                    break;
                }
            }
            logstream(LOG_DEBUG) << "Compressed/full size: " << compressedsize * 1.0 / edatafilesize <<
            " number of blocks: " << nblocks << std::endl;
            assert(blockid == nblocks);
            
        }
        
        
    public:
        
        // TODO: recycle ptr!
        void load() {
            is_loaded = true;
            adjfilesize = get_filesize(filename_adj);
            
#ifdef SUPPORT_DELETIONS
            async_edata_loading = false;  // Currently we encode the deleted status of an edge into the edge value (should be changed!),
            // so we need the edge data while loading
#endif
            
            //preada(adjf, adjdata, adjfilesize, 0);
            
            adj_session = iomgr->open_session(filename_adj, true);
            iomgr->managed_malloc(adj_session, &adjdata, adjfilesize, 0);
            adj_stream_session = streaming_task(iomgr, adj_session, adjfilesize, (char**) &adjdata);
            
            iomgr->launch_stream_reader(&adj_stream_session);
            /* Initialize edge data asynchonous reading */
            if (!only_adjacency) {
                edatafilesize = get_shard_edata_filesize<ET>(filename_edata);
                load_edata();
            }
        }
        
        
        
        inline void check_stream_progress(int toread, size_t pos) {
            if (adj_stream_session.curpos == adjfilesize) return;
            
            while(adj_stream_session.curpos < toread+pos) {
                usleep(20000);
                if (adj_stream_session.curpos == adjfilesize) return;
            }
        }
        
        void load_vertices(vid_t window_st, vid_t window_en, std::vector<svertex_t> & prealloc, bool inedges=true, bool outedges=true) {
            /* Find file size */
            m.start_time("memoryshard_create_edges");
            
            assert(adjdata != NULL);
            
            // Now start creating vertices
            uint8_t * ptr = adjdata;
            uint8_t * end = ptr + adjfilesize;
            vid_t vid = 0;
            edgeptr = 0;
            
            streaming_offset = 0;
            streaming_offset_vid = 0;
            streaming_offset_edge_ptr = 0;
            range_start_offset = adjfilesize;
            range_start_edge_ptr = edatafilesize;
            
            bool setoffset = false;
            bool setrangeoffset = false;
            while (ptr < end) {
                check_stream_progress(6, ptr-adjdata); // read at least 6 bytes
                if (!setoffset && vid > range_end) {
                    // This is where streaming should continue. Notice that because of the
                    // non-zero counters, this might be a bit off.
                    streaming_offset = ptr-adjdata;
                    streaming_offset_vid = vid;
                    streaming_offset_edge_ptr = edgeptr;
                    setoffset = true;
                }
                if (!setrangeoffset && vid>=range_st) {
                    range_start_offset = ptr-adjdata;
                    range_start_edge_ptr = edgeptr;
                    setrangeoffset = true;
                }
                
                uint8_t ns = *ptr;
                int n;
                
                ptr += sizeof(uint8_t);
                
                if (ns == 0x00) {
                    // next value tells the number of vertices with zeros
                    uint8_t nz = *ptr;
                    ptr += sizeof(uint8_t);
                    vid++;
                    vid += nz;
                    continue;
                }
                
                if (ns == 0xff) {  // If 255 is not enough, then stores a 32-bit integer after.
                    n = *((uint32_t*)ptr);
                    ptr += sizeof(uint32_t);
                } else {
                    n = ns;
                }
                svertex_t* vertex = NULL;
                
                if (vid>=window_st && vid <=window_en) { // TODO: Make more efficient
                    vertex = &prealloc[vid-window_st];
                    if (!vertex->scheduled) vertex = NULL;
                }
                check_stream_progress(n * 4, ptr - adjdata);
                bool any_edges = false;
                while(--n>=0) {
                    int blockid = (int) (edgeptr / blocksize);
                    if (!async_edata_loading && !only_adjacency) {
                        /* Wait until blocks loaded (non-asynchronous version) */
                        while(doneptr[edgeptr / blocksize] != 0) { usleep(10); }
                    }
                    
                    vid_t target = *((vid_t*) ptr);
                    ptr += sizeof(vid_t);
                    if (vertex != NULL && outedges)
                    {
                        char * eptr = (only_adjacency ? NULL  : &(edgedata[blockid][edgeptr % blocksize]));
                        vertex->add_outedge(target, (only_adjacency ? NULL : (ET*) eptr), false);
                    }
                    
                    if (target >= window_st)  {
                        if (target <= window_en) {                        /* In edge */
                            if (inedges) {
                                svertex_t & dstvertex = prealloc[target - window_st];
                                if (dstvertex.scheduled) {
                                    any_edges = true;
                                    //  assert(only_adjacency ||  edgeptr < edatafilesize);
                                    char * eptr = (only_adjacency ? NULL  : &(edgedata[blockid][edgeptr % blocksize]));
                                    
                                    dstvertex.add_inedge(vid,  (only_adjacency ? NULL : (ET*) eptr), false);
                                    dstvertex.parallel_safe = dstvertex.parallel_safe && (vertex == NULL); // Avoid if
                                }
                            }
                        } else { // Note, we cannot skip if there can be "special edges". FIXME so dirty.
                            // This vertex has no edges any more for this window, bail out
                            if (vertex == NULL) {
                                ptr += sizeof(vid_t) * n;
                                edgeptr += (n + 1) * sizeof(ET);
                                break;
                            }
                        }
                    }
                    edgeptr += sizeof(ET);
                    
                }
                
                if (any_edges && vertex != NULL) {
                    vertex->parallel_safe = false;
                }
                vid++;
            }
            m.stop_time("memoryshard_create_edges", false);
        }
        
        size_t offset_for_stream_cont() {
            return streaming_offset;
        }
        vid_t offset_vid_for_stream_cont() {
            return streaming_offset_vid;
        }
        size_t edata_ptr_for_stream_cont() {
            return streaming_offset_edge_ptr;
        }
        
        
        
        
    };
};

#endif
#endif
