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
 * The basic GraphChi engine.
 */


#ifndef DEF_GRAPHCHI_GRAPHCHI_ENGINE
#define DEF_GRAPHCHI_GRAPHCHI_ENGINE


#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <omp.h>
#include <vector>
#include <sys/time.h>

#include <am/graphchi/api/chifilenames.hpp>
#include <am/graphchi/api/graph_objects.hpp>
#include <am/graphchi/api/graphchi_context.hpp>
#include <am/graphchi/api/graphchi_program.hpp>
#include <am/graphchi/engine/auxdata/degree_data.hpp>
#include <am/graphchi/engine/auxdata/vertex_data.hpp>
#include <am/graphchi/engine/bitset_scheduler.hpp>
#include <am/graphchi/io/stripedio.hpp>
#include <am/graphchi/logger/logger.hpp>
#include <am/graphchi/metrics/metrics.hpp>
#include <am/graphchi/shards/memoryshard.hpp>
#include <am/graphchi/shards/slidingshard.hpp>
#include <am/graphchi/util/pthread_tools.hpp>


namespace graphchi {
    
    template <typename VertexDataType, typename EdgeDataType,   
    typename svertex_t = graphchi_vertex<VertexDataType, EdgeDataType> >
    
    class graphchi_engine {
    public:     
        typedef sliding_shard<VertexDataType, EdgeDataType, svertex_t> slidingshard_t;
        typedef memory_shard<VertexDataType, EdgeDataType, svertex_t> memshard_t;
        
    protected:
        std::string base_filename;
        int nshards;
        
        /* IO manager */
        stripedio * iomgr;
        
        /* Shards */
        std::vector<slidingshard_t *> sliding_shards;
        memshard_t * memoryshard;
        std::vector<std::pair<vid_t, vid_t> > intervals;
        
        /* Auxilliary data handlers */
        degree_data * degree_handler;
        vertex_data_store<VertexDataType> * vertex_data_handler;
        
        /* Computational context */
        graphchi_context chicontext;
        
        /* Scheduler */
        bitset_scheduler * scheduler;
        
        /* Configuration */
        bool modifies_outedges;
        bool modifies_inedges;
        bool only_adjacency;
        bool use_selective_scheduling;
        bool enable_deterministic_parallelism;
        bool store_inedges;
        bool disable_vertexdata_storage;
        bool preload_commit; //alow storing of modified edge data on preloaded data into memory

        size_t blocksize;
        int membudget_mb;
        int load_threads;
        int exec_threads;
        
        /* State */
        vid_t sub_interval_st;
        vid_t sub_interval_en;
        int iter;
        int niters;
        int exec_interval;
        size_t nupdates;
        size_t nedges;
        size_t work; // work is the number of edges processed
        unsigned int maxwindow;
        mutex modification_lock;
        
        bool reset_vertexdata;
        
        
        /* Metrics */
        metrics &m;
        
        void print_config() {
            logstream(LOG_INFO) << "Engine configuration: " << std::endl;
            logstream(LOG_INFO) << " exec_threads = " << exec_threads << std::endl;
            logstream(LOG_INFO) << " load_threads = " << load_threads << std::endl;
            logstream(LOG_INFO) << " membudget_mb = " << membudget_mb << std::endl;
            logstream(LOG_INFO) << " blocksize = " << blocksize << std::endl;
            logstream(LOG_INFO) << " scheduler = " << use_selective_scheduling << std::endl;
        }
        
    public:
        
        /**
         * Initialize GraphChi engine
         * @param base_filename prefix of the graph files
         * @param nshards number of shards
         * @param selective_scheduling if true, uses selective scheduling 
         */
        graphchi_engine(std::string _base_filename, int _nshards, bool _selective_scheduling, metrics &_m) : base_filename(_base_filename), nshards(_nshards), use_selective_scheduling(_selective_scheduling), m(_m) {
            /* Initialize IO */
            m.start_time("iomgr_init");
            iomgr = new stripedio(m);
            if (disable_preloading()) {
                iomgr->set_disable_preloading(true);
            }
            m.stop_time("iomgr_init");
#ifndef DYNAMICEDATA
            logstream(LOG_INFO) << "Initializing graphchi_engine. This engine expects " << sizeof(EdgeDataType)
            << "-byte edge data. " << std::endl;
#else
            logstream(LOG_INFO) << "Initializing graphchi_engine with dynamic edge-data. This engine expects " << sizeof(int)
            << "-byte edge data. " << std::endl;

#endif
            /* If number of shards is unspecified - discover */
            if (nshards < 1) {
                nshards = get_option_int("nshards", 0);
                if (nshards < 1) {
                    logstream(LOG_WARNING) << "Number of shards was not specified (command-line argument 'nshards'). Trying to detect. " << std::endl;
                    nshards = discover_shard_num();
                }
            }
            
            /* Initialize a plenty of fields */
            memoryshard = NULL;
            modifies_outedges = true;
            modifies_inedges = true;
            preload_commit = true;
            only_adjacency = false;
            reset_vertexdata = false;
            blocksize = get_option_long("blocksize", 4096 * 1024);
#ifndef DYNAMICEDATA
            while (blocksize % sizeof(EdgeDataType) != 0) blocksize++;
#endif
            
            disable_vertexdata_storage = false;

            membudget_mb = get_option_int("membudget_mb", 1024);
            nupdates = 0;
            iter = 0;
            work = 0;
            nedges = 0;
            scheduler = NULL;
            store_inedges = true;
            degree_handler = NULL;
            vertex_data_handler = NULL;
            enable_deterministic_parallelism = true;
            load_threads = get_option_int("loadthreads", 2);
            exec_threads = get_option_int("execthreads", omp_get_max_threads());
            maxwindow = 40000000;

            /* Load graph shard interval information */
            _load_vertex_intervals();
            
            _m.set("file", _base_filename);
            _m.set("engine", "default");
            _m.set("nshards", (size_t)nshards);
        }
        
        virtual ~graphchi_engine() {
            if (degree_handler != NULL) delete degree_handler;
            if (vertex_data_handler != NULL) delete vertex_data_handler;
            if (memoryshard != NULL) {
                delete memoryshard;
                memoryshard = NULL;
            }
            for(int i=0; i < (int)sliding_shards.size(); i++) {
                if (sliding_shards[i] != NULL) {
                    delete sliding_shards[i];
                }
                sliding_shards[i] = NULL;
            }
            degree_handler = NULL;
            vertex_data_handler = NULL;
            delete iomgr;
        }
        
        
    protected:
        
        virtual degree_data * create_degree_handler() {
            return new degree_data(base_filename, iomgr);
        }
        
        virtual bool disable_preloading() {
            return false;
        }
        
        /**
         * Try to find suitable shards by trying with different
         * shard numbers. Looks up to shard number 2000.
         */
        int discover_shard_num() {
#ifndef DYNAMICEDATA
            int _nshards = find_shards<EdgeDataType>(base_filename);
#else
            int _nshards = find_shards<int>(base_filename);
#endif
            if (_nshards == 0) {
                logstream(LOG_ERROR) << "Could not find suitable shards - maybe you need to run sharder to create them?" << std::endl;
                logstream(LOG_ERROR) << "Was looking with filename [" << base_filename << "]" << std::endl;
                logstream(LOG_ERROR) << "You need to create the shards with edge data-type of size " << sizeof(EdgeDataType) << " bytes." << std::endl;
                logstream(LOG_ERROR) << "To specify the number of shards, use command-line parameter 'nshards'" << std::endl;
                assert(0);
            }
            return _nshards;
        }
        
        
        virtual void initialize_sliding_shards() {
            assert(sliding_shards.size() == 0);
            for(int p=0; p < nshards; p++) {
#ifndef DYNAMICEDATA
                std::string edata_filename = filename_shard_edata<EdgeDataType>(base_filename, p, nshards);
                std::string adj_filename = filename_shard_adj(base_filename, p, nshards);
                /* Let the IO manager know that we will be reading these files, and
                 it should decide whether to preload them or not.
                 */
                iomgr->allow_preloading(edata_filename);
                iomgr->allow_preloading(adj_filename);
#else
                std::string edata_filename = filename_shard_edata<int>(base_filename, p, nshards);
                std::string adj_filename = filename_shard_adj(base_filename, p, nshards);
#endif
                
                
                sliding_shards.push_back(
                                         new slidingshard_t(iomgr, edata_filename, 
                                                            adj_filename,
                                                            intervals[p].first, 
                                                            intervals[p].second, 
                                                            blocksize, 
                                                            m, 
                                                            !modifies_outedges, 
                                                            only_adjacency));
                if (!only_adjacency) 
                    nedges += sliding_shards[sliding_shards.size() - 1]->num_edges();
            }
            
        }
        
        virtual void initialize_scheduler() {
            if (use_selective_scheduling) {
                if (scheduler != NULL) delete scheduler;
                scheduler = new bitset_scheduler((int) num_vertices());
                scheduler->add_task_to_all();
            } else {
                scheduler = NULL;
            }
        }
        
        /**
         * If the data is only in one shard, we can just
         * keep running from memory.
         */
        bool is_inmemory_mode() {
            return nshards == 1;
        }
        
        
        /**
         * Extends the window to fill the memory budget, but not over maxvid
         */
        virtual vid_t determine_next_window(vid_t iinterval, vid_t fromvid, vid_t maxvid, size_t membudget) {
            /* Load degrees */
            degree_handler->load(fromvid, maxvid);
            
            /* If is in-memory-mode, memory budget is not considered. */
            if (is_inmemory_mode()) {
                return maxvid;
            } else {
                size_t memreq = 0;
                int max_interval = maxvid - fromvid;
                for(int i=0; i < max_interval; i++) {
                    degree deg = degree_handler->get_degree(fromvid + i);
                    int inc = deg.indegree;
                    int outc = deg.outdegree;
                    
                    // Raw data and object cost included
                    memreq += sizeof(svertex_t) + (sizeof(EdgeDataType) + sizeof(vid_t) + sizeof(graphchi_edge<EdgeDataType>))*(outc + inc);
                    if (memreq > membudget) {
                        logstream(LOG_DEBUG) << "Memory budget exceeded with " << memreq << " bytes." << std::endl;
                        return fromvid + i - 1;  // Previous was enough
                    }
                }
                return maxvid;
            }
        }
        
        /** 
         * Calculates the exact number of edges
         * required to load in the subinterval.
         */
        size_t num_edges_subinterval(vid_t st, vid_t en) {
            size_t num_edges = 0;
            int nvertices = en - st + 1;
            if (scheduler != NULL) {
                for(int i=0; i < nvertices; i++) {
                    bool is_sched = scheduler->is_scheduled(st + i);
                    if (is_sched) {
                        degree d = degree_handler->get_degree(st + i);
                        num_edges += d.indegree * store_inedges + d.outdegree;
                    }
                }
            } else {
                for(int i=0; i < nvertices; i++) {
                    degree d = degree_handler->get_degree(st + i);
                    num_edges += d.indegree * store_inedges + d.outdegree;
                }
            }
            return num_edges;
        }
        
        virtual void load_before_updates(std::vector<svertex_t> &vertices) {
            omp_set_num_threads(load_threads);
#pragma omp parallel for schedule(dynamic, 1)
            for(int p=-1; p < nshards; p++)  {
                if (p==(-1)) {
                    /* Load memory shard */
                    if (!memoryshard->loaded()) {
                        memoryshard->load();
                    }
                    
                    /* Load vertex edges from memory shard */
                    memoryshard->load_vertices(sub_interval_st, sub_interval_en, vertices);
                    
                    /* Load vertices */ 
                    vertex_data_handler->load(sub_interval_st, sub_interval_en);

                    /* Load vertices */
                    if (!disable_vertexdata_storage) {
                        vertex_data_handler->load(sub_interval_st, sub_interval_en);
                    }
                } else {
                    /* Load edges from a sliding shard */
                    if (p != exec_interval) {
                        sliding_shards[p]->read_next_vertices((int) vertices.size(), sub_interval_st, vertices,
                                                              scheduler != NULL && chicontext.iteration == 0);
                        
                    }
                }
            }
            
            /* Wait for all reads to complete */
            iomgr->wait_for_reads();
        }
        
        void exec_updates(GraphChiProgram<VertexDataType, EdgeDataType, svertex_t> &userprogram,
                          std::vector<svertex_t> &vertices) {
            metrics_entry me = m.start_time();
            size_t nvertices = vertices.size();
            if (!enable_deterministic_parallelism) {
                for(int i=0; i < (int)nvertices; i++) vertices[i].parallel_safe = true;
            }
            
            omp_set_num_threads(exec_threads);
            
#pragma omp parallel sections 
            {
#pragma omp section
                {
#pragma omp parallel for schedule(dynamic)
                    for(int vid=sub_interval_st; vid <= (int)sub_interval_en; vid++) {
                        svertex_t & v = vertices[vid - sub_interval_st];
                        
                        if (exec_threads == 1 || v.parallel_safe) {
                            if (!disable_vertexdata_storage)
                                v.dataptr = vertex_data_handler->vertex_data_ptr(vid);
                            if (v.scheduled) 
                                userprogram.update(v, chicontext);
                        }
                    }
                }
#pragma omp section
                {
                    if (exec_threads > 1 && enable_deterministic_parallelism) {
                        int nonsafe_count = 0;
                        for(int vid=sub_interval_st; vid <= (int)sub_interval_en; vid++) {
                            svertex_t & v = vertices[vid - sub_interval_st];
                            if (!v.parallel_safe && v.scheduled) {
                                if (!disable_vertexdata_storage)
                                    v.dataptr = vertex_data_handler->vertex_data_ptr(vid);
                                userprogram.update(v, chicontext);
                                nonsafe_count++;
                            }
                        }
                        
                        m.add("serialized-updates", nonsafe_count);
                    }
                }
            }
            m.stop_time(me, "execute-updates");
        }
        

        /**
         Special method for running all iterations with the same vertex-vector.
         This is a hacky solution.

         FIXME:  this does not work well with deterministic parallelism. Needs a
         a separate analysis phase to check which vertices can be run in parallel, and
         then run it in chunks. Not difficult.
         **/
        void exec_updates_inmemory_mode(GraphChiProgram<VertexDataType, EdgeDataType, svertex_t> &userprogram,
                                        std::vector<svertex_t> &vertices) {
            work = nupdates = 0;
            for(iter=0; iter<niters; iter++) {
                logstream(LOG_INFO) << "In-memory mode: Iteration " << iter << " starts." << std::endl;
                chicontext.iteration = iter;
                userprogram.before_iteration(iter, chicontext);
                userprogram.before_exec_interval(0, (int)num_vertices(), chicontext);

                if (use_selective_scheduling) {
                    if (iter > 0 && !scheduler->has_new_tasks) {
                        logstream(LOG_INFO) << "No new tasks to run!" << std::endl;
                        break;
                    }
                    for(int i=0; i < (int)vertices.size(); i++) { // Could, should parallelize
                        if (iter == 0 || scheduler->is_scheduled(i)) {
                            vertices[i].scheduled =  true;
                            nupdates++;
                            work += vertices[i].inc + vertices[i].outc;
                        } else {
                            vertices[i].scheduled = false;
                        }
                    }
                    
                    scheduler->has_new_tasks = false; // Kind of misleading since scheduler may still have tasks - but no new tasks.
                    scheduler->remove_tasks(0, (int)num_vertices());
                } else {
                    nupdates += num_vertices();
                    work += num_edges();
                }
                
                exec_updates(userprogram, vertices);
                load_after_updates(vertices);
                
                userprogram.after_exec_interval(0, (int)num_vertices(), chicontext);
                userprogram.after_iteration(iter, chicontext);
                if (chicontext.last_iteration > 0 && chicontext.last_iteration <= iter){
                   logstream(LOG_INFO)<<"Stopping engine since last iteration was set to: " << chicontext.last_iteration << std::endl;
                   break;
                }

            }
        }
        

        virtual void init_vertices(std::vector<svertex_t> &vertices, graphchi_edge<EdgeDataType> * &edata) {
            size_t nvertices = vertices.size();
            
            /* Compute number of edges */
            size_t num_edges = num_edges_subinterval(sub_interval_st, sub_interval_en);
            
            /* Allocate edge buffer */
            edata = (graphchi_edge<EdgeDataType>*) malloc(num_edges * sizeof(graphchi_edge<EdgeDataType>));
            
            /* Assign vertex edge array pointers */
            size_t ecounter = 0;
            for(int i=0; i < (int)nvertices; i++) {
                degree d = degree_handler->get_degree(sub_interval_st + i);
                int inc = d.indegree;
                int outc = d.outdegree;
                vertices[i] = svertex_t(sub_interval_st + i, &edata[ecounter], 
                                        &edata[ecounter + inc * store_inedges], inc, outc);
                if (scheduler != NULL) {
                    bool is_sched = ( scheduler->is_scheduled(sub_interval_st + i));
                    if (is_sched) {
                        vertices[i].scheduled =  true;
                        nupdates++;
                        ecounter += inc * store_inedges + outc;
                    }
                } else {
                    nupdates++; 
                    vertices[i].scheduled =  true;
                    ecounter += inc * store_inedges + outc;               
                }
            }                   
            work += ecounter;
            assert(ecounter <= num_edges);
        }
        
        
        void save_vertices(std::vector<svertex_t> &vertices) {
            if (disable_vertexdata_storage) return;
            size_t nvertices = vertices.size();
            bool modified_any_vertex = false;
            for(int i=0; i < (int)nvertices; i++) {
                if (vertices[i].modified) {
                    modified_any_vertex = true;
                    break;
                }
            }
            if (modified_any_vertex) {
                vertex_data_handler->save();
            }
        }
        
        virtual void load_after_updates(std::vector<svertex_t> &vertices) {
            // Do nothing.
        }   
        
        virtual void write_delta_log() {
            // Write delta log
            std::string deltafname = iomgr->multiplexprefix(0) + base_filename + ".deltalog";
            FILE * df = fopen(deltafname.c_str(), (chicontext.iteration == 0  ? "w" : "a"));
            fprintf(df, "%d,%lu,%lu,%lf\n", chicontext.iteration, nupdates, work, chicontext.get_delta()); 
            fclose(df);
        }
        
    public:
        
        virtual std::pair<vid_t, vid_t> get_interval(int i) {
            return intervals[i];
        }
        
        /**
         * Returns first vertex of i'th interval.
         */
        vid_t get_interval_start(int i) {
            return get_interval(i).first;
        }
        
        /** 
         * Returns last vertex (inclusive) of i'th interval.
         */
        vid_t get_interval_end(int i) {
            return get_interval(i).second;
        }
        
        virtual size_t num_vertices() {
            return 1 + intervals[nshards - 1].second;
        }
        
        graphchi_context &get_context() {
            return chicontext;
        }
        
        virtual int get_nshards() {
            return nshards;
        }
        
        size_t num_updates() {
            return nupdates;
        }
        
        /**
         * Thread-safe version of num_edges
         */
        virtual size_t num_edges_safe() {
            return num_edges();
        }
        
        virtual size_t num_buffered_edges() {
            return 0;
        }
        
        /** 
         * Counts the number of edges from shard sizes.
         */
        virtual size_t num_edges() {
            if (sliding_shards.size() == 0) {
                logstream(LOG_ERROR) << "engine.num_edges() can be called only after engine has been started. To be fixed later. As a workaround, put the engine into a global variable, and query the number afterwards in begin_iteration(), for example." << std::endl;
                assert(false);
            }
            if (only_adjacency) {
                // TODO: fix.
                logstream(LOG_ERROR) << "Asked number of edges, but engine was run without edge-data." << std::endl; 
                return 0;
            }
            return nedges;
        }
        
        /**
         * Checks whether any vertex is scheduled in the given interval.
         * If no scheduler is configured, returns always true.
         */
        // TODO: support for a minimum fraction of scheduled vertices
        bool is_any_vertex_scheduled(vid_t st, vid_t en) {
            if (scheduler == NULL) return true;
            for(vid_t v=st; v<=en; v++) {
                if (scheduler->is_scheduled(v)) {
                    return true;
                }
            }
            return false;
        }
        
        virtual void initialize_iter() {
            // Do nothing
        }
        
        virtual void initialize_before_run() {
            if (reset_vertexdata) {
                vertex_data_handler->clear(num_vertices());
            }
        }
        
        virtual memshard_t * create_memshard(vid_t interval_st, vid_t interval_en) {
#ifndef DYNAMICEDATA
            return new memshard_t(this->iomgr,
                                  filename_shard_edata<EdgeDataType>(base_filename, exec_interval, nshards),  
                                  filename_shard_adj(base_filename, exec_interval, nshards),  
                                  interval_st, 
                                  interval_en,
                                  blocksize,
                                  m);
#else
            return new memshard_t(this->iomgr,
                                  filename_shard_edata<int>(base_filename, exec_interval, nshards),
                                  filename_shard_adj(base_filename, exec_interval, nshards),
                                  interval_st,
                                  interval_en,
                                  blocksize,
                                  m);
#endif
        }
        
        /**
         * Run GraphChi program, specified as a template 
         * parameter. 
         * @param niters number of iterations
         */
        void run(GraphChiProgram<VertexDataType, EdgeDataType, svertex_t> &userprogram, int _niters) {
            m.start_time("runtime");
            if (degree_handler == NULL)
                degree_handler = create_degree_handler();


            niters = _niters;
            logstream(LOG_INFO) << "GraphChi starting" << std::endl;
            logstream(LOG_INFO) << "Licensed under the Apache License 2.0" << std::endl;
            logstream(LOG_INFO) << "Copyright Aapo Kyrola et al., Carnegie Mellon University (2012)" << std::endl;
            
            if (vertex_data_handler == NULL)
                vertex_data_handler = new vertex_data_store<VertexDataType>(base_filename, num_vertices(), iomgr);
        
            initialize_before_run();
            
            
            /* Setup */
            if (sliding_shards.size() == 0) {
                initialize_sliding_shards();
            } else {
                logstream(LOG_DEBUG) << "Engine being restarted, do not reinitialize." << std::endl;
            }
                
            initialize_scheduler();
            omp_set_nested(1);
            
            /* Install a 'mock'-scheduler to chicontext if scheduler
             is not used. */
            chicontext.scheduler = scheduler;
            if (scheduler == NULL) {
                chicontext.scheduler = new non_scheduler();
            }
            
            /* Print configuration */
            print_config();
            
            
            /* Main loop */
            for(iter=0; iter < niters; iter++) {
                logstream(LOG_INFO) << "Start iteration: " << iter << std::endl;
                
                initialize_iter();
                
                /* Check vertex data file has the right size (number of vertices may change) */
                if (!disable_vertexdata_storage)
                    vertex_data_handler->check_size(num_vertices());
                
                /* Keep the context object updated */
                chicontext.filename = base_filename;
                chicontext.iteration = iter;
                chicontext.num_iterations = niters;
                chicontext.nvertices = num_vertices();
                if (!only_adjacency) chicontext.nedges = num_edges();
                
                chicontext.execthreads = exec_threads;
                chicontext.reset_deltas(exec_threads);
                
                /* Call iteration-begin event handler */
                if (!is_inmemory_mode())  // Run sepately
                    userprogram.before_iteration(iter, chicontext);
                
                /* Check scheduler. If no scheduled tasks, terminate. */
                if (use_selective_scheduling) {
                    if (scheduler != NULL) {
                        if (!scheduler->has_new_tasks) {
                            logstream(LOG_INFO) << "No new tasks to run!" << std::endl;
                            break;
                        }
                        scheduler->has_new_tasks = false; // Kind of misleading since scheduler may still have tasks - but no new tasks.
                    }
                }
                
                /* Interval loop */
                for(exec_interval=0; exec_interval < nshards; ++exec_interval) {
                    /* Determine interval limits */
                    vid_t interval_st = get_interval_start(exec_interval);
                    vid_t interval_en = get_interval_end(exec_interval);
                    
                    if (interval_st > interval_en) continue; // Can happen on very very small graphs.

                    if (!is_inmemory_mode())
                        userprogram.before_exec_interval(interval_st, interval_en, chicontext);

                    /* Flush stream shard for the exec interval */
                    sliding_shards[exec_interval]->flush();
                    iomgr->wait_for_writes(); // Actually we would need to only wait for         writes of given shard. TODO.
                    
                    /* Initialize memory shard */
                    if (memoryshard != NULL) delete memoryshard;
                    memoryshard = create_memshard(interval_st, interval_en);
                    memoryshard->only_adjacency = only_adjacency;
                    
                    sub_interval_st = interval_st;
                    logstream(LOG_INFO) << chicontext.runtime() << "s: Starting: " 
                    << sub_interval_st << " -- " << interval_en << std::endl;
                    
                    while (sub_interval_st <= interval_en) {
                        
                        modification_lock.lock();
                        /* Determine the sub interval */
                        sub_interval_en = determine_next_window(exec_interval,
                                                                sub_interval_st, 
                                                                std::min(interval_en, sub_interval_st + maxwindow), 
                                                                size_t(membudget_mb) * 1024 * 1024);
                        assert(sub_interval_en >= sub_interval_st);
                        
                        logstream(LOG_INFO) << "Iteration " << iter << "/" << (niters - 1) << ", subinterval: " << sub_interval_st << " - " << sub_interval_en << std::endl;
                        
                        bool any_vertex_scheduled = is_any_vertex_scheduled(sub_interval_st, sub_interval_en);
                        if (!any_vertex_scheduled) {
                            logstream(LOG_INFO) << "No vertices scheduled, skip." << std::endl;
                            sub_interval_st = sub_interval_en + 1;
                            modification_lock.unlock();
                            continue;
                        }
                        
                        /* Initialize vertices */
                        int nvertices = sub_interval_en - sub_interval_st + 1;
                        graphchi_edge<EdgeDataType> * edata = NULL;
                        
                        std::vector<svertex_t> vertices(nvertices, svertex_t());
                        init_vertices(vertices, edata);
                        
                        /* Now clear scheduler bits for the interval */
                        if (scheduler != NULL)
                            scheduler->remove_tasks(sub_interval_st, sub_interval_en);
                        
                        /* Load data */
                        load_before_updates(vertices);                        
                        
                        modification_lock.unlock();
                        
                        logstream(LOG_INFO) << "Start updates" << std::endl;
                        /* Execute updates */
                        if (!is_inmemory_mode()) {
                            exec_updates(userprogram, vertices);
                            /* Load phase after updates (used by the functional engine) */
                            load_after_updates(vertices);
                        } else {

                            exec_updates_inmemory_mode(userprogram, vertices); 
                        }
                        logstream(LOG_INFO) << "Finished updates" << std::endl;
                        
                        
                        /* Save vertices */
                        if (!disable_vertexdata_storage) {
                            save_vertices(vertices);
                        }
                        sub_interval_st = sub_interval_en + 1;
                        
                        /* Delete edge buffer. TODO: reuse. */
                        if (edata != NULL) {
                            delete edata;
                            edata = NULL;
                        }
                       
                    } // while subintervals

                    if (memoryshard->loaded() && !is_inmemory_mode()) {
                        logstream(LOG_INFO) << "Commit memshard" << std::endl;

                        memoryshard->commit(modifies_inedges, modifies_outedges);

                        sliding_shards[exec_interval]->set_offset(memoryshard->offset_for_stream_cont(), memoryshard->offset_vid_for_stream_cont(),
                                                                  memoryshard->edata_ptr_for_stream_cont());
                        
                        delete memoryshard;
                        memoryshard = NULL;
                    }     
                    if (!is_inmemory_mode())
                        userprogram.after_exec_interval(interval_st, interval_en, chicontext);

                } // For exec_interval
                
                if (!is_inmemory_mode())  // Run sepately
                    userprogram.after_iteration(iter, chicontext);
                
                /* Move the sliding shard of the current interval to correct position and flush
                 writes of all shards for next iteration. */
                for(int p=0; p<nshards; p++) {
                    sliding_shards[p]->flush();
                    sliding_shards[p]->set_offset(0, 0, 0);
                }
                iomgr->wait_for_writes();
                
                /* Write progress log */
                write_delta_log();
                
                /* Check if user has defined a last iteration */
                if (chicontext.last_iteration >= 0) {
                    niters = chicontext.last_iteration + 1;
                    logstream(LOG_DEBUG) << "Last iteration is now: " << (niters-1) << std::endl;
                }
                iteration_finished();
            } // Iterations
            
            // Commit preloaded shards
            if (preload_commit)
              iomgr->commit_preloaded();
            
            m.stop_time("runtime");
            
            m.set("updates", nupdates);
            m.set("work", work);
            m.set("nvertices", num_vertices());
            m.set("execthreads", (size_t)exec_threads);
            m.set("loadthreads", (size_t)load_threads);
#ifndef GRAPHCHI_DISABLE_COMPRESSION
            m.set("compression", 1);
#endif
            
            m.set("scheduler", (size_t)use_selective_scheduling);
            m.set("niters", niters);
            // Stop HTTP admin
        }
        
        virtual void iteration_finished() {
            // Do nothing
        }
        
        stripedio * get_iomanager() {
            return iomgr;
        }
        
        virtual void set_modifies_inedges(bool b) {
            modifies_inedges = b;
        }
        
        virtual void set_modifies_outedges(bool b) {
            modifies_outedges = b;
        }
        
        virtual void set_only_adjacency(bool b) {
            only_adjacency = b;
        }

        virtual void set_preload_commit(bool b){
            preload_commit = b;
        }
        
        /**
         * Configure the blocksize used when loading shards.
         * Default is one megabyte.
         * @param blocksize_in_bytes the blocksize in bytes
         */
        void set_blocksize(size_t blocksize_in_bytes) {
            blocksize = blocksize_in_bytes;
        }
        
        /**
         * Set the amount of memory available for loading graph
         * data. Default is 1000 megabytes.
         * @param mbs amount of memory to be used.
         */
        void set_membudget_mb(int mbs) {
            membudget_mb = mbs;
        }
        
        
        void set_load_threads(int lt) {
            load_threads = lt;
        }
        
        void set_exec_threads(int et) {
            exec_threads = et;
        }
        
        /**
         * Sets whether the engine is run in the deterministic
         * mode. Default true.
         */
        void set_enable_deterministic_parallelism(bool b) {
#ifdef DYNAMICEDATA
            if (!b) {
                logstream(LOG_ERROR) << "With dynamic edge data, you cannot disable determinic parallelism." << std::endl;
                logstream(LOG_ERROR) << "Otherwise race conditions would corrupt the structure of the data." << std::endl;
                assert(b);
                return;
            }
#endif
            enable_deterministic_parallelism = b;
        }
      
    public:
        void set_disable_vertexdata_storage() {
            this->disable_vertexdata_storage = true;
        }
        
        void set_enable_vertexdata_storage() {
            this->disable_vertexdata_storage = false;
        }
       
        void set_maxwindow(unsigned int _maxwindow){ 
            maxwindow = _maxwindow;
        }; 
        
    protected:
              
        virtual void _load_vertex_intervals() {
            load_vertex_intervals(base_filename, nshards, intervals);
        }
        
    protected:
        mutex httplock;
        std::map<std::string, std::string> json_params;
        
    public:
        
        /**
         * Replace all shards with zero values in edges.
         */
        template<typename ET>
        void reinitialize_edge_data(ET zerovalue) {
            
            for(int p=0; p < nshards; p++) {
                std::string edatashardname =  filename_shard_edata<ET>(base_filename, p, nshards);
                std::string dirname = dirname_shard_edata_block(edatashardname, blocksize);
                size_t edatasize = get_shard_edata_filesize<ET>(edatashardname);
                logstream(LOG_INFO) << "Clearing data: " << edatashardname << " bytes: " << edatasize << std::endl;
                int nblocks = (edatasize / blocksize) + (edatasize % blocksize == 0 ? 0 : 1);
                for(int i=0; i < nblocks; i++) {
                    std::string block_filename = filename_shard_edata_block(edatashardname, i, blocksize);
                    int len = (int) std::min(edatasize - i * blocksize, blocksize);
                    int f = open(block_filename.c_str(), O_RDWR | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
                    ET * buf =  (ET *) malloc(len);
                    for(int i=0; i < (int) (len / sizeof(ET)); i++) {
                        buf[i] = zerovalue;
                    }
                    write_compressed(f, buf, len);
                    close(f);
                    
#ifdef DYNAMICEDATA
                    write_block_uncompressed_size(block_filename, len);
#endif
                    
                }
            }
        }
        
        
        /**
          * If true, the vertex data is initialized before
          * the engineis started. Default false.
          */
        void set_reset_vertexdata(bool reset) {
            reset_vertexdata = reset;
        }
        
        
        /**
         * HTTP admin management
         */
        
        void set_json(std::string key, std::string value) {
            httplock.lock();
            json_params[key] = value;
            httplock.unlock();
        }
        
        template <typename T>
        void set_json(std::string key, T val) {
            std::stringstream ss;
            ss << val;
            set_json(key, ss.str());
        }
        
        std::string get_info_json() {
            std::stringstream json;
            json << "{";
            json << "\"file\" : \"" << base_filename << "\",\n";
            json << "\"numOfShards\": " << nshards << ",\n";
            json << "\"iteration\": " << chicontext.iteration << ",\n";
            json << "\"numIterations\": " << chicontext.num_iterations << ",\n";
            json << "\"runTime\": " << chicontext.runtime() << ",\n";
            
            json << "\"updates\": " << nupdates << ",\n";
            json << "\"nvertices\": " << chicontext.nvertices << ",\n";
            json << "\"interval\":" << exec_interval << ",\n";
            json << "\"windowStart\":" << sub_interval_st << ",";
            json << "\"windowEnd\": " << sub_interval_en << ",";
            json << "\"shards\": [";
            
            for(int p=0; p < (int)nshards; p++) {
                if (p>0) json << ",";
                
                json << "{";
                json << "\"p\": " << p << ", ";
                json << sliding_shards[p]->get_info_json();
                json << "}";
            }
            
            json << "]";
            json << "}";
            return json.str();
        }
        
    };
    
    
};



#endif


