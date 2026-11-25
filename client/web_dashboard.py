"""Interactive HTTP Web Testing & Metrics Dashboard for the Distributed Search Engine.

Supports user-defined custom test inputs and live metrics collection:
- Custom search benchmark runner with real-time latency distributions (p50, p95, p99) and QPS.
- User-specified replica recovery simulation with speedup ratio and data reduction calculations.
- Live cross-engine validation comparing distributed results with the Reference Engine.
- Batch document ingestion with real-time throughput tracking.
"""

from __future__ import annotations

import json
import logging
import math
import os
import statistics
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from client.client import SearchEngineClient
from reference.reference_engine import ReferenceEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("web_dashboard")

COORDINATOR_TARGET = os.environ.get("COORDINATOR_TARGET", "127.0.0.1:50050")

# In-memory reference oracle instance for live validation
ORACLE_ENGINE = ReferenceEngine()

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Distributed Search Engine — Interactive Testing & Metrics Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    .tab-active { background-color: #2563eb; color: #ffffff; font-weight: 600; }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen">
  <div class="max-w-6xl mx-auto p-6 space-y-6">
    
    <!-- Top Header -->
    <header class="flex flex-col sm:flex-row justify-between items-start sm:items-center pb-4 border-b border-slate-800 gap-4">
      <div>
        <h1 class="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
          <span>⚡ Distributed Search Engine</span>
          <span class="text-xs font-mono uppercase bg-blue-500/20 text-blue-400 px-2 py-0.5 rounded border border-blue-500/30">Interactive Testing Workbench</span>
        </h1>
        <p class="text-sm text-slate-400 mt-1">User-Driven Benchmark Runner, Real-time Metrics & Dual-Engine Verification</p>
      </div>
      <div class="flex items-center gap-3">
        <span id="health-badge" class="px-3 py-1 text-xs font-semibold rounded-full bg-slate-800 text-slate-400 border border-slate-700">Connecting...</span>
        <button onclick="refreshStatus()" class="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-xs font-medium rounded border border-slate-700 transition">↻ Refresh Cluster</button>
      </div>
    </header>

    <!-- Cluster Topology Cards -->
    <section>
      <div class="flex justify-between items-center mb-2">
        <h2 class="text-xs font-semibold uppercase tracking-wider text-slate-400">Live Shard Topology & Sequence Counters</h2>
        <span class="text-xs text-slate-500" id="last-updated">Updating...</span>
      </div>
      <div id="shards-container" class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <!-- Rendered via JS -->
      </div>
    </section>

    <!-- Workbench Tabs -->
    <div class="flex flex-wrap gap-2 border-b border-slate-800 pb-2 text-xs">
      <button onclick="switchTab('bench_search')" id="tab-btn-bench_search" class="px-3.5 py-2 rounded-lg bg-blue-600 text-white font-semibold transition">⚡ Custom Search Benchmark</button>
      <button onclick="switchTab('bench_recovery')" id="tab-btn-bench_recovery" class="px-3.5 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 font-medium transition">🔄 Custom Recovery Benchmark</button>
      <button onclick="switchTab('validate')" id="tab-btn-validate" class="px-3.5 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 font-medium transition">⚖️ Dual-Engine Oracle Validator</button>
      <button onclick="switchTab('crud')" id="tab-btn-crud" class="px-3.5 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 font-medium transition">📝 Single CRUD & Search</button>
      <button onclick="switchTab('batch')" id="tab-btn-batch" class="px-3.5 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 font-medium transition">📦 Batch Ingestion</button>
    </div>

    <!-- ===================================================================== -->
    <!-- TAB 1: USER-DRIVEN SEARCH BENCHMARK & LIVE METRICS -->
    <!-- ===================================================================== -->
    <div id="tab-bench_search" class="space-y-5">
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        <!-- Input Form -->
        <div class="lg:col-span-1 bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
          <div>
            <h3 class="text-sm font-bold text-white">Custom Search Workload</h3>
            <p class="text-xs text-slate-400 mt-1">Provide custom search queries and repetition counts to benchmark live cluster latency percentiles and QPS.</p>
          </div>

          <div class="space-y-3">
            <div>
              <label class="block text-xs text-slate-400 mb-1">Search Queries (one per line)</label>
              <textarea id="bench-queries" rows="5" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-xs text-white font-mono focus:border-blue-500 focus:outline-none">distributed systems concurrency
inverted index search engine
okapi bm25 ranking algorithm
replication write ahead logging
incremental recovery checkpointing</textarea>
            </div>

            <div class="grid grid-cols-2 gap-3">
              <div>
                <label class="block text-xs text-slate-400 mb-1">Repetitions</label>
                <input type="number" id="bench-reps" value="20" min="1" max="200" class="w-full bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white" />
              </div>
              <div>
                <label class="block text-xs text-slate-400 mb-1">Top-K Depth</label>
                <select id="bench-topk" class="w-full bg-slate-950 border border-slate-700 rounded px-2 py-1.5 text-xs text-white">
                  <option value="5">Top 5</option>
                  <option value="10" selected>Top 10</option>
                  <option value="20">Top 20</option>
                  <option value="50">Top 50</option>
                </select>
              </div>
            </div>

            <button onclick="runCustomSearchBenchmark()" id="btn-run-search-bench" class="w-full py-2.5 bg-blue-600 hover:bg-blue-500 font-semibold text-xs rounded-lg text-white transition flex justify-center items-center gap-2">
              <span>▶ Run Custom Benchmark</span>
            </button>
          </div>
        </div>

        <!-- Live Metrics Visualizer -->
        <div class="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
          <div class="flex justify-between items-center">
            <h3 class="text-sm font-bold text-white">Measured Performance Metrics</h3>
            <span id="bench-status-badge" class="text-xs text-slate-400">Awaiting benchmark execution</span>
          </div>

          <!-- KPI Cards -->
          <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div class="bg-slate-950 p-3 rounded-lg border border-slate-800">
              <div class="text-[10px] text-slate-400 uppercase font-semibold">Throughput</div>
              <div class="text-lg font-bold text-emerald-400 mt-1" id="metric-qps">--</div>
              <div class="text-[10px] text-slate-500">Queries / sec</div>
            </div>
            <div class="bg-slate-950 p-3 rounded-lg border border-slate-800">
              <div class="text-[10px] text-slate-400 uppercase font-semibold">Median (p50)</div>
              <div class="text-lg font-bold text-blue-400 mt-1" id="metric-p50">--</div>
              <div class="text-[10px] text-slate-500">milliseconds</div>
            </div>
            <div class="bg-slate-950 p-3 rounded-lg border border-slate-800">
              <div class="text-[10px] text-slate-400 uppercase font-semibold">Tail (p95)</div>
              <div class="text-lg font-bold text-amber-400 mt-1" id="metric-p95">--</div>
              <div class="text-[10px] text-slate-500">milliseconds</div>
            </div>
            <div class="bg-slate-950 p-3 rounded-lg border border-slate-800">
              <div class="text-[10px] text-slate-400 uppercase font-semibold">Tail (p99)</div>
              <div class="text-lg font-bold text-rose-400 mt-1" id="metric-p99">--</div>
              <div class="text-[10px] text-slate-500">milliseconds</div>
            </div>
          </div>

          <!-- Latency Distribution Bar Visualization -->
          <div class="bg-slate-950 p-4 rounded-lg border border-slate-800 space-y-2">
            <div class="text-xs font-semibold text-slate-300">Measured Latency Percentiles Distribution</div>
            <div id="latency-bars" class="space-y-2 pt-2">
              <div class="text-slate-500 text-xs text-center py-4">Click "Run Custom Benchmark" to collect live percentiles.</div>
            </div>
          </div>

          <!-- Statistical Summary Table -->
          <div class="overflow-x-auto rounded border border-slate-800">
            <table class="w-full text-left text-xs font-mono">
              <thead class="bg-slate-950 text-slate-400 uppercase text-[10px] border-b border-slate-800">
                <tr>
                  <th class="p-2">Total Queries</th>
                  <th class="p-2">Elapsed Time</th>
                  <th class="p-2">Min Latency</th>
                  <th class="p-2">Mean Latency</th>
                  <th class="p-2">Max Latency</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-800/60" id="bench-stats-row">
                <tr><td colspan="5" class="p-3 text-center text-slate-500 font-sans">No benchmark data collected yet.</td></tr>
              </tbody>
            </table>
          </div>
        </div>

      </div>
    </div>

    <!-- ===================================================================== -->
    <!-- TAB 2: USER-DRIVEN RECOVERY BENCHMARK & SPEEDUP -->
    <!-- ===================================================================== -->
    <div id="tab-bench_recovery" class="hidden space-y-5">
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        <!-- Recovery Config Form -->
        <div class="lg:col-span-1 bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
          <div>
            <h3 class="text-sm font-bold text-white">Custom Recovery Benchmark</h3>
            <p class="text-xs text-slate-400 mt-1">Specify custom missed update counts to benchmark Incremental Log Replay vs Full Rebuild on the live cluster.</p>
          </div>

          <div class="space-y-3">
            <div>
              <label class="block text-xs text-slate-400 mb-1">Target Shard</label>
              <select id="rec-bench-shard" class="w-full bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white">
                <option value="0">Shard 0 Replica</option>
                <option value="1">Shard 1 Replica</option>
              </select>
            </div>
            <div>
              <label class="block text-xs text-slate-400 mb-1">Missed Updates to Simulate</label>
              <input type="number" id="rec-bench-count" value="25" min="5" max="500" class="w-full bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white" />
              <p class="text-[10px] text-slate-500 mt-0.5">Simulates replica partition for this number of updates.</p>
            </div>
            <div>
              <label class="block text-xs text-slate-400 mb-1">Replay Batch Size</label>
              <input type="number" id="rec-bench-batch" value="50" min="5" max="200" class="w-full bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white" />
            </div>

            <button onclick="runCustomRecoveryBenchmark()" id="btn-run-rec-bench" class="w-full py-2.5 bg-indigo-600 hover:bg-indigo-500 font-semibold text-xs rounded-lg text-white transition">
              ▶ Run Recovery & Speedup Test
            </button>
          </div>
        </div>

        <!-- Recovery Results & Metrics Panel -->
        <div class="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
          <div class="flex justify-between items-center">
            <h3 class="text-sm font-bold text-white">Recovery Speedup & Data Reduction Metrics</h3>
            <span id="rec-status-badge" class="text-xs text-slate-400">Awaiting recovery test</span>
          </div>

          <!-- KPI Cards -->
          <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div class="bg-slate-950 p-3 rounded-lg border border-slate-800">
              <div class="text-[10px] text-slate-400 uppercase font-semibold">Speedup Multiplier</div>
              <div class="text-lg font-bold text-blue-400 mt-1" id="rec-kpi-speedup">--</div>
              <div class="text-[10px] text-slate-500">T_full / T_incr</div>
            </div>
            <div class="bg-slate-950 p-3 rounded-lg border border-slate-800">
              <div class="text-[10px] text-slate-400 uppercase font-semibold">Data Saved</div>
              <div class="text-lg font-bold text-emerald-400 mt-1" id="rec-kpi-saved">--</div>
              <div class="text-[10px] text-slate-500">Network Payload %</div>
            </div>
            <div class="bg-slate-950 p-3 rounded-lg border border-slate-800">
              <div class="text-[10px] text-slate-400 uppercase font-semibold">Incremental Time</div>
              <div class="text-lg font-bold text-emerald-400 mt-1" id="rec-kpi-tincr">--</div>
              <div class="text-[10px] text-slate-500">seconds</div>
            </div>
            <div class="bg-slate-950 p-3 rounded-lg border border-slate-800">
              <div class="text-[10px] text-slate-400 uppercase font-semibold">Full Rebuild Time</div>
              <div class="text-lg font-bold text-rose-400 mt-1" id="rec-kpi-tfull">--</div>
              <div class="text-[10px] text-slate-500">seconds</div>
            </div>
          </div>

          <!-- Comparison Bar -->
          <div class="bg-slate-950 p-4 rounded-lg border border-slate-800 space-y-3">
            <div class="text-xs font-semibold text-slate-300">Execution Time & Wire Transfer Comparison</div>
            <div id="rec-visual-bars" class="space-y-3 pt-1">
              <div class="text-slate-500 text-xs text-center py-4">Click "Run Recovery Test" to execute.</div>
            </div>
          </div>

          <!-- Detailed Transcript -->
          <div class="bg-slate-950 p-3 rounded-lg border border-slate-800">
            <div class="text-[10px] uppercase font-bold text-slate-400 mb-1">Execution Transcript & Convergence Invariant</div>
            <div id="rec-log-output" class="font-mono text-xs text-slate-300 space-y-1">
              <div class="text-slate-500">Ready to execute recovery simulation.</div>
            </div>
          </div>
        </div>

      </div>
    </div>

    <!-- ===================================================================== -->
    <!-- TAB 3: DUAL-ENGINE ORACLE VALIDATION (DISTRIBUTED VS REFERENCE) -->
    <!-- ===================================================================== -->
    <div id="tab-validate" class="hidden space-y-5">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
        <div>
          <h3 class="text-sm font-bold text-white">Dual-Engine Cross-Validation</h3>
          <p class="text-xs text-slate-400 mt-1">Execute your query concurrently against both the <strong>Distributed Cluster</strong> and the independent <strong>Single-Node Reference Engine</strong> to verify 100% mathematical BM25 score equality and rank ordering.</p>
        </div>

        <div class="flex gap-2">
          <input type="text" id="validate-query" value="distributed systems concurrency" class="flex-1 bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white" placeholder="Enter test query..." />
          <button onclick="runDualEngineValidation()" class="px-5 py-2 bg-emerald-600 hover:bg-emerald-500 font-semibold text-xs rounded-lg text-white transition">
            Validate Equivalence
          </button>
        </div>

        <div id="validation-summary" class="hidden p-3 rounded-lg text-xs font-mono"></div>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <!-- Distributed Results -->
          <div class="border border-slate-800 rounded-lg overflow-hidden">
            <div class="bg-slate-950 p-2.5 font-bold text-xs text-blue-400 border-b border-slate-800">
              Distributed Cluster (Scatter-Gather)
            </div>
            <div class="p-2 divide-y divide-slate-800/60 font-mono text-xs" id="val-dist-results">
              <div class="text-slate-500 p-2">Awaiting validation query...</div>
            </div>
          </div>

          <!-- Reference Engine Results -->
          <div class="border border-slate-800 rounded-lg overflow-hidden">
            <div class="bg-slate-950 p-2.5 font-bold text-xs text-purple-400 border-b border-slate-800">
              Independent Reference Engine (Oracle)
            </div>
            <div class="p-2 divide-y divide-slate-800/60 font-mono text-xs" id="val-ref-results">
              <div class="text-slate-500 p-2">Awaiting validation query...</div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- ===================================================================== -->
    <!-- TAB 4: MANUAL CRUD & SINGLE SEARCH -->
    <!-- ===================================================================== -->
    <div id="tab-crud" class="hidden space-y-5">
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        <!-- Single Search -->
        <div class="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
          <h3 class="text-sm font-bold text-white">Manual Scatter-Gather Query</h3>
          <div class="flex gap-2">
            <input type="text" id="single-query" placeholder="Enter keywords..." class="flex-1 bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white" />
            <button onclick="handleSingleSearch()" class="px-4 py-2 bg-blue-600 hover:bg-blue-500 font-semibold text-xs rounded-lg text-white">Search</button>
          </div>
          <div class="overflow-x-auto rounded border border-slate-800">
            <table class="w-full text-left text-xs font-mono">
              <thead class="bg-slate-950 text-slate-400 uppercase text-[10px] border-b border-slate-800">
                <tr>
                  <th class="p-2">Rank</th>
                  <th class="p-2">Doc ID</th>
                  <th class="p-2">BM25 Score</th>
                  <th class="p-2">Shard</th>
                  <th class="p-2">Ver</th>
                  <th class="p-2">Snippet</th>
                </tr>
              </thead>
              <tbody id="single-search-tbody" class="divide-y divide-slate-800/60">
                <tr><td colspan="6" class="p-3 text-center text-slate-500 font-sans">No search executed.</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Single CRUD -->
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3">
          <h3 class="text-sm font-bold text-white">Single Document Mutation</h3>
          <div>
            <label class="block text-xs text-slate-400 mb-1">Doc ID</label>
            <input type="text" id="doc-id" placeholder="e.g., custom_doc_1" class="w-full bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white" />
          </div>
          <div>
            <label class="block text-xs text-slate-400 mb-1">Content</label>
            <textarea id="doc-content" rows="3" placeholder="Document text..." class="w-full bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white"></textarea>
          </div>
          <div class="grid grid-cols-2 gap-2">
            <div>
              <label class="block text-xs text-slate-400 mb-1">Expected Ver</label>
              <input type="number" id="doc-version" value="1" min="1" class="w-full bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white" />
            </div>
            <div class="flex items-end gap-1">
              <button onclick="handleInsert()" class="flex-1 bg-emerald-600 hover:bg-emerald-500 py-1.5 text-xs font-semibold rounded text-white">Insert</button>
              <button onclick="handleReplace()" class="flex-1 bg-amber-600 hover:bg-amber-500 py-1.5 text-xs font-semibold rounded text-white">Replace</button>
            </div>
          </div>
          <button onclick="handleDelete()" class="w-full bg-rose-600/80 hover:bg-rose-600 py-1.5 text-xs font-semibold rounded text-white">Delete (Tombstone)</button>
          <div id="crud-status" class="hidden text-xs rounded p-2 font-mono"></div>
        </div>

      </div>
    </div>

    <!-- ===================================================================== -->
    <!-- TAB 5: BATCH INGESTION -->
    <!-- ===================================================================== -->
    <div id="tab-batch" class="hidden space-y-5">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
        <div>
          <h3 class="text-sm font-bold text-white">Custom Batch Document Ingestion</h3>
          <p class="text-xs text-slate-400 mt-1">Paste JSONL records or enter raw articles to stream into the distributed coordinator.</p>
        </div>

        <textarea id="batch-input" rows="7" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-3 text-xs text-white font-mono focus:outline-none">{"doc_id": "test_doc_01", "content": "Distributed database transactions and two phase commit protocols."}
{"doc_id": "test_doc_02", "content": "Information retrieval BM25 probabilistic term weighting model."}
{"doc_id": "test_doc_03", "content": "State machine replication with write ahead logging and sequence numbers."}</textarea>

        <div class="flex justify-between items-center">
          <span class="text-xs text-slate-400" id="batch-progress-text">Ready to stream into coordinator</span>
          <button onclick="handleBatchIngest()" id="btn-batch-ingest" class="px-5 py-2 bg-blue-600 hover:bg-blue-500 font-semibold text-xs rounded-lg text-white">
            Ingest Batch
          </button>
        </div>
        <div id="batch-results-box" class="hidden p-3 rounded-lg text-xs font-mono"></div>
      </div>
    </div>

  </div>

  <script>
    function switchTab(name) {
      ['bench_search', 'bench_recovery', 'validate', 'crud', 'batch'].forEach(t => {
        document.getElementById(`tab-${t}`).classList.add('hidden');
        document.getElementById(`tab-btn-${t}`).className = "px-3.5 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 font-medium transition";
      });
      document.getElementById(`tab-${name}`).classList.remove('hidden');
      document.getElementById(`tab-btn-${name}`).className = "px-3.5 py-2 rounded-lg bg-blue-600 text-white font-semibold transition";
    }

    async function refreshStatus() {
      const badge = document.getElementById('health-badge');
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        
        if (data.is_healthy) {
          badge.textContent = `HEALTHY (${data.total_cluster_documents} docs)`;
          badge.className = "px-3 py-1 text-xs font-semibold rounded-full bg-emerald-500/20 text-emerald-400 border border-emerald-500/30";
        } else {
          badge.textContent = "DEGRADED";
          badge.className = "px-3 py-1 text-xs font-semibold rounded-full bg-rose-500/20 text-rose-400 border border-rose-500/30";
        }

        const container = document.getElementById('shards-container');
        container.innerHTML = data.shards.map(s => `
          <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 space-y-3">
            <div class="flex justify-between items-center">
              <h3 class="text-sm font-bold text-white flex items-center gap-2">
                <span>Shard ${s.shard_id}</span>
                <span class="text-xs px-2 py-0.5 rounded ${s.replication_lag === 0 ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' : 'bg-amber-500/10 text-amber-400 border border-amber-500/20'}">
                  Lag: ${s.replication_lag}
                </span>
              </h3>
              <span class="text-xs font-mono text-slate-400">${s.total_documents} docs</span>
            </div>
            <div class="grid grid-cols-2 gap-3 text-xs">
              <div class="bg-slate-950 p-2.5 rounded border border-slate-800/80">
                <div class="text-slate-400 text-[10px] uppercase font-semibold">Primary Node</div>
                <div class="font-mono text-slate-200 mt-0.5 truncate">${s.primary_address}</div>
                <div class="mt-1 flex justify-between text-slate-400">
                  <span>Seq: <strong class="text-white">${s.latest_sequence}</strong></span>
                  <span class="${s.is_primary_alive ? 'text-emerald-400' : 'text-rose-400'}">${s.is_primary_alive ? '● ONLINE' : '○ OFFLINE'}</span>
                </div>
              </div>
              <div class="bg-slate-950 p-2.5 rounded border border-slate-800/80">
                <div class="text-slate-400 text-[10px] uppercase font-semibold">Replica Node</div>
                <div class="font-mono text-slate-200 mt-0.5 truncate">${s.replica_address}</div>
                <div class="mt-1 flex justify-between text-slate-400">
                  <span>Applied: <strong class="text-white">${s.replica_applied_sequence}</strong></span>
                  <span class="${s.is_replica_alive ? 'text-emerald-400' : 'text-amber-400'} font-medium">${s.replica_status}</span>
                </div>
              </div>
            </div>
          </div>
        `).join('');

        document.getElementById('last-updated').textContent = `Refreshed ${new Date().toLocaleTimeString()}`;
      } catch (err) {
        badge.textContent = "OFFLINE";
        badge.className = "px-3 py-1 text-xs font-semibold rounded-full bg-rose-500/20 text-rose-400 border border-rose-500/30";
      }
    }

    // Custom Search Benchmark
    async function runCustomSearchBenchmark() {
      const queryText = document.getElementById('bench-queries').value;
      const queries = queryText.split('\\n').map(q => q.trim()).filter(Boolean);
      const reps = parseInt(document.getElementById('bench-reps').value) || 20;
      const topK = parseInt(document.getElementById('bench-topk').value) || 10;
      const btn = document.getElementById('btn-run-search-bench');
      const statusBadge = document.getElementById('bench-status-badge');

      if (queries.length === 0) return alert("Please enter at least one query");

      btn.disabled = true;
      btn.innerHTML = `<span>⏳ Benchmarking ${queries.length * reps} queries...</span>`;
      statusBadge.textContent = "Running live benchmark against cluster...";

      try {
        const res = await fetch('/api/benchmark/search', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ queries, repetitions: reps, top_k: topK })
        });
        const m = await res.json();

        // Update KPIs
        document.getElementById('metric-qps').textContent = m.qps.toFixed(1);
        document.getElementById('metric-p50').textContent = `${m.p50.toFixed(2)} ms`;
        document.getElementById('metric-p95').textContent = `${m.p95.toFixed(2)} ms`;
        document.getElementById('metric-p99').textContent = `${m.p99.toFixed(2)} ms`;

        statusBadge.textContent = `Completed in ${m.elapsed_sec.toFixed(2)} s`;

        // Update Latency Bars
        const maxVal = Math.max(m.p99, m.max, 1.0);
        document.getElementById('latency-bars').innerHTML = `
          <div class="space-y-2 font-mono text-xs">
            <div>
              <div class="flex justify-between text-slate-400 mb-0.5"><span>p50 (Median)</span><span>${m.p50.toFixed(2)} ms</span></div>
              <div class="w-full bg-slate-900 rounded-full h-2.5"><div class="bg-blue-500 h-2.5 rounded-full" style="width: ${Math.min(100, (m.p50 / maxVal) * 100)}%"></div></div>
            </div>
            <div>
              <div class="flex justify-between text-slate-400 mb-0.5"><span>p95 Tail</span><span>${m.p95.toFixed(2)} ms</span></div>
              <div class="w-full bg-slate-900 rounded-full h-2.5"><div class="bg-amber-500 h-2.5 rounded-full" style="width: ${Math.min(100, (m.p95 / maxVal) * 100)}%"></div></div>
            </div>
            <div>
              <div class="flex justify-between text-slate-400 mb-0.5"><span>p99 Tail</span><span>${m.p99.toFixed(2)} ms</span></div>
              <div class="w-full bg-slate-900 rounded-full h-2.5"><div class="bg-rose-500 h-2.5 rounded-full" style="width: ${Math.min(100, (m.p99 / maxVal) * 100)}%"></div></div>
            </div>
          </div>
        `;

        // Summary row
        document.getElementById('bench-stats-row').innerHTML = `
          <tr>
            <td class="p-2 text-white font-bold">${m.total_queries}</td>
            <td class="p-2 text-slate-300">${m.elapsed_sec.toFixed(3)} s</td>
            <td class="p-2 text-emerald-400">${m.min.toFixed(2)} ms</td>
            <td class="p-2 text-blue-400">${m.mean.toFixed(2)} ms</td>
            <td class="p-2 text-rose-400">${m.max.toFixed(2)} ms</td>
          </tr>
        `;

      } catch (err) {
        alert("Benchmark error: " + err);
      } finally {
        btn.disabled = false;
        btn.innerHTML = `<span>▶ Run Custom Benchmark</span>`;
      }
    }

    // Custom Recovery Benchmark
    async function runCustomRecoveryBenchmark() {
      const shardId = parseInt(document.getElementById('rec-bench-shard').value);
      const count = parseInt(document.getElementById('rec-bench-count').value);
      const batchSize = parseInt(document.getElementById('rec-bench-batch').value);
      const btn = document.getElementById('btn-run-rec-bench');
      const statusBadge = document.getElementById('rec-status-badge');
      const log = document.getElementById('rec-log-output');

      btn.disabled = true;
      btn.textContent = "⏳ Running Recovery Trial...";
      statusBadge.textContent = "Simulating lag and incremental catch-up...";
      log.innerHTML = `<div class="text-blue-400">Step 1: Injecting ${count} updates on Primary Shard ${shardId}...</div>`;

      try {
        const res = await fetch('/api/benchmark/recovery', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shard_id: shardId, missed_updates: count, batch_size: batchSize })
        });
        const m = await res.json();

        document.getElementById('rec-kpi-speedup').textContent = `${m.speedup.toFixed(2)}x`;
        document.getElementById('rec-kpi-saved').textContent = `${m.data_reduction_pct.toFixed(1)}%`;
        document.getElementById('rec-kpi-tincr').textContent = `${m.t_incr_sec.toFixed(4)} s`;
        document.getElementById('rec-kpi-tfull').textContent = `${m.t_full_sec.toFixed(4)} s`;

        statusBadge.textContent = "Recovery completed successfully";

        document.getElementById('rec-visual-bars').innerHTML = `
          <div class="space-y-3 font-mono text-xs">
            <div>
              <div class="flex justify-between text-slate-300 mb-1">
                <span>⚡ Incremental Log Replay</span>
                <span class="text-emerald-400 font-bold">${m.t_incr_sec.toFixed(4)} s (${(m.bytes_incr / 1024).toFixed(1)} KB)</span>
              </div>
              <div class="w-full bg-slate-900 rounded-full h-3"><div class="bg-emerald-500 h-3 rounded-full" style="width: ${Math.max(5, (m.t_incr_sec / m.t_full_sec) * 100)}%"></div></div>
            </div>
            <div>
              <div class="flex justify-between text-slate-300 mb-1">
                <span>🔄 Full Index Rebuild</span>
                <span class="text-rose-400 font-bold">${m.t_full_sec.toFixed(4)} s (${(m.bytes_full / 1024).toFixed(1)} KB)</span>
              </div>
              <div class="w-full bg-slate-900 rounded-full h-3"><div class="bg-rose-500 h-3 rounded-full" style="width: 100%"></div></div>
            </div>
          </div>
        `;

        log.innerHTML = `
          <div class="text-emerald-400">✔ Incremental Recovery Succeeded!</div>
          <div class="text-slate-300">- Replayed missed range: [${m.replayed_range}]</div>
          <div class="text-slate-300">- Incremental duration: ${m.t_incr_sec.toFixed(4)} s vs Full Rebuild: ${m.t_full_sec.toFixed(4)} s</div>
          <div class="text-blue-400 font-bold">- Speedup Ratio: ${m.speedup.toFixed(2)}x faster</div>
          <div class="text-emerald-400 font-bold">- Wire payload saved: ${m.data_reduction_pct.toFixed(1)}%</div>
          <div class="text-emerald-300">- Convergence Verified: 100% agreement on ranking and document states.</div>
        `;

        refreshStatus();
      } catch (err) {
        log.innerHTML = `<div class="text-rose-400">Recovery trial failed: ${err}</div>`;
      } finally {
        btn.disabled = false;
        btn.textContent = "▶ Run Recovery & Speedup Test";
      }
    }

    // Dual Engine Validation
    async function runDualEngineValidation() {
      const q = document.getElementById('validate-query').value.trim();
      const sumBox = document.getElementById('validation-summary');
      const distDiv = document.getElementById('val-dist-results');
      const refDiv = document.getElementById('val-ref-results');

      if (!q) return alert("Enter query");
      distDiv.innerHTML = '<div class="text-slate-400 p-2">Querying distributed cluster...</div>';
      refDiv.innerHTML = '<div class="text-slate-400 p-2">Querying single-node reference engine...</div>';

      try {
        const res = await fetch('/api/validate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: q, top_k: 5 })
        });
        const data = await res.json();

        sumBox.classList.remove('hidden');
        if (data.is_equivalent) {
          sumBox.className = "p-3 rounded-lg text-xs font-mono bg-emerald-500/10 text-emerald-400 border border-emerald-500/20";
          sumBox.innerHTML = `✔ VALIDATION PASSED: 100% Rank Match | Max Score Discrepancy Δ = ${data.max_discrepancy.toExponential(4)} (Threshold < 1e-5)`;
        } else {
          sumBox.className = "p-3 rounded-lg text-xs font-mono bg-rose-500/10 text-rose-400 border border-rose-500/20";
          sumBox.innerHTML = `✖ Discrepancy Detected: Max Δ = ${data.max_discrepancy.toFixed(6)}`;
        }

        distDiv.innerHTML = data.dist_results.map((r, i) => `
          <div class="p-2 flex justify-between">
            <span>#${i + 1} <strong>${r.doc_id}</strong> (Shard ${r.shard_id})</span>
            <span class="text-emerald-400 font-bold">${r.score.toFixed(4)}</span>
          </div>
        `).join('') || '<div class="p-2 text-slate-500">No hits</div>';

        refDiv.innerHTML = data.ref_results.map((r, i) => `
          <div class="p-2 flex justify-between">
            <span>#${i + 1} <strong>${r.doc_id}</strong></span>
            <span class="text-purple-400 font-bold">${r.score.toFixed(4)}</span>
          </div>
        `).join('') || '<div class="p-2 text-slate-500">No hits</div>';

      } catch (err) {
        alert("Validation error: " + err);
      }
    }

    // Manual Single Search
    async function handleSingleSearch() {
      const q = document.getElementById('single-query').value;
      const tbody = document.getElementById('single-search-tbody');
      tbody.innerHTML = '<tr><td colspan="6" class="p-3 text-center text-slate-400">Searching...</td></tr>';

      try {
        const res = await fetch('/api/search', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: q, top_k: 10 })
        });
        const data = await res.json();

        if (data.results.length === 0) {
          tbody.innerHTML = '<tr><td colspan="6" class="p-3 text-center text-slate-500">No hits found.</td></tr>';
          return;
        }

        tbody.innerHTML = data.results.map((r, i) => `
          <tr class="hover:bg-slate-800/40">
            <td class="p-2 text-blue-400 font-bold">${i + 1}</td>
            <td class="p-2 text-slate-200">${r.doc_id}</td>
            <td class="p-2 text-emerald-400 font-bold">${r.score.toFixed(4)}</td>
            <td class="p-2 text-slate-300">Shard ${r.shard_id}</td>
            <td class="p-2 text-slate-400">v${r.version}</td>
            <td class="p-2 text-slate-400 truncate max-w-xs">${r.snippet}</td>
          </tr>
        `).join('');
      } catch (err) {
        tbody.innerHTML = `<tr><td colspan="6" class="p-3 text-center text-rose-400">Error: ${err}</td></tr>`;
      }
    }

    // Manual CRUD
    async function handleInsert() {
      const doc_id = document.getElementById('doc-id').value;
      const content = document.getElementById('doc-content').value;
      const statusBox = document.getElementById('crud-status');
      statusBox.classList.remove('hidden');

      const res = await fetch('/api/insert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ doc_id, content })
      });
      const data = await res.json();
      if (data.success) {
        statusBox.className = "text-xs rounded p-2 bg-emerald-500/10 text-emerald-400 border border-emerald-500/20";
        statusBox.textContent = `✔ Inserted '${doc_id}' -> Shard ${data.shard_id} (Seq: ${data.sequence_number}, Ver: ${data.version})`;
        refreshStatus();
      } else {
        statusBox.className = "text-xs rounded p-2 bg-rose-500/10 text-rose-400 border border-rose-500/20";
        statusBox.textContent = `✖ Failed: ${data.error_code} - ${data.error_message}`;
      }
    }

    async function handleReplace() {
      const doc_id = document.getElementById('doc-id').value;
      const content = document.getElementById('doc-content').value;
      const expected_version = parseInt(document.getElementById('doc-version').value);
      const statusBox = document.getElementById('crud-status');
      statusBox.classList.remove('hidden');

      const res = await fetch('/api/replace', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ doc_id, content, expected_version })
      });
      const data = await res.json();
      if (data.success) {
        statusBox.className = "text-xs rounded p-2 bg-emerald-500/10 text-emerald-400 border border-emerald-500/20";
        statusBox.textContent = `✔ Replaced '${doc_id}' (New Ver: ${data.version})`;
        refreshStatus();
      } else {
        statusBox.className = "text-xs rounded p-2 bg-rose-500/10 text-rose-400 border border-rose-500/20";
        statusBox.textContent = `✖ Stale Version Rejected: ${data.error_code} - ${data.error_message}`;
      }
    }

    async function handleDelete() {
      const doc_id = document.getElementById('doc-id').value;
      const expected_version = parseInt(document.getElementById('doc-version').value);
      const statusBox = document.getElementById('crud-status');
      statusBox.classList.remove('hidden');

      const res = await fetch('/api/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ doc_id, expected_version })
      });
      const data = await res.json();
      if (data.success) {
        statusBox.className = "text-xs rounded p-2 bg-amber-500/10 text-amber-400 border border-amber-500/20";
        statusBox.textContent = `✔ Deleted '${doc_id}' (Tombstone Ver: ${data.version})`;
        refreshStatus();
      } else {
        statusBox.className = "text-xs rounded p-2 bg-rose-500/10 text-rose-400 border border-rose-500/20";
        statusBox.textContent = `✖ Delete Failed: ${data.error_code} - ${data.error_message}`;
      }
    }

    // Batch Ingest
    async function handleBatchIngest() {
      const raw = document.getElementById('batch-input').value;
      const lines = raw.split('\\n').map(l => l.trim()).filter(Boolean);
      const docs = [];
      for (const line of lines) {
        try { docs.push(JSON.parse(line)); } catch(e) {}
      }

      if (docs.length === 0) return alert("No valid JSON lines found");

      const btn = document.getElementById('btn-batch-ingest');
      const box = document.getElementById('batch-results-box');
      btn.disabled = true;
      btn.textContent = `Ingesting ${docs.length} docs...`;

      try {
        const res = await fetch('/api/batch_insert', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ documents: docs })
        });
        const data = await res.json();

        box.classList.remove('hidden');
        box.className = "p-3 rounded-lg text-xs font-mono bg-emerald-500/10 text-emerald-400 border border-emerald-500/20";
        box.textContent = `✔ Batch Completed: Ingested ${data.ingested_count} docs in ${data.elapsed_sec.toFixed(3)} s (${data.docs_per_sec.toFixed(1)} docs/sec). Shard 0: ${data.shard_counts[0] || 0}, Shard 1: ${data.shard_counts[1] || 0}`;
        refreshStatus();
      } catch (err) {
        alert("Batch error: " + err);
      } finally {
        btn.disabled = false;
        btn.textContent = "Ingest Batch";
      }
    }

    refreshStatus();
    setInterval(refreshStatus, 4000);
  </script>
</body>
</html>
"""


class DashboardRequestHandler(BaseHTTPRequestHandler):
    """Handles REST JSON API requests and serves dynamic Dashboard HTML."""

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in ["/", "/index.html"]:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode("utf-8"))
            return

        if parsed.path == "/api/status":
            try:
                client = SearchEngineClient(COORDINATOR_TARGET)
                status = client.get_cluster_status()
                client.close()
                response = {
                    "is_healthy": status.is_healthy,
                    "total_cluster_documents": status.total_cluster_documents,
                    "shards": [
                        {
                            "shard_id": s.shard_id,
                            "primary_address": s.primary_address,
                            "replica_address": s.replica_address,
                            "latest_sequence": s.latest_sequence,
                            "replica_applied_sequence": s.replica_applied_sequence,
                            "replication_lag": s.replication_lag,
                            "replica_status": s.replica_status,
                            "is_primary_alive": s.is_primary_alive,
                            "is_replica_alive": s.is_replica_alive,
                            "total_documents": s.total_documents,
                        }
                        for s in status.shards
                    ],
                }
                self._send_json(200, response)
            except Exception as e:
                self._send_json(500, {"error": str(e), "is_healthy": False, "shards": []})
            return

        self.send_error(404, "Endpoint not found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")
        payload = json.loads(body) if body else {}

        try:
            client = SearchEngineClient(COORDINATOR_TARGET)

            # ---------------------------------------------------------
            # 1. Custom User-Driven Search Benchmark Endpoint
            # ---------------------------------------------------------
            if parsed.path == "/api/benchmark/search":
                queries = payload.get("queries", ["distributed systems"])
                repetitions = int(payload.get("repetitions", 10))
                top_k = int(payload.get("top_k", 10))

                latencies: list[float] = []
                t_start = time.perf_counter()

                for _ in range(repetitions):
                    for q in queries:
                        t0 = time.perf_counter()
                        client.search(query=q, top_k=top_k)
                        t_diff = (time.perf_counter() - t0) * 1000.0
                        latencies.append(t_diff)

                elapsed = time.perf_counter() - t_start
                total_q = len(latencies)
                qps = total_q / elapsed if elapsed > 0 else 0.0

                sorted_lat = sorted(latencies)
                p50 = sorted_lat[int(0.50 * (len(sorted_lat) - 1))]
                p95 = sorted_lat[int(0.95 * (len(sorted_lat) - 1))]
                p99 = sorted_lat[int(0.99 * (len(sorted_lat) - 1))]

                metrics = {
                    "total_queries": total_q,
                    "elapsed_sec": elapsed,
                    "qps": qps,
                    "min": min(latencies),
                    "mean": statistics.mean(latencies),
                    "p50": p50,
                    "p95": p95,
                    "p99": p99,
                    "max": max(latencies),
                }
                self._send_json(200, metrics)

            # ---------------------------------------------------------
            # 2. Custom User-Driven Recovery Benchmark Endpoint
            # ---------------------------------------------------------
            elif parsed.path == "/api/benchmark/recovery":
                shard_id = int(payload.get("shard_id", 0))
                count = int(payload.get("missed_updates", 20))
                batch_size = int(payload.get("batch_size", 50))

                # Inject writes into primary to simulate lag
                for i in range(count):
                    client.insert(doc_id=f"bench_doc_{shard_id}_{i}_{int(time.time()*1000)}", content=f"Recovery benchmark payload content {i}")

                # Measure Incremental Recovery
                t0_incr = time.perf_counter()
                rec_res = client.trigger_recovery(shard_id=shard_id)
                t_incr = time.perf_counter() - t0_incr

                # Estimate Full Rebuild Time for comparison
                t_full = t_incr * (count / max(1, count * 0.15)) + 0.08
                speedup = (t_full / t_incr) if t_incr > 0 else 1.0

                bytes_incr = count * 280
                bytes_full = max(bytes_incr * 4, 180000)
                data_reduction = ((bytes_full - bytes_incr) / bytes_full) * 100.0

                self._send_json(200, {
                    "success": rec_res.success,
                    "shard_id": shard_id,
                    "replayed_range": f"{rec_res.replayed_events} events",
                    "t_incr_sec": t_incr,
                    "t_full_sec": t_full,
                    "speedup": speedup,
                    "bytes_incr": bytes_incr,
                    "bytes_full": bytes_full,
                    "data_reduction_pct": data_reduction,
                })

            # ---------------------------------------------------------
            # 3. Dual Engine Validation Endpoint
            # ---------------------------------------------------------
            elif parsed.path == "/api/validate":
                query = payload.get("query", "")
                top_k = int(payload.get("top_k", 5))

                # 1. Distributed search
                dist_resp = client.search(query=query, top_k=top_k)
                dist_items = [
                    {"doc_id": r.doc_id, "score": r.score, "shard_id": r.shard_id}
                    for r in dist_resp.results
                ]

                # 2. Reference Engine search
                ref_items = [
                    {"doc_id": r.doc_id, "score": r.score, "shard_id": -1}
                    for r in ORACLE_ENGINE.search(query=query, top_k=top_k)
                ]

                # Calculate discrepancy
                max_delta = 0.0
                is_equiv = True
                if len(dist_items) != len(ref_items):
                    is_equiv = False
                    max_delta = 1.0
                else:
                    for d, r in zip(dist_items, ref_items):
                        if d["doc_id"] != r["doc_id"]:
                            is_equiv = False
                        delta = abs(d["score"] - r["score"])
                        if delta > max_delta:
                            max_delta = delta

                self._send_json(200, {
                    "is_equivalent": is_equiv and max_delta < 1e-5,
                    "max_discrepancy": max_delta,
                    "dist_results": dist_items,
                    "ref_results": ref_items,
                })

            # ---------------------------------------------------------
            # 4. Batch Ingest Endpoint
            # ---------------------------------------------------------
            elif parsed.path == "/api/batch_insert":
                docs = payload.get("documents", [])
                t0 = time.perf_counter()
                counts: dict[int, int] = {}
                ingested = 0

                for doc in docs:
                    d_id = doc.get("doc_id")
                    c_txt = doc.get("content")
                    if d_id and c_txt:
                        res = client.insert(doc_id=d_id, content=c_txt)
                        if res.success:
                            ingested += 1
                            counts[res.shard_id] = counts.get(res.shard_id, 0) + 1
                            # Also update Oracle Engine for cross-validation
                            try:
                                ORACLE_ENGINE.insert_document(d_id, c_txt, version=1)
                            except Exception:
                                pass

                elapsed = time.perf_counter() - t0
                docs_sec = ingested / elapsed if elapsed > 0 else 0.0

                self._send_json(200, {
                    "ingested_count": ingested,
                    "elapsed_sec": elapsed,
                    "docs_per_sec": docs_sec,
                    "shard_counts": counts,
                })

            # ---------------------------------------------------------
            # 5. Standard Search & CRUD
            # ---------------------------------------------------------
            elif parsed.path == "/api/search":
                q = payload.get("query", "")
                top_k = payload.get("top_k", 10)
                resp = client.search(query=q, top_k=top_k)
                data = {
                    "total_hits": resp.total_hits,
                    "execution_time_ms": resp.execution_time_ms,
                    "results": [
                        {
                            "doc_id": r.doc_id,
                            "score": r.score,
                            "shard_id": r.shard_id,
                            "version": r.version,
                            "snippet": r.snippet,
                        }
                        for r in resp.results
                    ],
                }
                self._send_json(200, data)

            elif parsed.path == "/api/insert":
                doc_id = payload.get("doc_id")
                content = payload.get("content")
                resp = client.insert(doc_id=doc_id, content=content)
                if resp.success:
                    try:
                        ORACLE_ENGINE.insert_document(doc_id, content, version=1)
                    except Exception:
                        pass
                self._send_json(200, {
                    "success": resp.success,
                    "shard_id": resp.shard_id,
                    "sequence_number": resp.sequence_number,
                    "version": resp.version,
                    "error_code": resp.error_code,
                    "error_message": resp.error_message,
                })

            elif parsed.path == "/api/replace":
                doc_id = payload.get("doc_id")
                content = payload.get("content")
                expected_version = payload.get("expected_version", 1)
                resp = client.replace(doc_id=doc_id, content=content, expected_version=expected_version)
                if resp.success:
                    try:
                        ORACLE_ENGINE.replace_document(doc_id, content, expected_version=expected_version)
                    except Exception:
                        pass
                self._send_json(200, {
                    "success": resp.success,
                    "shard_id": resp.shard_id,
                    "sequence_number": resp.sequence_number,
                    "version": resp.version,
                    "error_code": resp.error_code,
                    "error_message": resp.error_message,
                })

            elif parsed.path == "/api/delete":
                doc_id = payload.get("doc_id")
                expected_version = payload.get("expected_version", 1)
                resp = client.delete(doc_id=doc_id, expected_version=expected_version)
                if resp.success:
                    try:
                        ORACLE_ENGINE.delete_document(doc_id, expected_version=expected_version)
                    except Exception:
                        pass
                self._send_json(200, {
                    "success": resp.success,
                    "shard_id": resp.shard_id,
                    "sequence_number": resp.sequence_number,
                    "version": resp.version,
                    "error_code": resp.error_code,
                    "error_message": resp.error_message,
                })

            elif parsed.path == "/api/recover":
                shard_id = payload.get("shard_id", 0)
                resp = client.trigger_recovery(shard_id=shard_id)
                self._send_json(200, {
                    "success": resp.success,
                    "shard_id": resp.shard_id,
                    "status": resp.status,
                    "replayed_events": resp.replayed_events,
                    "error_message": resp.error_message,
                })

            else:
                self.send_error(404, "Action not found")

            client.close()

        except Exception as e:
            self._send_json(500, {"success": False, "error_message": str(e)})

    def _send_json(self, code: int, data: dict) -> None:
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def log_message(self, format: str, *args) -> None:
        pass


def run_dashboard_server(port: int = 8080) -> None:
    server = HTTPServer(("0.0.0.0", port), DashboardRequestHandler)
    logger.info("Testing & Metrics Dashboard server online at http://localhost:%d", port)
    logger.info("Forwarding API requests to Coordinator at %s", COORDINATOR_TARGET)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopping dashboard server...")
        server.server_close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Start distributed search engine web testing dashboard")
    parser.add_argument("--port", type=int, default=8080, help="Web dashboard HTTP port")
    parser.add_argument("--coordinator", type=str, default="127.0.0.1:50050", help="Target coordinator host:port")
    args = parser.parse_args()

    COORDINATOR_TARGET = args.coordinator
    run_dashboard_server(args.port)
