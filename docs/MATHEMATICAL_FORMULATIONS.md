# Mathematical Formulations and Validation Reference

This document serves as the formal mathematical reference for ranking algorithms, distributed statistics aggregation, shard routing, replication lag, incremental recovery analysis, and experimental evaluation metrics in the **Distributed Search Engine with Incremental Index Recovery**.

---

## 1. Okapi BM25 Ranking Formulation

The primary relevance scoring function implemented in the search engine is **Okapi BM25** (Best Matching 25), a non-linear term-frequency probabilistic retrieval algorithm developed by Stephen Robertson and Karen Spärck Jones.

### 1.1 Complete Document Scoring Formula

For a multi-term query $Q = \{q_1, q_2, \dots, q_m\}$ and a candidate document $D$:

$$\text{Score}(D, Q) = \sum_{i=1}^{m} \text{IDF}(q_i) \cdot \frac{f(q_i, D) \cdot (k_1 + 1)}{f(q_i, D) + k_1 \cdot \left((1 - b) + b \cdot \frac{|D|}{\text{avgdl}}\right)}$$

Where:
- $q_i \in Q$ is an individual query term.
- $f(q_i, D)$ is the raw **term frequency** (TF) of term $q_i$ in document $D$.
- $|D|$ is the **document length** measured as the total number of normalized tokens in document $D$.
- $\text{avgdl}$ is the **average document length** across the entire corpus.
- $k_1 \ge 0$ is the term frequency saturation hyperparameter (default: $k_1 = 1.2$).
- $b \in [0, 1]$ is the document length normalization hyperparameter (default: $b = 0.75$).

---

### 1.2 Inverse Document Frequency (IDF) Formulation

Standard probabilistic IDF (Robertson-Spärck Jones) can produce negative values when a term occurs in more than half of the corpus ($DF > N/2$). To guarantee numerical stability, prevent rank reversal, and strictly maintain non-negative scoring, we use the standard smoothed Okapi BM25 / Lucene formulation:

$$\text{IDF}(q_i) = \ln\left(1.0 + \frac{N - DF(q_i) + 0.5}{DF(q_i) + 0.5}\right)$$

Where:
- $N$ is the total count of active (non-deleted) documents in the corpus.
- $DF(q_i)$ is the document frequency of term $q_i$ (number of non-deleted documents containing at least one occurrence of $q_i$).

#### Analytical Properties of Smoothed IDF:
1. **Non-Negativity:** For all $0 \le DF(q_i) \le N$:
   $$\frac{N - DF(q_i) + 0.5}{DF(q_i) + 0.5} \ge 0 \implies \text{IDF}(q_i) \ge \ln(1.0) = 0.0$$
2. **Monotonicity:** $\frac{\partial \text{IDF}}{\partial DF} < 0$. As a term becomes more ubiquitous in the corpus, its discriminative weight decreases monotonically.
3. **Extreme Bound ($DF = N$):**
   $$\text{IDF}_{\min} = \ln\left(1.0 + \frac{0.5}{N + 0.5}\right) > 0$$

---

### 1.3 Term Frequency Saturation & Length Normalization

Let the document length normalization factor be:
$$B(D) = (1 - b) + b \cdot \frac{|D|}{\text{avgdl}}$$

The term contribution function is defined as:
$$S(t, D) = \frac{f(t, D) \cdot (k_1 + 1)}{f(t, D) + k_1 \cdot B(D)}$$

#### Asymptotic Properties:
1. **Zero Frequency:** If $f(t, D) = 0 \implies S(t, D) = 0$.
2. **Infinite Frequency (Saturation Limit):**
   $$\lim_{f(t, D) \to \infty} S(t, D) = k_1 + 1$$
   Unlike linear TF-IDF where score grows unbounded with repetitions, BM25 caps the marginal impact of repeated words at $k_1 + 1$.
3. **Length Penalty Sensitivity ($b$):**
   - When $b = 1.0$: Full length penalty. A document twice as long must have twice the term frequency to achieve the same score.
   - When $b = 0.0$: No length penalty ($B(D) = 1.0$). Only raw term frequency matters.
   - Default $b = 0.75$: Moderate length penalization, favoring concise matches while accommodating naturally longer documents.

---

## 2. Distributed Statistics Aggregation (Scatter-Gather Phase)

In a distributed cluster with $S$ primary shards, scoring documents with shard-local statistics violates global ranking consistency.

### 2.1 The Distributed Inconsistency Problem
If Shard $s$ computes scores using only its local document count $N_s$ and local document frequency $DF_s(t)$:
$$\text{IDF}_s(t) = \ln\left(1.0 + \frac{N_s - DF_s(t) + 0.5}{DF_s(t) + 0.5}\right) \ne \text{IDF}_{\text{global}}(t)$$
Consequently:
- A document containing a rare term on Shard 1 would receive an artificially inflated score compared to an identical document on Shard 2 where the term was slightly more frequent.
- Local top-$k$ scores returned to the coordinator would have different scales, corrupting the global merged top-$k$ result.

---

### 2.2 Global Statistics Aggregation Formulation

To guarantee exact equivalence with a centralized single-node engine, the coordinator executes a two-phase scatter-gather protocol to compute a consistent `GlobalRankingSnapshot`:

1. **Global Document Count ($N_{\text{global}}$):**
   $$N_{\text{global}} = \sum_{s=1}^{S} N_s$$
   Where $N_s$ is the count of active (non-deleted) documents on shard $s$.

2. **Global Average Document Length ($\text{avgdl}_{\text{global}}$):**
   $$\text{avgdl}_{\text{global}} = \frac{\sum_{s=1}^{S} L_s}{N_{\text{global}}}$$
   Where $L_s = \sum_{D \in \text{Shard}_s} |D|$ is the sum of document lengths on shard $s$.

3. **Global Document Frequency ($DF_{\text{global}}(t)$):**
   $$DF_{\text{global}}(t) = \sum_{s=1}^{S} DF_s(t)$$
   Where $DF_s(t)$ is the number of documents on shard $s$ containing term $t$.

### 2.3 Global Equivalence Invariant
Let $\text{Score}_{\text{dist}}(D, Q)$ be the score computed on Shard $s$ using the snapshot, and $\text{Score}_{\text{ref}}(D, Q)$ be the score computed by the un-sharded reference engine:

$$\forall D \in \text{Cluster}, \quad |\text{Score}_{\text{dist}}(D, Q) - \text{Score}_{\text{ref}}(D, Q)| \le \epsilon \quad (\epsilon = 10^{-9})$$

---

## 3. Shard Routing Formulation

Documents are assigned to shards using a deterministic stable hash modulo function.

$$\text{ShardID}(d) = \left( \sum_{i=0}^{3} H_i \cdot 256^{3-i} \right) \pmod S$$

Where:
- $d = \text{doc\_id}$ (UTF-8 encoded string).
- $H = \text{MD5}(d)$ is the 128-bit hash digest.
- $(H_0, H_1, H_2, H_3)$ are the first 4 bytes (8 hexadecimal characters) of the MD5 digest parsed as a 32-bit unsigned big-endian integer.
- $S$ is the total number of shards.

### Uniformity Property
Under the assumption that MD5 approximates a uniform random oracle:
$$P(\text{ShardID}(d) = s) = \frac{1}{S}, \quad \forall s \in \{0, 1, \dots, S-1\}$$

---

## 4. Replication and Recovery Mathematics

### 4.1 Monotonic Sequence Progression
Each primary shard $s$ maintains an independent monotonic integer sequence counter:
$$S_{t+1} = S_t + 1, \quad S_0 = 0$$

### 4.2 Replication Lag Formulation
At any instant $t$, the replication lag for replica $r$ attached to primary $s$ is:
$$\text{Lag}(r, s) = S_{\text{primary\_latest}}(s) - S_{\text{replica\_checkpoint}}(r)$$

Where:
- $S_{\text{primary\_latest}}$ is the latest committed sequence in `shard_metadata`.
- $S_{\text{replica\_checkpoint}}$ is the durable `last_applied_sequence` in `replica_state`.
- In synchronized state: $\text{Lag}(r, s) = 0$.
- In disconnected or recovering state: $\text{Lag}(r, s) > 0$.

---

### 4.3 Incremental Sequence-Range Calculation
When replica $r$ initiates incremental recovery with primary $s$:
1. Let $S_{\text{checkpoint}} = S_{\text{replica\_checkpoint}}(r)$.
2. Let $S_{\text{latest}} = S_{\text{primary\_latest}}(s)$.
3. Let $S_{\text{oldest}} = S_{\text{primary\_earliest\_log}}(s)$.

#### Replay Range:
$$\mathcal{R}_{\text{replay}} = [S_{\text{checkpoint}} + 1, \, S_{\text{latest}}]$$

#### Total Events to Replay:
$$\Delta N_{\text{events}} = S_{\text{latest}} - S_{\text{checkpoint}}$$

#### Log Continuity Invariant:
$$\text{If } S_{\text{checkpoint}} < S_{\text{oldest}} - 1 \implies \text{Recovery Status} = \text{HISTORY\_UNAVAILABLE}$$
Incremental recovery is only provably sound if the update log contains an unbroken chain of events starting immediately at $S_{\text{checkpoint}} + 1$.

---

## 5. Experimental Evaluation Metrics

To evaluate search and recovery performance in Milestone M8, the following formal formulas are used:

### 5.1 Recovery Speedup Ratio
Let $T_{\text{full}}$ be the wall-clock time required to reconstruct the replica's inverted index from the full dataset, and $T_{\text{incr}}$ be the wall-clock time required to perform incremental sequence-range recovery:

$$\text{Speedup} = \frac{T_{\text{full}}}{T_{\text{incr}}}$$

- $\text{Speedup} > 1.0$: Demonstrates incremental recovery efficiency.
- $\text{Speedup} \gg 1.0$: Typically observed when missed updates are a small fraction of the total corpus size.

---

### 5.2 Data Transfer Reduction Percentage
Let $B_{\text{full}}$ be the total bytes transferred/read during a full rebuild, and $B_{\text{incr}}$ be the total bytes transferred during incremental replay:

$$\text{Data Reduction (\%)} = \left( \frac{B_{\text{full}} - B_{\text{incr}}}{B_{\text{full}}} \right) \times 100\%$$

---

### 5.3 Query Latency Percentiles
Given an ordered set of query response times $\{t_1, t_2, \dots, t_M\}$ such that $t_1 \le t_2 \le \dots \le t_M$:
- **Median ($p_{50}$):** $t_{\lceil 0.50 \cdot M \rceil}$
- **95th Percentile ($p_{95}$):** $t_{\lceil 0.95 \cdot M \rceil}$
- **99th Percentile ($p_{99}$):** $t_{\lceil 0.99 \cdot M \rceil}$

### 5.4 Search Throughput (QPS)
$$\text{QPS} = \frac{M}{\Delta T_{\text{bench}}}$$
Where $M$ is the total number of executed search queries and $\Delta T_{\text{bench}}$ is the total elapsed benchmark execution time in seconds.

---

## 6. Deterministic Result Ordering and Strict Total Order

To guarantee identical results across distributed search runs and when cross-validating against the reference engine, results are sorted using a strict weak ordering with deterministic tie-breaking.

Let $R_1 = (\text{score}_1, \text{doc\_id}_1)$ and $R_2 = (\text{score}_2, \text{doc\_id}_2)$.
The ordering relation $\succ$ (reads "$R_1$ ranks higher than $R_2$") is defined as:

$$R_1 \succ R_2 \iff \begin{cases} 
\text{score}_1 > \text{score}_2, & \text{if } |\text{score}_1 - \text{score}_2| > \epsilon \\ 
\text{doc\_id}_1 < \text{doc\_id}_2, & \text{if } |\text{score}_1 - \text{score}_2| \le \epsilon \quad (\text{lexicographical tie-break})
\end{cases}$$

Where $\epsilon = 10^{-9}$ accounts for standard 64-bit IEEE 754 floating-point inaccuracies.
This guarantees that any two search executions on identical corpora return an identical sequence of document identifiers.
