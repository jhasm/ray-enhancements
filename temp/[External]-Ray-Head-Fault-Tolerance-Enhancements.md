# **Ray Head Fault Tolerance Enhancements**

[Yuchen Zhou](mailto:yczhou@google.com) [Siyuan Zhang](mailto:sizhang@google.com)   
*Shared externally*

# Overview

This document analyzes gaps of Ray Head fault tolerance using Redis, a mechanism that effectively restores state for Ray GCS. To address these gaps, we evaluate different architectural enhancements: improving manageability of Redis via KubeRay, implementing true Ray HA, introducing a low-latency Ray Shadow Head, and utilizing an Embedded LMDB for simplified metadata persistence.

### Backup limitation

The GCS *strictly* persists architectural blueprints. While metadata can be recovered from the external store, the following high-frequency or transient data components are lost permanently during a hard restart:

* GCS in-memory only data, like in-flight task progress  
* Python driver memory state  
* Plasma shared-memory objects (tensors, dataframes)

**Out of scope:** The following architectural enhancements focus specifically on improving GCS metadata fault tolerance; however, the above data that are not currently persisted remain outside the scope of this document.

Related issues:

* [https://github.com/ray-project/ray/issues/45824](https://github.com/ray-project/ray/issues/45824)  
* [https://github.com/ray-project/ray/issues/53115](https://github.com/ray-project/ray/issues/53115)

# Identified Gaps

Despite the fault tolerance improvements provided by the Redis backup mechanism, several inherent gaps remain:

1. **Configuration Overhead:** KubeRay does not enable this by default, requiring users to manually manage Redis connection strings and password secrets.  
2. **Maintenance Demands:** While Ray aims to be lightweight by using external storage, users face the heavy burden of maintaining a highly available Redis cluster.  
3. **Head Node Downtime:** True high availability is not achieved as the Head node still faces a "black period" of downtime during the recovery process.

The process of restoring a head node to full operation involves several phases: Kubernetes detection, container initialization, Ray core bootstrapping, and GCS state restoration. Depending on the workload, fetching data from internal\_kv may also be necessary. Consequently, the total time required to bring the head node back online typically ranges from 15 seconds to 5 minutes.

# Enhancements

## **Option 1: Improve Redis manageability**

While the foundational Redis backup mechanism remains architecturally unchanged, it is now encapsulated within best-practice defaults to significantly reduce configuration overhead.

### Kuberay Managed Redis

Allow users to declare more configuration options for Redis within the KubeRay CRD. The KubeRay Operator acts as a meta-controller, communicating with a lightweight Redis Operator to automatically spin up a Redis cluster alongside the Ray Head node. When the Ray cluster is deleted, the Operator cleanly garbage-collects the associated Redis infrastructure.

**Pros:** Reduces configuration overhead by providing a simplified, automated entry point for users to initialize Ray Head node fault tolerance.

**Cons:** In production environments, users typically require more granular authority over the Redis cluster, like some specific custom configurations.

## **Option 2: Ray Head High Availability**

Instead of patching on top of the existing solution, we should dive into the ray core and refactor it to achieve real high availability, potentially utilizing high consistency data store like etcd.

We should do it in phases:

### Phase 1: Decouple ray core from redis 

[POC](https://github.com/YoyinZyc/ray/commit/2151ae9e331a15862e9da90a9a5516dbb6ce067f)  
While Ray has transitioned toward a more Redis-independent architecture since v2, the Global Control Store (GCS) remains tied to it to some degree. By developing a more pluggable, abstract backend interface, GCS storage can be further decoupled from Redis. This enables a smooth transition to reliable consensus stores such as etcd, leveraging Kubernetes' native infrastructure for rigorous consistency.

### Phase 2: Decrease the volume of GCS update

Ray GCS is designed for high-throughput and low-latency metadata processing, making **Redis** an ideal backend due to its performance capabilities. To achieve true **High Availability (HA)** for the Ray Head, the architecture must support cluster-wide consensus, though the overhead of maintaining consistency inevitably introduces greater latency.

Before refactoring the Ray core, the Global Control Service (GCS) capabilities must be minimized to establish a pure control plane containing only critical data. For instance, job submission records within the internal\_kv table are not sufficiently critical to justify the overhead of maintaining a long-running Ray cluster and could potentially be migrated to worker nodes. 

Generally, the internal\_kv table remains highly susceptible to user abuse; as a schemaless storage layer without garbage collection mechanism, it introduces significant uncertainty regarding GCS storage volumes on a cluster-by-cluster basis.

### Phase 3: Refactor ray core and backend replace

The Ray core will be refactored to support clusters starting with multiple head replicas. This transition includes replacing the current backend with etcd, which offers a more Kubernetes-native infrastructure for managing state.

**Pros:** 

1. Pluggable GCS backend provides significant architectural flexibility.  
2. Integrating with etcd allows Ray to align more closely with Kubernetes-native infrastructure, where we have more expertise.  
3. Ensures the Ray cluster operates with zero Head node downtime, effectively eliminating recovery-induced service interruptions.

**Cons:**

1. The architectural shift from Ray v1 to v2 [removes](https://github.com/ray-project/ray/issues/20494) the hard dependencies on redis and makes it [pluggable](https://www.anyscale.com/blog/redis-in-ray-past-and-future). It is arguable if we should bring the storage back. Unlike Kubernetes, which orchestrates long-lived pods and containers, Ray manages actors—units characterized by shorter lifespans and a significantly higher frequency of state updates. Consequently, the design is optimized for high-throughput and low-latency performance over the strict consistency.  
2. Introducing a consistency-focused store like etcd involves a tradeoff between availability and performance; Ray's current lightweight design may be compromised by consensus latency.   
3. Executing a deep refactoring of the Ray Core introduces significant complexity, and the necessary approval from Anyscale remains indeterministic.  
4. Potentially we still cannot save the stateful workload running on head, like the python driver state.

## **Option 3: Ray Shadow Head**

Implement a **toggleable** "Shadow Head" mode for the Ray GCS server to reduce recovery time from minutes to seconds in KubeRay-managed clusters. By default, Ray clusters will still start with a single head node. However, when enabled, a secondary head node runs in "Shadow" mode. It monitors the leader election status and remains in standby mode until promoted. This option can utilize the Lease resource in kube-apiserver to coordinate the leader election.

**Primary Head (Active)**: Owns the active GCS (Global Control Service), handles all worker heartbeats, and manages resource scheduling. The primary rents the leadership by periodically updating the timestamp of a \`Lease\` object. The Primary pod should be labeled with “primary”.

**Shadow Head (Standby)**: A live Pod running a head node in "Observer Mode." It runs a subprocess to monitor the status of the \`Lease\` object. It does not respond to worker RPCs. The shadow pod should be labeled with “shadow”. Once promoted, it bootstraps the ray core and restores the GCS state from redis or other standalone backend.

**Leader Promotion**:  If the current Primary crashes, experiences a network partition, or freezes, it will fail to renew the Lease before the deadline expires. Once the Lease expires, the Shadow head instantly detects this and initiates a new race to claim the leadership. The newly promoted leader updates its own Pod label to primary, which updates K8s service endpoints and re-routes the traffic. 

### Changes

#### Ray Core

* Introduce a flag to enable leader election mode for head node  
* Add a new subprocess to watch the lease object in Kubernetes APIServer to decide if the head is leader or follower

**Pros:** 

1. **Minimal Recovery Latency:** Mitigates downtime to achieve a seamless HA experience. Whereas standard recovery entails a four-step sequence (k8s detection, container initialization, Ray core bootstrapping, and GCS state restoration), the Shadow Head architecture enables failover within seconds. Because the shadow pod is already preloaded with big images and saves the latency for container initialization.  
2. **Reduced Complexity:** Involves significantly less architectural overhead and engineering effort compared to the deep Ray Core refactoring required for **Option 2**.  
3. **Prioritized performance**: Ensures that the lightweight architecture is not compromised by the overhead of strict consistency.  
4. **Opt-in Feature:** Designed as an optional toggle to prevent architectural overkill and unnecessary resource overhead on short-lived clusters.

**Cons**:

1. **Extra Resource** : extra shadow pod incurs extra resource utilization.  
2. **External Dependencies**: have to depend on Redis or other DB backend outside Ray head. And it is only available for Ray clusters running on K8s.

The above proposal effectively mitigates latency associated with image loading and container initialization during the instantiation of a new Ray Head Pod. To further optimize recovery performance, the Shadow Head can be pre-heated with active GCS data through a pub/sub mechanism with the Primary Head, ensuring in-memory state consistency prior to promotion. However, to prevent race conditions and split-brain scenarios, additional design work is required to ensure consistency between the primary head, shadow head, and external storage.

## **Option 4: Embedded LMDB**

\[Similar to [https://github.com/ray-project/enhancements/pull/64](https://github.com/ray-project/enhancements/pull/64)\]

Instead of relying on an external cluster like Redis or etcd to persist the data, this approach uses LMDB (Lightning Memory-Mapped Database) directly within the Ray GCS process. It stores metadata on a network persistent disk, which simplifies the infrastructure significantly.

**Pros**:

* **High Performance**: Achieves fast read/write throughput by using asynchronous mode, which avoids network latency and leverages LMDB's memory-mapped architecture. Data loss is on the order of seconds even in asyn mode if the process dies.  
* **Operational Simplicity**: Eliminates the burden of managing and maintaining an external database cluster like Redis or etcd.  
* **Resiliency via Snapshots**: Resiliency can be enhanced by using a Kubernetes sidecar to perform periodic snapshots of the database file and upload them to cloud storage.

**Cons**:

* **Not High Availability**: This is not a true HA solution; if the head node fails, there is still a period of downtime required for pod replacement and state restoration.  
* **Non-Extensible to HA**: The embedded architecture cannot be easily extended to a multi-replica configuration in the future.  
* **Recovery Risk**: If the disk fails, recovery is difficult and relies on some special process to recover from the latest snapshot.

## **Comparison**

|  | Improvement(comparing to existing redis mechanism) | Complexity | User experience | Cost to user |
| :---- | :---- | :---- | :---- | :---- |
| KubeRay Managed Redis  | Simplify the user journey | Medium (Wrap the existing redis solution) | Just enable the feature, without further configuration Lost some flexibility in the meantime | Redis storage cost |
| Ray HA  | Simplify the user journey No head node downtime | Highest, huge refractor in ray core | Nothing to configure | Ray storage |
| Ray shadow head  | Minimal head node downtime | Medium  | Need to set up the redis and enable the shadow head feature | Additional head node pod, redis storage cost |
| Embedded LMDB | Zero external DB network overhead; fast local I/OSimplify the user journey | Low-Medium (Embedded in GCS) | Simple: No external DB cluster or credentials to manage | Network PV cost |

### **Recommendation**

These options do not conflict with each other. We can potentially pursue the following paths in parallel work streams:

* KubeRay Managed Redis  
* Ray shadow head  
* Pluggable GCS backend to extend the backend options (like etcd, GCP Memorystore, AWS DynamoDB)