package com.madeeasy;

import com.madeeasy.exception.InputValidationException;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


@RequiredArgsConstructor
public class WorkflowDagExecutorApplication {

    /**
     * Represents a node in the DAG with its dependencies
     */
    public static class Node {
        final int id;                   // Unique identifier for the node
        final String name;              // Human-readable name for the node
        final List<Node> children = new ArrayList<>();  // Nodes that depend on this node
        final List<Node> parents = new ArrayList<>();   // Nodes this node depends on

        public Node(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    // Map of all nodes in the DAG keyed by their IDs
    Node root;                      // The root node of the DAG
    final Map<Integer, Node> nodes = new HashMap<>();
    private final boolean enableLogging;     // Flag to enable/disable execution logging
    private final ExecutorService executorService;  // Thread pool for parallel execution
    // Tracks the order of node executions (thread-safe)
    private final List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
    // Tracks completed nodes (thread-safe)
    final Set<Integer> completedNodes = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean latchReleased = new AtomicBoolean(false);


    public WorkflowDagExecutorApplication(int threadPoolSize, boolean enableLogging) {
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        this.enableLogging = enableLogging;
    }

    /**
     * Builds the DAG (Directed Acyclic Graph) using the provided nodes and edges.
     * Adds child and parent relationships between nodes and checks for cycles.
     */
    public void buildDAG(DagInput input) throws IllegalArgumentException {
        // Clear previous state
        nodes.clear();
        root = null;

        // Create nodes
        input.nodes().forEach((id, name) ->
                nodes.put(id, new Node(id, name))
        );

        // Build edges with validation
        Set<String> uniqueEdges = new HashSet<>();
        for (int[] edge : input.edges()) {
            String edgeKey = edge[0] + "->" + edge[1];

            // Skip duplicates
            if (!uniqueEdges.add(edgeKey)) {
                System.out.printf("⚠️ Duplicate edge ignored: %s%n", edgeKey);
                continue;
            }

            // Validate nodes exist
            if (!nodes.containsKey(edge[0]) || !nodes.containsKey(edge[1])) {
                throw new IllegalArgumentException(
                        String.format("Edge %s references non-existent nodes", edgeKey)
                );
            }

            Node source = nodes.get(edge[0]);
            Node target = nodes.get(edge[1]);

            // Add relationships
            if (!source.children.contains(target) && !target.parents.contains(source)) {
                source.children.add(target);
                target.parents.add(source);
            }
        }

        Set<Integer> connected = new HashSet<>();
        for (int[] edge : input.edges()) {
            connected.add(edge[0]);
            connected.add(edge[1]);
        }

        Set<Integer> orphanNodes = new LinkedHashSet<>(nodes.keySet());
        orphanNodes.removeAll(connected);

        if (!orphanNodes.isEmpty()) {
            System.out.printf("⚠️ Unconnected node(s) detected: %s%n",
                    orphanNodes.stream()
                            .map(id -> id + " (" + nodes.get(id).name + ")")
                            .toList());
        }


        // Check for Duplicate Node Names (Optional, if names should be unique)
        Set<String> names = new HashSet<>();
        for (Node node : nodes.values()) {
            if (!names.add(node.name)) {
                System.out.printf("⚠️ Duplicate node name found: '%s'%n", node.name);
            }
        }


        // Set root
        root = nodes.values().stream()
                .filter(n -> n.parents.isEmpty())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No root node found (no node without parents)"));

        if (root == null) {
            throw new IllegalArgumentException("No root node found (ID 1)");
        }

        // Validate acyclic
        if (!isDAG()) {
            throw new IllegalArgumentException("Graph contains cycles");
        }

        System.out.printf("✅ Built DAG with %d nodes and %d edges%n",
                nodes.size(), uniqueEdges.size());
    }


    /**
     * Prints the DAG structure in a readable format showing each node and its children.
     */
    public void printDAG() {
        System.out.println("\n📌 DAG Structure (Node -> Children):");
        for (Node node : nodes.values()) {
            String childrenStr = node.children.stream()
                    .map(child -> child.name + "(ID:" + child.id + ")")
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("None");

            String parentsStr = node.parents.stream()
                    .map(parent -> parent.name + "(ID:" + parent.id + ")")
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("None");

            System.out.printf("• %s (ID: %d)\n", node.name, node.id);
            System.out.printf("   ├─ Parents  : [%s]\n", parentsStr);
            System.out.printf("   └─ Children : [%s]\n", childrenStr);
        }
    }

    /**
     * Checks if the graph is a Directed Acyclic Graph (DAG).
     *
     * @return true if the graph is a DAG, false otherwise
     */
    boolean isDAG() {
        Set<Node> visited = new HashSet<>();      // Tracks all visited nodes
        Set<Node> recursionStack = new HashSet<>(); // Tracks nodes in current recursion path

        for (Node node : nodes.values()) {
            if (!visited.contains(node) && hasCycle(node, visited, recursionStack)) {
                return false;
            }
        }
        return true;
    }


    /**
     * Performs DFS to check if a cycle exists starting from the given node.
     * Tracks recursion path to detect back edges.
     */
    boolean hasCycle(Node node, Set<Node> visited, Set<Node> recursionStack) {
        visited.add(node);
        recursionStack.add(node);

        // Check all children for cycles
        for (Node child : node.children) {
            if (!visited.contains(child)) {
                if (hasCycle(child, visited, recursionStack)) {
                    return true;
                }
            } else if (recursionStack.contains(child)) {
                // If child is in current recursion path, we found a cycle
                return true;
            }
        }

        recursionStack.remove(node);
        return false;
    }

    /**
     * Executes the DAG starting from the root node.
     * Uses parallel threads where possible and waits until all nodes are finished.
     */
    public List<String> execute() throws InterruptedException {
        // Log the start of execution
        LocalTime globalStart = LocalTime.now();
        if (enableLogging) {
            System.out.printf("[%s] 🚀 [%s] Starting DAG execution from root node: %s (ID: %d) at %s%n",
                    Thread.currentThread().getName(),
                    java.time.LocalTime.now(),
                    root.name, root.id, globalStart);
        }
        Map<Integer, CompletableFuture<Void>> futures = new ConcurrentHashMap<>();
        // Using CountDownLatch to detect when all nodes have completed
        CountDownLatch completionLatch = new CountDownLatch(1);

        // Execute all nodes
        System.out.printf("[%s] 🟡 [%s] Beginning to trigger executeNodeAsync() for all nodes...\n",
                Thread.currentThread().getName(), LocalTime.now());

        for (Node node : nodes.values()) {
            System.out.printf("[%s] ⏩ [%s] Triggering executeNodeAsync() for node: %s (ID: %d)\n",
                    Thread.currentThread().getName(), LocalTime.now(), node.name, node.id);

            executeNodeAsync(node, futures, completionLatch);
        }

        System.out.printf("[%s] 🟢 [%s] Finished submitting all nodes to executeNodeAsync(). Now waiting for latch.\n",
                Thread.currentThread().getName(), LocalTime.now());


        // Log waiting for completion
        if (enableLogging) {
            System.out.printf("[%s] 🧍 [%s]  Main thread waiting for all nodes to complete (via completion latch)...%n",
                    Thread.currentThread().getName(), LocalTime.now());
        }

        // Wait for all nodes to finish execution
        completionLatch.await();

        LocalTime globalEnd = LocalTime.now();
        long durationMs = Duration.between(globalStart, globalEnd).toMillis();

        // Log successful execution
        if (enableLogging) {
            System.out.printf("[%s] ✅ DAG execution completed at %s. Duration: %d ms. Total executed nodes: %d%n",
                    Thread.currentThread().getName(), globalEnd, durationMs, executionOrder.size());
        }

        return new ArrayList<>(executionOrder);
    }

    /**
     * Schedules a node for execution only after all its parent nodes have completed.
     * Uses CompletableFuture to manage asynchronous dependencies and avoid duplicates.
     */
    void executeNodeAsync(Node node,
                          Map<Integer, CompletableFuture<Void>> futures,
                          CountDownLatch completionLatch) {

        // Already scheduled? Skip
        if (futures.containsKey(node.id)) {
            if (enableLogging) {
                System.out.printf("[%s] ❌ [%s] SKIPPING async node %s (ID: %d) - already in futures map%n",
                        Thread.currentThread().getName(),
                        java.time.LocalTime.now(),
                        node.name,
                        node.id);
            }
            return;
        }

        // Schedule node logic here
        CompletableFuture<Void> nodeFuture = CompletableFuture
                .allOf(
                        getParentFutures(node, futures, completionLatch, enableLogging, executorService)
                )
                .thenRunAsync(() -> {

                    System.out.printf(
                            "[%s] 🚀 Lambda@%d (identityHash=%d) for Node %s (ID: %d)%n",
                            Thread.currentThread().getName(),
                            this.hashCode(),                      // Same for all lambdas (class hash)
                            System.identityHashCode(this),        // Unique per instance
                            node.name,
                            node.id
                    );

                    if (enableLogging) {
                        System.out.printf("[%s] 🚀 [%s] START Node %s (ID: %d) at %s%n",
                                Thread.currentThread().getName(), LocalTime.now(), node.name, node.id, LocalTime.now());
                    }

                    try {
                        Thread.sleep(100); // Simulated work
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        if (enableLogging) {
                            System.out.printf("[%s] ⚠️ INTERRUPTED during execution of node %s (ID: %d)%n",
                                    Thread.currentThread().getName(), node.name, node.id);
                        }
                        return;
                    }


                    try {

                        if (!Thread.currentThread().isInterrupted()) {
                            executionOrder.add(node.name);
                            completedNodes.add(node.id);
                        }
                    } catch (Exception e) {
                        System.err.printf("[%s] ❌ Node %s failed: %s%n",
                                Thread.currentThread().getName(),
                                node.name,
                                e.getMessage());
                        throw e; // Re-throw to trigger exceptionally
                    }

                    // ✅ Completion log
                    if (enableLogging) {
                        System.out.printf("[%s] ✅ [%s] COMPLETED async node: %s (ID: %d) | Total Completed: %d%n",
                                Thread.currentThread().getName(),
                                LocalTime.now(),
                                node.name,
                                node.id,
                                completedNodes.size());
                    }

                    // ✅ Latch release condition
                    if (completedNodes.size() == nodes.size() && latchReleased.compareAndSet(false, true)) {
                        if (enableLogging) {
                            System.out.printf("[%s] ✅✅ [%s] All async nodes completed! Releasing completion latch.%n",
                                    Thread.currentThread().getName(),
                                    LocalTime.now());
                        }
                        completionLatch.countDown();
                    }
                }, executorService)
                .whenComplete((result, exception) -> {
                    if (exception == null) {

                        System.out.printf("[%s] ✅ [%s] All parents for node %s (ID: %d) completed successfully: %s%n",
                                Thread.currentThread().getName(),
                                LocalTime.now(),
                                node.name,
                                node.id,
                                Arrays.toString(node.parents.stream()
                                        .map(p -> p.name + "(ID:" + p.id + ")")
                                        .toArray())
                        );
                    } else {
                        System.out.printf("[%s] ❌ [%s] All parents for node %s (ID: %d) failed with exception: %s%n",
                                Thread.currentThread().getName(),
                                LocalTime.now(),
                                node.name, node.id,
                                exception.getMessage());
                    }
                })
                .exceptionally(ex -> {
                    System.err.printf("[%s] ❌ CRITICAL ERROR processing node %s: %s%n",
                            Thread.currentThread().getName(),
                            node.name,
                            ex.getMessage());
                    // Only release latch if ALL nodes failed
                    if (completedNodes.isEmpty() && latchReleased.compareAndSet(false, true)) {
                        completionLatch.countDown();
                    }
                    return null;
                });

        System.out.printf("[%s] 📥 [%s] Stored future for node %s (ID: %d) into futures map%n",
                Thread.currentThread().getName(),
                java.time.LocalTime.now(),
                node.name,
                node.id);

        // the node is marked as scheduled:
        futures.put(node.id, nodeFuture);
    }

    /**
     * Collects and returns CompletableFutures for all parent nodes of a given node.
     */
    @SuppressWarnings("unchecked")
    CompletableFuture<Void>[] getParentFutures(Node node,
                                               Map<Integer, CompletableFuture<Void>> futures,
                                               CountDownLatch completionLatch,
                                               boolean enableLogging,
                                               ExecutorService executorService) {
        return node.parents.stream()
                .map(parent -> {
                    // Step 1: Recursively trigger parent node execution
                    executeNodeAsync(parent, futures, completionLatch);

                    // Step 2: Get parent's future
                    CompletableFuture<Void> parentFuture = futures.get(parent.id);

                    // Step 3: Optional logging when parent completes
                    parentFuture
                            .thenRunAsync(() -> {
                                if (enableLogging) {
                                    System.out.printf(
                                            "[%s] 🔚 [%s] Parent %s (ID: %d) future completed%n",
                                            Thread.currentThread().getName(),
                                            LocalTime.now(),
                                            parent.name,
                                            parent.id
                                    );
                                }
                            }, executorService)
                            .exceptionally(ex -> {
                                // Mark child nodes as failed if parent fails
                                node.children.forEach(child ->
                                        futures.get(child.id).completeExceptionally(ex)
                                );
                                return null;
                            });

                    return parentFuture;
                })
                .toArray(CompletableFuture[]::new);
    }

    /**
     * Shuts down the thread pool gracefully.
     * Tries to wait for ongoing tasks to finish, then forces shutdown if needed.
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }


    /**
     * Main method with example usage.
     */
    public static void main(String[] args) {
        // Example input defining a DAG
        String input = """
                    12
                    1:Node-1
                    2:Node-2
                    3:Node-3
                    4:Node-4
                    5:Node-5
                    6:Node-6
                    7:Node-7
                    8:Node-8
                    9:Node-9
                    10:Node-10
                    11:Node-11
                    12:Node-9
                    13
                    1:2
                    1:3
                    1:4
                    2:5
                    3:5
                    3:6
                    4:7
                    5:8
                    5:9
                    3:6
                    9:11
                    6:10
                    3:6
                """;


        // Create executor with 4 threads and logging enabled
        WorkflowDagExecutorApplication executor = new WorkflowDagExecutorApplication(4, true);


        try {
            // Parse with validation
            DagInput dagInput = DagInput.parse(input);

            executor.buildDAG(dagInput);
            executor.printDAG();

            List<String> executionOrder = executor.execute();
            System.out.println("\nExecution Order:");
            executionOrder.forEach(System.out::println);

            executor.shutdown();
        } catch (InputValidationException e) {
            System.err.println("❌ Input Error: " + e.getMessage());
            System.err.println("Please verify:");
            System.err.println("- Node counts match header");
            System.err.println("- No duplicate node IDs");
            System.err.println("- Edge format is correct");
            System.exit(1);
        } catch (Exception e) {
            System.err.println("❌ Execution Error: " + e.getMessage());
            System.exit(1);
        }
    }

}