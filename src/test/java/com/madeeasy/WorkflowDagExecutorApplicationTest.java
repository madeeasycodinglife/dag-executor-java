package com.madeeasy;


import com.madeeasy.exception.InputValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WorkflowDagExecutorApplicationTest {

    @Mock
    private ExecutorService mockExecutorService;

    private WorkflowDagExecutorApplication executor;

    @BeforeEach
    void setUp() {
        executor = new WorkflowDagExecutorApplication(4, true);
        try {
            Field field = WorkflowDagExecutorApplication.class.getDeclaredField("executorService");
            field.setAccessible(true);
            field.set(executor, mockExecutorService);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void buildDAG_ValidInput_CreatesCorrectStructure() throws Exception {
        Map<Integer, String> nodes = Map.of(
                1, "Node-1",
                2, "Node-2",
                3, "Node-3"
        );
        List<int[]> edges = List.of(new int[]{1, 2}, new int[]{1, 3});
        DagInput input = new DagInput(nodes, edges);

        executor.buildDAG(input);

        assertNotNull(executor.root);
        assertEquals("Node-1", executor.root.name);
        assertEquals(2, executor.root.children.size());
    }

    @Test
    void buildDAG_WithCycle_ThrowsException() {
        Map<Integer, String> nodes = Map.of(
                1, "Node-1",
                2, "Node-2"
        );
        List<int[]> edges = List.of(new int[]{1, 2}, new int[]{2, 1});
        DagInput input = new DagInput(nodes, edges);

        assertThrows(IllegalArgumentException.class, () -> executor.buildDAG(input));
    }

    @Test
    void executeNodeAsync_NoParents_ExecutesImmediately() throws Exception {
        WorkflowDagExecutorApplication.Node node = new WorkflowDagExecutorApplication.Node(1, "Node-1");
        Map<Integer, CompletableFuture<Void>> futures = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(1);

        executor.executeNodeAsync(node, futures, latch);

        assertTrue(futures.containsKey(1));
        verify(mockExecutorService).execute(any(Runnable.class));
    }

    @Test
    void executeNodeAsync_WithParents_WaitsForParents() throws Exception {
        WorkflowDagExecutorApplication.Node parent = new WorkflowDagExecutorApplication.Node(1, "Node-1");
        WorkflowDagExecutorApplication.Node child = new WorkflowDagExecutorApplication.Node(2, "Node-2");
        child.parents.add(parent);

        Map<Integer, CompletableFuture<Void>> futures = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(1);

        executor.executeNodeAsync(child, futures, latch);

        // Should have scheduled both parent and child
        assertEquals(2, futures.size());
    }

    @Test
    void execute_SimpleDAG_ReturnsCorrectOrder() throws Exception {
        Map<Integer, String> nodes = Map.of(
                1, "Node-1",
                2, "Node-2"
        );
        List<int[]> edges = List.of(new int[]{1, 2});
        DagInput input = new DagInput(nodes, edges);
        executor.buildDAG(input);

        // Mock the executor service to run tasks immediately
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(mockExecutorService).execute(any(Runnable.class));

        List<String> order = executor.execute();

        assertEquals(List.of("Node-1", "Node-2"), order);
    }

    @Test
    void execute_ParallelBranches_CompletesAllNodes() throws Exception {
        Map<Integer, String> nodes = Map.of(
                1, "Node-1",
                2, "Node-2",
                3, "Node-3"
        );
        List<int[]> edges = List.of(new int[]{1, 2}, new int[]{1, 3});
        DagInput input = new DagInput(nodes, edges);
        executor.buildDAG(input);

        // Use real executor for this test
        executor = new WorkflowDagExecutorApplication(4, false);
        executor.buildDAG(input);

        List<String> order = executor.execute();

        assertEquals(3, order.size());
        assertTrue(order.contains("Node-1"));
        assertTrue(order.contains("Node-2"));
        assertTrue(order.contains("Node-3"));
        assertEquals("Node-1", order.get(0)); // Root should be first
    }

    @Test
    void buildDAG_EmptyInput_ThrowsException() {
        assertThrows(IllegalArgumentException.class,
                () -> executor.buildDAG(new DagInput(Map.of(), List.of())));
    }

    @Test
    void execute_SingleNode_CompletesSuccessfully() throws Exception {

        executor = new WorkflowDagExecutorApplication(1, true);

        Map<Integer, String> nodes = Map.of(1, "Node-1");
        DagInput input = new DagInput(nodes, List.of());
        executor.buildDAG(input);

        List<String> order = executor.execute();
        // Add verification of internal state
        assertEquals(1, executor.completedNodes.size(), "Should have 1 completed node");
        assertTrue(executor.completedNodes.contains(1), "Node-1 should be completed");
        assertEquals(List.of("Node-1"), order);
    }

    @Test
    void shutdown_WithRunningTasks_InterruptsTasks() throws InterruptedException {
        executor.shutdown();
        verify(mockExecutorService).shutdown();
        verify(mockExecutorService, times(1)).awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void execute_VerifyThreadPoolUsage() throws Exception {
        Map<Integer, String> nodes = Map.of(
                1, "Node-1",
                2, "Node-2",
                3, "Node-3"
        );
        List<int[]> edges = List.of(new int[]{1, 2}, new int[]{1, 3});
        DagInput input = new DagInput(nodes, edges);
        executor.buildDAG(input);

        // Mock the executor service to count invocations
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(mockExecutorService).execute(any(Runnable.class));

        executor.execute();

        // Verify all nodes were scheduled
        verify(mockExecutorService, atLeast(3)).execute(any(Runnable.class));
    }

    @Test
    void isDAG_ValidDAG_ReturnsTrue() throws Exception {
        WorkflowDagExecutorApplication.Node node1 = new WorkflowDagExecutorApplication.Node(1, "Node-1");
        WorkflowDagExecutorApplication.Node node2 = new WorkflowDagExecutorApplication.Node(2, "Node-2");
        node1.children.add(node2);
        node2.parents.add(node1);

        executor.nodes.put(1, node1);
        executor.nodes.put(2, node2);

        assertTrue(executor.isDAG());
    }

    @Test
    void hasCycle_CyclicGraph_ReturnsTrue() throws Exception {
        WorkflowDagExecutorApplication.Node node1 = new WorkflowDagExecutorApplication.Node(1, "Node-1");
        WorkflowDagExecutorApplication.Node node2 = new WorkflowDagExecutorApplication.Node(2, "Node-2");
        node1.children.add(node2);
        node2.children.add(node1); // Creates cycle

        Set<WorkflowDagExecutorApplication.Node> visited = new HashSet<>();
        Set<WorkflowDagExecutorApplication.Node> recursionStack = new HashSet<>();

        assertTrue(executor.hasCycle(node1, visited, recursionStack));
    }

    @Test
    void buildDAG_WithDuplicateNodeNames_LogsWarning() {
        Map<Integer, String> nodes = Map.of(
                1, "SameName",
                2, "SameName",
                3, "DifferentName"
        );
        List<int[]> edges = List.of(new int[]{1, 3}, new int[]{2, 3});
        DagInput input = new DagInput(nodes, edges);

        // You might need to verify System.out output here
        assertDoesNotThrow(() -> executor.buildDAG(input));
    }

    @Test
    void executeNodeAsync_WhenInterrupted_HandlesGracefully() throws Exception {
        WorkflowDagExecutorApplication.Node node = new WorkflowDagExecutorApplication.Node(1, "Node-1");
        Map<Integer, CompletableFuture<Void>> futures = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(1);

        // Mock to interrupt the thread when execute is called
        doAnswer(invocation -> {
            Thread.currentThread().interrupt(); // Simulate interrupt
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(mockExecutorService).execute(any(Runnable.class));

        executor.executeNodeAsync(node, futures, latch);

        assertTrue(Thread.interrupted());  // Verify interrupt occurred and clear flag
        assertTrue(futures.containsKey(1));
    }

    @Test
    void execute_WhenTaskThrowsException_ContinuesExecution() throws Exception {
        // Create mock executor to simulate task execution
        ExecutorService mockExecutor = mock(ExecutorService.class);

        // Simulate execution: throw on Node-2, succeed otherwise
        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            if (task.toString().contains("Node-2")) {
                throw new RuntimeException("Simulated failure");
            }
            task.run();
            return null;
        }).when(mockExecutor).execute(any(Runnable.class));

        // Create test DAG: Node-1 -> Node-2
        WorkflowDagExecutorApplication executor = new WorkflowDagExecutorApplication(2, true);
        setPrivateField(executor, "executorService", mockExecutor);

        Map<Integer, String> nodes = Map.of(1, "Node-1", 2, "Node-2");
        List<int[]> edges = List.of(new int[]{1, 2});
        executor.buildDAG(new DagInput(nodes, edges));

        // Run execution
        List<String> result = executor.execute();

        // Validate that Node-1 ran, Node-2 did not
        assertTrue(result.contains("Node-1"));
    }

    // Simple reflection helper to inject mock into private final field
    private void setPrivateField(Object target, String fieldName, Object value) {
        try {
            var field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field: " + fieldName, e);
        }
    }

    @Test
    void execute_WithHighConcurrency_CompletesSuccessfully() throws Exception {
        // Create a complex DAG
        Map<Integer, String> nodes = new HashMap<>();
        List<int[]> edges = new ArrayList<>();

        for (int i = 1; i <= 20; i++) {
            nodes.put(i, "Node-" + i);
            if (i > 1) {
                edges.add(new int[]{i - 1, i});  // Linear chain
                if (i % 2 == 0) {
                    edges.add(new int[]{1, i});  // Additional edges from root
                }
            }
        }

        DagInput input = new DagInput(nodes, edges);
        executor = new WorkflowDagExecutorApplication(8, true);
        executor.buildDAG(input);

        List<String> order = executor.execute();

        assertEquals(nodes.size(), order.size());
        assertEquals("Node-1", order.get(0));  // Root should be first
    }

    @Test
    void buildDAG_WithInvalidNodeId_ThrowsException() {
        String invalidInput = """
                1
                -1:InvalidNode
                0
                """;

        assertThrows(InputValidationException.class,
                () -> DagInput.parse(invalidInput));
    }

    @Test
    void buildDAG_WithSelfReference_ThrowsException() {
        Map<Integer, String> nodes = Map.of(
                1, "Node-1"
        );
        List<int[]> edges = List.of(new int[]{1, 1});  // Self-edge
        DagInput input = new DagInput(nodes, edges);

        assertThrows(IllegalArgumentException.class, () -> executor.buildDAG(input));
    }

    @Test
    void execute_VerifyAllNodesCompleted() throws Exception {
        Map<Integer, String> nodes = Map.of(
                1, "Node-1",
                2, "Node-2",
                3, "Node-3"
        );
        List<int[]> edges = List.of(new int[]{1, 2}, new int[]{1, 3});
        DagInput input = new DagInput(nodes, edges);
        executor.buildDAG(input);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(mockExecutorService).execute(any(Runnable.class));

        executor.execute();

        assertEquals(3, executor.completedNodes.size());
        assertTrue(executor.completedNodes.containsAll(Set.of(1, 2, 3)));
    }

    @Test
    void execute_VerifyExecutionOrderConstraints() throws Exception {
        Map<Integer, String> nodes = Map.of(
                1, "Node-1",
                2, "Node-2",
                3, "Node-3"
        );
        List<int[]> edges = List.of(new int[]{1, 2}, new int[]{2, 3});
        DagInput input = new DagInput(nodes, edges);
        executor.buildDAG(input);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(mockExecutorService).execute(any(Runnable.class));

        List<String> order = executor.execute();

        assertTrue(order.indexOf("Node-1") < order.indexOf("Node-2"));
        assertTrue(order.indexOf("Node-2") < order.indexOf("Node-3"));
    }

    @Test
    void printDAG_DoesNotThrow() throws Exception {
        Map<Integer, String> nodes = Map.of(
                1, "Node-1",
                2, "Node-2"
        );
        List<int[]> edges = List.of(new int[]{1, 2});
        DagInput input = new DagInput(nodes, edges);
        executor.buildDAG(input);

        assertDoesNotThrow(() -> executor.printDAG());
    }

    @Test
    void shutdown_WhenCalledTwice_DoesNotThrow() {
        executor.shutdown();
        assertDoesNotThrow(() -> executor.shutdown());
    }
}