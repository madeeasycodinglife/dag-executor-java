package com.madeeasy;

import com.madeeasy.exception.InputValidationException;

import java.util.*;

/**
 * Immutable container for parsed DAG input data
 *
 * @param nodes Map of node IDs to node names
 * @param edges List of edges as int arrays [source, target]
 */
public record DagInput(
        Map<Integer, String> nodes,
        List<int[]> edges
) {
    public static DagInput parse(String input) throws InputValidationException {
        List<String> lines = input.lines()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .toList();

        if (lines.isEmpty()) {
            throw new InputValidationException("Input is empty.");
        }

        try {
            // --- Step 1: Node count ---
            int declaredNodeCount = parsePositiveInt(lines.getFirst(), "Node count");

            // --- Step 2: Parse nodes ---
            Map<Integer, String> nodes = new LinkedHashMap<>();
            int i = 1;
            while (i < lines.size() && nodes.size() < declaredNodeCount) {
                String line = lines.get(i);

                if (!line.contains(":")) break; // Possibly edge count

                String[] parts = line.split(":", 2);
                if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                    throw new InputValidationException("Invalid node format. Expected 'id:name' with non-empty values.");
                }

                int id = parseNodeId(parts[0].trim());
                String name = parts[1].trim();

                if (nodes.containsKey(id)) {
                    throw new InputValidationException("Duplicate node ID found: " + id);
                }

                nodes.put(id, name);
                i++;
            }

            if (nodes.size() != declaredNodeCount) {
                throw new InputValidationException(String.format(
                        "Mismatch in node count: Declared %d but found %d. Check for missing or malformed node definitions.",
                        declaredNodeCount, nodes.size()));
            }

            // --- Step 3: Edge count ---
            if (i >= lines.size()) {
                throw new InputValidationException("Missing edge count after node definitions.");
            }

            int declaredEdgeCount = parsePositiveInt(lines.get(i), "Edge count");
            i++;

            // --- Step 4: Parse edges ---
            List<int[]> edges = new ArrayList<>();
            while (i < lines.size()) {
                String[] parts = lines.get(i).split(":", 2);

                if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                    throw new InputValidationException("Invalid edge format. Expected 'source:target'.");
                }

                int source = parseNodeId(parts[0].trim());
                int target = parseNodeId(parts[1].trim());

                if (!nodes.containsKey(source)) {
                    throw new InputValidationException("Edge references unknown source node ID: " + source);
                }
                if (!nodes.containsKey(target)) {
                    throw new InputValidationException("Edge references unknown target node ID: " + target);
                }

                edges.add(new int[]{source, target});
                i++;
            }

            if (edges.size() != declaredEdgeCount) {
                throw new InputValidationException(String.format(
                        "Mismatch in edge count: Declared %d but found %d.",
                        declaredEdgeCount, edges.size()));
            }

            return new DagInput(Collections.unmodifiableMap(nodes), Collections.unmodifiableList(edges));

        } catch (IndexOutOfBoundsException e) {
            throw new InputValidationException("Input ended unexpectedly. Check that node and edge counts match their declarations.");
        }
    }

    private static int parsePositiveInt(String value, String fieldName) throws InputValidationException {
        try {
            int num = Integer.parseInt(value.trim());
            if (num <= 0) {
                throw new InputValidationException(fieldName + " must be a positive number.");
            }
            return num;
        } catch (NumberFormatException e) {
            throw new InputValidationException("Invalid " + fieldName.toLowerCase() + ". Must be an integer.");
        }
    }

    private static int parseNodeId(String value) throws InputValidationException {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new InputValidationException("Invalid node ID. Must be an integer.");
        }
    }
}
