# ⚙️ Workflow DAG Executor

Hey! 👋  
This is something I put together to experiment with how task execution can be modeled using a **DAG (Directed Acyclic Graph)**. Basically, some tasks depend on others, and this project figures out how to run them in the right order — ideally in parallel, where possible.

I built this to understand multithreading, topological execution, and how to manage dependencies programmatically.

---

## 🧠 What’s the Point?

It simulates task execution where tasks have dependencies — like in data pipelines or CI/CD systems. This tool:

- Parses a custom text-based input (nothing fancy, just raw strings).
- Builds a DAG from nodes and edges.
- Validates:
    - No duplicate nodes or edges
    - No cycles (it really doesn’t like loops)
    - Warns you about unconnected (orphaned) nodes
- Runs tasks in parallel if their dependencies are done
- Logs the whole thing so you can follow what’s happening

---

## 🛠️ Tech Stuff

- Java 17
- `ExecutorService` + `CompletableFuture` for async/parallelism
- Plain old Java data structures
- Logging with `System.out.printf` (I know... but it works)
- Uses Lombok just to reduce boilerplate (e.g. constructors)

---

## ▶️ Running It

You can clone this project (or just grab the main class files) and run the `main()` method directly. No frameworks, no setup.

Here’s an example input it expects:
## ▶️ Running It

You can clone this project (or just grab the main class files) and run the `main()` method directly. No frameworks, no setup.

Here’s an example of the kind of input it expects:

```text
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
```

You can copy-paste this into the app to test it.

## 🧾 Summary of Execution

Here's a quick rundown of how the input DAG was parsed and executed:

* ⚠️ **2 duplicate edges** (`3->6`) were found and ignored
* ⚠️ **Duplicate node name** detected: `Node-9` appears with two different IDs (9 and 12)
* ⚠️ **Unconnected node** detected: `Node-9 (ID: 12)` had no links (orphaned but still executed)
* ✅ DAG successfully built with **12 nodes** and **11 unique edges**

### 🧠 Execution Highlights

* DAG execution started from the root: `Node-1 (ID: 1)`
* All nodes were executed respecting their dependencies using multithreaded execution
* Execution included handling of concurrent tasks (e.g., `Node-2`, `Node-3`, and `Node-4` were triggered in parallel once `Node-1` completed)
* Final node e.g. `Node-11 (ID: 11)` ran after all its dependencies were satisfied

### 🧩 Final Execution Order (as logged):

```text
Node-1
Node-9 (ID: 12)
Node-3
Node-2
Node-4
Node-6
Node-7
Node-5
Node-9 (ID: 9)
Node-10
Node-8
Node-11
```

---

---

## 🧪 What I Liked About Building It

- It figures out which tasks can run in parallel — not easy at first!
- You get real-time logs showing when tasks start/finish
- Cycle detection actually works (uses DFS)
- The execution order list at the end shows what ran and when

---

## ⚠️ A Few Warnings

- It assumes the root node is the one without any parents
- This is not battle-tested — just a side project, not something you'd use in production (yet)
- No JSON or YAML support right now — just the custom input format
- There's probably some logging spam 😅

---

## 🚧 Things I Might Add Later

- Support for reading inputs from a file or JSON
- A UI that visualizes the DAG before/after execution
- Custom task runners or retry support

---

## ✍️ Author

Hi! I'm just someone who loves building things from scratch and seeing how complex systems can be broken into simple steps. I built this mostly for learning, so feel free to poke around or improve it if you'd like.

Cheers! 🍻

