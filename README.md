# 🔁 NBRewind

**NBRewind** is a command-line tool and custom Jupyter kernel for accelerating notebook re-execution through **incremental checkpointing**, **deduplication**, and **application virtualization**.

> Developed as part of the research project:  
> _"NBRewind: Accelerating Jupyter Notebook Re-execution using Checkpointing and Application Virtualization"_  

---

## 📦 Installation

Clone the repository and install:

```bash
git clone https://github.com/talha129/NBRewind.git
cd NBRewind
pip install .
```

## Usage

The `nbrewind` CLI supports the following commands:

### 1. `init`

Creates a new Sciunit project.

```bash
nbrewind init
```

### 2. `list`

List all available notebook versions.

```bash
nbrewind list
```


### 3. `audit`

Launch a notebook in **audit mode** using a custom kernel that captures dependencies and checkpoints cell executions

```bash
nbrewind audit --notebook <path_to_notebook.ipynb>
```

### 4. `repeat`

Launch a given notebook version in **repeat mode** using a custom kernel in a containerized environment with restorable checkpoints.

```bash
nbrewind repeat --notebook <notebook_version>
```

### 5. `extend`

Launch a given notebook version in **extend mode** in a new virtual environment with all dependencies and restorable checkpoints.

```bash
nbrewind extend --notebook <notebook_version>
```

### 📺 Demo Video

Watch the following demo to see **NbRewind** in action — auditing a notebook, listing checkpoints, and repeating executions with restored state and outputs:

[Watch the demo](https://drive.google.com/file/d/13sAYgW7wNyK_y2CYnX4OlkaUs8YrwQ_X/view?usp=drive_link)
