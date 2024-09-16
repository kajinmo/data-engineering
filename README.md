# Data Engineering training projects

### Setup - Instructions for installation and configuration in bash

1. Clone the git repository:
```bash
git clone https://github.com/kajinmo/data-engineering.git
cd data-engineering
```

2. Configure the correct Python version with `pyenv`:
```bash
pyenv install 3.12.3
pyenv local 3.12.3
```

3. Configure `poetry` for Python version 3.12.3 and activate the virtual environment:
```bash
poetry env use 3.12.3
poetry shell
```

4. Install project dependencies:
```bash
poetry install
```

5. Open vscode:
```
code
```
