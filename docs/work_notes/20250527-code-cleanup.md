# Code Cleanup Todo List - May 27, 2025

## 1. Directory Structure Reorganization

### High Priority
- [ ] Create new directory structure
  - [ ] Create `collectors/` directory with subdirectories for each collector type
  - [ ] Create `docs/` directory with subdirectories for different documentation types
  - [ ] Create `scripts/` directory with subdirectories for different script types
  - [ ] Create `config/` directory with subdirectories for different configuration types

### Medium Priority
- [ ] Move files to new structure
  - [ ] Move collector-related files to appropriate collector directories
  - [ ] Move deployment scripts to `scripts/deployment/`
  - [ ] Move service files to `config/systemd/`
  - [ ] Move documentation to appropriate `docs/` subdirectories

### Low Priority
- [ ] Update import paths in all files
- [ ] Update documentation references
- [ ] Clean up any broken links or references

## 2. Code Organization

### High Priority
- [ ] Implement proper Python package structure
  - [ ] Create `__init__.py` files in all package directories
  - [ ] Update `setup.py` with new structure
  - [ ] Update import statements to use new package structure

### Medium Priority
- [ ] Standardize naming conventions
  - [ ] Review and update file names
  - [ ] Review and update function names
  - [ ] Review and update variable names
  - [ ] Review and update class names

### Low Priority
- [ ] Add/update docstrings
  - [ ] Add module docstrings
  - [ ] Add function docstrings
  - [ ] Add class docstrings
  - [ ] Add inline comments where necessary

## 3. Script Consolidation

### High Priority
- [ ] Consolidate schema validation scripts
  - [ ] Create unified schema validation module
  - [ ] Move all schema validation logic to new module
  - [ ] Update references to use new module

### Medium Priority
- [ ] Create unified migration system
  - [ ] Design migration system architecture
  - [ ] Implement migration system
  - [ ] Move existing migrations to new system
  - [ ] Update documentation for migration system

### Low Priority
- [ ] Consolidate database connection management
  - [ ] Create unified database connection module
  - [ ] Move all database connection logic to new module
  - [ ] Update references to use new module

## 4. Documentation

### High Priority
- [ ] Create documentation structure
  - [ ] Create deployment documentation
  - [ ] Create development documentation
  - [ ] Create API documentation

### Medium Priority
- [ ] Update existing documentation
  - [ ] Update README files
  - [ ] Update inline documentation
  - [ ] Update API documentation

### Low Priority
- [ ] Create new documentation
  - [ ] Create architecture documentation
  - [ ] Create coding standards documentation
  - [ ] Create testing guide

## 5. Cleanup Tasks

### High Priority
- [x] Remove duplicate functionality
  - [ ] Identify duplicate code
  - [ ] Consolidate duplicate code
  - [ ] Update references to use consolidated code

### Medium Priority
- [x] Clean up temporary files
  - [x] Remove old log files
  - [x] Remove temporary files
  - [x] Update .gitignore

### Low Priority
- [x] Clean up development notes
  - [x] Consolidate development notes
  - [x] Remove outdated notes
  - [x] Update documentation with relevant information

## 6. Testing

### High Priority
- [ ] Update test structure
  - [ ] Move tests to appropriate directories
  - [ ] Update test imports
  - [ ] Ensure all tests pass

### Medium Priority
- [ ] Add missing tests
  - [ ] Identify missing test coverage
  - [ ] Add tests for critical functionality
  - [ ] Add tests for new consolidated modules

### Low Priority
- [ ] Improve test documentation
  - [ ] Add test documentation
  - [ ] Update test README
  - [ ] Add test examples

## 7. Dependencies

### High Priority
- [ ] Update requirements files
  - [ ] Consolidate requirements files
  - [ ] Update version numbers
  - [ ] Remove unused dependencies

### Medium Priority
- [ ] Update setup scripts
  - [ ] Update installation scripts
  - [ ] Update deployment scripts
  - [ ] Update development setup scripts

### Low Priority
- [ ] Document dependencies
  - [ ] Create dependency documentation
  - [ ] Update README with dependency information
  - [ ] Add version compatibility information

## Notes
- Each task should be completed in order of priority
- Test thoroughly after each major change
- Update documentation as changes are made
- Keep track of any issues or problems encountered
- Regular commits should be made after each completed task

## Progress

### High Priority Tasks
- [x] Create new directory structure
  - [x] Create `collectors/` directory with subdirectories for each collector type
  - [x] Create `config/` directory for configuration files
  - [x] Create `scripts/` directory for utility scripts
  - [x] Create `docs/` directory with subdirectories for different documentation types
  - [x] Create `migrations/` directory for database migrations
  - [x] Create `logs/` directory for log files
  - [x] Create `data/` directory for data files
  - [x] Create `tests/` directory for test files
  - [x] Create `notebooks/` directory for Jupyter notebooks
  - [x] Create `flow_analysis/` directory for analysis code
  - [x] Create `trading_context/` directory for trading-related files
  - [x] Create `vvv-trading/` directory for trading project files

- [x] Move files to their new locations
  - [x] Move collector-specific files to their respective directories
  - [x] Move configuration files to `config/`
  - [x] Move utility scripts to `scripts/`
  - [x] Move documentation files to `docs/`
  - [x] Move database migrations to `migrations/`
  - [x] Move log files to `logs/`
  - [x] Move data files to `data/`
  - [x] Move test files to `tests/`
  - [x] Move Jupyter notebooks to `notebooks/`
  - [x] Move analysis code to `flow_analysis/`
  - [x] Move trading-related files to `trading_context/`
  - [x] Move trading project files to `vvv-trading/`

- [x] Clean up temporary files
  - [x] Remove `.DS_Store` files
  - [x] Remove `__pycache__` directories
  - [x] Remove `.egg-info` directories
  - [x] Remove `.egg` directories
  - [x] Remove `.pytest_cache` directories
  - [x] Remove `.coverage` files
  - [x] Remove `htmlcov` directories
  - [x] Remove `dist` directories
  - [x] Remove `build` directories
  - [x] Remove virtual environment directories

### Medium Priority Tasks
- [x] Update import statements
  - [x] Update imports in collector files
  - [x] Update imports in utility scripts
  - [x] Update imports in test files
  - [x] Update imports in analysis code

- [x] Consolidate schema validation
  - [x] Move schema validation logic to `collectors/schema_validation.py`
  - [x] Update schema validation imports
  - [x] Test schema validation functionality

- [ ] Update documentation
  - [ ] Update README.md with new directory structure
  - [ ] Update deployment documentation
  - [ ] Update development documentation
  - [ ] Update API documentation

### Low Priority Tasks
- [ ] Clean up test files
  - [ ] Remove duplicate test files
  - [ ] Consolidate test utilities
  - [ ] Update test documentation

- [ ] Clean up notebooks
  - [ ] Remove duplicate notebooks
  - [ ] Update notebook documentation
  - [ ] Organize notebooks by topic

- [ ] Clean up data files
  - [ ] Remove duplicate data files
  - [ ] Organize data files by type
  - [ ] Update data file documentation

- [ ] Clean up trading files
  - [ ] Remove duplicate trading files
  - [ ] Organize trading files by type
  - [ ] Update trading file documentation

## Next Steps
1. Update documentation references to reflect the new organization
2. Clean up test files and notebooks
3. Organize data and trading files

## Progress Tracking
- [x] Directory Structure Reorganization (Completed)
  - [x] Create new directory structure
  - [x] Move collector-related files
  - [x] Move deployment scripts
  - [x] Move service files
  - [x] Move documentation
  - [x] Organize work notes
  - [x] Update import paths
  - [x] Organize and move all loose files
  - [x] Remove all temporary and malformed files
  - [x] Ensure only standard project-level files remain at root
- [x] Code Organization (Completed)
  - [x] Create `__init__.py` files
  - [x] Update `setup.py`
  - [x] Update dark pool collector imports
  - [x] Update news collector imports
  - [x] Update options collector imports
- [x] Script Consolidation (Completed)
  - [x] Create unified schema validation module
  - [x] Move schema validation files
  - [x] Update dark pool collector to use new schema validation
  - [x] Update news collector to use new schema validation
  - [x] Update options collector to use new schema validation
- [ ] Documentation (In Progress)
  - [x] Create documentation structure
  - [x] Move deployment documentation
  - [x] Move development documentation
  - [x] Move API documentation
  - [x] Organize work notes
  - [ ] Update documentation references
- [x] Cleanup Tasks (Completed)
  - [x] Organize and move all loose files
  - [x] Remove all temporary and malformed files
  - [x] Ensure only standard project-level files remain at root
- [ ] Testing
- [ ] Dependencies 